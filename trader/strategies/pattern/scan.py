"""Pattern 策略扫描：单日扫描、批量历史回填。

来源：app.py perform_pattern_scan（~4394）、perform_pattern_scan_with_histories（~4480）、
get_pattern_backfill_trade_dates（~4593）、run_pattern_backfill（~4610）、
run_pattern_backfill_job（~4777）。
"""
from __future__ import annotations

import json
import time
import threading
from datetime import datetime, timedelta

import requests

from trader.core import config
from trader.core.db import connect_existing
from trader.core.utils import to_float, chunked, local_now_text
from trader.data.realtime import (
    load_stock_universe,
    latest_daily_trade_date,
    recent_market_trade_dates,
    daily_price_coverage_threshold,
    infer_market,
)
from trader.strategies.pattern.params import (
    build_empty_pattern_meta,
    pattern_history_load_days,
    default_pattern_backfill_days,
    normalize_pattern_backfill_params,
)
from trader.strategies.pattern.candle import (
    load_daily_histories_for_pattern,
    load_daily_histories_for_pattern_range,
)
from trader.strategies.pattern.detection import evaluate_pattern_candidate
from trader.strategies.pattern.storage import (
    ensure_pattern_tables,
    save_pattern_scan_result,
    save_pattern_progress,
)

_SCAN_LOCK = threading.Lock()


def _ensure_daily_price_indexes(conn):
    table = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='daily_prices'"
    ).fetchone()
    if not table:
        return
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dp_date ON daily_prices(trade_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dp_code_date ON daily_prices(code, trade_date)")
    conn.commit()


def fetch_eastmoney_market_caps(codes: list) -> dict:
    caps = {}
    if not codes:
        return caps
    fields = "f12,f2,f20,f21"
    em_headers = {
        "User-Agent": config.HTTP_HEADERS["User-Agent"],
        "Referer": "https://quote.eastmoney.com/",
    }
    for batch in chunked(list(dict.fromkeys(codes)), 80):
        params = {
            "fltt": 2, "invt": 2, "fields": fields,
            "secids": ",".join(f"{infer_market(code)}.{code}" for code in batch),
        }
        diff = []
        for url in config.EASTMONEY_QUOTE_URLS:
            for attempt in range(2):
                try:
                    resp = requests.get(url, params=params, headers=em_headers, timeout=8)
                    resp.raise_for_status()
                    data = resp.json().get("data") or {}
                    diff = data.get("diff") or []
                    if diff:
                        break
                except Exception:
                    time.sleep(0.25 * (attempt + 1))
            if diff:
                break
        for item in diff:
            code = str(item.get("f12") or "")
            total_cap = to_float(item.get("f20"))
            float_cap = to_float(item.get("f21"))
            caps[code] = {
                "price": to_float(item.get("f2")),
                "market_cap_yi": total_cap / 100000000.0 if total_cap else None,
                "float_market_cap_yi": float_cap / 100000000.0 if float_cap else None,
            }
    return caps


def apply_market_cap_filter(rows: list, params: dict) -> tuple[list, dict]:
    min_cap = params.get("min_market_cap_yi") or 0
    if not min_cap or not rows:
        return rows, {"market_cap_checked": 0, "market_cap_missing": 0, "market_cap_filtered": 0}
    cached_caps = params.get("_market_caps")
    if cached_caps is None:
        caps = fetch_eastmoney_market_caps([row["code"] for row in rows])
        cap_source = "request"
    else:
        caps = cached_caps
        cap_source = "cache"
    kept = []
    missing = 0
    filtered = 0
    for row in rows:
        cap = caps.get(row["code"]) or {}
        market_cap = cap.get("market_cap_yi")
        float_cap = cap.get("float_market_cap_yi")
        if market_cap is None:
            missing += 1
            filtered += 1
            continue
        row["market_cap_yi"] = round(market_cap, 2)
        row["float_market_cap_yi"] = round(float_cap, 2) if float_cap is not None else None
        if market_cap < min_cap:
            filtered += 1
            continue
        kept.append(row)
    return kept, {
        "market_cap_checked": len(rows), "market_cap_missing": missing,
        "market_cap_filtered": filtered, "market_cap_source": cap_source,
    }


def _build_scan_params_meta(params: dict) -> dict:
    return {
        "pattern_type": params["pattern_type"],
        "max_body_pct": params["max_body_pct"],
        "doji_body_pct": params["doji_body_pct"],
        "max_amp_pct": params["max_amp_pct"],
        "max_ma40_distance": params["max_ma40_distance"],
        "max_pair_distance": params["max_pair_distance"],
        "max_close_pair_distance": params["max_close_pair_distance"],
        "min_level_gap": params["min_level_gap"],
        "min_shadow_pct": params["min_shadow_pct"],
        "max_shadowless_count": params["max_shadowless_count"],
        "bottom_lookback_days": params["bottom_lookback_days"],
        "max_bottom_position": params["max_bottom_position"],
        "min_prior_drop_pct": params["min_prior_drop_pct"],
        "bottom_max_body_pct": params["bottom_max_body_pct"],
        "bottom_only_bullish_engulfing": params["bottom_only_bullish_engulfing"],
        "bottom_pattern_group": params["bottom_pattern_group"],
        "min_bottom_volume_ratio": params["min_bottom_volume_ratio"],
        "max_bottom_volume_ratio": params["max_bottom_volume_ratio"],
        "min_bottom_rebound_pct": params["min_bottom_rebound_pct"],
        "min_bottom_pct_change": params["min_bottom_pct_change"],
        "min_bottom_strong_gain_pct": params["min_bottom_strong_gain_pct"],
        "require_bottom_confirm": params["require_bottom_confirm"],
        "min_bottom_close_position": params["min_bottom_close_position"],
        "require_bottom_close_above_prev": params["require_bottom_close_above_prev"],
        "require_bottom_above_ma5": params["require_bottom_above_ma5"],
        "min_bottom_ma5_slope_pct": params["min_bottom_ma5_slope_pct"],
        "require_bottom_not_close_new_low": params["require_bottom_not_close_new_low"],
        "bottom_new_low_lookback_days": params["bottom_new_low_lookback_days"],
        "pattern_win_lookback_days": params["pattern_win_lookback_days"],
        "pattern_win_hold_days": params["pattern_win_hold_days"],
        "pattern_win_target_pct": params["pattern_win_target_pct"],
        "min_amount_wan": params["min_amount_wan"],
        "min_turnover": params["min_turnover"],
        "min_market_cap_yi": params["min_market_cap_yi"],
    }


# ── 单日扫描 ──────────────────────────────────────────────────────────────

def perform_pattern_scan(params: dict, started_at=None) -> tuple[dict, int]:
    started_at = started_at or time.time()
    conn = connect_existing()
    try:
        if not params.get("trade_date"):
            params["trade_date"] = latest_daily_trade_date(conn)
        params["required_pattern_dates"] = recent_market_trade_dates(
            conn, params["trade_date"], 4,
        )
        stocks = load_stock_universe(conn, pool=params["pool"], index_code=params["index_code"])
        if not stocks:
            return {"error": "股票池为空", "meta": build_empty_pattern_meta(params)}, 400
        codes = [s["code"] for s in stocks]
        histories = load_daily_histories_for_pattern(
            conn, codes, params["trade_date"], pattern_history_load_days(params)
        )
    finally:
        conn.close()

    rows = []
    scanned = 0
    for stock in stocks:
        series = histories.get(stock["code"]) or []
        if not series:
            continue
        scanned += 1
        row = evaluate_pattern_candidate(stock, series, params)
        if row:
            rows.append(row)

    pre_cap_matched = len(rows)
    rows, cap_meta = apply_market_cap_filter(rows, params)
    rows.sort(key=lambda r: (r["score"], r["amount_yi"]), reverse=True)
    rows = rows[:params["limit"]]
    meta = {
        "pool": params["pool"], "trade_date": params["trade_date"],
        "index_code": params["index_code"], "universe": len(stocks),
        "scanned": scanned, "pre_cap_matched": pre_cap_matched,
        "matched": len(rows), "elapsed_s": round(time.time() - started_at, 1),
        **cap_meta, "params": _build_scan_params_meta(params),
    }
    return {"meta": meta, "rows": rows}, 200


def perform_pattern_scan_with_histories(params: dict, stocks: list, histories: dict,
                                         market_dates: list, started_at=None) -> tuple[dict, int]:
    started_at = started_at or time.time()
    trade_date = params["trade_date"]
    if trade_date not in market_dates:
        return {"meta": build_empty_pattern_meta(params, universe=len(stocks)), "rows": []}, 200
    date_index = market_dates.index(trade_date)
    if date_index < 3:
        return {"meta": build_empty_pattern_meta(params, universe=len(stocks)), "rows": []}, 200
    params["required_pattern_dates"] = market_dates[date_index - 3:date_index + 1]

    rows = []
    scanned = 0
    history_days = pattern_history_load_days(params)
    for stock in stocks:
        series = histories.get(stock["code"]) or []
        if not series:
            continue
        idx = None
        for i in range(len(series) - 1, -1, -1):
            if series[i]["trade_date"] == trade_date:
                idx = i
                break
            if series[i]["trade_date"] < trade_date:
                break
        if idx is None:
            continue
        scanned += 1
        window_start = idx - history_days
        window = series[max(0, window_start):idx + 1]
        row = evaluate_pattern_candidate(stock, window, params)
        if row:
            rows.append(row)

    pre_cap_matched = len(rows)
    rows, cap_meta = apply_market_cap_filter(rows, params)
    rows.sort(key=lambda r: (r["score"], r["amount_yi"]), reverse=True)
    rows = rows[:params["limit"]]
    meta = {
        "pool": params["pool"], "trade_date": trade_date,
        "index_code": params["index_code"], "universe": len(stocks),
        "scanned": scanned, "pre_cap_matched": pre_cap_matched,
        "matched": len(rows), "elapsed_s": round(time.time() - started_at, 1),
        **cap_meta, "params": _build_scan_params_meta(params),
    }
    return {"meta": meta, "rows": rows}, 200


# ── 回填 ─────────────────────────────────────────────────────────────────

def get_pattern_backfill_trade_dates(conn, end_date=None, days: int = 30) -> list[str]:
    end_date = end_date or latest_daily_trade_date(conn)
    start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=days)).strftime("%Y-%m-%d")
    min_count = daily_price_coverage_threshold(conn)
    rows = conn.execute("""
        SELECT trade_date FROM daily_prices
        WHERE trade_date >= ? AND trade_date <= ?
        GROUP BY trade_date HAVING COUNT(DISTINCT code) >= ?
        ORDER BY trade_date
    """, (start_date, end_date, min_count)).fetchall()
    return [r["trade_date"] for r in rows]


def run_pattern_backfill(params: dict, days=None, end_date=None, progress=None) -> dict:
    started_at = time.time()
    days = days or default_pattern_backfill_days(params)
    conn = connect_existing()
    try:
        ensure_pattern_tables(conn)
        if progress:
            progress({"phase": "indexes", "message": "检查历史K线索引",
                      "picked": 0, "saved": 0, "matched_rows_so_far": 0, "matched_days_so_far": 0,
                      "trade_date": end_date or params.get("trade_date")}, 0, 0)
        _ensure_daily_price_indexes(conn)
        trade_dates = get_pattern_backfill_trade_dates(
            conn, end_date=end_date or params.get("trade_date"), days=days,
        )
        if not trade_dates:
            return {
                "start_date": None, "end_date": end_date, "days": 0,
                "matched_days": 0, "matched_rows": 0, "elapsed_s": 0, "results": [],
            }
        if progress:
            progress({"phase": "trade_dates", "trade_date": trade_dates[0],
                      "message": f"找到 {len(trade_dates)} 个交易日，准备加载股票池",
                      "picked": 0, "saved": 0, "matched_rows_so_far": 0, "matched_days_so_far": 0},
                     0, len(trade_dates))

        stocks = load_stock_universe(conn, pool=params["pool"], index_code=params["index_code"])
        if params.get("min_market_cap_yi"):
            market_caps = fetch_eastmoney_market_caps([s["code"] for s in stocks])
            params["_market_caps"] = market_caps

        def history_progress(bi, bt, rc):
            if progress:
                progress({"phase": "history", "trade_date": trade_dates[0],
                          "message": f"正在加载历史K线 {bi}/{bt} 批，本批 {rc} 条",
                          "picked": 0, "saved": 0, "matched_rows_so_far": 0, "matched_days_so_far": 0},
                         0, len(trade_dates))

        histories = load_daily_histories_for_pattern_range(
            conn, [s["code"] for s in stocks],
            trade_dates[0], trade_dates[-1],
            pattern_history_load_days(params), progress=history_progress,
        )
    finally:
        conn.close()

    results = []
    matched_rows = 0
    matched_days = 0
    for i, trade_date in enumerate(trade_dates, 1):
        day_params = dict(params)
        day_params["trade_date"] = trade_date
        payload, status_code = perform_pattern_scan_with_histories(
            day_params, stocks, histories, trade_dates, started_at=time.time(),
        )
        conn = connect_existing()
        try:
            run_id, saved = save_pattern_scan_result(conn, day_params, payload, status_code)
        finally:
            conn.close()
        picked = len(payload.get("rows") or [])
        matched_rows += picked
        if picked > 0:
            matched_days += 1
        item = {
            "trade_date": trade_date, "status": status_code,
            "run_id": run_id, "saved": saved, "picked": picked,
            "matched_rows_so_far": matched_rows, "matched_days_so_far": matched_days,
            "meta": payload.get("meta") or {},
        }
        results.append(item)
        if progress:
            progress(item, i, len(trade_dates))

    return {
        "start_date": trade_dates[0], "end_date": trade_dates[-1],
        "days": len(trade_dates), "matched_days": matched_days,
        "matched_rows": matched_rows, "elapsed_s": round(time.time() - started_at, 1),
        "results": results,
    }


def run_pattern_backfill_job(params: dict, days=None, end_date=None,
                              job_key: str = "pattern_backfill"):
    started_at = time.time()
    days = days or default_pattern_backfill_days(params)
    save_pattern_progress(
        job_key, job_type="backfill", status="running",
        started_at=local_now_text(),
        trade_date=end_date or params.get("trade_date"),
        current_index=0, total=0, picked=0, matched_rows=0, matched_days=0,
        elapsed_s=0, message="准备回扫",
        params_json=json.dumps(params, ensure_ascii=False, sort_keys=True),
        result_json=None, error=None,
    )

    def on_progress(item, index, total):
        picked = item.get("picked", 0)
        message = item.get("message") or f"正在回扫 {item.get('trade_date')}，当天命中 {picked} 条"
        save_pattern_progress(
            job_key, job_type="backfill", status="running",
            trade_date=item.get("trade_date"),
            current_index=index, total=total, picked=picked,
            matched_rows=item.get("matched_rows_so_far", 0),
            matched_days=item.get("matched_days_so_far", 0),
            elapsed_s=round(time.time() - started_at, 1),
            message=message, error=None,
        )

    try:
        result = run_pattern_backfill(params, days=days, end_date=end_date, progress=on_progress)
        save_pattern_progress(
            job_key, job_type="backfill", status="done",
            trade_date=result.get("end_date"),
            current_index=result.get("days"), total=result.get("days"),
            picked=0, matched_rows=result.get("matched_rows", 0),
            matched_days=result.get("matched_days", 0),
            elapsed_s=result.get("elapsed_s", round(time.time() - started_at, 1)),
            message="回扫完成",
            result_json=json.dumps({
                "start_date": result.get("start_date"), "end_date": result.get("end_date"),
                "days": result.get("days"), "matched_days": result.get("matched_days"),
                "matched_rows": result.get("matched_rows"), "elapsed_s": result.get("elapsed_s"),
            }, ensure_ascii=False, sort_keys=True),
            error=None,
        )
    except Exception as exc:
        save_pattern_progress(
            job_key, job_type="backfill", status="error",
            elapsed_s=round(time.time() - started_at, 1),
            message="回扫失败", error=str(exc),
        )
    finally:
        _SCAN_LOCK.release()
