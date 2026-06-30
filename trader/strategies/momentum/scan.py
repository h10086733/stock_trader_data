"""动量扫描核心：实时/历史/日线 fallback 三种模式的候选股评估逻辑。

来源：app.py 中以下函数（已解耦 Flask 运行时）：
  - passes_momentum_filters（~5501）
  - evaluate_candidate_with_bars（~5405）
  - perform_momentum_scan（~9160）
  - perform_historical_momentum_scan（~9406）
  - perform_daily_fallback_momentum_scan（~9523）
  - load_daily_metrics_before（~9319）
  - load_historical_daily_quotes（~9372）
  - build_daily_fallback_row（~9671）
  - metric_from_previous_series（~内嵌）
  - build_daily_fallback_payload_from_history（~9765）
"""
from __future__ import annotations

import time

from trader.core.utils import clamp, parse_cutoff_time, trade_elapsed_ratio, chunked
from trader.core.db import connect_existing
from trader.data.realtime import (
    load_stock_universe,
    load_daily_metrics,
    fetch_realtime_quotes,
    fetch_baostock_5m_klines_parallel,
    position_in_range,
    score_candidate,
    build_sparkline,
)
from trader.strategies.momentum.params import build_empty_scan_meta


# ── 过滤与评分 ────────────────────────────────────────────────────────────

def passes_momentum_filters(row: dict, min_gain: float, max_gain: float,
                             min_vol_ratio: float) -> bool:
    """综合过滤：涨幅、趋势、日内资金结构、收盘强度。"""
    if row["pct"] < min_gain or row["pct"] > max_gain:
        return False
    # A层：趋势过滤
    if not row.get("trend_above_ma5"):
        return False
    if not row.get("ma5_up"):
        return False
    if not row.get("not_break_prev_low"):
        return False
    # B层：日内资金结构
    volume_ok = (
        row.get("volume_full_ratio", 0) >= 1.0
        or row.get("volume_ratio", 0) >= min_vol_ratio
    )
    if not volume_ok:
        return False
    if not row.get("above_vwap"):
        return False
    if not row.get("high_after_14"):
        return False
    # C层：收盘强度
    if not row.get("close_strong"):
        return False
    return True


def evaluate_candidate_with_bars(stock: dict, quote: dict, daily: dict,
                                  cutoff_text: str, elapsed_ratio: float,
                                  bars: list) -> dict | None:
    """用分钟 K 线评估单只候选股，返回结构化 row 或 None（无 bars 时）。"""
    if not bars:
        return None
    price = bars[-1]["close"] or quote["price"]
    high = max((b["high"] for b in bars if b["high"] is not None), default=quote["high"])
    low = min((b["low"] for b in bars if b["low"] is not None), default=quote["low"])
    volume = sum(b["volume"] for b in bars)
    amount = sum(b["amount"] for b in bars)
    vwap = amount / (volume * 100.0) if volume else None

    high_time = None
    if high is not None:
        high_times = [b["time"] for b in bars if b.get("high") == high]
        high_time = max(high_times) if high_times else None

    afternoon_bars = [b for b in bars if b["time"] >= "13:00" and b.get("close")]
    afternoon_pct = None
    if afternoon_bars and afternoon_bars[0]["close"]:
        afternoon_pct = (price - afternoon_bars[0]["close"]) / afternoon_bars[0]["close"] * 100

    avg_volume20 = daily.get("avg_volume20")
    expected_volume = avg_volume20 * elapsed_ratio if avg_volume20 else None
    volume_ratio = volume / expected_volume if expected_volume else 0
    volume_full_ratio = volume / avg_volume20 if avg_volume20 else 0
    close_position = position_in_range(price, low, high)
    pullback_pct = ((high - price) / price * 100) if high and price else None
    above_vwap = bool(vwap and price >= vwap)
    trend_above_ma5 = bool(daily.get("ma5") and price > daily["ma5"])
    ma5_up = bool(daily.get("ma5_up"))
    prev_low = daily.get("prev_low")
    not_break_prev_low = bool(prev_low and low and low >= prev_low)
    high_after_14 = bool(high_time and high_time >= "14:00")
    close_strong = bool(
        (close_position is not None and close_position >= 0.80)
        or (pullback_pct is not None and pullback_pct <= 1.0)
    )

    minute = {
        "price": price,
        "volume_ratio": volume_ratio,
        "close_position": close_position,
        "pullback_pct": pullback_pct,
        "above_vwap": above_vwap,
        "afternoon_pct": afternoon_pct,
    }
    sc = score_candidate(quote, daily, minute)

    reasons = []
    if trend_above_ma5:
        reasons.append("强于5日线")
    if ma5_up:
        reasons.append("5日线向上")
    if not_break_prev_low:
        reasons.append("未破前低")
    if volume_full_ratio >= 1.0 or volume_ratio >= 1.5:
        reasons.append("放量")
    if above_vwap:
        reasons.append("站上VWAP")
    if high_after_14:
        reasons.append("14点后高点")
    if close_strong:
        reasons.append("收盘强")

    return {
        "code": stock["code"],
        "name": stock["name"] or quote.get("name") or "",
        "price": round(price, 3),
        "pct": round((price - quote["prev_close"]) / quote["prev_close"] * 100, 2),
        "amount_yi": round((amount or quote["amount"]) / 100000000, 2),
        "volume_ratio": round(volume_ratio, 2),
        "volume_full_ratio": round(volume_full_ratio, 2),
        "close_position": round(close_position * 100, 1) if close_position is not None else None,
        "pullback_pct": round(pullback_pct, 2) if pullback_pct is not None else None,
        "afternoon_pct": round(afternoon_pct, 2) if afternoon_pct is not None else None,
        "above_vwap": above_vwap,
        "trend_above_ma5": trend_above_ma5,
        "ma5_up": ma5_up,
        "not_break_prev_low": not_break_prev_low,
        "high_time": high_time,
        "high_after_14": high_after_14,
        "close_strong": close_strong,
        "has_minute": True,
        "score": sc,
        "reasons": " / ".join(reasons),
        "sparkline": build_sparkline(bars),
        "quote_time": quote.get("quote_time"),
        "trade_date": quote.get("trade_date"),
    }


# ── 实时扫描 ──────────────────────────────────────────────────────────────

def perform_momentum_scan(params: dict, started_at=None) -> tuple[dict, int]:
    """实时行情 + 分钟 K 线扫描，返回 (payload_dict, http_status_code)。"""
    started_at = started_at or time.time()
    pool = params["pool"]
    index_code = params["index_code"]
    cutoff_text = params["cutoff"]
    scan_trade_date = params["trade_date"]
    min_gain = params["min_gain"]
    max_gain = params["max_gain"]
    min_vol_ratio = params["min_vol_ratio"]
    min_amount_yuan = params["min_amount_wan"] * 10000
    limit = params["limit"]
    verify_limit = params["verify_limit"]
    max_workers = params["workers"]
    cutoff = parse_cutoff_time(cutoff_text)
    elapsed_ratio = trade_elapsed_ratio(cutoff)

    conn = connect_existing()
    try:
        stocks = load_stock_universe(conn, pool=pool, index_code=index_code)
        if not stocks:
            return {"error": "股票池为空", "meta": build_empty_scan_meta(params)}, 400
        stock_by_code = {s["code"]: s for s in stocks}
        codes = list(stock_by_code.keys())
        quotes = fetch_realtime_quotes(codes)
        if not quotes:
            return {
                "error": "实时行情获取失败：新浪和东方财富均无有效返回",
                "meta": build_empty_scan_meta(params, universe=len(stocks)),
                "rows": [],
            }, 502
        valid_codes = [code for code in codes if code in quotes]
        daily_metrics = load_daily_metrics(conn, valid_codes)
    finally:
        conn.close()

    prefiltered = []
    for code in valid_codes:
        quote = quotes[code]
        daily = daily_metrics.get(code)
        if not daily:
            continue
        pct = quote["pct"]
        if pct < min_gain or pct > max_gain:
            continue
        if quote["amount"] < min_amount_yuan:
            continue
        avg_volume20 = daily.get("avg_volume20")
        if not avg_volume20:
            continue
        live_volume_ratio = quote["volume"] / (avg_volume20 * elapsed_ratio)
        if live_volume_ratio < min_vol_ratio * 0.75:
            continue
        ma5 = daily.get("ma5")
        ma20 = daily.get("ma20")
        price = quote["price"]
        if not ma5 or price <= ma5:
            continue
        if not daily.get("ma5_up"):
            continue
        if ma20 and price < ma20 * 0.97:
            continue
        pre_score = (
            pct * 5
            + min(live_volume_ratio, 4) * 12
            + min(quote["amount"] / 100000000, 5) * 4
        )
        prefiltered.append((pre_score, stock_by_code[code], quote, daily))

    prefiltered.sort(key=lambda x: x[0], reverse=True)
    verify_items = prefiltered[:verify_limit]
    stock_items = [stock for _, stock, _, _ in verify_items]
    kline_map, cache_hits = fetch_baostock_5m_klines_parallel(
        stock_items, cutoff_text, max_workers=max_workers, trade_date=scan_trade_date,
    )

    rows: list = []
    minute_success = 0
    minute_failed = 0

    # 先探测前 5 只，验证分钟线可用
    probe_items = verify_items[:min(5, len(verify_items))]
    for _, stock, quote, daily in probe_items:
        bars = kline_map.get(stock["code"])
        row = evaluate_candidate_with_bars(stock, quote, daily, cutoff_text, elapsed_ratio, bars)
        if not row:
            minute_failed += 1
            continue
        minute_success += 1
        if passes_momentum_filters(row, min_gain, max_gain, min_vol_ratio):
            rows.append(row)

    if probe_items and minute_success == 0:
        meta = {
            "pool": pool, "cutoff": cutoff_text, "trade_date": scan_trade_date,
            "index_code": index_code, "universe": len(stocks),
            "quoted": len(quotes), "prefiltered": len(prefiltered),
            "verified": len(probe_items), "minute_success": 0,
            "minute_failed": minute_failed, "cache_hits": cache_hits,
            "elapsed_s": round(time.time() - started_at, 1),
        }
        return {
            "error": "分钟线接口暂不可用，候选股无法做14:30分时验证",
            "meta": meta, "rows": [],
        }, 503

    for _, stock, quote, daily in verify_items[len(probe_items):]:
        bars = kline_map.get(stock["code"])
        row = evaluate_candidate_with_bars(stock, quote, daily, cutoff_text, elapsed_ratio, bars)
        if not row:
            minute_failed += 1
            continue
        minute_success += 1
        if passes_momentum_filters(row, min_gain, max_gain, min_vol_ratio):
            rows.append(row)

    rows.sort(key=lambda r: (r["score"], r["volume_ratio"], r["amount_yi"]), reverse=True)
    rows = rows[:limit]
    meta = {
        "pool": pool, "cutoff": cutoff_text, "trade_date": scan_trade_date,
        "index_code": index_code, "universe": len(stocks),
        "quoted": len(quotes), "prefiltered": len(prefiltered),
        "verified": len(verify_items), "minute_success": minute_success,
        "minute_failed": minute_failed, "cache_hits": cache_hits,
        "elapsed_s": round(time.time() - started_at, 1),
    }
    if verify_items and minute_success == 0:
        return {
            "error": "分钟线接口暂不可用，候选股无法做14:30分时验证",
            "meta": meta, "rows": [],
        }, 503
    return {"meta": meta, "rows": rows}, 200


# ── 历史指标（截止指定日期） ──────────────────────────────────────────────

def load_daily_metrics_before(conn, codes: list[str], trade_date: str) -> dict:
    """读取 trade_date 之前最近 80 根日 K（不含当日），计算 MA 等指标。"""
    metrics = {}
    for batch in chunked(codes, 600):
        placeholders = ",".join("?" for _ in batch)
        rows = conn.execute(f"""
            SELECT code, trade_date, close, high, low, volume, amount, pct_change
            FROM (
                SELECT code, trade_date, close, high, low, volume, amount, pct_change,
                       ROW_NUMBER() OVER (
                           PARTITION BY code ORDER BY trade_date DESC
                       ) AS rn
                FROM daily_prices
                WHERE code IN ({placeholders})
                  AND trade_date < ?
            )
            WHERE rn <= 80
            ORDER BY code, trade_date DESC
        """, batch + [trade_date]).fetchall()

        grouped: dict[str, list] = {}
        for row in rows:
            grouped.setdefault(row["code"], []).append(row)

        for code, series_desc in grouped.items():
            series = list(reversed(series_desc[:80]))
            closes = [r["close"] for r in series if r["close"] is not None]
            volumes = [r["volume"] for r in series[-20:] if r["volume"] is not None]
            if not closes:
                continue
            ma5 = sum(closes[-5:]) / min(len(closes), 5)
            ma20 = sum(closes[-20:]) / min(len(closes), 20)
            high20 = max((r["high"] for r in series[-20:] if r["high"] is not None), default=None)
            low20 = min((r["low"] for r in series[-20:] if r["low"] is not None), default=None)
            avg_volume20 = sum(volumes) / len(volumes) if volumes else None
            prev_ma5 = (sum(closes[-6:-1]) / 5) if len(closes) >= 6 else None
            last = series[-1]
            metrics[code] = {
                "last_trade_date": last["trade_date"],
                "last_close": last["close"],
                "prev_low": last["low"],
                "ma5": ma5,
                "ma5_prev": prev_ma5,
                "ma5_up": bool(prev_ma5 is not None and ma5 > prev_ma5),
                "ma20": ma20,
                "high20": high20,
                "low20": low20,
                "avg_volume20": avg_volume20,
            }
    return metrics


def load_historical_daily_quotes(conn, codes: list[str], trade_date: str,
                                  daily_metrics: dict) -> dict:
    """读取指定日期的收盘价作为"报价"（历史回测用）。"""
    quotes = {}
    for batch in chunked(codes, 600):
        placeholders = ",".join("?" for _ in batch)
        rows = conn.execute(f"""
            SELECT code, open, close, high, low, volume, amount, pct_change
            FROM daily_prices
            WHERE code IN ({placeholders})
              AND trade_date = ?
        """, batch + [trade_date]).fetchall()
        for row in rows:
            daily = daily_metrics.get(row["code"])
            prev_close = daily.get("last_close") if daily else None
            close = row["close"]
            if not prev_close or not close:
                continue
            pct = row["pct_change"]
            if pct is None:
                pct = (close - prev_close) / prev_close * 100.0
            quotes[row["code"]] = {
                "open": row["open"],
                "prev_close": prev_close,
                "price": close,
                "high": row["high"],
                "low": row["low"],
                "volume": row["volume"] or 0,
                "amount": row["amount"] or 0,
                "trade_date": trade_date,
                "quote_time": "15:00:00",
                "pct": pct,
            }
    return quotes


# ── 历史扫描（分钟 K 线） ─────────────────────────────────────────────────

def perform_historical_momentum_scan(params: dict, started_at=None) -> tuple[dict, int]:
    """历史日期扫描：用历史日 K 作为报价，分钟 K 线仍需拉取。"""
    started_at = started_at or time.time()
    pool = params["pool"]
    index_code = params["index_code"]
    cutoff_text = params["cutoff"]
    scan_trade_date = params["trade_date"]
    min_gain = params["min_gain"]
    max_gain = params["max_gain"]
    min_vol_ratio = params["min_vol_ratio"]
    min_amount_yuan = params["min_amount_wan"] * 10000
    limit = params["limit"]
    verify_limit = params["verify_limit"]
    max_workers = params["workers"]
    cutoff = parse_cutoff_time(cutoff_text)
    elapsed_ratio = trade_elapsed_ratio(cutoff)

    conn = connect_existing()
    try:
        stocks = load_stock_universe(conn, pool=pool, index_code=index_code)
        if not stocks:
            return {"error": "股票池为空", "meta": build_empty_scan_meta(params)}, 400
        stock_by_code = {s["code"]: s for s in stocks}
        codes = list(stock_by_code.keys())
        daily_metrics = load_daily_metrics_before(conn, codes, scan_trade_date)
        quotes = load_historical_daily_quotes(conn, codes, scan_trade_date, daily_metrics)
    finally:
        conn.close()

    valid_codes = [code for code in codes if code in quotes]
    prefiltered = []
    for code in valid_codes:
        quote = quotes[code]
        daily = daily_metrics.get(code)
        if not daily:
            continue
        pct = quote["pct"]
        if pct < min_gain - 2.5 or pct > max_gain + 3.0:
            continue
        if quote["amount"] < min_amount_yuan * 0.45:
            continue
        avg_volume20 = daily.get("avg_volume20")
        if not avg_volume20:
            continue
        day_volume_ratio = quote["volume"] / avg_volume20
        if day_volume_ratio < min_vol_ratio * 0.35:
            continue
        ma5 = daily.get("ma5")
        ma20 = daily.get("ma20")
        price = quote["price"]
        if not ma5 or price <= ma5 * 0.96:
            continue
        if not daily.get("ma5_up"):
            continue
        if ma20 and price < ma20 * 0.94:
            continue
        pre_score = (
            pct * 5
            + min(day_volume_ratio, 4) * 12
            + min(quote["amount"] / 100000000, 5) * 4
        )
        prefiltered.append((pre_score, stock_by_code[code], quote, daily))

    prefiltered.sort(key=lambda x: x[0], reverse=True)
    verify_items = prefiltered[:verify_limit]
    stock_items = [stock for _, stock, _, _ in verify_items]
    kline_map, cache_hits = fetch_baostock_5m_klines_parallel(
        stock_items, cutoff_text, max_workers=max_workers, trade_date=scan_trade_date,
    )

    rows: list = []
    minute_success = 0
    minute_failed = 0
    for _, stock, quote, daily in verify_items:
        bars = kline_map.get(stock["code"])
        row = evaluate_candidate_with_bars(stock, quote, daily, cutoff_text, elapsed_ratio, bars)
        if not row:
            minute_failed += 1
            continue
        minute_success += 1
        if not passes_momentum_filters(row, min_gain, max_gain, min_vol_ratio):
            continue
        rows.append(row)

    rows.sort(key=lambda r: (r["score"], r["volume_ratio"], r["amount_yi"]), reverse=True)
    rows = rows[:limit]
    meta = {
        "pool": pool, "cutoff": cutoff_text, "trade_date": scan_trade_date,
        "index_code": index_code, "universe": len(stocks),
        "quoted": len(quotes), "prefiltered": len(prefiltered),
        "verified": len(verify_items), "minute_success": minute_success,
        "minute_failed": minute_failed, "cache_hits": cache_hits,
        "elapsed_s": round(time.time() - started_at, 1),
        "historical": True,
    }
    if verify_items and minute_success == 0:
        return {
            "error": "历史分钟线接口暂不可用，候选股无法做14:30分时验证",
            "meta": meta, "rows": [],
        }, 503
    return {"meta": meta, "rows": rows}, 200


# ── 日线 fallback 扫描 ────────────────────────────────────────────────────

def _build_daily_fallback_row(stock: dict, quote: dict, daily: dict, params: dict):
    """用日线收盘数据构造 fallback row，不满足条件返回 None。"""
    min_gain = params["min_gain"]
    max_gain = params["max_gain"]
    min_vol_ratio = params["min_vol_ratio"]
    min_amount_yuan = params["min_amount_wan"] * 10000
    pct = quote["pct"]
    price = quote["price"]
    avg_volume20 = daily.get("avg_volume20")

    if pct < min_gain or pct > max_gain:
        return None
    if quote["amount"] < min_amount_yuan:
        return None
    if not avg_volume20:
        return None
    volume_ratio = quote["volume"] / avg_volume20
    if volume_ratio < min_vol_ratio:
        return None
    ma5 = daily.get("ma5")
    if not ma5 or price <= ma5:
        return None
    if not daily.get("ma5_up"):
        return None
    prev_low = daily.get("prev_low")
    if prev_low and quote.get("low") and quote["low"] < prev_low:
        return None
    ma20 = daily.get("ma20")
    if ma20 and price < ma20 * 0.97:
        return None

    close_position = position_in_range(price, quote.get("low"), quote.get("high"))
    pullback_pct = (
        (quote["high"] - price) / price * 100.0
        if quote.get("high") and price else None
    )
    if close_position is not None and close_position < 0.65:
        return None
    if pullback_pct is not None and pullback_pct > 3.0:
        return None

    score = round(
        clamp(20 - abs(pct - 4.8) * 3.0, 0, 20)
        + clamp((volume_ratio - 1.0) / 1.8 * 25, 0, 25)
        + clamp((quote["amount"] or 0) / 100000000 / 3.0 * 10, 0, 10)
        + (close_position or 0) * 15
        + 10,
        1,
    )
    return {
        "code": stock["code"],
        "name": stock["name"] or "",
        "price": round(price, 3),
        "pct": round(pct, 2),
        "amount_yi": round((quote["amount"] or 0) / 100000000, 2),
        "volume_ratio": round(volume_ratio, 2),
        "volume_full_ratio": round(volume_ratio, 2),
        "close_position": round(close_position * 100, 1) if close_position is not None else None,
        "pullback_pct": round(pullback_pct, 2) if pullback_pct is not None else None,
        "afternoon_pct": None,
        "above_vwap": None,
        "trend_above_ma5": True,
        "ma5_up": True,
        "not_break_prev_low": True,
        "high_time": "15:00",
        "high_after_14": True,
        "close_strong": True,
        "has_minute": False,
        "historical_fallback": "daily_close_buy_next_open_sell",
        "score": score,
        "reasons": "日线回退 / 强于5日线 / 5日线向上 / 放量",
        "sparkline": "",
        "quote_time": "15:00:00",
        "trade_date": quote["trade_date"],
    }


def _metric_from_previous_series(series: list) -> dict | None:
    closes = [r["close"] for r in series if r["close"] is not None]
    volumes = [r["volume"] for r in series[-20:] if r["volume"] is not None]
    if not closes:
        return None
    ma5 = sum(closes[-5:]) / min(len(closes), 5)
    ma20 = sum(closes[-20:]) / min(len(closes), 20)
    prev_ma5 = (sum(closes[-6:-1]) / 5) if len(closes) >= 6 else None
    last = series[-1]
    return {
        "last_trade_date": last["trade_date"],
        "last_close": last["close"],
        "prev_low": last["low"],
        "ma5": ma5,
        "ma5_prev": prev_ma5,
        "ma5_up": bool(prev_ma5 is not None and ma5 > prev_ma5),
        "ma20": ma20,
        "high20": max((r["high"] for r in series[-20:] if r["high"] is not None), default=None),
        "low20": min((r["low"] for r in series[-20:] if r["low"] is not None), default=None),
        "avg_volume20": sum(volumes) / len(volumes) if volumes else None,
    }


def perform_daily_fallback_momentum_scan(params: dict, started_at=None) -> tuple[dict, int]:
    """历史日线 fallback：纯用收盘数据过滤，无需分钟 K 线。"""
    started_at = started_at or time.time()
    pool = params["pool"]
    index_code = params["index_code"]
    scan_trade_date = params["trade_date"]
    min_amount_yuan = params["min_amount_wan"] * 10000
    limit = params["limit"]

    conn = connect_existing()
    try:
        stocks = load_stock_universe(conn, pool=pool, index_code=index_code)
        if not stocks:
            return {"error": "股票池为空", "meta": build_empty_scan_meta(params)}, 400
        stock_by_code = {s["code"]: s for s in stocks}
        codes = list(stock_by_code.keys())
        daily_metrics = load_daily_metrics_before(conn, codes, scan_trade_date)
        quotes = load_historical_daily_quotes(conn, codes, scan_trade_date, daily_metrics)
    finally:
        conn.close()

    rows = []
    for code, quote in quotes.items():
        stock = stock_by_code.get(code)
        daily = daily_metrics.get(code)
        if not stock or not daily:
            continue
        row = _build_daily_fallback_row(stock, quote, daily, params)
        if row:
            rows.append(row)

    rows.sort(key=lambda r: (r["score"], r["volume_ratio"], r["amount_yi"]), reverse=True)
    rows = rows[:limit]
    meta = {
        "pool": pool, "cutoff": params["cutoff"],
        "trade_date": scan_trade_date, "index_code": index_code,
        "universe": len(stocks), "quoted": len(quotes),
        "prefiltered": len(rows), "verified": len(rows),
        "minute_success": 0, "minute_failed": 0, "cache_hits": 0,
        "elapsed_s": round(time.time() - started_at, 1),
        "historical": True, "fallback": "daily",
    }
    return {"meta": meta, "rows": rows}, 200


def load_daily_history_for_backfill(conn, codes: list[str],
                                     start_date: str, end_date: str) -> dict:
    """批量读取回测所需历史日 K（包含 start_date 前 180 天作为指标窗口）。"""
    from datetime import datetime, timedelta
    histories: dict[str, list] = {code: [] for code in codes}
    start_dt = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=180)
    history_start = start_dt.strftime("%Y-%m-%d")
    code_set = set(codes)
    rows = conn.execute("""
        SELECT code, trade_date, open, close, high, low, volume, amount, pct_change
        FROM daily_prices
        WHERE trade_date >= ? AND trade_date <= ?
        ORDER BY code, trade_date
    """, (history_start, end_date)).fetchall()
    for row in rows:
        code = row["code"]
        if code in code_set:
            histories[code].append(row)
    return histories


def build_daily_fallback_payload_from_history(params: dict, stocks: list,
                                               histories: dict, trade_date: str,
                                               started_at=None) -> tuple[dict, int]:
    """从预加载历史 dict 快速构造日线 fallback payload（回测专用）。"""
    started_at = started_at or time.time()
    rows: list = []
    quoted = 0
    for stock in stocks:
        series = histories.get(stock["code"]) or []
        idx = None
        for i in range(len(series) - 1, -1, -1):
            if series[i]["trade_date"] == trade_date:
                idx = i
                break
        if idx is None or idx == 0:
            continue
        prev_series = series[max(0, idx - 80):idx]
        daily = _metric_from_previous_series(prev_series)
        if not daily or not daily.get("last_close"):
            continue
        current = series[idx]
        close = current["close"]
        if not close:
            continue
        pct = current["pct_change"]
        if pct is None:
            pct = (close - daily["last_close"]) / daily["last_close"] * 100.0
        quote = {
            "trade_date": trade_date,
            "price": close,
            "pct": pct,
            "open": current["open"],
            "high": current["high"],
            "low": current["low"],
            "volume": current["volume"] or 0,
            "amount": current["amount"] or 0,
        }
        quoted += 1
        row = _build_daily_fallback_row(stock, quote, daily, params)
        if row:
            rows.append(row)

    rows.sort(key=lambda r: (r["score"], r["volume_ratio"], r["amount_yi"]), reverse=True)
    rows = rows[:params["limit"]]
    meta = {
        "pool": params["pool"], "cutoff": params["cutoff"],
        "trade_date": trade_date, "index_code": params["index_code"],
        "universe": len(stocks), "quoted": quoted,
        "prefiltered": len(rows), "verified": len(rows),
        "minute_success": 0, "minute_failed": 0, "cache_hits": 0,
        "elapsed_s": round(time.time() - started_at, 1),
        "historical": True, "fallback": "daily-fast",
    }
    return {"meta": meta, "rows": rows}, 200

