"""高置信选股核心流程：扫描面板、历史质量过滤、聚合排序、精筛、payload 组装、同步线程。

来源：app.py 的 HC 主流程函数（约第 922-2210 行，去掉已拆出的市值与配置部分）。
运行时状态（扫描缓存、进度表、同步线程表及其锁）集中在本模块顶部。
"""
from __future__ import annotations

import json
import math
import os
import pickle
import threading
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from trader.core import config
from trader.core.db import connect_existing
from trader.core.schema import ensure_high_confidence_tables
from trader.core.utils import clean_number, coerce_int, normalize_trade_date
from trader.patterns.scanner import (
    PANEL_CACHE_VERSION as HC_SCAN_PANEL_CACHE_VERSION,
    build_signals as build_hc_scan_signals,
    prepare_scan_frame as prepare_hc_scan_frame,
)
from trader.indicators.enrich import add_indicators  # noqa: F401  (parity with app import surface)
from trader.strategies.high_confidence import config as hc
from trader.strategies.high_confidence.config import (
    HC_DEFAULT_HISTORY_DAYS,
    HC_DEFAULT_MAX_PER_DATE,
    HC_DEFAULT_MIN_MARKET_CAP_YI,
    HC_DEFAULT_PATTERNS,
    HC_ENABLE_FOCUS_MODEL,
    HC_ENABLE_PRECISION_FILTER,
    HC_ENABLE_QUALITY_CACHE,
    HC_ENABLE_QUALITY_CACHE_FALLBACK,
    HC_FOCUS_MODEL_CALIBRATION_DATE,
    HC_FOCUS_MODEL_THRESHOLD,
    HC_FOCUS_MODEL_TRAIN_RULE_VERSION,
    HC_FOCUS_SIGNAL_COMBOS,
    HC_MIN_FULL_MARKET_ROWS,
    HC_PRECISION_RULE_VERSION,
    HC_QUALITY_COUPLING_MATCH_MODE,
    HC_QUALITY_FORWARD_DAYS,
    HC_QUALITY_LOOKBACK_DAYS,
    HC_QUALITY_MIN_SAMPLES,
    HC_QUALITY_MIN_WIN_RATE,
    HC_QUALITY_OUTCOME,
    HC_QUALITY_WIN_RETURN_THRESHOLD,
    HC_RULE_VERSION,
    HC_SCAN_COUPLING_MATCH_MODE,
    HC_SCAN_LOOKBACK_DAYS,
    HC_SCAN_MIN_HISTORY,
    HC_SCAN_MODE,
    HC_SCAN_PATTERN_ENGINE,
)
from trader.strategies.high_confidence.focus_model import score_focus_rows
from trader.strategies.high_confidence.market_cap import apply_hc_market_cap_filter
from trader.strategies.high_confidence.quality import load_or_build_quality as build_hc_signal_quality

BASE_DIR = str(config.BASE_DIR)
DB_PATH = str(config.DB_PATH)

HC_SCAN_CACHE = {}
HC_SCAN_CACHE_LOCK = threading.Lock()
HC_PROGRESS = {}
HC_PROGRESS_LOCK = threading.Lock()
HC_SYNC_THREADS = {}
HC_SYNC_THREADS_LOCK = threading.Lock()


def get_db():
    return connect_existing(DB_PATH, tune=False)


def build_hc_params(args):
    date = normalize_trade_date(args.get("date") or args.get("tradeDate"), None)
    explicit_days = args.get("days")
    days = coerce_int(explicit_days, HC_DEFAULT_HISTORY_DAYS, 1, 250)
    if date and explicit_days in (None, ""):
        days = 1
    return {
        "max_per_date": HC_DEFAULT_MAX_PER_DATE,
        "patterns": list(HC_DEFAULT_PATTERNS),
        "date": date,
        "days": days,
        "refresh": str(args.get("refresh") or "").lower() in ("1", "true", "yes", "on"),
    }


def get_hc_available_dates(conn, limit=120):
    rows = conn.execute(
        """
        SELECT trade_date, COUNT(DISTINCT code) AS daily_rows
        FROM daily_prices
        GROUP BY trade_date
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [
        {"date": row["trade_date"], "daily_rows": row["daily_rows"]}
        for row in rows
    ]


def get_hc_recent_complete_dates(conn, days=HC_DEFAULT_HISTORY_DAYS, end_date=None):
    rows = get_hc_available_dates(conn, limit=max(days * 8, 240))
    dates = []
    for row in rows:
        trade_date = row["date"]
        if end_date and trade_date > end_date:
            continue
        if int(row["daily_rows"] or 0) < HC_MIN_FULL_MARKET_ROWS:
            continue
        dates.append(row)
        if len(dates) >= days:
            break
    return dates


def set_hc_progress(trade_date, phase, message, percent=None, **extra):
    if not trade_date:
        trade_date = "_latest"
    payload = {
        "trade_date": trade_date,
        "phase": phase,
        "message": message,
        "percent": percent,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "updated_ts": time.time(),
        **extra,
    }
    with HC_PROGRESS_LOCK:
        HC_PROGRESS[trade_date] = payload
    return payload


def get_hc_progress(trade_date=None):
    with HC_PROGRESS_LOCK:
        if trade_date:
            return dict(HC_PROGRESS.get(trade_date) or {})
        if not HC_PROGRESS:
            return {}
        latest = max(HC_PROGRESS.values(), key=lambda item: item.get("updated_ts", 0))
        return dict(latest)


def hc_limit_pct(code, name):
    code = str(code or "").zfill(6)
    name = str(name or "").upper()
    if "ST" in name:
        return 4.8
    if code.startswith(("300", "301", "688", "689")):
        return 19.5
    return 9.8


def load_hc_scan_panel(conn, trade_date, use_cache=True):
    cache_key = (
        trade_date,
        HC_SCAN_LOOKBACK_DAYS,
        HC_SCAN_MIN_HISTORY,
        False,
    )
    if use_cache:
        with HC_SCAN_CACHE_LOCK:
            cached = HC_SCAN_CACHE.get(cache_key)
        if cached is not None:
            return cached.copy(), True

    cache_dir = os.path.join(BASE_DIR, "outputs", "cache")
    cache_path = os.path.join(
        cache_dir,
        f"hc_scan_panel_{HC_SCAN_PANEL_CACHE_VERSION}_{trade_date.replace('-', '')}"
        f"_lb{HC_SCAN_LOOKBACK_DAYS}_min{HC_SCAN_MIN_HISTORY}_nonbj.pkl",
    )
    if use_cache and os.path.exists(cache_path):
        with open(cache_path, "rb") as f:
            panel = pickle.load(f)
        with HC_SCAN_CACHE_LOCK:
            HC_SCAN_CACHE[cache_key] = panel.copy()
        return panel.copy(), True

    panel = prepare_hc_scan_frame(
        conn,
        trade_date,
        HC_SCAN_LOOKBACK_DAYS,
        HC_SCAN_MIN_HISTORY,
        include_bj=False,
    )
    if use_cache:
        os.makedirs(cache_dir, exist_ok=True)
        with open(cache_path, "wb") as f:
            pickle.dump(panel, f, protocol=pickle.HIGHEST_PROTOCOL)
        with HC_SCAN_CACHE_LOCK:
            HC_SCAN_CACHE[cache_key] = panel.copy()
    return panel, False


def load_hc_signal_quality(trade_date, use_cache=True, allow_fallback=True):
    if not use_cache:
        return None, False, None
    cache_dir = os.path.join(BASE_DIR, "outputs", "cache")
    base_name = (
        f"hc_signal_quality_{trade_date.replace('-', '')}_{HC_SCAN_MODE}"
        f"_lb{HC_QUALITY_LOOKBACK_DAYS}_fwd{HC_QUALITY_FORWARD_DAYS}"
    )
    if HC_QUALITY_OUTCOME != "close":
        base_name = f"{base_name}_{HC_QUALITY_OUTCOME}"
    candidates = [
        os.path.join(cache_dir, f"{base_name}_wr{HC_QUALITY_WIN_RETURN_THRESHOLD:g}.pkl"),
        os.path.join(cache_dir, f"{base_name}.pkl"),
    ]
    cache_path = next((path for path in candidates if os.path.exists(path)), None)
    if cache_path:
        with open(cache_path, "rb") as f:
            return pickle.load(f), True, trade_date
    if not allow_fallback or not HC_ENABLE_QUALITY_CACHE_FALLBACK:
        return None, False, None

    prefix = "hc_signal_quality_"
    suffix = (
        f"_{HC_SCAN_MODE}_lb{HC_QUALITY_LOOKBACK_DAYS}"
        f"_fwd{HC_QUALITY_FORWARD_DAYS}"
    )
    if HC_QUALITY_OUTCOME != "close":
        suffix = f"{suffix}_{HC_QUALITY_OUTCOME}"
    suffix = f"{suffix}_wr{HC_QUALITY_WIN_RETURN_THRESHOLD:g}.pkl"
    target_dt = datetime.strptime(trade_date, "%Y-%m-%d")
    fallback = None
    for name in os.listdir(cache_dir) if os.path.isdir(cache_dir) else []:
        if not name.startswith(prefix) or not name.endswith(suffix):
            continue
        date_part = name[len(prefix):len(prefix) + 8]
        try:
            cache_dt = datetime.strptime(date_part, "%Y%m%d")
        except ValueError:
            continue
        if cache_dt > target_dt:
            continue
        if fallback is None or cache_dt > fallback[0]:
            fallback = (cache_dt, os.path.join(cache_dir, name))
    if not fallback:
        return None, False, None
    with open(fallback[1], "rb") as f:
        return pickle.load(f), True, fallback[0].strftime("%Y-%m-%d")


def build_hc_quality_args(use_cache=True):
    return SimpleNamespace(
        mode=HC_SCAN_MODE,
        quality_lookback_days=HC_QUALITY_LOOKBACK_DAYS,
        forward_days=HC_QUALITY_FORWARD_DAYS,
        outcome=HC_QUALITY_OUTCOME,
        win_return_threshold=HC_QUALITY_WIN_RETURN_THRESHOLD,
        quality_chunk_size=200,
        coupling_match_mode=HC_QUALITY_COUPLING_MATCH_MODE,
        pattern_engine=HC_SCAN_PATTERN_ENGINE,
        cache=use_cache,
    )


def apply_hc_quality_filter(signal_rows, trade_date, conn=None, use_cache=True, allow_fallback=True):
    if signal_rows.empty:
        return signal_rows, {"quality_rows": 0, "quality_cache_hit": False}
    quality_cache_enabled = use_cache and HC_ENABLE_QUALITY_CACHE
    quality, cache_hit, quality_source_date = load_hc_signal_quality(
        trade_date,
        use_cache=quality_cache_enabled,
        allow_fallback=allow_fallback,
    )
    if quality is not None and quality_source_date and quality_source_date != trade_date:
        set_hc_progress(
            trade_date,
            "quality_cache",
            f"未找到当天质量缓存，复用 {quality_source_date} 历史质量缓存",
            68,
            quality_source_date=quality_source_date,
        )
    if quality is None and conn is not None:
        set_hc_progress(
            trade_date,
            "quality_build",
            "未找到历史质量缓存，正在计算历史胜率/样本数",
            60,
            raw_signal_rows=int(len(signal_rows)),
        )
        quality = build_hc_signal_quality(
            conn,
            trade_date,
            signal_rows,
            build_hc_quality_args(use_cache=quality_cache_enabled),
            Path(BASE_DIR) / "outputs" / "cache",
        )
        cache_hit = False
        quality_source_date = trade_date
    if quality is None or quality.empty:
        filtered = signal_rows.iloc[0:0].copy()
        return filtered, {"quality_rows": 0, "quality_cache_hit": False}
    signal_rows = signal_rows.copy()
    quality = quality.copy()
    signal_rows["code"] = signal_rows["code"].astype(str).str.zfill(6)
    quality["code"] = quality["code"].astype(str).str.zfill(6)
    if "signal_fid" not in signal_rows.columns:
        signal_rows["signal_fid"] = ""
    if "signal_fid" not in quality.columns:
        quality["signal_fid"] = ""
    if "耦合条件" not in signal_rows.columns:
        signal_rows["耦合条件"] = signal_rows["signal_fid"].astype(str) + "-" + signal_rows["coupling_family"].astype(str)
    if "耦合条件" not in quality.columns:
        quality["耦合条件"] = quality["signal_fid"].astype(str) + "-" + quality["coupling_family"].astype(str)
    enriched = signal_rows.merge(
        quality,
        on=["code", "形态名称", "signal_fid", "coupling_family", "耦合条件"],
        how="left",
    )
    filtered = enriched[
        (enriched["hist_samples"] >= HC_QUALITY_MIN_SAMPLES)
        & (enriched["hist_win_rate"] >= HC_QUALITY_MIN_WIN_RATE)
    ].copy()
    return filtered, {
        "quality_rows": int(len(quality)),
        "quality_cache_hit": cache_hit,
        "quality_source_date": quality_source_date,
    }


def build_hc_payload_from_candidate_rows(conn, trade_date, base_payload, candidate_rows):
    candidate_rows = [dict(row) for row in candidate_rows]
    filtered_rows, cap_meta = apply_hc_market_cap_filter(conn, trade_date, candidate_rows)
    output_rows = filtered_rows
    pattern_counts = {}
    coupling_counts = {}
    close_limit_count = 0
    touch_limit_count = 0
    for row in output_rows:
        for pattern in row.get("patterns") or []:
            pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1
        for coupling in row.get("couplings") or []:
            coupling_counts[coupling] = coupling_counts.get(coupling, 0) + 1
        if row.get("close_limit"):
            close_limit_count += 1
        if row.get("touch_limit"):
            touch_limit_count += 1

    payload = json.loads(json.dumps(base_payload, ensure_ascii=False))
    payload["groups"] = [{
        "date": trade_date,
        "raw_count": len(filtered_rows),
        "count": len(output_rows),
        "rows": output_rows,
    }]
    meta = payload.setdefault("meta", {})
    meta.update({
        "trade_date": trade_date,
        "raw_filtered_rows": len(filtered_rows),
        "output_rows": len(output_rows),
        "min_market_cap_yi": HC_DEFAULT_MIN_MARKET_CAP_YI,
        "close_limit_rows": close_limit_count,
        "touch_limit_rows": touch_limit_count,
        "result_source": "auto_cap_fill",
        "rule_version": HC_RULE_VERSION,
        "pattern_counts": sorted(pattern_counts.items(), key=lambda x: (-x[1], x[0])),
        "coupling_counts": sorted(coupling_counts.items(), key=lambda x: (-x[1], x[0])),
        **cap_meta,
    })
    payload.setdefault("params", {})
    payload["params"]["date"] = trade_date
    payload["params"]["refresh"] = False
    return payload


def rank_hc_scan_stocks(scan):
    if scan.empty:
        return []
    ordered = scan.sort_values(
        ["scan_score", "pct_change"],
        ascending=[False, False],
    ).copy()
    rows = []
    for (code, secucode, name, trade_date), group in ordered.groupby(
        ["code", "secucode", "name", "trade_date"], sort=False
    ):
        best = group.sort_values(
            ["hist_win_rate", "hist_samples", "hist_pl_ratio", "scan_score"],
            ascending=[False, False, False, False],
        ).iloc[0]
        patterns = sorted({str(v) for v in group["形态名称"].dropna()})
        couplings = sorted({str(v) for v in group["coupling_family"].dropna()})
        signal_keys = sorted({str(v) for v in group["耦合条件"].dropna()}) if "耦合条件" in group else []
        pct_change = clean_number(best.get("pct_change"))
        prev_close = clean_number(best.get("prev_close"))
        high = clean_number(best.get("high"))
        limit_pct = hc_limit_pct(code, name)
        high_pct = (high / prev_close - 1) * 100 if prev_close > 0 else pct_change
        hist_win_rate = clean_number(group["hist_win_rate"].max()) if "hist_win_rate" in group else 0
        hist_samples = int(clean_number(group["hist_samples"].max())) if "hist_samples" in group else 0
        hist_pl_ratio = clean_number(group["hist_pl_ratio"].max()) if "hist_pl_ratio" in group else 0
        rank_score = (
            clean_number(group["scan_score"].max())
            + math.log1p(len(group)) * 2.5
            + math.log1p(len(patterns)) * 2.0
            + math.log1p(len(couplings)) * 1.5
            + max(min(pct_change, 10), -5) * 0.15
            + hist_win_rate * 12
            + math.log1p(hist_samples) * 0.6
            + min(hist_pl_ratio, 5) * 0.4
        )
        row = {
            "date": trade_date,
            "code": code,
            "secucode": secucode,
            "name": name,
            "pattern": str(best.get("形态名称") or ""),
            "coupling": str(best.get("coupling_family") or ""),
            "signal_fid": str(best.get("signal_fid") or ""),
            "signal_key": str(best.get("耦合条件") or ""),
            "patterns": patterns,
            "couplings": couplings,
            "signal_keys": signal_keys,
            "signal_count": int(len(group)),
            "pattern_count": int(len(patterns)),
            "coupling_count": int(len(couplings)),
            "hist_win_rate": hist_win_rate,
            "hist_samples": hist_samples,
            "hist_pl_ratio": hist_pl_ratio,
            "rank_score": rank_score,
            "scan_score": clean_number(group["scan_score"].max()),
            "close": clean_number(best.get("close")),
            "pct_change": pct_change,
            "turnover": clean_number(best.get("turnover")),
            "amount_yi": clean_number(best.get("amount")) / 100000000,
            "touch_limit": high_pct >= limit_pct,
            "close_limit": pct_change >= limit_pct,
        }
        for col in (
            "range_close_pct",
            "body_range_pct",
            "upper_range_pct",
            "lower_range_pct",
            "close_position_day_pct",
            "close_ma5_dist_pct",
            "close_ma10_dist_pct",
            "close_ma30_dist_pct",
            "close_ma60_dist_pct",
            "close_hma20_dist_pct",
            "close_hma30_dist_pct",
            "ma30_slope_pct",
            "ma60_slope_pct",
            "hma20_slope_pct",
            "hma30_slope_pct",
            "macd",
            "macd_dif",
            "macd_dea",
            "amount_ratio20",
            "volume_ratio20",
        ):
            row[col] = clean_number(best.get(col))
        rows.append(row)
    return sorted(
        rows,
        key=lambda r: (
            -r["rank_score"],
            -r["scan_score"],
            -r["pct_change"],
            -r["signal_count"],
        ),
    )


def hc_precision_rule_names(row):
    signal_key = str(row.get("signal_key") or "")
    top_sparse = signal_key in {"fid28-Fscore", "fid40-ash", "fid23-MA5"}
    high_prec_signal = signal_key in {
        "fid28-Fscore",
        "fid40-ash",
        "fid23-MA5",
        "fid40-MACD",
        "fid26-MACD",
        "fid47-MACD",
    }
    rules = []
    if (
        clean_number(row.get("close_ma5_dist_pct")) <= -3
        and clean_number(row.get("hist_samples")) <= 50
        and clean_number(row.get("pct_change")) >= 0
        and clean_number(row.get("range_close_pct")) <= 3
        and clean_number(row.get("lower_range_pct")) <= 0.5
    ):
        rules.append("R1_pullback_small_sample")
    if (
        top_sparse
        and clean_number(row.get("close_position_day_pct")) >= 70
        and clean_number(row.get("hist_pl_ratio")) <= 50
        and clean_number(row.get("hma30_slope_pct")) <= 1
        and clean_number(row.get("range_close_pct")) >= 1.5
    ):
        rules.append("R2_sparse_close_high")
    if (
        clean_number(row.get("hist_samples")) >= 300
        and clean_number(row.get("ma30_slope_pct")) >= 1
        and clean_number(row.get("range_close_pct")) >= 8
        and clean_number(row.get("scan_score")) >= 28
        and clean_number(row.get("macd_dea")) <= 1
    ):
        rules.append("R3_strong_wide_trend")
    if HC_PRECISION_RULE_VERSION != "precision_top3_p80" and (
        signal_key == "fid39-Fscore"
        and clean_number(row.get("hist_win_rate")) >= 0.78
        and clean_number(row.get("hma20_slope_pct")) >= 4
        and clean_number(row.get("amount_yi")) <= 20
    ):
        rules.append("R4_fid39_fscore_accel")
    if HC_PRECISION_RULE_VERSION != "precision_top3_p80" and (
        clean_number(row.get("close_position_day_pct")) >= 99
        and clean_number(row.get("hist_samples")) >= 120
        and clean_number(row.get("hist_win_rate")) >= 0.85
        and clean_number(row.get("turnover")) >= 5
    ):
        rules.append("R5_limit_high_quality")
    return rules


def apply_hc_precision_filter(rows):
    if not HC_ENABLE_PRECISION_FILTER:
        return rows, {
            "precision_filter_enabled": False,
            "precision_rule_version": HC_PRECISION_RULE_VERSION,
            "precision_input_rows": len(rows),
            "precision_output_rows": len(rows),
        }
    filtered = []
    for row in rows:
        rule_names = hc_precision_rule_names(row)
        if not rule_names:
            continue
        item = dict(row)
        item["precision_rules"] = rule_names
        item["precision_rule_count"] = len(rule_names)
        item["precision_score"] = (
            len(rule_names) * 10
            + clean_number(item.get("hist_win_rate")) * 5
            + min(clean_number(item.get("hist_pl_ratio")), 50) * 0.05
            + min(clean_number(item.get("rank_score")), 80) * 0.03
        )
        filtered.append(item)
    filtered = sorted(
        filtered,
        key=lambda row: (
            -clean_number(row.get("precision_score")),
            -clean_number(row.get("rank_score")),
            -clean_number(row.get("hist_win_rate")),
        ),
    )
    return filtered, {
        "precision_filter_enabled": True,
        "precision_rule_version": HC_PRECISION_RULE_VERSION,
        "precision_input_rows": len(rows),
        "precision_output_rows": len(filtered),
    }


def build_high_confidence_payload_realtime(params):
    use_cache = not params.get("no_cache")
    allow_quality_fallback = not params.get("no_quality_fallback")
    conn = get_db()
    try:
        ensure_high_confidence_tables(conn)
        available_date_rows = get_hc_available_dates(conn)
        available_dates = [row["date"] for row in available_date_rows]
        default_date = next(
            (
                row["date"]
                for row in available_date_rows
                if row["daily_rows"] >= HC_MIN_FULL_MARKET_ROWS
            ),
            available_dates[0] if available_dates else "",
        )
        trade_date = params.get("date") or default_date
        if not trade_date:
            return {"error": "没有可用日 K 数据", "meta": {}, "groups": []}, 400
        set_hc_progress(trade_date, "date", f"已确认交易日 {trade_date}", 5)
        daily_rows = next(
            (row["daily_rows"] for row in available_date_rows if row["date"] == trade_date),
            conn.execute(
                "SELECT COUNT(DISTINCT code) FROM daily_prices WHERE trade_date = ?",
                (trade_date,),
            ).fetchone()[0],
        )
        if not daily_rows:
            return {
                "params": {**params, "date": trade_date},
                "meta": {
                    "trade_date": trade_date,
                    "available_dates": available_dates,
                    "daily_rows": 0,
                    "scanned_stocks": 0,
                    "raw_signal_rows": 0,
                    "raw_filtered_rows": 0,
                    "output_rows": 0,
                    "close_limit_rows": 0,
                    "touch_limit_rows": 0,
                    "pattern_counts": [],
                    "coupling_counts": [],
                    "cache_hit": False,
                },
                "groups": [],
            }

        set_hc_progress(
            trade_date,
            "panel",
            f"正在加载 {trade_date} 全市场日K指标面板",
            15,
            daily_rows=int(daily_rows),
        )
        panel, cache_hit = load_hc_scan_panel(conn, trade_date, use_cache=use_cache)
        set_hc_progress(
            trade_date,
            "signals",
            f"正在从 {len(panel)} 只股票中识别K线形态和耦合信号",
            45,
            scanned_stocks=int(len(panel)),
            panel_cache_hit=cache_hit,
        )
        signal_rows = build_hc_scan_signals(
            panel,
            HC_SCAN_MODE,
            top=0,
            patterns_filter=set(params["patterns"]),
            couplings_filter=None,
            exclude_proxy=False,
            pattern_engine=HC_SCAN_PATTERN_ENGINE,
            signal_combos_filter=HC_FOCUS_SIGNAL_COMBOS,
            coupling_match_mode=HC_SCAN_COUPLING_MATCH_MODE,
        )
        set_hc_progress(
            trade_date,
            "quality",
            f"正在进行历史胜率/样本数过滤，原始信号 {len(signal_rows)} 条",
            70,
            raw_signal_rows=int(len(signal_rows)),
        )
        quality_signal_rows, quality_meta = apply_hc_quality_filter(
            signal_rows,
            trade_date,
            conn,
            use_cache=use_cache,
            allow_fallback=allow_quality_fallback,
        )
        set_hc_progress(
            trade_date,
            "quality_done",
            f"历史质量过滤完成，保留信号 {len(quality_signal_rows)} 条",
            82,
            quality_signal_rows=int(len(quality_signal_rows)),
            quality_cache_hit=quality_meta.get("quality_cache_hit"),
        )
        set_hc_progress(trade_date, "rank", "正在聚合股票并排序", 88)
        ranked_rows = rank_hc_scan_stocks(quality_signal_rows)
        set_hc_progress(
            trade_date,
            "market_cap",
            f"正在应用市值过滤，候选股票 {len(ranked_rows)} 只",
            92,
            candidate_stocks=len(ranked_rows),
        )
        ranked_rows, cap_meta = apply_hc_market_cap_filter(
            conn,
            trade_date,
            ranked_rows,
            use_cache=use_cache,
        )
    finally:
        conn.close()

    pre_focus_rows = ranked_rows
    focus_meta = {
        "focus_model_enabled": False,
        "focus_input_rows": len(pre_focus_rows),
        "focus_output_rows": len(pre_focus_rows),
    }
    if HC_ENABLE_FOCUS_MODEL:
        ranked_rows, focus_meta = score_focus_rows(
            pre_focus_rows,
            DB_PATH,
            HC_FOCUS_MODEL_TRAIN_RULE_VERSION,
            threshold=HC_FOCUS_MODEL_THRESHOLD,
            calibration_date=HC_FOCUS_MODEL_CALIBRATION_DATE,
            lizi_dir=os.path.join(BASE_DIR, "lizi"),
            exclude_dates=[trade_date],
        )
        ranked_rows = sorted(
            ranked_rows,
            key=lambda row: (
                -clean_number(row.get("focus_score")),
                -clean_number(row.get("rank_score")),
                -clean_number(row.get("hist_win_rate")),
            ),
        )

    pre_precision_rows = ranked_rows
    ranked_rows, precision_meta = apply_hc_precision_filter(pre_precision_rows)
    output_rows = ranked_rows[:params["max_per_date"]] if params["max_per_date"] else ranked_rows

    pattern_counts = {}
    coupling_counts = {}
    close_limit_count = 0
    touch_limit_count = 0
    for row in output_rows:
        for pattern in row["patterns"]:
            pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1
        for coupling in row["couplings"]:
            coupling_counts[coupling] = coupling_counts.get(coupling, 0) + 1
        if row["close_limit"]:
            close_limit_count += 1
        if row["touch_limit"]:
            touch_limit_count += 1

    groups = [{
        "date": trade_date,
        "raw_count": len(pre_precision_rows),
        "count": len(output_rows),
        "rows": output_rows,
    }]

    return {
        "params": {**params, "date": trade_date},
        "meta": {
            "trade_date": trade_date,
            "available_dates": available_dates,
            "daily_rows": int(daily_rows),
            "scanned_stocks": int(len(panel)),
            "raw_signal_rows": int(len(signal_rows)),
            "quality_signal_rows": int(len(quality_signal_rows)),
            "quality_rows": quality_meta["quality_rows"],
            "quality_cache_hit": quality_meta["quality_cache_hit"],
            "quality_source_date": quality_meta.get("quality_source_date"),
            "quality_min_win_rate": HC_QUALITY_MIN_WIN_RATE,
            "quality_min_samples": HC_QUALITY_MIN_SAMPLES,
            "raw_filtered_rows": len(pre_focus_rows),
            "pre_focus_rows": len(pre_focus_rows),
            "pre_precision_rows": len(pre_precision_rows),
            "output_rows": len(output_rows),
            "min_market_cap_yi": HC_DEFAULT_MIN_MARKET_CAP_YI,
            **cap_meta,
            **focus_meta,
            **precision_meta,
            "close_limit_rows": close_limit_count,
            "touch_limit_rows": touch_limit_count,
            "cache_hit": cache_hit,
            "no_cache": bool(params.get("no_cache")),
            "result_source": "realtime",
            "rule_version": HC_RULE_VERSION,
            "pattern_counts": sorted(pattern_counts.items(), key=lambda x: (-x[1], x[0])),
            "coupling_counts": sorted(coupling_counts.items(), key=lambda x: (-x[1], x[0])),
        },
        "groups": groups,
    }


def resolve_hc_trade_date(conn, params):
    available_date_rows = get_hc_available_dates(conn)
    available_dates = [row["date"] for row in available_date_rows]
    default_date = next(
        (
            row["date"]
            for row in available_date_rows
            if row["daily_rows"] >= HC_MIN_FULL_MARKET_ROWS
        ),
        available_dates[0] if available_dates else "",
    )
    return params.get("date") or default_date, available_dates


def load_high_confidence_cached_payload(conn, trade_date):
    row = conn.execute(
        """
        SELECT payload_json, updated_at
        FROM high_confidence_scans
        WHERE trade_date = ? AND rule_version = ? AND status = 'ok'
        """,
        (trade_date, HC_RULE_VERSION),
    ).fetchone()
    if not row or not row["payload_json"]:
        return None
    payload = json.loads(row["payload_json"])
    payload.setdefault("meta", {})
    payload["meta"]["result_source"] = "cache"
    payload["meta"]["rule_version"] = HC_RULE_VERSION
    payload["meta"]["cache_updated_at"] = row["updated_at"]
    return payload


def load_high_confidence_cached_payloads(conn, trade_dates):
    if not trade_dates:
        return {}
    placeholders = ",".join("?" for _ in trade_dates)
    rows = conn.execute(
        f"""
        SELECT trade_date, payload_json, updated_at
        FROM high_confidence_scans
        WHERE trade_date IN ({placeholders})
          AND rule_version = ?
          AND status = 'ok'
          AND payload_json IS NOT NULL
        """,
        [*trade_dates, HC_RULE_VERSION],
    ).fetchall()
    payloads = {}
    for row in rows:
        payload = json.loads(row["payload_json"])
        payload.setdefault("meta", {})
        payload["meta"]["result_source"] = "cache"
        payload["meta"]["rule_version"] = HC_RULE_VERSION
        payload["meta"]["cache_updated_at"] = row["updated_at"]
        payloads[row["trade_date"]] = payload
    return payloads


def extract_hc_candidate_rows(payload):
    rows = []
    seen = set()
    for group in payload.get("groups") or []:
        for row in group.get("rows") or []:
            code = str(row.get("code") or "").zfill(6)
            if not code or code in seen:
                continue
            seen.add(code)
            rows.append(row)
    return rows


def load_hc_capoff_candidate_payload(conn, trade_date):
    row = conn.execute(
        """
        SELECT payload_json, updated_at, rule_version
        FROM high_confidence_scans
        WHERE trade_date = ?
          AND rule_version LIKE '%_capoff'
          AND status = 'ok'
          AND payload_json IS NOT NULL
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (trade_date,),
    ).fetchone()
    if not row:
        return None
    payload = json.loads(row["payload_json"])
    payload.setdefault("meta", {})
    payload["meta"]["candidate_source_rule_version"] = row["rule_version"]
    payload["meta"]["candidate_cache_updated_at"] = row["updated_at"]
    return payload


def maybe_autofill_cached_market_caps(conn, trade_date, cached_payload):
    meta = cached_payload.get("meta") or {}
    if not meta.get("market_cap_unavailable") and not meta.get("market_cap_missing"):
        return cached_payload

    candidate_payload = load_hc_capoff_candidate_payload(conn, trade_date)
    if not candidate_payload:
        return cached_payload
    candidate_rows = extract_hc_candidate_rows(candidate_payload)
    if not candidate_rows:
        return cached_payload

    set_hc_progress(
        trade_date,
        "market_cap_autofill",
        f"正在补齐候选票市值，候选 {len(candidate_rows)} 只",
        92,
        candidate_stocks=len(candidate_rows),
    )
    payload = build_hc_payload_from_candidate_rows(
        conn,
        trade_date,
        candidate_payload,
        candidate_rows,
    )
    save_high_confidence_payload(conn, trade_date, payload)
    set_hc_progress(
        trade_date,
        "done",
        f"市值补齐完成，候选 {payload.get('meta', {}).get('output_rows', 0)} 只",
        100,
        output_rows=payload.get("meta", {}).get("output_rows", 0),
    )
    return payload


def save_high_confidence_payload(conn, trade_date, payload):
    row_count = payload.get("meta", {}).get("output_rows", 0)
    conn.execute(
        """
        INSERT INTO high_confidence_scans (
            trade_date, rule_version, row_count, status, payload_json, error,
            created_at, updated_at
        )
        VALUES (?, ?, ?, 'ok', ?, NULL, datetime('now','localtime'), datetime('now','localtime'))
        ON CONFLICT(trade_date, rule_version) DO UPDATE SET
            row_count = excluded.row_count,
            status = excluded.status,
            payload_json = excluded.payload_json,
            error = NULL,
            updated_at = datetime('now','localtime')
        """,
        (
            trade_date,
            HC_RULE_VERSION,
            row_count,
            json.dumps(payload, ensure_ascii=False),
        ),
    )
    conn.commit()


def merge_hc_count_pairs(items):
    counts = {}
    for pairs in items:
        for key, value in pairs or []:
            counts[key] = counts.get(key, 0) + int(value or 0)
    return sorted(counts.items(), key=lambda x: (-x[1], x[0]))


def sum_hc_meta(payloads, key):
    total = 0
    for payload in payloads:
        value = (payload.get("meta") or {}).get(key)
        if isinstance(value, (int, float)):
            total += value
    return total


def high_confidence_history_payload(params):
    days = coerce_int(params.get("days"), HC_DEFAULT_HISTORY_DAYS, 1, 250)
    end_date = params.get("date")
    conn = get_db()
    try:
        ensure_high_confidence_tables(conn)
        date_rows = get_hc_recent_complete_dates(conn, days=days, end_date=end_date)
        _trade_date, available_dates = resolve_hc_trade_date(conn, params)
        cached_by_date = {}
        if not params.get("refresh") and not params.get("no_cache"):
            cached_by_date = load_high_confidence_cached_payloads(
                conn,
                [row["date"] for row in date_rows],
            )
    finally:
        conn.close()

    if not date_rows:
        return {"error": "没有可用的完整日 K 数据", "meta": {}, "groups": []}

    dates = [row["date"] for row in date_rows]
    payloads = []
    missing_cache_dates = []
    total = len(dates)
    batch_key = f"_latest_{total}"
    for index, trade_date in enumerate(dates, start=1):
        set_hc_progress(
            batch_key,
            "batch",
            f"正在同步最近{total}个交易日：{trade_date} ({index}/{total})",
            5 + int((index - 1) / total * 90),
            current_date=trade_date,
            current_index=index,
            total=total,
        )
        payload = cached_by_date.get(trade_date)
        if payload is not None:
            payload.setdefault("params", {})
            payload["params"]["date"] = trade_date
            payload["params"]["refresh"] = False
            payload["meta"]["available_dates"] = available_dates
        else:
            if not params.get("refresh") and not params.get("no_cache"):
                missing_cache_dates.append(trade_date)
                continue
            payload = high_confidence_payload({
                **params,
                "date": trade_date,
                "days": 1,
            })
        payloads.append(payload)
        set_hc_progress(
            batch_key,
            "batch",
            f"已完成 {trade_date} ({index}/{total})",
            5 + int(index / total * 90),
            current_date=trade_date,
            current_index=index,
            total=total,
        )

    groups = []
    for payload in payloads:
        groups.extend(payload.get("groups") or [])

    sources = {
        (payload.get("meta") or {}).get("result_source")
        for payload in payloads
        if (payload.get("meta") or {}).get("result_source")
    }
    result_source = next(iter(sources)) if len(sources) == 1 else "mixed"
    latest_meta = payloads[0].get("meta") if payloads else {}
    meta = {
        "trade_date": dates[0],
        "date_start": dates[-1],
        "date_end": dates[0],
        "days": len(payloads),
        "requested_days": days,
        "cache_missing_days": len(missing_cache_dates),
        "cache_missing_dates": missing_cache_dates,
        "available_dates": available_dates,
        "daily_rows": latest_meta.get("daily_rows"),
        "scanned_stocks": sum_hc_meta(payloads, "scanned_stocks"),
        "raw_signal_rows": sum_hc_meta(payloads, "raw_signal_rows"),
        "quality_signal_rows": sum_hc_meta(payloads, "quality_signal_rows"),
        "raw_filtered_rows": sum_hc_meta(payloads, "raw_filtered_rows"),
        "output_rows": sum_hc_meta(payloads, "output_rows"),
        "min_market_cap_yi": HC_DEFAULT_MIN_MARKET_CAP_YI,
        "market_cap_fetched": sum_hc_meta(payloads, "market_cap_fetched"),
        "market_cap_missing": sum_hc_meta(payloads, "market_cap_missing"),
        "market_cap_filtered": sum_hc_meta(payloads, "market_cap_filtered"),
        "close_limit_rows": sum_hc_meta(payloads, "close_limit_rows"),
        "touch_limit_rows": sum_hc_meta(payloads, "touch_limit_rows"),
        "result_source": result_source,
        "rule_version": HC_RULE_VERSION,
        "pattern_counts": merge_hc_count_pairs(
            (payload.get("meta") or {}).get("pattern_counts") for payload in payloads
        ),
        "coupling_counts": merge_hc_count_pairs(
            (payload.get("meta") or {}).get("coupling_counts") for payload in payloads
        ),
    }
    set_hc_progress(
        batch_key,
        "done",
        f"最近{len(payloads)}个已缓存交易日加载完成，输出 {meta['output_rows']} 只次",
        100,
        output_rows=meta["output_rows"],
        total=len(payloads),
    )
    return {
        "params": {**params, "date": end_date, "days": days},
        "meta": meta,
        "groups": groups,
    }


def high_confidence_payload(params):
    if coerce_int(params.get("days"), 1, 1, 250) > 1:
        return high_confidence_history_payload(params)

    conn = get_db()
    try:
        ensure_high_confidence_tables(conn)
        trade_date, available_dates = resolve_hc_trade_date(conn, params)
        if not trade_date:
            return {"error": "没有可用日 K 数据", "meta": {}, "groups": []}
        if not params.get("refresh") and not params.get("no_cache"):
            cached = load_high_confidence_cached_payload(conn, trade_date)
            if cached is not None:
                cached = maybe_autofill_cached_market_caps(conn, trade_date, cached)
                cached.setdefault("params", {})
                cached["params"]["date"] = trade_date
                cached["params"]["refresh"] = False
                cached["meta"]["available_dates"] = available_dates
                return cached
    finally:
        conn.close()

    try:
        payload = build_high_confidence_payload_realtime({**params, "date": trade_date})
        if not payload.get("error") and not params.get("no_cache"):
            set_hc_progress(trade_date, "save", "正在保存同步结果", 97)
            conn = get_db()
            try:
                ensure_high_confidence_tables(conn)
                save_high_confidence_payload(conn, trade_date, payload)
            finally:
                conn.close()
            set_hc_progress(
                trade_date,
                "done",
                f"同步完成，候选 {payload.get('meta', {}).get('output_rows', 0)} 只",
                100,
                output_rows=payload.get("meta", {}).get("output_rows", 0),
            )
    except Exception as exc:
        set_hc_progress(trade_date, "error", f"同步失败：{exc}", 100, error=str(exc))
        raise
    return payload


def run_high_confidence_sync_job(sync_key, params):
    try:
        high_confidence_payload({**params, "refresh": True})
    finally:
        with HC_SYNC_THREADS_LOCK:
            HC_SYNC_THREADS.pop(sync_key, None)


def start_high_confidence_sync(params):
    days = coerce_int(params.get("days"), HC_DEFAULT_HISTORY_DAYS, 1, 250)
    if days > 1:
        sync_key = f"_latest_{days}"
        with HC_SYNC_THREADS_LOCK:
            existing = HC_SYNC_THREADS.get(sync_key)
            if existing and existing.is_alive():
                return {
                    "started": False,
                    "days": days,
                    "progress": get_hc_progress(sync_key),
                }, 200
            set_hc_progress(sync_key, "queued", f"最近{days}个交易日已加入同步队列", 1)
            thread = threading.Thread(
                target=run_high_confidence_sync_job,
                args=(sync_key, {**params, "date": None, "days": days}),
                daemon=True,
            )
            HC_SYNC_THREADS[sync_key] = thread
            thread.start()
        return {
            "started": True,
            "days": days,
            "progress": get_hc_progress(sync_key),
        }, 200

    conn = get_db()
    try:
        ensure_high_confidence_tables(conn)
        trade_date, _available_dates = resolve_hc_trade_date(conn, params)
    finally:
        conn.close()
    if not trade_date:
        return {"error": "没有可用日 K 数据"}, 400

    with HC_SYNC_THREADS_LOCK:
        existing = HC_SYNC_THREADS.get(trade_date)
        if existing and existing.is_alive():
            return {
                "started": False,
                "trade_date": trade_date,
                "progress": get_hc_progress(trade_date),
            }, 200
        set_hc_progress(trade_date, "queued", f"{trade_date} 已加入同步队列", 1)
        thread = threading.Thread(
            target=run_high_confidence_sync_job,
            args=(trade_date, {**params, "date": trade_date, "days": 1}),
            daemon=True,
        )
        HC_SYNC_THREADS[trade_date] = thread
        thread.start()
    return {
        "started": True,
        "trade_date": trade_date,
        "progress": get_hc_progress(trade_date),
    }, 200
