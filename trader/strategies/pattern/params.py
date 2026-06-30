"""形态策略参数构建：API 参数解析、公开参数过滤、空 meta 工厂。

来源：app.py build_pattern_params（~2282）、public_pattern_params（~2451）、
saved_pattern_filters_enabled（~2455）、build_empty_pattern_meta（~4859）、
default_pattern_backfill_days（~4570）、normalize_pattern_backfill_params（~4574）、
pattern_history_load_days（~4582）、pattern_type_from_params_json（~4872）。
"""
from __future__ import annotations

import json

from trader.core.utils import (
    coerce_int,
    coerce_float,
    get_source_value,
    normalize_trade_date,
)


def build_pattern_params(source=None) -> dict:
    source = source or {}
    pool = source.get("pool", "all")
    index_code = get_source_value(source, "indexCode", "index_code", default="") or ""
    if pool == "index" and not index_code:
        pool = "all"
    pattern_type = get_source_value(
        source, "patternType", "pattern_type", default="bottom_reversal"
    )
    if pattern_type not in ("four_pin", "bottom_reversal"):
        pattern_type = "bottom_reversal"
    return {
        "pattern_type": pattern_type,
        "pool": pool,
        "index_code": index_code,
        "trade_date": normalize_trade_date(
            get_source_value(source, "tradeDate", "trade_date"), None,
        ),
        "lookback_days": coerce_int(
            get_source_value(source, "lookbackDays", "lookback_days"), 120, 60, 260,
        ),
        "chart_bars": coerce_int(
            get_source_value(source, "chartBars", "chart_bars"), 70, 40, 140,
        ),
        "min_amount_wan": coerce_float(
            get_source_value(source, "minAmount", "min_amount"), 8000, 0, 1000000,
        ),
        "min_turnover": coerce_float(
            get_source_value(source, "minTurnover", "min_turnover"), 0, 0, 100,
        ),
        "min_market_cap_yi": coerce_float(
            get_source_value(source, "minMarketCapYi", "min_market_cap_yi"), 0, 0, 100000,
        ),
        "max_body_pct": coerce_float(
            get_source_value(source, "maxBodyPct", "max_body_pct"), 1.05, 0.1, 10,
        ),
        "max_body_range_pct": coerce_float(
            get_source_value(source, "maxBodyRangePct", "max_body_range_pct"), 35, 5, 95,
        ),
        "max_amp_pct": coerce_float(
            get_source_value(source, "maxAmpPct", "max_amp_pct"), 6.0, 0.5, 20,
        ),
        "doji_body_pct": coerce_float(
            get_source_value(source, "dojiBodyPct", "doji_body_pct"), 1.05, 0.1, 5,
        ),
        "max_ma40_distance": coerce_float(
            get_source_value(source, "maxMa40Distance", "max_ma40_distance"), 0.0, 0, 50,
        ),
        "max_pair_distance": coerce_float(
            get_source_value(source, "maxPairDistance", "max_pair_distance"), 0.5, 0.1, 10,
        ),
        "max_close_pair_distance": coerce_float(
            get_source_value(source, "maxClosePairDistance", "max_close_pair_distance"), 1.0, 0.1, 10,
        ),
        "min_level_gap": coerce_float(
            get_source_value(source, "minLevelGap", "min_level_gap"), 0.8, 0.0, 10,
        ),
        "min_shadow_pct": coerce_float(
            get_source_value(source, "minShadowPct", "min_shadow_pct"), 1.0, 0.0, 50,
        ),
        "max_shadowless_count": coerce_int(
            get_source_value(source, "maxShadowlessCount", "max_shadowless_count"), 0, 0, 4,
        ),
        "bottom_lookback_days": coerce_int(
            get_source_value(source, "bottomLookbackDays", "bottom_lookback_days"), 60, 20, 160,
        ),
        "max_bottom_position": coerce_float(
            get_source_value(source, "maxBottomPosition", "max_bottom_position"), 25, 5, 90,
        ),
        "min_prior_drop_pct": coerce_float(
            get_source_value(source, "minPriorDropPct", "min_prior_drop_pct"), 10.0, 0, 60,
        ),
        "bottom_max_body_pct": coerce_float(
            get_source_value(source, "bottomMaxBodyPct", "bottom_max_body_pct"), 3.0, 0.2, 12,
        ),
        "bottom_only_bullish_engulfing": coerce_int(
            get_source_value(source, "bottomOnlyBullishEngulfing", "bottom_only_bullish_engulfing"), 1, 0, 1,
        ),
        "bottom_pattern_group": (
            get_source_value(source, "bottomPatternGroup", "bottom_pattern_group")
            if get_source_value(source, "bottomPatternGroup", "bottom_pattern_group")
               in ("engulfing", "strong", "single", "all")
            else "engulfing"
        ),
        "min_bottom_volume_ratio": coerce_float(
            get_source_value(source, "minBottomVolumeRatio", "min_bottom_volume_ratio"), 2.0, 0, 10,
        ),
        "max_bottom_volume_ratio": coerce_float(
            get_source_value(source, "maxBottomVolumeRatio", "max_bottom_volume_ratio"), 3.0, 0, 20,
        ),
        "min_bottom_rebound_pct": coerce_float(
            get_source_value(source, "minBottomReboundPct", "min_bottom_rebound_pct"), 3.0, 0, 30,
        ),
        "min_bottom_pct_change": coerce_float(
            get_source_value(source, "minBottomPctChange", "min_bottom_pct_change"), 2.5, -20, 20,
        ),
        "min_bottom_strong_gain_pct": coerce_float(
            get_source_value(source, "minBottomStrongGainPct", "min_bottom_strong_gain_pct"), 4.0, 0, 20,
        ),
        "require_bottom_confirm": coerce_int(
            get_source_value(source, "requireBottomConfirm", "require_bottom_confirm"), 1, 0, 1,
        ),
        "min_bottom_close_position": coerce_float(
            get_source_value(source, "minBottomClosePosition", "min_bottom_close_position"), 75.0, 0, 100,
        ),
        "require_bottom_close_above_prev": coerce_int(
            get_source_value(source, "requireBottomCloseAbovePrev", "require_bottom_close_above_prev"), 1, 0, 1,
        ),
        "require_bottom_above_ma5": coerce_int(
            get_source_value(source, "requireBottomAboveMa5", "require_bottom_above_ma5"), 1, 0, 1,
        ),
        "min_bottom_ma5_slope_pct": coerce_float(
            get_source_value(source, "minBottomMa5SlopePct", "min_bottom_ma5_slope_pct"), -1.0, -10, 10,
        ),
        "require_bottom_not_close_new_low": coerce_int(
            get_source_value(source, "requireBottomNotCloseNewLow", "require_bottom_not_close_new_low"), 1, 0, 1,
        ),
        "bottom_new_low_lookback_days": coerce_int(
            get_source_value(source, "bottomNewLowLookbackDays", "bottom_new_low_lookback_days"), 20, 5, 80,
        ),
        "pattern_win_lookback_days": coerce_int(
            get_source_value(source, "patternWinLookbackDays", "pattern_win_lookback_days"), 720, 120, 2000,
        ),
        "pattern_win_hold_days": coerce_int(
            get_source_value(source, "patternWinHoldDays", "pattern_win_hold_days"), 1, 1, 30,
        ),
        "pattern_win_target_pct": coerce_float(
            get_source_value(source, "patternWinTargetPct", "pattern_win_target_pct"), 3.0, 0, 30,
        ),
        "limit": coerce_int(source.get("limit"), 10 if pattern_type == "bottom_reversal" else 80, 1, 300),
    }


def public_pattern_params(params: dict) -> dict:
    return {k: v for k, v in params.items() if not str(k).startswith("_")}


def saved_pattern_filters_enabled(source) -> bool:
    value = get_source_value(source, "strict", "filterSaved", "filter_saved")
    return str(value).lower() in ("1", "true", "yes", "on")


def build_empty_pattern_meta(params: dict, universe: int = 0) -> dict:
    return {
        "pool": params["pool"],
        "trade_date": params.get("trade_date"),
        "index_code": params["index_code"],
        "universe": universe,
        "scanned": 0,
        "matched": 0,
        "elapsed_s": 0,
        "params": {"pattern_type": params.get("pattern_type", "four_pin")},
    }


def default_pattern_backfill_days(params: dict) -> int:
    return 365 if (params or {}).get("pattern_type") == "four_pin" else 30


def normalize_pattern_backfill_params(params: dict) -> dict:
    params = dict(params)
    if params.get("pattern_type") == "bottom_reversal":
        params["bottom_pattern_group"] = "all"
        params["bottom_only_bullish_engulfing"] = 0
    return params


def pattern_history_load_days(params: dict) -> int:
    if (params or {}).get("pattern_type") != "bottom_reversal":
        return params["lookback_days"]
    return max(
        params["lookback_days"],
        (params.get("pattern_win_lookback_days") or 720)
        + (params.get("pattern_win_hold_days") or 1)
        + 90,
    )


def pattern_type_from_params_json(params_json: str) -> str:
    try:
        return (json.loads(params_json or "{}").get("pattern_type") or "four_pin")
    except (TypeError, json.JSONDecodeError):
        return "four_pin"


def build_cli_pattern_params(args) -> dict:
    """从 argparse Namespace 构建 pattern 参数（CLI 入口用）。"""
    return build_pattern_params({
        "pattern_type": args.pattern_type,
        "pool": args.pool,
        "index_code": args.index_code,
        "trade_date": args.trade_date,
        "min_amount": args.pattern_min_amount,
        "limit": args.limit,
        "lookback_days": args.pattern_lookback_days,
        "chart_bars": args.pattern_chart_bars,
        "max_body_pct": args.pattern_max_body_pct,
        "max_body_range_pct": args.pattern_max_body_range_pct,
        "max_amp_pct": args.pattern_max_amp_pct,
        "doji_body_pct": args.pattern_doji_body_pct,
        "max_ma40_distance": args.pattern_max_ma40_distance,
        "max_pair_distance": args.pattern_max_pair_distance,
        "max_close_pair_distance": args.pattern_max_close_pair_distance,
        "min_level_gap": args.pattern_min_level_gap,
        "min_shadow_pct": args.pattern_min_shadow_pct,
        "max_shadowless_count": args.pattern_max_shadowless_count,
        "bottom_lookback_days": args.pattern_bottom_lookback_days,
        "max_bottom_position": args.pattern_max_bottom_position,
        "min_prior_drop_pct": args.pattern_min_prior_drop_pct,
        "bottom_max_body_pct": args.pattern_bottom_max_body_pct,
        "bottom_only_bullish_engulfing": args.pattern_bottom_only_bullish_engulfing,
        "bottom_pattern_group": args.pattern_bottom_group,
        "min_bottom_volume_ratio": args.pattern_min_bottom_volume_ratio,
        "max_bottom_volume_ratio": args.pattern_max_bottom_volume_ratio,
        "min_bottom_rebound_pct": args.pattern_min_bottom_rebound_pct,
        "min_bottom_pct_change": args.pattern_min_bottom_pct_change,
        "min_bottom_strong_gain_pct": args.pattern_min_bottom_strong_gain_pct,
        "require_bottom_confirm": args.pattern_require_bottom_confirm,
        "min_bottom_close_position": args.pattern_min_bottom_close_position,
        "require_bottom_close_above_prev": args.pattern_require_bottom_close_above_prev,
        "require_bottom_above_ma5": args.pattern_require_bottom_above_ma5,
        "min_bottom_ma5_slope_pct": args.pattern_min_bottom_ma5_slope_pct,
        "require_bottom_not_close_new_low": args.pattern_require_bottom_not_close_new_low,
        "bottom_new_low_lookback_days": args.pattern_bottom_new_low_lookback_days,
        "pattern_win_lookback_days": args.pattern_win_lookback_days,
        "pattern_win_hold_days": args.pattern_win_hold_days,
        "pattern_win_target_pct": args.pattern_win_target_pct,
        "min_turnover": args.pattern_min_turnover,
        "min_market_cap_yi": args.pattern_min_market_cap_yi,
    })
