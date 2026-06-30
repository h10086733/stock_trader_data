"""动量策略参数构建：API 请求参数解析、CLI 参数映射、空 meta 工厂。

来源：app.py build_momentum_params（~2242）、build_empty_scan_meta（~9827）、
build_cli_momentum_params（~10467）。
"""
from __future__ import annotations

from trader.core.utils import (
    coerce_int,
    coerce_float,
    get_source_value,
    normalize_trade_date,
    parse_cutoff_time,
    default_scan_trade_date,
)


def build_momentum_params(source=None) -> dict:
    """从 Flask request.args / dict / argparse namespace 构建规范化参数 dict。"""
    source = source or {}
    pool = source.get("pool", "all")
    index_code = get_source_value(source, "indexCode", "index_code", default="") or ""
    if pool == "index" and not index_code:
        pool = "all"
    cutoff_text = source.get("cutoff", "14:30")
    cutoff = parse_cutoff_time(cutoff_text)
    cutoff_text = cutoff.strftime("%H:%M")
    min_gain = coerce_float(get_source_value(source, "minGain", "min_gain"), 2.0, -5, 15)
    max_gain = coerce_float(get_source_value(source, "maxGain", "max_gain"),
                            7.5, min_gain, 20)
    return {
        "pool": pool,
        "index_code": index_code,
        "cutoff": cutoff_text,
        "trade_date": normalize_trade_date(
            get_source_value(source, "tradeDate", "trade_date"),
            default_scan_trade_date(),
        ),
        "min_gain": min_gain,
        "max_gain": max_gain,
        "min_vol_ratio": coerce_float(
            get_source_value(source, "minVolRatio", "min_vol_ratio"), 1.5, 0.2, 10,
        ),
        "min_amount_wan": coerce_float(
            get_source_value(source, "minAmount", "min_amount"), 8000, 0, 1000000,
        ),
        "limit": coerce_int(source.get("limit"), 80, 1, 300),
        "verify_limit": coerce_int(
            get_source_value(source, "verifyLimit", "verify_limit"), 50, 1, 1000,
        ),
        "workers": coerce_int(source.get("workers"), 6, 2, 12),
    }


def build_empty_scan_meta(params: dict, universe: int = 0) -> dict:
    return {
        "pool": params["pool"],
        "cutoff": params["cutoff"],
        "trade_date": params["trade_date"],
        "index_code": params["index_code"],
        "universe": universe,
        "quoted": 0,
        "prefiltered": 0,
        "verified": 0,
        "minute_success": 0,
        "minute_failed": 0,
        "cache_hits": 0,
        "elapsed_s": 0,
    }


def build_cli_momentum_params(args) -> dict:
    """从 argparse Namespace 构建 momentum 参数（CLI 入口用）。"""
    return build_momentum_params({
        "pool": args.pool,
        "index_code": args.index_code,
        "cutoff": args.cutoff,
        "trade_date": args.trade_date,
        "min_gain": args.min_gain,
        "max_gain": args.max_gain,
        "min_vol_ratio": args.min_vol_ratio,
        "min_amount": args.min_amount,
        "limit": args.limit,
        "verify_limit": args.verify_limit,
        "workers": args.workers,
    })
