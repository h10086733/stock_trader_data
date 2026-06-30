"""
华创样例反推扫描器（K 线形态 + MA/MACD/HMA 耦合）。

注意：实现已迁移至 trader.patterns.scanner。本文件保留为向后兼容 shim，
从该模块 re-export 全部公开符号，并保留 CLI 入口。下游 import 无需改动：
  from hc_strategy_scanner import build_signals, prepare_scan_frame, ...

典型用法：
  python hc_strategy_scanner.py --date 2026-06-17 --compare
"""
from __future__ import annotations

from trader.patterns.scanner import *  # noqa: F401,F403
from trader.patterns.scanner import (  # 显式 re-export 非 __all__ 覆盖的名字
    BASE_DIR,
    COUPLING_WEIGHTS,
    DEFAULT_DB,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SELECTED,
    FOCUS_LONG_100YI_SIGNAL_COMBOS,
    KNOWN_PATTERNS,
    PANEL_CACHE_VERSION,
    PATTERN_FID_MAP,
    PATTERN_WEIGHTS,
    add_shape_metrics,
    build_parser,
    build_signals,
    detect_couplings,
    detect_patterns,
    infer_secucode,
    is_bj_code,
    load_stock_universe,
    main,
    normalize_pattern,
    normalize_signal_combos,
    pattern_fid,
    prepare_scan_frame,
    signal_coupling_override,
    signal_pattern_override,
    start_date_by_trading_days,
    trueish,
)

if __name__ == "__main__":
    raise SystemExit(main())
