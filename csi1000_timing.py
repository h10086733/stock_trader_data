"""
中证1000择时策略：指数行情、信号提醒与历史回测。

实现已迁移至 trader.strategies.csi1000.timing。本文件保留为向后兼容 shim，
re-export 全部公开符号并保留 CLI 入口。下游 import 无需改动：
  import csi1000_timing                       # app.py
  import csi1000_timing as timing             # scripts/csi1000_*.py

常用命令：
  python csi1000_timing.py --init-db
  python csi1000_timing.py --fetch-index-prices --start 20100101
  python csi1000_timing.py --backtest --start 2026-02-03
  python csi1000_timing.py --signal
"""

from __future__ import annotations

from trader.strategies.csi1000.timing import *  # noqa: F401,F403
from trader.strategies.csi1000.timing import (  # noqa: F401  (显式 re-export 下游使用的名字)
    STRATEGY_PRESETS,
    backtest,
    cmd_backfill_width,
    cmd_fetch_index_prices,
    connect,
    fetch_index_realtime_quotes,
    fetch_index_realtime_sina,
    generate_and_save_signals,
    init_db,
    load_db_price_excel_width_frame,
    load_feature_frame,
    load_frame_by_source,
    main,
    replace,
    save_index_prices,
)

if __name__ == "__main__":
    raise SystemExit(main())
