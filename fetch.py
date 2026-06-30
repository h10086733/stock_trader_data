"""
股票数据入库系统 — 阶段一：股票列表 + 历史价格。

实现已迁移到 trader.data.kline。本文件保留为向后兼容 shim（cron 与
supplement_selected_klines.py 仍可 `python fetch.py --sync` / `import fetch`）。

使用方式：
  python fetch.py --init / --sync / --backfill-days N / --status
"""
from __future__ import annotations

from trader.data.kline import *  # noqa: F401,F403
from trader.data.kline import main  # noqa: F401

if __name__ == "__main__":
    raise SystemExit(main())
