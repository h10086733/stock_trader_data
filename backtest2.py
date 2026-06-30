"""
低PB+ROE亏损策略回测 v2 — backtest2.py。

实现已迁移到 trader.strategies.low_pb.backtest。本文件保留为向后兼容
shim（既有命令行用法 `python backtest2.py --run/--report/...` 与
`import backtest2` 仍可用）。

使用方式：
  python backtest2.py --import-stocks / --fetch-data / --run / --report
  python backtest2.py --trades / --status
"""
from __future__ import annotations

from trader.strategies.low_pb.backtest import *  # noqa: F401,F403
from trader.strategies.low_pb.backtest import main  # noqa: F401


if __name__ == "__main__":
    main()
