"""
华创 K 线信号样例解析与选股排序工具。

实现已迁移至 trader.strategies.surge.signal_tool。本文件保留为向后兼容
shim，仍可 `python signal_tool.py ...` / `from signal_tool import limit_rate_for_code`。

典型用法：
  python signal_tool.py analyze --examples lizi
  python signal_tool.py select --input lizi/【20260618】60%信号精选版.xlsx --examples lizi --cap-scope 百亿以上
  python signal_tool.py parse --examples lizi --out /tmp/hc_signals.csv
"""

from __future__ import annotations

from trader.strategies.surge.signal_tool import *  # noqa: F401,F403
from trader.strategies.surge.signal_tool import (  # noqa: F401  (re-export for backward compat)
    limit_rate_for_code,
    main,
)

if __name__ == "__main__":
    raise SystemExit(main())
