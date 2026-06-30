#!/usr/bin/env python3
"""
Research next-day limit-up / large-up probabilities for HC shape signals.

实现已迁移至 trader.strategies.surge.research。本文件保留为向后兼容 shim
（app.py 仍以 `import next_day_surge_research as surge_research` 使用，
cron 仍可 `python next_day_surge_research.py ...`）。
"""

from __future__ import annotations

from trader.strategies.surge.research import *  # noqa: F401,F403
from trader.strategies.surge.research import (  # noqa: F401  (re-export for backward compat)
    aggregate_target_rows,
    historical_surge_stats,
    latest_complete_date,
    main,
    scan_target_signals,
)

if __name__ == "__main__":
    raise SystemExit(main())
