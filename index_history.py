"""向后兼容 shim：实现已迁移至 trader.data.indices.history。

保留本文件以兼容旧的调用方式（python index_history.py ...）。
"""

from __future__ import annotations

import sys

from trader.data.indices.history import *  # noqa: F401,F403
from trader.data.indices.history import main


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
