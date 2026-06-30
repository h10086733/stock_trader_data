"""
财务数据入库 — PB/ROE。

实现已迁移到 trader.data.financial。本文件保留为向后兼容 shim。

使用方式：
  python findata.py --stocks "000630,002092" / --index CODE / --all / --status
"""
from __future__ import annotations

from trader.data.financial import *  # noqa: F401,F403
from trader.data.financial import main  # noqa: F401

if __name__ == "__main__":
    main()
