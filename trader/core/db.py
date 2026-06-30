"""数据库连接工厂与小工具。

统一 ``sqlite3.connect`` 的散落调用。历史上 fetch.py 设置了
WAL/synchronous/cache_size，app.py 设置了 row_factory=Row，
csi1000_timing.py 两者都设。这里提供统一入口。
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from . import config


def connect(db_path=None, row_factory: bool = True, tune: bool = True) -> sqlite3.Connection:
    """打开数据库连接。

    row_factory=True 时使用 sqlite3.Row（可按列名访问）。
    tune=True 时应用 WAL / synchronous=NORMAL / cache_size 性能 pragma。
    """
    path = Path(db_path) if db_path is not None else config.DB_PATH
    conn = sqlite3.connect(str(path))
    if row_factory:
        conn.row_factory = sqlite3.Row
    if tune:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-64000")  # 64MB
    return conn


def connect_existing(db_path=None, **kwargs) -> sqlite3.Connection:
    """打开已存在的数据库；文件不存在时抛 FileNotFoundError。

    用于 Web 层等只读已有数据、不应隐式创建空库的场景（来源：app.py:151 get_db）。
    """
    path = Path(db_path) if db_path is not None else config.DB_PATH
    if not path.exists():
        raise FileNotFoundError(f"数据库文件不存在: {path}")
    return connect(path, **kwargs)


def row_to_plain(row) -> dict:
    """sqlite3.Row -> dict。来源：app.py:639。"""
    if row is None:
        return {}
    return {key: row[key] for key in row.keys()}


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    """返回某张表的列名集合。"""
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None
