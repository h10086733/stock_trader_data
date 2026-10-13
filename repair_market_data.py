#!/usr/bin/env python3
"""清理和回补生产行情数据。

示例：
  # 只检查 9/23~10/9 是否有非交易日数据
  python repair_market_data.py --start 2026-09-23 --end 2026-10-09 \
      --delete-non-trading-days --dry-run

  # 删除 9/23~10/9 的行情和派生指标，然后重新回补
  python repair_market_data.py --reset-from 2026-09-23 --end 2026-10-09 --rerun

  # 只删除非交易日数据，不回补
  python repair_market_data.py --start 2026-09-01 --end 2026-10-09 \
      --delete-non-trading-days
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from datetime import datetime

import fetch

try:
    import baostock as bs
except ImportError:  # pragma: no cover
    bs = None


DB_PATH = "stock_data.db"
MARKET_TABLES = (
    ("daily_prices", "trade_date"),
    ("high_confidence_market_caps", "trade_date"),
    ("index_daily_stats", "trade_date"),
    ("index_intraday_stats", "trade_date"),
    ("intraday_5m_cache", "trade_date"),
)
DERIVED_TABLES = (
    ("high_confidence_scans", "trade_date"),
    ("momentum_picks", "trade_date"),
    ("momentum_scan_runs", "trade_date"),
    ("pattern_picks", "trade_date"),
    ("pattern_scan_runs", "trade_date"),
    ("surge_scan_batches", "trade_date"),
    ("surge_scan_rows", "trade_date"),
    ("csi1000_timing_signals", "trade_date"),
)


def parse_date(value: str) -> str:
    try:
        return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"日期必须是 YYYY-MM-DD: {value}") from exc


def load_trading_days(start: str, end: str) -> set[str]:
    if bs is None:
        raise RuntimeError("清理非交易日数据需要安装 baostock")
    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"BaoStock 登录失败: {login.error_code} {login.error_msg}")
    try:
        result = bs.query_trade_dates(start, end)
        if result.error_code != "0":
            raise RuntimeError(f"交易日历查询失败: {result.error_code} {result.error_msg}")
        days = set()
        while result.next():
            row = result.get_row_data()
            if len(row) >= 2 and row[1] == "1":
                days.add(row[0])
        if not days:
            raise RuntimeError(f"交易日历为空: {start} ~ {end}")
        return days
    finally:
        bs.logout()


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone() is not None


def count_non_trading(conn: sqlite3.Connection, start: str, end: str, days: set[str]):
    result = {}
    placeholders = ",".join("?" for _ in days)
    params = [start, end, *sorted(days)]
    for table, column in MARKET_TABLES:
        if not table_exists(conn, table):
            continue
        result[table] = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {column} BETWEEN ? AND ? "
            f"AND {column} NOT IN ({placeholders})", params
        ).fetchone()[0]
    return result


def delete_non_trading(conn: sqlite3.Connection, start: str, end: str, days: set[str]) -> dict[str, int]:
    placeholders = ",".join("?" for _ in days)
    params = [start, end, *sorted(days)]
    deleted = {}
    for table, column in (*MARKET_TABLES, *DERIVED_TABLES):
        if not table_exists(conn, table):
            continue
        deleted[table] = conn.execute(
            f"DELETE FROM {table} WHERE {column} BETWEEN ? AND ? "
            f"AND {column} NOT IN ({placeholders})", params
        ).rowcount
    return deleted


def delete_from(conn, start: str, end: str) -> dict[str, int]:
    deleted = {}
    for table, column in (*MARKET_TABLES, *DERIVED_TABLES):
        if not table_exists(conn, table):
            continue
        deleted[table] = conn.execute(
            f"DELETE FROM {table} WHERE {column} BETWEEN ? AND ?", (start, end)
        ).rowcount
    return deleted


def refresh_stock_ranges(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        UPDATE stocks SET
          history_start=(SELECT MIN(trade_date) FROM daily_prices p WHERE p.code=stocks.code),
          history_end=(SELECT MAX(trade_date) FROM daily_prices p WHERE p.code=stocks.code),
          price_latest=(SELECT close FROM daily_prices p WHERE p.code=stocks.code
                        ORDER BY trade_date DESC LIMIT 1),
          updated_at=datetime('now','localtime')
        """
    )


def rerun(conn: sqlite3.Connection, start: str, end: str, trading_days: set[str], sleep: float) -> None:
    rows = conn.execute("SELECT code, market FROM stocks ORDER BY code").fetchall()
    total = len(rows)
    failed = []
    started = time.time()
    for index, (code, market) in enumerate(rows, 1):
        try:
            payload = fetch.fetch_kline(code, market, start.replace("-", ""), end.replace("-", ""))
            payload = [row for row in payload if row.get("trade_date") in trading_days]
            fetch.insert_daily_prices(conn, code, payload)
            fetch.update_stock_price_range(conn, code, payload)
        except Exception as exc:  # keep going; failed codes are reported for retry
            failed.append((code, str(exc)))
        if index % 100 == 0 or index == total:
            print(f"回补进度 {index}/{total} 失败={len(failed)}", flush=True)
        if sleep:
            time.sleep(sleep)
    if failed:
        print(f"回补完成但有 {len(failed)} 只失败，前20只: {failed[:20]}", file=sys.stderr)
    print(f"回补耗时 {(time.time() - started) / 60:.1f} 分钟")


def main() -> int:
    parser = argparse.ArgumentParser(description="清理非交易日行情并按指定区间回补")
    parser.add_argument("--db", default=DB_PATH)
    parser.add_argument("--start", type=parse_date, help="检查区间开始日期")
    parser.add_argument("--end", type=parse_date, help="区间结束日期（含）")
    parser.add_argument("--reset-from", type=parse_date, help="从该日期开始删除并可配合 --rerun 回补")
    parser.add_argument("--delete-non-trading-days", action="store_true", help="删除区间内所有非交易日数据")
    parser.add_argument("--rerun", action="store_true", help="清理后按交易日回补行情")
    parser.add_argument("--sleep", type=float, default=0.0, help="每只股票请求后的等待秒数")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    start = args.reset_from or args.start
    end = args.end
    if not start or not end:
        parser.error("必须同时指定 --start/--end，或 --reset-from/--end")
    if start > end:
        parser.error("开始日期不能晚于结束日期")
    if not args.delete_non_trading_days and not args.reset_from and not args.rerun:
        parser.error("至少指定 --delete-non-trading-days、--reset-from 或 --rerun")

    days = load_trading_days(start, end)
    print(f"交易日 {len(days)} 个: {min(days)} -> {max(days)}")
    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA busy_timeout=60000")
    try:
        if args.dry_run:
            if args.reset_from:
                print("将删除:", delete_from(conn, start, end))
                conn.rollback()
            if args.delete_non_trading_days:
                print("将删除非交易日:", count_non_trading(conn, start, end, days))
            return 0

        conn.execute("BEGIN IMMEDIATE")
        if args.reset_from:
            print("删除区间:", delete_from(conn, start, end))
        if args.delete_non_trading_days:
            print("删除非交易日:", delete_non_trading(conn, start, end, days))
        refresh_stock_ranges(conn)
        conn.commit()
        if args.rerun:
            rerun(conn, start, end, days, args.sleep)
            conn.commit()
            refresh_stock_ranges(conn)
            conn.commit()
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
