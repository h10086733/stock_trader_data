#!/usr/bin/env python3
"""
预热每日高置信选股结果。

默认处理最近 N 个日 K 覆盖完整的交易日，按时间正序执行，便于后续日期复用
前面已经存在的历史质量缓存。
"""

from __future__ import annotations

import argparse
import sys
import time

import app


def parse_args():
    parser = argparse.ArgumentParser(description="预热高置信每日选股缓存")
    parser.add_argument("--days", type=int, default=30, help="需要预热的交易日数量")
    parser.add_argument("--start", default=None, help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="结束日期 YYYY-MM-DD")
    parser.add_argument(
        "--include-incomplete",
        action="store_true",
        help="包含日 K 覆盖不足的日期",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="强制重新扫描，忽略已有缓存",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只打印将要处理的日期，不执行扫描",
    )
    return parser.parse_args()


def load_trade_dates(args):
    conn = app.get_db()
    try:
        app.ensure_high_confidence_tables(conn)
        rows = app.get_hc_available_dates(conn, limit=max(args.days * 5, 180))
    finally:
        conn.close()

    dates = []
    for row in rows:
        trade_date = row["date"]
        daily_rows = int(row["daily_rows"] or 0)
        if args.start and trade_date < args.start:
            continue
        if args.end and trade_date > args.end:
            continue
        if not args.include_incomplete and daily_rows < app.HC_MIN_FULL_MARKET_ROWS:
            continue
        dates.append({"date": trade_date, "daily_rows": daily_rows})

    dates = dates[:args.days]
    dates.sort(key=lambda item: item["date"])
    return dates


def main():
    args = parse_args()
    dates = load_trade_dates(args)
    if not dates:
        print("没有找到符合条件的交易日")
        return 1

    print(
        f"准备预热 {len(dates)} 个交易日: "
        f"{dates[0]['date']} -> {dates[-1]['date']} "
        f"refresh={args.refresh}"
    )
    for item in dates:
        print(f"  {item['date']} daily_rows={item['daily_rows']}")
    if args.dry_run:
        return 0

    total = len(dates)
    started_at = time.time()
    for index, item in enumerate(dates, start=1):
        trade_date = item["date"]
        t0 = time.time()
        print(f"[{index}/{total}] {trade_date} 开始", flush=True)
        try:
            payload = app.high_confidence_payload({
                "date": trade_date,
                "refresh": args.refresh,
                "max_per_date": app.HC_DEFAULT_MAX_PER_DATE,
                "patterns": list(app.HC_DEFAULT_PATTERNS),
            })
            meta = payload.get("meta") or {}
            print(
                f"[{index}/{total}] {trade_date} 完成 "
                f"输出={meta.get('output_rows', 0)} "
                f"质量来源={meta.get('quality_source_date') or '-'} "
                f"市值补抓={meta.get('market_cap_fetched', 0)} "
                f"耗时={time.time() - t0:.1f}s",
                flush=True,
            )
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            print(f"[{index}/{total}] {trade_date} 失败: {exc}", file=sys.stderr, flush=True)

    print(f"全部完成，耗时={(time.time() - started_at) / 60:.1f}min")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
