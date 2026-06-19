#!/usr/bin/env python3
"""
高置信选股历史验证。

选股阶段只使用目标日及目标日前的数据；验证阶段再读取目标日之后的 K 线，
统计次日收益、未来 N 日最高收益等表现。
"""

from __future__ import annotations

import argparse
import sqlite3
import time
from pathlib import Path

import pandas as pd

import app


def parse_args():
    parser = argparse.ArgumentParser(description="验证高置信选股历史表现")
    parser.add_argument("--start", default="2021-01-01", help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end", default="2024-12-31", help="结束日期 YYYY-MM-DD")
    parser.add_argument("--forward-days", type=int, default=5, help="向后验证交易日数量")
    parser.add_argument("--min-daily-rows", type=int, default=3500, help="当天最少日K股票数")
    parser.add_argument("--max-dates", type=int, default=0, help="最多验证多少个交易日，0表示不限制")
    parser.add_argument("--refresh", action="store_true", help="强制重算每日选股")
    parser.add_argument("--use-cache", action="store_true", help="允许使用页面/面板/质量缓存；默认不使用")
    parser.add_argument("--dry-run", action="store_true", help="只打印交易日，不执行验证")
    parser.add_argument("--output-dir", default=str(Path(app.BASE_DIR) / "outputs"))
    return parser.parse_args()


def trade_dates(conn: sqlite3.Connection, args) -> list[dict]:
    rows = conn.execute(
        """
        SELECT trade_date, COUNT(DISTINCT code) AS daily_rows
        FROM daily_prices
        WHERE trade_date >= ?
          AND trade_date <= ?
        GROUP BY trade_date
        HAVING COUNT(DISTINCT code) >= ?
        ORDER BY trade_date
        """,
        (args.start, args.end, args.min_daily_rows),
    ).fetchall()
    dates = [{"date": row["trade_date"], "daily_rows": int(row["daily_rows"])} for row in rows]
    if args.max_dates:
        dates = dates[:args.max_dates]
    return dates


def load_forward_rows(conn: sqlite3.Connection, code: str, trade_date: str, forward_days: int):
    return conn.execute(
        """
        SELECT trade_date, open, high, low, close, pct_change
        FROM daily_prices
        WHERE code = ?
          AND trade_date > ?
        ORDER BY trade_date
        LIMIT ?
        """,
        (code, trade_date, forward_days),
    ).fetchall()


def enrich_pick_outcome(conn: sqlite3.Connection, pick: dict, forward_days: int) -> dict:
    row = dict(pick)
    trade_date = row["date"]
    code = str(row["code"]).zfill(6)
    entry_close = float(row.get("close") or 0)
    future = load_forward_rows(conn, code, trade_date, forward_days)
    row["forward_days_available"] = len(future)
    if not future or entry_close <= 0:
        row.update({
            "next_trade_date": None,
            "next_close_return_pct": None,
            "fwd_close_return_pct": None,
            "fwd_max_high_return_pct": None,
            "fwd_min_low_return_pct": None,
            "fwd_hit_2pct": False,
            "fwd_hit_5pct": False,
            "fwd_hit_10pct": False,
        })
        return row

    next_row = future[0]
    last_row = future[-1]
    max_high = max(float(item["high"] or 0) for item in future)
    min_low = min(float(item["low"] or 0) for item in future)
    next_close_return = (float(next_row["close"]) / entry_close - 1) * 100
    fwd_close_return = (float(last_row["close"]) / entry_close - 1) * 100
    max_high_return = (max_high / entry_close - 1) * 100
    min_low_return = (min_low / entry_close - 1) * 100
    row.update({
        "next_trade_date": next_row["trade_date"],
        "next_close_return_pct": round(next_close_return, 4),
        "fwd_close_return_pct": round(fwd_close_return, 4),
        "fwd_max_high_return_pct": round(max_high_return, 4),
        "fwd_min_low_return_pct": round(min_low_return, 4),
        "fwd_hit_2pct": max_high_return >= 2.0,
        "fwd_hit_5pct": max_high_return >= 5.0,
        "fwd_hit_10pct": max_high_return >= 10.0,
    })
    return row


def flatten_picks(payload: dict) -> list[dict]:
    rows = []
    for group in payload.get("groups") or []:
        for row in group.get("rows") or []:
            rows.append(dict(row))
    return rows


def assert_payload_trade_date(payload: dict, expected_date: str):
    meta_date = (payload.get("meta") or {}).get("trade_date")
    if meta_date != expected_date:
        raise RuntimeError(f"扫描返回日期异常: expected={expected_date} actual={meta_date}")
    for group in payload.get("groups") or []:
        group_date = group.get("date")
        if group_date != expected_date:
            raise RuntimeError(f"分组日期异常: expected={expected_date} actual={group_date}")
        for row in group.get("rows") or []:
            row_date = row.get("date")
            if row_date != expected_date:
                raise RuntimeError(
                    f"选股行日期异常: expected={expected_date} actual={row_date} "
                    f"code={row.get('code')}"
                )


def summarize(picks: pd.DataFrame, key: str | None = None) -> pd.DataFrame:
    if picks.empty:
        return pd.DataFrame()
    frame = picks.copy()
    if key is None:
        frame["_all"] = "all"
        key = "_all"
    grouped = frame.groupby(key, dropna=False)
    summary = grouped.agg(
        trade_dates=("date", "nunique"),
        picks=("code", "count"),
        avg_picks_per_day=("code", lambda s: len(s) / frame.loc[s.index, "date"].nunique()),
        avg_next_close_return_pct=("next_close_return_pct", "mean"),
        median_next_close_return_pct=("next_close_return_pct", "median"),
        next_close_win_rate=("next_close_return_pct", lambda s: (s > 0).mean()),
        avg_fwd_close_return_pct=("fwd_close_return_pct", "mean"),
        avg_fwd_max_high_return_pct=("fwd_max_high_return_pct", "mean"),
        median_fwd_max_high_return_pct=("fwd_max_high_return_pct", "median"),
        hit_2pct_rate=("fwd_hit_2pct", "mean"),
        hit_5pct_rate=("fwd_hit_5pct", "mean"),
        hit_10pct_rate=("fwd_hit_10pct", "mean"),
        avg_drawdown_pct=("fwd_min_low_return_pct", "mean"),
    ).reset_index()
    for col in summary.columns:
        if col.endswith("_rate"):
            summary[col] = (summary[col] * 100).round(2)
        elif col not in (key, "trade_dates", "picks"):
            summary[col] = summary[col].round(4)
    if key == "_all":
        summary = summary.drop(columns=["_all"])
    return summary


def main():
    args = parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = app.get_db()
    try:
        app.ensure_high_confidence_tables(conn)
        dates = trade_dates(conn, args)
    finally:
        conn.close()

    if not dates:
        print("没有符合条件的交易日")
        return 1
    print(
        f"准备验证 {len(dates)} 个交易日: {dates[0]['date']} -> {dates[-1]['date']} "
        f"forward_days={args.forward_days} use_cache={args.use_cache}"
    )
    if args.dry_run:
        for item in dates:
            print(f"  {item['date']} daily_rows={item['daily_rows']}")
        return 0

    all_picks = []
    daily_rows = []
    started_at = time.time()
    for index, item in enumerate(dates, start=1):
        trade_date = item["date"]
        t0 = time.time()
        print(f"[{index}/{len(dates)}] {trade_date} 扫描", flush=True)
        payload = app.build_high_confidence_payload_realtime({
            "date": trade_date,
            "no_cache": not args.use_cache,
            "no_quality_fallback": not args.use_cache,
            "max_per_date": app.HC_DEFAULT_MAX_PER_DATE,
            "patterns": list(app.HC_DEFAULT_PATTERNS),
        })
        if isinstance(payload, tuple):
            payload, status_code = payload
            if status_code >= 400:
                raise RuntimeError(payload.get("error") or f"扫描失败 status={status_code}")
        assert_payload_trade_date(payload, trade_date)
        meta = payload.get("meta") or {}
        picks = flatten_picks(payload)
        conn = app.get_db()
        try:
            enriched = [
                enrich_pick_outcome(conn, pick, args.forward_days)
                for pick in picks
            ]
        finally:
            conn.close()
        all_picks.extend(enriched)
        daily_rows.append({
            "date": trade_date,
            "daily_rows": item["daily_rows"],
            "picks": len(enriched),
            "raw_signal_rows": meta.get("raw_signal_rows"),
            "quality_signal_rows": meta.get("quality_signal_rows"),
            "raw_filtered_rows": meta.get("raw_filtered_rows"),
            "quality_source_date": meta.get("quality_source_date"),
            "no_cache": meta.get("no_cache"),
            "market_cap_checked": meta.get("market_cap_checked"),
            "market_cap_fetched": meta.get("market_cap_fetched"),
            "market_cap_missing": meta.get("market_cap_missing"),
            "market_cap_filtered": meta.get("market_cap_filtered"),
            "elapsed_s": round(time.time() - t0, 2),
        })
        print(
            f"[{index}/{len(dates)}] {trade_date} picks={len(enriched)} "
            f"raw={meta.get('raw_signal_rows', 0)} "
            f"quality={meta.get('quality_signal_rows', 0)} "
            f"stock_after_quality={meta.get('raw_filtered_rows', 0)} "
            f"mcap_missing={meta.get('market_cap_missing', 0)} "
            f"mcap_filtered={meta.get('market_cap_filtered', 0)} "
            f"quality={meta.get('quality_source_date') or '-'} "
            f"elapsed={time.time() - t0:.1f}s",
            flush=True,
        )

    start_tag = args.start.replace("-", "")
    end_tag = args.end.replace("-", "")
    picks_df = pd.DataFrame(all_picks)
    daily_df = pd.DataFrame(daily_rows)
    if not picks_df.empty:
        picks_df["year"] = picks_df["date"].str.slice(0, 4)
        picks_df["month"] = picks_df["date"].str.slice(0, 7)

    cache_tag = "cache" if args.use_cache else "nocache"
    picks_path = out_dir / f"hc_backtest_picks_{start_tag}_{end_tag}_{cache_tag}.csv"
    daily_path = out_dir / f"hc_backtest_daily_{start_tag}_{end_tag}_{cache_tag}.csv"
    summary_path = out_dir / f"hc_backtest_summary_{start_tag}_{end_tag}_{cache_tag}.csv"
    year_path = out_dir / f"hc_backtest_summary_year_{start_tag}_{end_tag}_{cache_tag}.csv"
    month_path = out_dir / f"hc_backtest_summary_month_{start_tag}_{end_tag}_{cache_tag}.csv"

    picks_df.to_csv(picks_path, index=False, encoding="utf-8-sig")
    daily_df.to_csv(daily_path, index=False, encoding="utf-8-sig")
    summary = summarize(picks_df)
    by_year = summarize(picks_df, "year") if not picks_df.empty else pd.DataFrame()
    by_month = summarize(picks_df, "month") if not picks_df.empty else pd.DataFrame()
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    by_year.to_csv(year_path, index=False, encoding="utf-8-sig")
    by_month.to_csv(month_path, index=False, encoding="utf-8-sig")

    print(f"写入: {picks_path}")
    print(f"写入: {daily_path}")
    print(f"写入: {summary_path}")
    if not summary.empty:
        print(summary.to_string(index=False))
    print(f"全部完成，耗时={(time.time() - started_at) / 60:.1f}min")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
