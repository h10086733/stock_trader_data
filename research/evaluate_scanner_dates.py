"""
批量评估扫描器在样例日期上的召回率。

用法：
  python evaluate_scanner_dates.py
  python evaluate_scanner_dates.py --dates 2026-06-05,2026-06-17 --mode loose
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import pandas as pd

from hc_strategy_scanner import (
    DEFAULT_DB,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SELECTED,
    PANEL_CACHE_VERSION,
    build_signals,
    compare_with_selected,
    load_selected_for_date,
    prepare_scan_frame,
    split_filter,
)


def available_dates(selected_path: Path) -> list[str]:
    selected = pd.read_csv(selected_path)
    selected["判断日期"] = pd.to_datetime(selected["判断日期"]).dt.strftime("%Y-%m-%d")
    if "是否北交所" in selected.columns:
        selected = selected[~selected["是否北交所"].fillna(False).astype(bool)]
    return sorted(selected["判断日期"].dropna().unique().tolist())


def load_or_build_panel(
    conn: sqlite3.Connection,
    date: str,
    args,
    cache_dir: Path,
) -> pd.DataFrame:
    bj_part = "withbj" if args.include_bj else "nonbj"
    cache_path = cache_dir / (
        f"hc_scan_panel_{PANEL_CACHE_VERSION}_{date.replace('-', '')}_lb{args.lookback_days}"
        f"_min{args.min_history}_{bj_part}.pkl"
    )
    if args.cache and cache_path.exists():
        return pd.read_pickle(cache_path)
    panel = prepare_scan_frame(
        conn,
        date,
        args.lookback_days,
        args.min_history,
        args.include_bj,
    )
    if args.cache:
        panel.to_pickle(cache_path)
    return panel


def evaluate_date(conn: sqlite3.Connection, date: str, args, cache_dir: Path) -> tuple[dict, pd.DataFrame]:
    market_rows = int(pd.read_sql_query(
        "SELECT COUNT(DISTINCT code) AS rows FROM daily_prices WHERE trade_date = ?",
        conn,
        params=[date],
    )["rows"].iloc[0])
    selected = load_selected_for_date(Path(args.selected), date)
    panel = load_or_build_panel(conn, date, args, cache_dir)
    scan = build_signals(
        panel,
        args.mode,
        None,
        patterns_filter=split_filter(args.patterns),
        couplings_filter=split_filter(args.couplings),
        exclude_proxy=args.exclude_proxy,
        pattern_engine=args.pattern_engine,
    )
    scan, stats = compare_with_selected(scan, selected)
    missing = selected.copy()
    hit_codes = set(scan.loc[scan["命中入选股票"].astype(bool), "code"]) if not scan.empty else set()
    missing["扫描命中股票"] = missing["code"].isin(hit_codes)
    missing = missing[~missing["扫描命中股票"]].copy()

    summary = {
        "date": date,
        "market_rows": market_rows,
        "selected_rows": stats["selected_rows"],
        "selected_stocks": stats["selected_stocks"],
        "scan_rows": stats["scan_rows"],
        "scan_stocks": stats["scan_stocks"],
        "hit_selected_stocks": stats["hit_selected_stocks"],
        "hit_exact_signals": stats["hit_exact_signals"],
        "stock_recall": round(stats["stock_recall"], 4),
        "exact_signal_recall": round(stats["exact_signal_recall"], 4),
        "missing_stocks": stats["selected_stocks"] - stats["hit_selected_stocks"],
    }
    return summary, missing


def run(args) -> int:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = out_dir / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(args.db)

    if args.dates:
        dates = [pd.to_datetime(d.strip()).strftime("%Y-%m-%d") for d in args.dates.split(",") if d.strip()]
    else:
        dates = available_dates(Path(args.selected))

    summaries = []
    missing_frames = []
    for date in dates:
        print(f"scan {date} ...", flush=True)
        summary, missing = evaluate_date(conn, date, args, cache_dir)
        summaries.append(summary)
        if not missing.empty:
            missing["评估日期"] = date
            missing_frames.append(missing)
        print(
            f"{date} stock_recall={summary['stock_recall']:.2%} "
            f"exact_recall={summary['exact_signal_recall']:.2%} "
            f"scan_rows={summary['scan_rows']} missing={summary['missing_stocks']}",
            flush=True,
        )

    summary_df = pd.DataFrame(summaries)
    suffix = args.output_suffix or args.mode
    summary_path = out_dir / f"hc_scan_eval_summary_{suffix}.csv"
    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")

    missing_path = out_dir / f"hc_scan_eval_missing_{suffix}.csv"
    if missing_frames:
        pd.concat(missing_frames, ignore_index=True).to_csv(missing_path, index=False, encoding="utf-8-sig")
    else:
        pd.DataFrame().to_csv(missing_path, index=False, encoding="utf-8-sig")

    print()
    print(f"wrote {summary_path}")
    print(f"wrote {missing_path}")
    print(summary_df.to_string(index=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="批量评估扫描器样例日期召回率")
    parser.add_argument("--dates", help="逗号分隔日期；默认使用样例里的全部日期")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--selected", default=str(DEFAULT_SELECTED))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--output-suffix")
    parser.add_argument("--mode", choices=["loose", "strict"], default="loose")
    parser.add_argument("--lookback-days", type=int, default=420)
    parser.add_argument("--min-history", type=int, default=120)
    parser.add_argument("--patterns")
    parser.add_argument("--couplings")
    parser.add_argument("--exclude-proxy", action="store_true")
    parser.add_argument("--pattern-engine", choices=["recall", "talib_like"], default="recall")
    parser.add_argument("--include-bj", action="store_true")
    parser.add_argument("--cache", action=argparse.BooleanOptionalAction, default=True)
    return parser


def main() -> int:
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
