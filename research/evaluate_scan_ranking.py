"""
评估扫描候选池的股票级排序能力。

扫描器现在是召回优先，候选行很多。本脚本把候选聚合到股票级别，
评估 Top N 股票能覆盖多少样例入选股票。

用法：
  python evaluate_scan_ranking.py
  python evaluate_scan_ranking.py --dates 2026-06-17 --score ranked
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from evaluate_scanner_dates import available_dates, load_or_build_panel
from hc_strategy_scanner import (
    DEFAULT_DB,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SELECTED,
    build_signals,
    load_selected_for_date,
    split_filter,
)


MULTIPLIERS = (1, 1.5, 2, 3, 5, 10)
FIXED_TOPS = (50, 100, 200, 500, 1000, 2000)


def stock_rank_table(scan: pd.DataFrame, score_mode: str) -> pd.DataFrame:
    if scan.empty:
        return pd.DataFrame()

    scan = scan.copy()
    scan["is_proxy"] = scan["规则来源"].eq("proxy")
    agg = scan.groupby(["code", "secucode", "name"], as_index=False).agg(
        max_scan_score=("scan_score", "max"),
        signal_count=("scan_score", "size"),
        pattern_count=("形态名称", "nunique"),
        coupling_count=("coupling_family", "nunique"),
        price_signal_count=("is_proxy", lambda s: int((~s).sum())),
        proxy_signal_count=("is_proxy", lambda s: int(s.sum())),
        pct_change=("pct_change", "max"),
        turnover=("turnover", "max"),
        amount=("amount", "max"),
        best_pattern=("形态名称", "first"),
        best_coupling=("coupling_family", "first"),
    )

    if score_mode == "max":
        agg["rank_score"] = agg["max_scan_score"]
    elif score_mode == "count":
        agg["rank_score"] = agg["signal_count"]
    else:
        agg["rank_score"] = (
            agg["max_scan_score"]
            + np.log1p(agg["signal_count"]) * 2.5
            + np.log1p(agg["pattern_count"]) * 2.0
            + np.log1p(agg["coupling_count"]) * 1.5
            + agg["price_signal_count"].clip(0, 10) * 0.25
            + agg["pct_change"].fillna(0).clip(-5, 10) * 0.15
            + agg["turnover"].fillna(0).clip(0, 20) * 0.03
        )

    return agg.sort_values(
        ["rank_score", "max_scan_score", "pct_change", "signal_count"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)


def recall_at(ranked: pd.DataFrame, selected_codes: set[str], top_n: int) -> tuple[int, float]:
    if top_n <= 0 or ranked.empty or not selected_codes:
        return 0, 0.0
    picked = set(ranked.head(top_n)["code"].astype(str).str.zfill(6))
    hit = len(picked & selected_codes)
    return hit, hit / len(selected_codes)


def overlap_at(ranked: pd.DataFrame, selected_codes: set[str], top_n: int) -> tuple[int, float, float]:
    if top_n <= 0 or ranked.empty or not selected_codes:
        return 0, 0.0, 0.0
    actual_n = min(top_n, len(ranked))
    picked = set(ranked.head(actual_n)["code"].astype(str).str.zfill(6))
    hit = len(picked & selected_codes)
    overlap_rate = hit / actual_n if actual_n else 0.0
    coverage_rate = hit / len(selected_codes)
    return hit, overlap_rate, coverage_rate


def first_top_for_recall(ranked: pd.DataFrame, selected_codes: set[str], target: float) -> int | None:
    if ranked.empty or not selected_codes:
        return None
    seen: set[str] = set()
    for i, code in enumerate(ranked["code"].astype(str).str.zfill(6), 1):
        if code in selected_codes:
            seen.add(code)
            if len(seen) / len(selected_codes) >= target:
                return i
    return None


def evaluate_date(conn: sqlite3.Connection, date: str, args, cache_dir: Path) -> tuple[list[dict], pd.DataFrame]:
    selected = load_selected_for_date(Path(args.selected), date)
    selected_codes = set(selected["code"].astype(str).str.zfill(6))
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
    ranked = stock_rank_table(scan, args.score)
    ranked["date"] = date
    ranked["selected"] = ranked["code"].astype(str).str.zfill(6).isin(selected_codes)

    rows = []
    selected_count = len(selected_codes)
    for label, top_n in [
        ("0.5x", max(1, int(round(selected_count * 0.5)))),
        ("1x", selected_count),
        ("1.5x", max(1, int(round(selected_count * 1.5)))),
        ("2x", selected_count * 2),
    ]:
        hit, overlap_rate, coverage_rate = overlap_at(ranked, selected_codes, top_n)
        rows.append({
            "date": date,
            "kind": label,
            "top_n": top_n,
            "selected_stocks": selected_count,
            "scan_stocks": len(ranked),
            "hit": hit,
            "overlap_rate": round(overlap_rate, 4),
            "coverage_rate": round(coverage_rate, 4),
        })
    for top_n in (10, 20, 50, 100, 200, 500, 1000):
        hit, overlap_rate, coverage_rate = overlap_at(ranked, selected_codes, top_n)
        rows.append({
            "date": date,
            "kind": f"top{top_n}",
            "top_n": top_n,
            "selected_stocks": selected_count,
            "scan_stocks": len(ranked),
            "hit": hit,
            "overlap_rate": round(overlap_rate, 4),
            "coverage_rate": round(coverage_rate, 4),
        })

    # Keep recall-oriented rows for backwards comparison.
    for mult in MULTIPLIERS:
        top_n = max(1, int(round(selected_count * mult)))
        hit, recall = recall_at(ranked, selected_codes, top_n)
        rows.append({
            "date": date,
            "kind": f"recall_{mult:g}x",
            "top_n": top_n,
            "selected_stocks": selected_count,
            "scan_stocks": len(ranked),
            "hit": hit,
            "overlap_rate": round(hit / min(top_n, len(ranked)), 4) if len(ranked) else 0,
            "coverage_rate": round(recall, 4),
        })
    for top_n in FIXED_TOPS:
        hit, recall = recall_at(ranked, selected_codes, top_n)
        rows.append({
            "date": date,
            "kind": f"recall_top{top_n}",
            "top_n": top_n,
            "selected_stocks": selected_count,
            "scan_stocks": len(ranked),
            "hit": hit,
            "overlap_rate": round(hit / min(top_n, len(ranked)), 4) if len(ranked) else 0,
            "coverage_rate": round(recall, 4),
        })

    for target in (0.8, 0.9, 0.95):
        top_n = first_top_for_recall(ranked, selected_codes, target)
        rows.append({
            "date": date,
            "kind": f"need_{int(target * 100)}pct",
            "top_n": top_n,
            "selected_stocks": selected_count,
            "scan_stocks": len(ranked),
            "hit": None,
            "overlap_rate": None,
            "coverage_rate": target if top_n is not None else None,
        })
    return rows, ranked


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

    all_rows = []
    top_frames = []
    for date in dates:
        print(f"rank {date} ...", flush=True)
        rows, ranked = evaluate_date(conn, date, args, cache_dir)
        all_rows.extend(rows)
        top_frames.append(ranked.head(args.save_top).copy())
        need_90 = next((r["top_n"] for r in rows if r["kind"] == "need_90pct"), None)
        at_1x = next((r["overlap_rate"] for r in rows if r["kind"] == "1x"), None)
        at_half = next((r["overlap_rate"] for r in rows if r["kind"] == "0.5x"), None)
        print(f"{date} overlap_0.5x={at_half:.2%} overlap_1x={at_1x:.2%} need90cov={need_90}", flush=True)

    suffix = args.output_suffix or f"{args.mode}_{args.score}"
    summary = pd.DataFrame(all_rows)
    summary_path = out_dir / f"hc_scan_rank_eval_{suffix}.csv"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")

    top_path = out_dir / f"hc_scan_rank_top_{suffix}.csv"
    pd.concat(top_frames, ignore_index=True).to_csv(top_path, index=False, encoding="utf-8-sig")

    print()
    print(f"wrote {summary_path}")
    print(f"wrote {top_path}")
    pivot = summary[summary["kind"].isin(["0.5x", "1x", "1.5x", "2x", "top10", "top20", "top50", "need_90pct"])].copy()
    print(pivot.to_string(index=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="评估股票级 Top N 排名召回")
    parser.add_argument("--dates")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--selected", default=str(DEFAULT_SELECTED))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--output-suffix")
    parser.add_argument("--mode", choices=["loose", "strict"], default="loose")
    parser.add_argument("--score", choices=["ranked", "max", "count"], default="ranked")
    parser.add_argument("--lookback-days", type=int, default=420)
    parser.add_argument("--min-history", type=int, default=120)
    parser.add_argument("--patterns")
    parser.add_argument("--couplings")
    parser.add_argument("--exclude-proxy", action="store_true")
    parser.add_argument("--pattern-engine", choices=["recall", "talib_like"], default="recall")
    parser.add_argument("--include-bj", action="store_true")
    parser.add_argument("--cache", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--save-top", type=int, default=300)
    return parser


def main() -> int:
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
