"""
Compare lizi selected signals with the local high-confidence scanner.

This is a calibration tool, not production filtering. It answers two questions:
1. Can the scanner detect the exact code + fid + pattern + coupling signal?
2. For that exact signal, what local historical sample count / win rate do we get?
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

import app
from evaluate_historical_quality import load_or_build_quality
from hc_strategy_scanner import build_signals, prepare_scan_frame


def load_lizi(path: Path, sheet: str) -> pd.DataFrame:
    rows = pd.read_excel(path, sheet_name=sheet)
    rows["code"] = rows["资产代码"].astype(str).str.split(".").str[0].str.zfill(6)
    rows["pattern"] = rows["形态名称"].astype(str).str.replace("名称", "", regex=False)
    rows["signal_fid"] = rows["耦合条件"].astype(str).str.extract(r"(fid\d+)")[0]
    rows["coupling_family"] = rows["耦合条件"].astype(str).str.split("-").str[-1]
    return rows


def quality_args(lookback_days: int) -> SimpleNamespace:
    return SimpleNamespace(
        mode=app.HC_SCAN_MODE,
        quality_lookback_days=lookback_days,
        forward_days=app.HC_QUALITY_FORWARD_DAYS,
        outcome=app.HC_QUALITY_OUTCOME,
        win_return_threshold=app.HC_QUALITY_WIN_RETURN_THRESHOLD,
        quality_chunk_size=200,
        cache=False,
        coupling_match_mode=app.HC_QUALITY_COUPLING_MATCH_MODE,
    )


def run(args: argparse.Namespace) -> int:
    date = pd.to_datetime(args.date).strftime("%Y-%m-%d")
    lizi = load_lizi(Path(args.lizi), args.sheet)
    exact_keys = set(zip(
        lizi["code"],
        lizi["signal_fid"],
        lizi["pattern"],
        lizi["coupling_family"],
    ))

    conn = sqlite3.connect(args.db)
    panel = prepare_scan_frame(
        conn,
        date,
        app.HC_SCAN_LOOKBACK_DAYS,
        app.HC_SCAN_MIN_HISTORY,
        include_bj=False,
    )
    panel["code"] = panel["code"].astype(str).str.zfill(6)
    scan = build_signals(
        panel,
        app.HC_SCAN_MODE,
        top=0,
        patterns_filter=set(lizi["pattern"]),
        signal_combos_filter=app.HC_FOCUS_SIGNAL_COMBOS,
        coupling_match_mode=app.HC_SCAN_COUPLING_MATCH_MODE,
        pattern_engine=app.HC_SCAN_PATTERN_ENGINE,
    )
    scan["code"] = scan["code"].astype(str).str.zfill(6)
    scan = scan[[
        key in exact_keys
        for key in zip(scan["code"], scan["signal_fid"], scan["形态名称"], scan["coupling_family"])
    ]].copy()

    quality = load_or_build_quality(
        conn,
        date,
        scan,
        quality_args(args.quality_lookback_days),
        Path(args.output_dir) / "cache",
    )
    merged = scan.merge(
        quality,
        on=["code", "形态名称", "signal_fid", "coupling_family", "耦合条件"],
        how="left",
    )
    report = lizi.merge(
        merged[[
            "code",
            "形态名称",
            "signal_fid",
            "coupling_family",
            "hist_samples",
            "hist_win_rate",
            "hist_pl_ratio",
        ]],
        left_on=["code", "pattern", "signal_fid", "coupling_family"],
        right_on=["code", "形态名称", "signal_fid", "coupling_family"],
        how="left",
    )
    report["local_pass"] = (
        (report["hist_samples"] >= app.HC_QUALITY_MIN_SAMPLES)
        & (report["hist_win_rate"] >= app.HC_QUALITY_MIN_WIN_RATE)
    )
    report["sample_diff"] = report["hist_samples"] - report["形态总出现次数"]
    report["win_rate_diff"] = report["hist_win_rate"] - report["胜率"]

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"hc_lizi_exact_quality_{date.replace('-', '')}.csv"
    report.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"exact_scan_rows={len(scan)} lizi_rows={len(lizi)}")
    print(f"local_pass={int(report['local_pass'].fillna(False).sum())}/{len(report)}")
    print(f"wrote {out_path}")
    cols = [
        "code", "资产名称", "pattern", "耦合条件", "形态总出现次数", "胜率",
        "hist_samples", "hist_win_rate", "local_pass",
    ]
    print(report[cols].to_string(index=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Calibrate lizi exact signals against local quality stats")
    parser.add_argument("--date", default="2026-06-22")
    parser.add_argument("--lizi", default="lizi/【20260622】70%信号精选版(1).xlsx")
    parser.add_argument("--sheet", default="个股精选信号-看多-百亿以上")
    parser.add_argument("--db", default=app.DB_PATH)
    parser.add_argument("--output-dir", default="outputs")
    parser.add_argument("--quality-lookback-days", type=int, default=app.HC_QUALITY_LOOKBACK_DAYS)
    return parser


if __name__ == "__main__":
    raise SystemExit(run(build_parser().parse_args()))
