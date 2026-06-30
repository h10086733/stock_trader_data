"""
给扫描候选补历史胜率/样本数过滤层。

这是对原 Excel 最关键过滤逻辑的近似：
  同一只股票 + 同一 K 线形态 + 同一耦合族
  在判断日前历史出现后的 forward return 胜率、样本数、盈亏比。

默认 forward return 使用次日收盘相对当日收盘。
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from trader.patterns.scanner import (
    DEFAULT_DB,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SELECTED,
    PANEL_CACHE_VERSION,
    add_shape_metrics,
    build_signals,
    detect_couplings,
    detect_patterns,
    pattern_fid,
    signal_coupling_override,
    signal_pattern_override,
    signal_win_return_threshold,
    load_selected_for_date,
    normalize_pattern,
    prepare_scan_frame,
    split_filter,
    start_date_by_trading_days,
    trueish,
)
from trader.indicators.enrich import add_indicators, load_histories


def selected_threshold(selected: pd.DataFrame) -> float:
    if selected.empty:
        return 0.60
    values = pd.to_numeric(selected["阈值"].astype(str).str.replace("%", "", regex=False), errors="coerce")
    values = values.dropna()
    if values.empty:
        return 0.60
    value = float(values.iloc[0])
    return value / 100 if value > 1 else value


def load_or_build_today_panel(conn: sqlite3.Connection, date: str, args, cache_dir: Path) -> pd.DataFrame:
    bj_part = "withbj" if args.include_bj else "nonbj"
    cache_path = cache_dir / (
        f"hc_scan_panel_{PANEL_CACHE_VERSION}_{date.replace('-', '')}_lb{args.lookback_days}"
        f"_min{args.min_history}_{bj_part}.pkl"
    )
    if args.cache and cache_path.exists():
        return pd.read_pickle(cache_path)
    panel = prepare_scan_frame(conn, date, args.lookback_days, args.min_history, args.include_bj)
    if args.cache:
        panel.to_pickle(cache_path)
    return panel


def build_signal_events(hist: pd.DataFrame, mode: str, date: str, pattern_engine: str = "recall") -> pd.DataFrame:
    rows = []
    pattern_masks = detect_patterns(hist, pattern_engine)
    coupling_masks = detect_couplings(hist, mode)
    base = hist[["code", "trade_date", "close"]].copy()
    base["next_close"] = hist.groupby("code")["close"].shift(-1)
    base["forward_return"] = base["next_close"] / base["close"] - 1
    valid_date = hist["trade_date"] < date

    for pattern, p_mask in pattern_masks.items():
        p_mask = trueish(p_mask)
        if not p_mask.any():
            continue
        for family, (c_mask, _rule, _source) in coupling_masks.items():
            fid = pattern_fid(pattern)
            pattern_override = signal_pattern_override(hist, fid, pattern, family)
            signal_p_mask = trueish(pattern_override[0]) if pattern_override is not None else p_mask
            override = signal_coupling_override(hist, fid, pattern, family)
            if override is not None:
                c_mask = override[0]
            mask = valid_date & signal_p_mask & trueish(c_mask) & base["forward_return"].notna()
            if not mask.any():
                continue
            part = base.loc[mask, ["code", "forward_return"]].copy()
            part["形态名称"] = pattern
            part["signal_fid"] = fid
            part["coupling_family"] = family
            part["耦合条件"] = part["signal_fid"] + "-" + part["coupling_family"]
            rows.append(part)
    if not rows:
        return pd.DataFrame(columns=["code", "形态名称", "signal_fid", "coupling_family", "耦合条件", "forward_return"])
    return pd.concat(rows, ignore_index=True)


def quality_from_events(events: pd.DataFrame, win_return_threshold: float = 0.0) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()
    events = events.copy()
    events["win_threshold"] = [
        signal_win_return_threshold(fid, pattern, family, win_return_threshold)
        for fid, pattern, family in zip(events["signal_fid"], events["形态名称"], events["coupling_family"])
    ]
    events["win"] = events["forward_return"] > events["win_threshold"]
    events["up_return"] = events["forward_return"].where(events["win"])
    events["down_return"] = events["forward_return"].where(~events["win"])
    quality = events.groupby(["code", "形态名称", "signal_fid", "coupling_family", "耦合条件"], as_index=False).agg(
        hist_samples=("forward_return", "size"),
        hist_win_rate=("win", "mean"),
        hist_up_avg=("up_return", "mean"),
        hist_down_avg=("down_return", "mean"),
    )
    quality["hist_pl_ratio"] = (
        quality["hist_up_avg"].abs()
        / quality["hist_down_avg"].abs().replace(0, np.nan)
    )
    return quality


def load_or_build_quality(conn: sqlite3.Connection, date: str, scan: pd.DataFrame, args, cache_dir: Path) -> pd.DataFrame:
    suffix = (
        f"{date.replace('-', '')}_{args.mode}_lb{args.quality_lookback_days}"
        f"_fwd{args.forward_days}_{args.outcome}_wr{args.win_return_threshold:g}"
    )
    quality_path = cache_dir / f"hc_signal_quality_{suffix}.pkl"
    if args.cache and quality_path.exists():
        return pd.read_pickle(quality_path)

    scan = scan.copy()
    scan["code"] = scan["code"].astype(str).str.zfill(6)
    if "signal_fid" not in scan.columns:
        scan["signal_fid"] = scan["形态名称"].map(pattern_fid)
    if "耦合条件" not in scan.columns:
        scan["耦合条件"] = scan["signal_fid"] + "-" + scan["coupling_family"]
    needed_pairs = (
        scan.groupby("code")[["signal_fid", "形态名称", "coupling_family", "耦合条件"]]
        .apply(lambda g: set(map(tuple, g.drop_duplicates().to_numpy())))
        .to_dict()
    )
    codes = sorted(needed_pairs)
    start = start_date_by_trading_days(conn, date, args.quality_lookback_days)
    end = date

    rows = []
    for chunk_start in range(0, len(codes), args.quality_chunk_size):
        chunk = codes[chunk_start:chunk_start + args.quality_chunk_size]
        histories = load_histories(conn, chunk, start, end)
        if histories.empty:
            continue
        for code, group in histories.groupby("code", sort=False):
            code = str(code).zfill(6)
            pairs = needed_pairs.get(code)
            if not pairs:
                continue
            hist = add_shape_metrics(add_indicators(group))
            hist = hist.sort_values("trade_date").copy()
            if args.outcome == "max_high":
                future_window = pd.concat(
                    [hist["high"].shift(-i) for i in range(1, args.forward_days + 1)],
                    axis=1,
                )
                future_price = future_window.max(axis=1)
                complete_forward_window = future_window.notna().all(axis=1)
            else:
                future_price = hist["close"].shift(-args.forward_days)
                complete_forward_window = future_price.notna()
            hist["future_price"] = future_price
            hist["forward_return"] = hist["future_price"] / hist["close"] - 1
            valid = (
                (hist["trade_date"] < date)
                & hist["forward_return"].notna()
                & complete_forward_window
            )
            if not valid.any():
                continue
            pattern_masks = detect_patterns(hist, getattr(args, "pattern_engine", "recall"))
            coupling_masks = detect_couplings(hist, args.mode)
            for fid, pattern, family, coupling in pairs:
                if pattern not in pattern_masks or family not in coupling_masks:
                    continue
                pattern_override = signal_pattern_override(hist, fid, pattern, family)
                pattern_mask = trueish(pattern_override[0]) if pattern_override is not None else trueish(pattern_masks[pattern])
                if getattr(args, "coupling_match_mode", "rule") == "label":
                    coupling_mask = pd.Series(True, index=hist.index)
                else:
                    override = signal_coupling_override(hist, fid, pattern, family)
                    coupling_mask = trueish(override[0] if override is not None else coupling_masks[family][0])
                mask = valid & pattern_mask & coupling_mask
                if not mask.any():
                    continue
                returns = hist.loc[mask, "forward_return"].astype(float)
                win_threshold = signal_win_return_threshold(
                    fid,
                    pattern,
                    family,
                    args.win_return_threshold,
                )
                wins = returns[returns > win_threshold]
                losses = returns[returns <= win_threshold]
                up_avg = float(wins.mean()) if len(wins) else np.nan
                down_avg = float(losses.mean()) if len(losses) else np.nan
                rows.append({
                    "code": code,
                    "形态名称": pattern,
                    "signal_fid": fid,
                    "coupling_family": family,
                    "耦合条件": coupling,
                    "hist_samples": int(len(returns)),
                    "hist_win_rate": float((returns > win_threshold).mean()),
                    "hist_up_avg": up_avg,
                    "hist_down_avg": down_avg,
                    "hist_pl_ratio": abs(up_avg) / abs(down_avg) if pd.notna(up_avg) and pd.notna(down_avg) and down_avg != 0 else np.nan,
                })
        print(
            f"  quality chunk {min(chunk_start + len(chunk), len(codes))}/{len(codes)}",
            flush=True,
        )

    quality = pd.DataFrame(rows)
    if args.cache:
        quality.to_pickle(quality_path)
    return quality


def compare_stock_recall(filtered: pd.DataFrame, selected: pd.DataFrame) -> dict:
    selected_codes = set(selected["code"].astype(str).str.zfill(6))
    filtered_codes = set(filtered["code"].astype(str).str.zfill(6)) if not filtered.empty else set()
    hit = len(selected_codes & filtered_codes)
    return {
        "selected_stocks": len(selected_codes),
        "scan_rows": len(filtered),
        "scan_stocks": len(filtered_codes),
        "hit_selected_stocks": hit,
        "stock_recall": hit / len(selected_codes) if selected_codes else 0,
    }


def run(args) -> int:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = out_dir / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(args.db)

    dates = [pd.to_datetime(d.strip()).strftime("%Y-%m-%d") for d in args.dates.split(",") if d.strip()]
    summaries = []
    for date in dates:
        print(f"quality {date} ...", flush=True)
        selected = load_selected_for_date(Path(args.selected), date)
        threshold = args.min_win_rate if args.min_win_rate is not None else selected_threshold(selected)
        panel = load_or_build_today_panel(conn, date, args, cache_dir)
        scan = build_signals(
            panel,
            args.mode,
            None,
            patterns_filter=split_filter(args.patterns),
            couplings_filter=split_filter(args.couplings),
            exclude_proxy=args.exclude_proxy,
            coupling_match_mode=args.coupling_match_mode,
        )
        quality = load_or_build_quality(conn, date, scan, args, cache_dir)
        enriched = scan.merge(
            quality,
            on=["code", "形态名称", "signal_fid", "coupling_family", "耦合条件"],
            how="left",
        )
        filtered = enriched[
            (enriched["hist_samples"] >= args.min_samples)
            & (enriched["hist_win_rate"] >= threshold)
        ].copy()
        if args.min_pl_ratio is not None:
            filtered = filtered[filtered["hist_pl_ratio"] >= args.min_pl_ratio].copy()

        stats = compare_stock_recall(filtered, selected)
        stats.update({
            "date": date,
            "threshold": threshold,
            "raw_scan_rows": len(scan),
            "raw_scan_stocks": scan["code"].nunique() if not scan.empty else 0,
            "quality_rows": len(quality),
            "min_samples": args.min_samples,
            "forward_days": args.forward_days,
            "outcome": args.outcome,
            "win_return_threshold": args.win_return_threshold,
        })
        summaries.append(stats)

        output = out_dir / f"hc_quality_scan_{date.replace('-', '')}_{args.mode}.csv"
        filtered.sort_values(
            ["hist_win_rate", "hist_samples", "scan_score"],
            ascending=[False, False, False],
        ).to_csv(output, index=False, encoding="utf-8-sig")
        print(
            f"{date} raw_stocks={stats['raw_scan_stocks']} "
            f"filtered_stocks={stats['scan_stocks']} "
            f"recall={stats['stock_recall']:.2%}",
            flush=True,
        )

    summary = pd.DataFrame(summaries)
    summary_path = out_dir / f"hc_quality_eval_{args.output_suffix}.csv"
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    print()
    print(f"wrote {summary_path}")
    print(summary.to_string(index=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="扫描候选的历史胜率过滤评估")
    parser.add_argument("--dates", default="2026-06-17")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--selected", default=str(DEFAULT_SELECTED))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--output-suffix", default="default")
    parser.add_argument("--mode", choices=["loose", "strict"], default="loose")
    parser.add_argument("--lookback-days", type=int, default=420)
    parser.add_argument("--quality-lookback-days", type=int, default=4200)
    parser.add_argument("--quality-chunk-size", type=int, default=200)
    parser.add_argument("--min-history", type=int, default=120)
    parser.add_argument("--min-samples", type=int, default=31)
    parser.add_argument("--min-win-rate", type=float)
    parser.add_argument("--min-pl-ratio", type=float)
    parser.add_argument("--forward-days", type=int, default=1)
    parser.add_argument("--outcome", choices=["close", "max_high"], default="close")
    parser.add_argument("--win-return-threshold", type=float, default=0.0)
    parser.add_argument("--patterns")
    parser.add_argument("--couplings")
    parser.add_argument("--coupling-match-mode", choices=["rule", "label"], default="rule")
    parser.add_argument("--pattern-engine", default="recall")
    parser.add_argument("--exclude-proxy", action="store_true")
    parser.add_argument("--include-bj", action="store_true")
    parser.add_argument("--cache", action=argparse.BooleanOptionalAction, default=True)
    return parser


def main() -> int:
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
