#!/usr/bin/env python3
"""
Research next-day limit-up / large-up probabilities for HC shape signals.

The script scans one target date with the existing high-confidence shape +
coupling engine, then looks back historically for the same stock + pattern +
coupling events and measures next-day outcomes:

  - next-day touch limit
  - next-day close limit
  - next-day high gain >= threshold, default 7%
  - next-day close gain >= threshold, default 7%

It is a research layer only. It does not place orders.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from hc_strategy_scanner import (
    FOCUS_LONG_100YI_SIGNAL_COMBOS,
    add_shape_metrics,
    build_signals,
    detect_couplings,
    detect_patterns,
    normalize_signal_combos,
    pattern_fid,
    prepare_scan_frame,
    signal_coupling_override,
    signal_pattern_override,
    start_date_by_trading_days,
    trueish,
)
from indicator_enrich import DEFAULT_DB, DEFAULT_OUTPUT_DIR, add_indicators, load_histories
from signal_tool import limit_rate_for_code


DEFAULT_SCAN_MODE = "loose"
DEFAULT_PATTERN_ENGINE = "lizi_relaxed"
DEFAULT_LOOKBACK_DAYS = 400
DEFAULT_MIN_HISTORY = 120
DEFAULT_HISTORY_DAYS = 1800


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="统计高置信形态信号的次日涨停/大涨历史概率"
    )
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite 数据库路径")
    parser.add_argument("--date", default=None, help="目标交易日 YYYY-MM-DD，默认最新完整日")
    parser.add_argument("--history-days", type=int, default=DEFAULT_HISTORY_DAYS)
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--min-history", type=int, default=DEFAULT_MIN_HISTORY)
    parser.add_argument("--mode", default=DEFAULT_SCAN_MODE, choices=["loose", "strict"])
    parser.add_argument("--pattern-engine", default=DEFAULT_PATTERN_ENGINE)
    parser.add_argument("--big-threshold", type=float, default=7.0, help="大涨阈值，百分比")
    parser.add_argument("--min-samples", type=int, default=20)
    parser.add_argument("--top", type=int, default=50, help="打印排名前 N 只")
    parser.add_argument(
        "--exclude-signal-limit",
        action="store_true",
        help="排除信号日已经封板涨停的股票",
    )
    parser.add_argument(
        "--continuation-only",
        action="store_true",
        help="只研究强势延续：排除长阴线和当日明显下跌候选",
    )
    parser.add_argument(
        "--min-signal-pct",
        type=float,
        default=-3.0,
        help="强势延续模式下，信号日最低涨跌幅",
    )
    parser.add_argument("--include-bj", action="store_true", help="包含北交所")
    parser.add_argument(
        "--include-risky",
        action="store_true",
        help="包含 ST、退市/退字样股票；默认排除",
    )
    parser.add_argument(
        "--all-combos",
        action="store_true",
        help="扫描所有图形+耦合组合；默认使用高置信生产组合",
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="输出目录",
    )
    return parser.parse_args()


def latest_complete_date(conn: sqlite3.Connection, min_rows: int = 4500) -> str:
    row = conn.execute(
        """
        SELECT trade_date
        FROM daily_prices
        GROUP BY trade_date
        HAVING COUNT(DISTINCT code) >= ?
        ORDER BY trade_date DESC
        LIMIT 1
        """,
        (min_rows,),
    ).fetchone()
    if not row:
        raise RuntimeError("没有找到完整交易日")
    return row["trade_date"]


def complete_trade_dates(conn: sqlite3.Connection, end_date: str, days: int, min_rows: int = 4500) -> list[str]:
    rows = conn.execute(
        """
        SELECT trade_date
        FROM daily_prices
        WHERE trade_date <= ?
        GROUP BY trade_date
        HAVING COUNT(DISTINCT code) >= ?
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        (end_date, min_rows, days),
    ).fetchall()
    return sorted(row["trade_date"] for row in rows)


def scan_target_signals(conn: sqlite3.Connection, args: argparse.Namespace, trade_date: str) -> pd.DataFrame:
    panel = prepare_scan_frame(
        conn,
        trade_date,
        args.lookback_days,
        args.min_history,
        args.include_bj,
    )
    signal_combos = None if args.all_combos else FOCUS_LONG_100YI_SIGNAL_COMBOS
    patterns = None
    if signal_combos:
        patterns = {pattern for _fid, pattern, _family in normalize_signal_combos(signal_combos)}
    scan = build_signals(
        panel,
        args.mode,
        top=0,
        patterns_filter=patterns,
        couplings_filter=None,
        exclude_proxy=False,
        pattern_engine=args.pattern_engine,
        signal_combos_filter=signal_combos,
        coupling_match_mode="rule",
    )
    if scan.empty:
        return scan
    scan["code"] = scan["code"].astype(str).str.zfill(6)
    scan = scan[
        scan["open"].notna()
        & scan["high"].notna()
        & scan["low"].notna()
        & scan["close"].notna()
        & (scan["open"] > 0)
        & (scan["high"] > 0)
        & (scan["low"] > 0)
        & (scan["close"] > 0)
    ].copy()
    if not args.include_risky:
        name = scan["name"].fillna("").astype(str)
        risky = name.str.upper().str.contains("ST", regex=False) | name.str.contains("退", regex=False)
        scan = scan[~risky].copy()
    scan["signal_variant"] = signal_variant(scan)
    if args.continuation_only:
        scan = scan[
            (scan["signal_variant"] != "长阴线")
            & (scan["pct_change"].fillna(-999) >= args.min_signal_pct)
        ].copy()
    return scan


def signal_variant(df: pd.DataFrame) -> pd.Series:
    pattern = df["形态名称"].astype(str)
    variant = pattern.copy()
    long_candle = pattern == "长蜡烛"
    if long_candle.any():
        is_up = df["close"] > df["open"]
        is_down = df["close"] < df["open"]
        variant.loc[long_candle & is_up] = "长阳线"
        variant.loc[long_candle & is_down] = "长阴线"
        variant.loc[long_candle & ~(is_up | is_down)] = "长十字实体"
    return variant


def apply_variant_mask(hist: pd.DataFrame, pattern: str, variant: str, mask: pd.Series) -> pd.Series:
    if pattern != "长蜡烛":
        return mask
    if variant == "长阳线":
        return mask & (hist["close"] > hist["open"])
    if variant == "长阴线":
        return mask & (hist["close"] < hist["open"])
    if variant == "长十字实体":
        return mask & (hist["close"] == hist["open"])
    return mask


def needed_pairs_by_code(scan: pd.DataFrame) -> dict[str, set[tuple[str, str, str, str, str]]]:
    scan = scan.copy()
    if "signal_fid" not in scan.columns:
        scan["signal_fid"] = scan["形态名称"].map(pattern_fid)
    if "耦合条件" not in scan.columns:
        scan["耦合条件"] = scan["signal_fid"] + "-" + scan["coupling_family"]
    if "signal_variant" not in scan.columns:
        scan["signal_variant"] = signal_variant(scan)
    pairs = (
        scan.groupby("code")[["signal_fid", "形态名称", "signal_variant", "coupling_family", "耦合条件"]]
        .apply(lambda g: set(map(tuple, g.drop_duplicates().to_numpy())))
        .to_dict()
    )
    return {str(code).zfill(6): value for code, value in pairs.items()}


def historical_surge_stats(
    conn: sqlite3.Connection,
    scan: pd.DataFrame,
    trade_date: str,
    args: argparse.Namespace,
) -> pd.DataFrame:
    if scan.empty:
        return pd.DataFrame()
    pairs_by_code = needed_pairs_by_code(scan)
    codes = sorted(pairs_by_code)
    start = start_date_by_trading_days(conn, trade_date, args.history_days)
    names = (
        scan[["code", "name"]]
        .drop_duplicates("code")
        .set_index("code")["name"]
        .to_dict()
    )
    big_threshold = args.big_threshold / 100.0
    rows = []
    for chunk_start in range(0, len(codes), 200):
        chunk = codes[chunk_start:chunk_start + 200]
        histories = load_histories(conn, chunk, start, trade_date)
        if histories.empty:
            continue
        histories["code"] = histories["code"].astype(str).str.zfill(6)
        for code, group in histories.groupby("code", sort=False):
            code = str(code).zfill(6)
            pairs = pairs_by_code.get(code)
            if not pairs:
                continue
            hist = add_shape_metrics(add_indicators(group)).sort_values("trade_date").copy()
            hist["next_high"] = hist["high"].shift(-1)
            hist["next_close"] = hist["close"].shift(-1)
            hist["next_open"] = hist["open"].shift(-1)
            hist["next_pct_change"] = hist["pct_change"].shift(-1)
            hist["next_high_gain"] = hist["next_high"] / hist["close"] - 1
            hist["next_close_gain"] = hist["next_close"] / hist["close"] - 1
            hist["next_open_gain"] = hist["next_open"] / hist["close"] - 1
            valid = (
                (hist["trade_date"] < trade_date)
                & hist["close"].notna()
                & (hist["close"] > 0)
                & hist["next_high"].notna()
                & hist["next_close"].notna()
                & (hist["next_high"] > 0)
                & (hist["next_close"] > 0)
                & hist["next_high_gain"].notna()
                & hist["next_close_gain"].notna()
                & np.isfinite(hist["next_high_gain"])
                & np.isfinite(hist["next_close_gain"])
            )
            if not valid.any():
                continue
            pattern_masks = detect_patterns(hist, args.pattern_engine)
            coupling_masks = detect_couplings(hist, args.mode)
            limit_pct = limit_rate_for_code(code, names.get(code, "")) * 100.0
            for fid, pattern, variant, family, coupling in pairs:
                if pattern not in pattern_masks or family not in coupling_masks:
                    continue
                pattern_override = signal_pattern_override(hist, fid, pattern, family)
                p_mask = trueish(pattern_override[0]) if pattern_override is not None else trueish(pattern_masks[pattern])
                p_mask = apply_variant_mask(hist, pattern, variant, p_mask)
                coupling_override = signal_coupling_override(hist, fid, pattern, family)
                c_mask = trueish(coupling_override[0] if coupling_override is not None else coupling_masks[family][0])
                mask = valid & p_mask & c_mask
                if not mask.any():
                    continue
                events = hist.loc[mask, [
                    "next_high_gain",
                    "next_close_gain",
                    "next_open_gain",
                    "next_pct_change",
                ]].astype(float)
                next_high_pct = events["next_high_gain"] * 100.0
                next_close_pct = events["next_close_gain"] * 100.0
                touch_limit = next_high_pct >= limit_pct - 0.5
                close_limit = events["next_pct_change"] >= limit_pct - 0.5
                high_big = events["next_high_gain"] >= big_threshold
                close_big = events["next_close_gain"] >= big_threshold
                samples = int(len(events))
                rows.append({
                    "code": code,
                    "形态名称": pattern,
                    "signal_variant": variant,
                    "signal_fid": fid,
                    "coupling_family": family,
                    "耦合条件": coupling,
                    "surge_samples": samples,
                    "next_touch_limit_rate": float(touch_limit.mean()),
                    "next_close_limit_rate": float(close_limit.mean()),
                    "next_high_ge_big_rate": float(high_big.mean()),
                    "next_close_ge_big_rate": float(close_big.mean()),
                    "avg_next_high_gain_pct": float(next_high_pct.mean()),
                    "median_next_high_gain_pct": float(next_high_pct.median()),
                    "avg_next_close_gain_pct": float(next_close_pct.mean()),
                    "avg_next_open_gain_pct": float(events["next_open_gain"].mean() * 100.0),
                    "max_next_high_gain_pct": float(next_high_pct.max()),
                })
        print(f"history chunk {min(chunk_start + 200, len(codes))}/{len(codes)}", flush=True)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def aggregate_target_rows(
    scan: pd.DataFrame,
    stats: pd.DataFrame,
    min_samples: int,
    exclude_signal_limit: bool = False,
) -> pd.DataFrame:
    if scan.empty or stats.empty:
        return pd.DataFrame()
    merged = scan.merge(
        stats,
        on=["code", "形态名称", "signal_variant", "signal_fid", "coupling_family", "耦合条件"],
        how="left",
    )
    merged = merged[merged["surge_samples"].fillna(0) >= min_samples].copy()
    if merged.empty:
        return merged
    merged["surge_score"] = (
        merged["next_high_ge_big_rate"].fillna(0) * 50
        + merged["next_touch_limit_rate"].fillna(0) * 35
        + merged["next_close_limit_rate"].fillna(0) * 20
        + np.log1p(merged["surge_samples"].fillna(0)) * 2
        + merged["avg_next_high_gain_pct"].fillna(0) * 0.3
        + merged["scan_score"].fillna(0) * 0.08
    )
    merged = merged.sort_values(
        [
            "surge_score",
            "next_touch_limit_rate",
            "next_high_ge_big_rate",
            "surge_samples",
            "scan_score",
        ],
        ascending=[False, False, False, False, False],
    )
    grouped = []
    for (code, name, secucode), group in merged.groupby(["code", "name", "secucode"], sort=False):
        best = group.iloc[0]
        limit_pct = limit_rate_for_code(str(code), str(name or "")) * 100.0
        prev_close = float(best["prev_close"]) if pd.notna(best.get("prev_close")) else np.nan
        high = float(best["high"]) if pd.notna(best.get("high")) else np.nan
        pct_change = float(best["pct_change"]) if pd.notna(best.get("pct_change")) else np.nan
        high_pct = (high / prev_close - 1.0) * 100.0 if prev_close and np.isfinite(prev_close) and prev_close > 0 else pct_change
        signal_touch_limit = bool(np.isfinite(high_pct) and high_pct >= limit_pct - 0.5)
        signal_close_limit = bool(np.isfinite(pct_change) and pct_change >= limit_pct - 0.5)
        if exclude_signal_limit and signal_close_limit:
            continue
        grouped.append({
            "date": best["trade_date"],
            "code": code,
            "name": name,
            "secucode": secucode,
            "pattern": best["signal_variant"],
            "raw_pattern": best["形态名称"],
            "coupling": best["coupling_family"],
            "signal_key": best["耦合条件"],
            "signal_count": int(len(group)),
            "close": best["close"],
            "pct_change": pct_change,
            "turnover": best.get("turnover"),
            "amount_yi": best.get("amount", 0) / 100000000 if pd.notna(best.get("amount")) else np.nan,
            "signal_touch_limit": signal_touch_limit,
            "signal_close_limit": signal_close_limit,
            "scan_score": best["scan_score"],
            "surge_score": best["surge_score"],
            "surge_samples": int(best["surge_samples"]),
            "next_touch_limit_rate": best["next_touch_limit_rate"],
            "next_close_limit_rate": best["next_close_limit_rate"],
            "next_high_ge_big_rate": best["next_high_ge_big_rate"],
            "next_close_ge_big_rate": best["next_close_ge_big_rate"],
            "avg_next_high_gain_pct": best["avg_next_high_gain_pct"],
            "median_next_high_gain_pct": best["median_next_high_gain_pct"],
            "avg_next_close_gain_pct": best["avg_next_close_gain_pct"],
            "avg_next_open_gain_pct": best["avg_next_open_gain_pct"],
            "max_next_high_gain_pct": best["max_next_high_gain_pct"],
            "patterns": ",".join(sorted(set(group["signal_variant"].astype(str)))),
            "couplings": ",".join(sorted(set(group["coupling_family"].astype(str)))),
        })
    if not grouped:
        return pd.DataFrame()
    return pd.DataFrame(grouped).sort_values("surge_score", ascending=False)


def combo_leaderboard(stats: pd.DataFrame, min_samples: int) -> pd.DataFrame:
    if stats.empty:
        return stats
    expanded = stats.copy()
    weights = expanded["surge_samples"].astype(float)
    grouped = expanded.groupby(["signal_variant", "形态名称", "signal_fid", "coupling_family", "耦合条件"], as_index=False).apply(
        lambda g: pd.Series({
            "combo_rows": len(g),
            "total_samples": int(g["surge_samples"].sum()),
            "next_touch_limit_rate": np.average(g["next_touch_limit_rate"], weights=g["surge_samples"]),
            "next_close_limit_rate": np.average(g["next_close_limit_rate"], weights=g["surge_samples"]),
            "next_high_ge_big_rate": np.average(g["next_high_ge_big_rate"], weights=g["surge_samples"]),
            "next_close_ge_big_rate": np.average(g["next_close_ge_big_rate"], weights=g["surge_samples"]),
            "avg_next_high_gain_pct": np.average(g["avg_next_high_gain_pct"], weights=g["surge_samples"]),
        }),
        include_groups=False,
    )
    grouped = grouped[grouped["total_samples"] >= min_samples].copy()
    if grouped.empty:
        return grouped
    grouped["combo_surge_score"] = (
        grouped["next_high_ge_big_rate"] * 50
        + grouped["next_touch_limit_rate"] * 35
        + grouped["next_close_limit_rate"] * 20
        + np.log1p(grouped["total_samples"]) * 1.5
        + grouped["avg_next_high_gain_pct"] * 0.3
    )
    return grouped.sort_values("combo_surge_score", ascending=False)


def fmt_pct(value) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value) * 100:.1f}%"


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        trade_date = args.date or latest_complete_date(conn)
        scan = scan_target_signals(conn, args, trade_date)
        if scan.empty:
            print(f"{trade_date} 没有扫描信号")
            return 1
        print(f"{trade_date} raw signals={len(scan)} stocks={scan['code'].nunique()}")
        stats = historical_surge_stats(conn, scan, trade_date, args)
    finally:
        conn.close()

    if stats.empty:
        print("没有历史统计结果")
        return 1

    candidates = aggregate_target_rows(
        scan,
        stats,
        args.min_samples,
        exclude_signal_limit=args.exclude_signal_limit,
    )
    combos = combo_leaderboard(stats, args.min_samples)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    date_part = trade_date.replace("-", "")
    candidates_path = out_dir / f"next_day_surge_candidates_{date_part}.csv"
    combo_path = out_dir / f"next_day_surge_combos_{date_part}.csv"
    stats_path = out_dir / f"next_day_surge_signal_stats_{date_part}.csv"
    candidates.to_csv(candidates_path, index=False)
    combos.to_csv(combo_path, index=False)
    stats.to_csv(stats_path, index=False)

    print(f"\n候选输出: {candidates_path}")
    print(f"组合排行: {combo_path}")
    print(f"信号统计: {stats_path}")
    print(f"\n候选股票数={len(candidates)} min_samples={args.min_samples} big_threshold={args.big_threshold:.1f}%")
    if not candidates.empty:
        cols = [
            "code", "name", "pattern", "coupling", "surge_samples",
            "next_touch_limit_rate", "next_high_ge_big_rate",
            "avg_next_high_gain_pct", "pct_change", "surge_score",
        ]
        print("\nTop candidates:")
        display = candidates[cols].head(args.top).copy()
        display["next_touch_limit_rate"] = display["next_touch_limit_rate"].map(fmt_pct)
        display["next_high_ge_big_rate"] = display["next_high_ge_big_rate"].map(fmt_pct)
        for _, row in display.iterrows():
            print(
                f"{row['code']} {row['name']} {row['pattern']}-{row['coupling']} "
                f"样本={int(row['surge_samples'])} "
                f"触板={row['next_touch_limit_rate']} "
                f"高点>={args.big_threshold:.1f}%={row['next_high_ge_big_rate']} "
                f"均高={row['avg_next_high_gain_pct']:.2f}% "
                f"当日涨跌={row['pct_change']:.2f}% "
                f"score={row['surge_score']:.2f}"
            )
    if not combos.empty:
        print("\nTop combos:")
        combo_display = combos.head(10)
        for _, row in combo_display.iterrows():
            print(
                f"{row['signal_variant']}-{row['coupling_family']} "
                f"样本={int(row['total_samples'])} "
                f"触板={fmt_pct(row['next_touch_limit_rate'])} "
                f"高点>={args.big_threshold:.1f}%={fmt_pct(row['next_high_ge_big_rate'])} "
                f"均高={row['avg_next_high_gain_pct']:.2f}%"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
