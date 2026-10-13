"""
对照分析华创样例耦合条件与常见技术指标候选规则。

默认只使用当日全市场日 K 覆盖较完整的样例日期，避免用补齐过的局部行情
作为全市场基准。

输出：
  outputs/hc_indicator_candidate_compare.csv
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import pandas as pd

from indicator_enrich import DEFAULT_DB, DEFAULT_OUTPUT_DIR, add_indicators, load_histories


DEFAULT_SELECTED = DEFAULT_OUTPUT_DIR / "hc_daily_long_all_indicators.csv"


def is_bj_code(code: str) -> bool:
    text = str(code).zfill(6)
    return text.startswith(("8", "9"))


def load_universe(
    conn: sqlite3.Connection,
    selected_dates: list[str],
    min_market_rows: int,
    lookback_days: int,
) -> tuple[pd.DataFrame, list[str]]:
    counts = pd.read_sql_query(
        """
        SELECT trade_date, COUNT(DISTINCT code) AS rows
        FROM daily_prices
        WHERE trade_date IN ({})
        GROUP BY trade_date
        """.format(",".join("?" for _ in selected_dates)),
        conn,
        params=selected_dates,
    )
    complete_dates = sorted(
        counts.loc[counts["rows"] >= min_market_rows, "trade_date"].astype(str).tolist()
    )
    if not complete_dates:
        raise RuntimeError(f"没有日K覆盖 >= {min_market_rows} 只的样例日期")

    stocks = pd.read_sql_query("SELECT code, secucode FROM stocks", conn)
    stocks["code"] = stocks["code"].astype(str).str.zfill(6)
    stocks["secucode"] = stocks["secucode"].fillna("").astype(str).str.upper()
    stocks = stocks[
        ~stocks["code"].map(is_bj_code)
        & ~stocks["secucode"].str.endswith(".BJ")
    ].copy()

    start = (pd.to_datetime(min(complete_dates)) - pd.Timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end = max(complete_dates)
    histories = load_histories(conn, stocks["code"].tolist(), start, end)
    frames = [add_indicators(group) for _, group in histories.groupby("code", sort=False)]
    universe = pd.concat(frames, ignore_index=True)
    universe = universe[universe["trade_date"].isin(complete_dates)].copy()
    return universe, complete_dates


def add_candidate_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for period in (5, 10, 20, 30, 60):
        dist = out[f"close_ma{period}_dist_pct"]
        slope = out[f"ma{period}_slope_pct"]
        out[f"close_above_ma{period}"] = dist >= 0
        out[f"close_below_ma{period}"] = dist < 0
        out[f"close_ma{period}_below_3pct"] = dist <= -3
        out[f"close_ma{period}_below_5pct"] = dist <= -5
        out[f"close_ma{period}_below_10pct"] = dist <= -10
        out[f"close_ma{period}_near_1pct"] = dist.abs() <= 1
        out[f"close_ma{period}_near_3pct"] = dist.abs() <= 3
        out[f"ma{period}_slope_gt0"] = slope > 0
        out[f"ma{period}_slope_lt0"] = slope < 0

    for period in (20, 30, 60):
        dist = out[f"close_hma{period}_dist_pct"]
        slope = out[f"hma{period}_slope_pct"]
        out[f"close_above_hma{period}"] = dist >= 0
        out[f"close_below_hma{period}"] = dist < 0
        out[f"close_hma{period}_near_1pct"] = dist.abs() <= 1
        out[f"close_hma{period}_near_3pct"] = dist.abs() <= 3
        out[f"hma{period}_slope_gt0"] = slope > 0
        out[f"hma{period}_slope_lt0"] = slope < 0

    out["macd_gt0"] = out["macd"] > 0
    out["macd_lt0"] = out["macd"] < 0
    out["macd_dif_gt_dea"] = out["macd_dif"] > out["macd_dea"]
    out["macd_dif_lt_dea"] = out["macd_dif"] < out["macd_dea"]
    out["macd_dif_gt0"] = out["macd_dif"] > 0
    out["macd_dif_lt0"] = out["macd_dif"] < 0
    out["macd_dea_gt0"] = out["macd_dea"] > 0
    out["macd_dea_lt0"] = out["macd_dea"] < 0
    return out


def candidates_for_family(family: str) -> list[str]:
    if family in {"MA5", "MA30", "MA60"}:
        period = family[2:]
        return [
            f"close_above_ma{period}",
            f"close_below_ma{period}",
            f"close_ma{period}_below_3pct",
            f"close_ma{period}_below_5pct",
            f"close_ma{period}_below_10pct",
            f"close_ma{period}_near_1pct",
            f"close_ma{period}_near_3pct",
            f"ma{period}_slope_gt0",
            f"ma{period}_slope_lt0",
        ]
    if family == "MACD":
        return [
            "macd_gt0",
            "macd_lt0",
            "macd_dif_gt_dea",
            "macd_dif_lt_dea",
            "macd_cross_up",
            "macd_cross_down",
            "macd_rising",
            "macd_dif_gt0",
            "macd_dif_lt0",
            "macd_dea_gt0",
            "macd_dea_lt0",
        ]
    if family == "HMA":
        cols = []
        for period in (20, 30, 60):
            cols.extend([
                f"close_above_hma{period}",
                f"close_below_hma{period}",
                f"close_hma{period}_near_1pct",
                f"close_hma{period}_near_3pct",
                f"hma{period}_slope_gt0",
                f"hma{period}_slope_lt0",
            ])
        return cols

    cols = ["macd_rising", "macd_gt0", "macd_lt0"]
    for period in (5, 10, 20, 30, 60):
        cols.extend([f"close_above_ma{period}", f"close_below_ma{period}"])
    return cols


def compare(selected: pd.DataFrame, universe: pd.DataFrame, complete_dates: list[str]) -> pd.DataFrame:
    selected = selected[selected["判断日期"].isin(complete_dates)].copy()
    selected["trade_date"] = selected["判断日期"]
    selected = add_candidate_columns(selected)
    universe = add_candidate_columns(universe)

    rows = []
    for family, group in selected.groupby("coupling_family"):
        universe_same_dates = universe[universe["trade_date"].isin(group["trade_date"].unique())]
        for col in candidates_for_family(family):
            if col not in group.columns or col not in universe_same_dates.columns:
                continue
            selected_rate = float(group[col].fillna(False).mean())
            universe_rate = float(universe_same_dates[col].fillna(False).mean())
            lift = selected_rate / universe_rate if universe_rate else None
            rows.append({
                "coupling_family": family,
                "candidate": col,
                "selected_rows": len(group),
                "selected_true_rate": round(selected_rate, 4),
                "universe_rows": len(universe_same_dates),
                "universe_true_rate": round(universe_rate, 4),
                "lift_vs_universe": round(lift, 4) if lift is not None else None,
            })
    result = pd.DataFrame(rows)
    result["score"] = result["selected_true_rate"] * result["lift_vs_universe"].fillna(0)
    return result.sort_values(
        ["coupling_family", "score", "selected_true_rate"],
        ascending=[True, False, False],
    ).drop(columns=["score"])


def run(args) -> int:
    selected = pd.read_csv(args.selected)
    selected["判断日期"] = pd.to_datetime(selected["判断日期"]).dt.strftime("%Y-%m-%d")
    selected_dates = sorted(selected["判断日期"].dropna().unique().tolist())

    conn = sqlite3.connect(args.db)
    universe, complete_dates = load_universe(
        conn,
        selected_dates,
        args.min_market_rows,
        args.lookback_days,
    )
    result = compare(selected, universe, complete_dates)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output, index=False, encoding="utf-8-sig")

    print(f"used dates: {', '.join(complete_dates)}")
    print(f"wrote {output} {len(result)}")
    for family, group in result.groupby("coupling_family", sort=False):
        print()
        print(family)
        print(group.head(args.top).to_string(index=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="对照入选样本与同日期全市场的指标候选条件")
    parser.add_argument("--selected", default=str(DEFAULT_SELECTED))
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_DIR / "hc_indicator_candidate_compare.csv"))
    parser.add_argument("--min-market-rows", type=int, default=5000)
    parser.add_argument("--lookback-days", type=int, default=420)
    parser.add_argument("--top", type=int, default=5)
    return parser


def main() -> int:
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
