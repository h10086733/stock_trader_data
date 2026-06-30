"""
给华创样例入选股票补充价格技术指标。

注意：核心指标函数（wma/hma/add_indicators/load_histories）已迁移至
trader.indicators.enrich，本文件从该模块 re-export 以保持向后兼容，
并保留研究用的 enrich_file/summarize/run 流程。

输出：
  outputs/hc_daily_long_all_indicators.csv
  outputs/hc_daily_long_100yi_indicators.csv
  outputs/hc_indicator_coupling_summary.csv
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from trader.indicators.enrich import (  # noqa: F401  (re-export for backward compat)
    DEFAULT_DB,
    DEFAULT_OUTPUT_DIR,
    add_indicators,
    hma,
    load_histories,
    wma,
)


BASE_DIR = Path(__file__).resolve().parent


def enrich_file(input_path: Path, output_path: Path, conn: sqlite3.Connection) -> pd.DataFrame:
    selected = pd.read_csv(input_path)
    selected["判断日期"] = pd.to_datetime(selected["判断日期"]).dt.strftime("%Y-%m-%d")
    selected["code"] = selected["资产代码"].astype(str).str.split(".").str[0].str.zfill(6)
    selected["coupling_family"] = selected["耦合条件"].astype(str).str.split("-").str[-1]
    if "是否北交所" in selected.columns:
        selected = selected[~selected["是否北交所"].fillna(False)].copy()
    else:
        selected = selected[
            ~selected["资产代码"].astype(str).str.endswith(".BJ")
            & ~selected["code"].astype(str).str.startswith(("8", "9"))
        ].copy()

    min_date = pd.to_datetime(selected["判断日期"].min()) - pd.Timedelta(days=420)
    max_date = selected["判断日期"].max()
    histories = load_histories(
        conn,
        sorted(selected["code"].unique().tolist()),
        min_date.strftime("%Y-%m-%d"),
        max_date,
    )
    if histories.empty:
        raise RuntimeError("没有读取到日K历史")

    enriched_hist = []
    for _, group in histories.groupby("code", sort=False):
        enriched_hist.append(add_indicators(group))
    ind = pd.concat(enriched_hist, ignore_index=True)
    ind = ind.rename(columns={"trade_date": "判断日期"})

    indicator_cols = [
        c for c in ind.columns
        if c not in {"open", "high", "low", "close", "volume", "amount", "pct_change", "turnover"}
    ]
    result = selected.merge(
        ind[indicator_cols],
        on=["code", "判断日期"],
        how="left",
    )
    result.to_csv(output_path, index=False, encoding="utf-8-sig")
    return result


def summarize(indicators: pd.DataFrame) -> pd.DataFrame:
    rows = []
    checks = {
        "MA5": ["close_above_ma5", "ma5_slope_pct"],
        "MA30": ["close_above_ma30", "ma30_slope_pct"],
        "MA60": ["close_above_ma60", "ma60_slope_pct"],
        "MACD": ["macd_gt0", "macd_dif_gt_dea", "macd_cross_up", "macd_rising", "macd_dif_gt0"],
        "HMA": [
            "close_above_hma20", "hma20_slope_pct",
            "close_above_hma30", "hma30_slope_pct",
            "close_above_hma60", "hma60_slope_pct",
        ],
    }
    for family, group in indicators.groupby("coupling_family"):
        item = {"coupling_family": family, "rows": len(group)}
        for col in checks.get(family, []):
            if col not in group.columns:
                continue
            s = group[col]
            if s.dtype == bool:
                item[f"{col}_true_rate"] = round(float(s.mean()), 4)
            else:
                item[f"{col}_gt0_rate"] = round(float((s > 0).mean()), 4)
                item[f"{col}_median"] = round(float(s.median()), 4)
        rows.append(item)
    return pd.DataFrame(rows).sort_values("rows", ascending=False)


def run(args) -> int:
    out_dir = Path(args.output_dir)
    conn = sqlite3.connect(args.db)
    all_df = enrich_file(
        out_dir / "hc_daily_long_all_enriched.csv",
        out_dir / "hc_daily_long_all_indicators.csv",
        conn,
    )
    cap_df = enrich_file(
        out_dir / "hc_daily_long_100yi_enriched.csv",
        out_dir / "hc_daily_long_100yi_indicators.csv",
        conn,
    )
    summary = summarize(all_df)
    summary.to_csv(out_dir / "hc_indicator_coupling_summary.csv", index=False, encoding="utf-8-sig")

    print(f"wrote {out_dir / 'hc_daily_long_all_indicators.csv'} {len(all_df)}")
    print(f"wrote {out_dir / 'hc_daily_long_100yi_indicators.csv'} {len(cap_df)}")
    print(f"wrote {out_dir / 'hc_indicator_coupling_summary.csv'}")
    print(summary.to_string(index=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="补充 MA/MACD/HMA 指标并按耦合条件汇总")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser


def main() -> int:
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
