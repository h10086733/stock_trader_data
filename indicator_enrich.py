"""
给华创样例入选股票补充价格技术指标。

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


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB = BASE_DIR / "stock_data.db"
DEFAULT_OUTPUT_DIR = BASE_DIR / "outputs"


def wma(values: pd.Series, period: int) -> pd.Series:
    arr = values.to_numpy(dtype="float64")
    result = np.full(len(arr), np.nan)
    if len(arr) < period:
        return pd.Series(result, index=values.index)
    weights = np.arange(1, period + 1, dtype="float64")
    windows = np.lib.stride_tricks.sliding_window_view(arr, period)
    result[period - 1:] = windows @ (weights / weights.sum())
    return pd.Series(result, index=values.index)


def hma(values: pd.Series, period: int) -> pd.Series:
    half = max(1, period // 2)
    root = max(1, int(period ** 0.5))
    raw = 2 * wma(values, half) - wma(values, period)
    return wma(raw, root)


def add_indicators(hist: pd.DataFrame) -> pd.DataFrame:
    hist = hist.sort_values("trade_date").copy()
    close = hist["close"]
    high = hist["high"]
    low = hist["low"]
    volume = hist["volume"]

    for period in (5, 10, 20, 30, 60):
        ma = close.rolling(period).mean()
        hist[f"ma{period}"] = ma
        hist[f"ma{period}_slope_pct"] = (ma / ma.shift(1) - 1) * 100
        hist[f"close_ma{period}_dist_pct"] = (close / ma - 1) * 100
        hist[f"close_above_ma{period}"] = close >= ma

    for period in (20, 30, 60):
        line = hma(close, period)
        hist[f"hma{period}"] = line
        hist[f"hma{period}_slope_pct"] = (line / line.shift(1) - 1) * 100
        hist[f"close_hma{period}_dist_pct"] = (close / line - 1) * 100
        hist[f"close_above_hma{period}"] = close >= line

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    macd = 2 * (dif - dea)
    hist["macd_dif"] = dif
    hist["macd_dea"] = dea
    hist["macd"] = macd
    hist["macd_gt0"] = macd > 0
    hist["macd_dif_gt_dea"] = dif > dea
    hist["macd_cross_up"] = (dif > dea) & (dif.shift(1) <= dea.shift(1))
    hist["macd_cross_down"] = (dif < dea) & (dif.shift(1) >= dea.shift(1))
    hist["macd_rising"] = macd > macd.shift(1)
    hist["macd_dif_gt0"] = dif > 0
    hist["macd_dea_gt0"] = dea > 0

    hist["amount_ma20"] = hist["amount"].rolling(20).mean()
    hist["amount_ratio20"] = hist["amount"] / hist["amount_ma20"]
    hist["volume_ma20"] = volume.rolling(20).mean()
    hist["volume_ratio20"] = volume / hist["volume_ma20"]
    hist["close_position_day_pct"] = (close - low) / (high - low).replace(0, pd.NA) * 100
    return hist


def load_histories(conn: sqlite3.Connection, codes: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    frames = []
    for chunk_start in range(0, len(codes), 500):
        chunk = codes[chunk_start:chunk_start + 500]
        placeholders = ",".join("?" for _ in chunk)
        frame = pd.read_sql_query(
            f"""
            SELECT code, trade_date, open, high, low, close, volume, amount,
                   pct_change, turnover
            FROM daily_prices
            WHERE code IN ({placeholders})
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY code, trade_date
            """,
            conn,
            params=chunk + [start_date, end_date],
        )
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


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
