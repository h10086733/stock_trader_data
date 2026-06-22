from __future__ import annotations

import itertools
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))

import csi1000_timing as timing  # noqa: E402


DB_PATH = BASE_DIR / "stock_data.db"
EXCEL_PATH = BASE_DIR / "data" / "历史新高新低300和1000.xlsx"
OUT_DIR = BASE_DIR / "outputs"

CSI_LONG_THRESHOLDS = [10, 20, 30]
CSI_SHORT_THRESHOLDS = [10, 20, 30]
HS300_LONG_FILTERS = [-30, -20, -10, 0]
HS300_SHORT_FILTERS = [0, 10, 20, 30]
LONG_VOLUME_MULTIPLIERS = [1.0, 1.05, 1.1, 1.15]
SHORT_VOLUME_MULTIPLIERS = [1.0, 1.05, 1.1, 1.15]
LOW_DISTANCES = [0.02, 0.04, 0.06]
HIGH_DRAWDOWNS = [0.03, 0.05, 0.07]
FEE_BPS = 2.0


def load_frame() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    try:
        df = timing.load_db_price_excel_width_frame(conn, EXCEL_PATH)
    finally:
        conn.close()
    if df.empty:
        raise RuntimeError("没有可用的 excel_width 回测数据")
    df = df.sort_values("trade_date").reset_index(drop=True)
    for col in (
        "open",
        "close",
        "csi_score_ma3",
        "hs300_score_ma3",
        "vol_ratio_5_20",
        "price_from_low10",
        "drawdown_from_high10",
    ):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def run_one(arr: dict[str, np.ndarray], params: tuple[float, ...]) -> dict[str, float]:
    (
        csi_long_threshold,
        csi_short_threshold,
        hs300_long_filter,
        hs300_short_filter,
        long_volume_multiplier,
        short_volume_multiplier,
        low_distance,
        high_drawdown,
    ) = params

    opens = arr["open"]
    closes = arr["close"]
    csi = arr["csi"]
    hs300 = arr["hs300"]
    vol = arr["vol"]
    low10 = arr["low10"]
    dd10 = arr["dd10"]
    valid = arr["valid"]

    fee = FEE_BPS / 10000.0
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    position = 0  # 0 flat, 1 long, -1 short
    pending = 0
    current_entry_price = 0.0
    current_direction = 0
    current_entry_index = 0

    trade_returns: list[float] = []
    long_returns: list[float] = []
    short_returns: list[float] = []
    long_days = 0
    short_days = 0
    flat_days = 0

    long_wave_active = False
    long_wave_blocked = False
    short_wave_active = False
    short_wave_target = 0

    n = len(opens)
    for i in range(n - 1):
        if pending != position:
            open_price = opens[i]
            if current_direction != 0:
                if current_direction == 1:
                    ret = open_price / current_entry_price - 1.0
                else:
                    ret = current_entry_price / open_price - 1.0
                trade_ret = (ret - fee) * 100.0
                trade_returns.append(trade_ret)
                if current_direction == 1:
                    long_returns.append(trade_ret)
                else:
                    short_returns.append(trade_ret)
                equity *= 1.0 - fee
                current_direction = 0

            position = pending
            if position != 0:
                equity *= 1.0 - fee
                current_direction = position
                current_entry_price = open_price
                current_entry_index = i

        if position == 1:
            interval_ret = opens[i + 1] / opens[i] - 1.0
            long_days += 1
        elif position == -1:
            interval_ret = opens[i] / opens[i + 1] - 1.0
            short_days += 1
        else:
            interval_ret = 0.0
            flat_days += 1
        equity *= 1.0 + interval_ret
        if equity > peak:
            peak = equity
        dd = equity / peak - 1.0
        if dd < max_dd:
            max_dd = dd

        if valid[i]:
            long_context = (
                csi[i] > csi_long_threshold
                and hs300[i] > hs300_long_filter
                and vol[i] > long_volume_multiplier
            )
            short_context = (
                csi[i] < -csi_short_threshold
                and hs300[i] < hs300_short_filter
                and vol[i] < short_volume_multiplier
            )
        else:
            long_context = False
            short_context = False

        if long_wave_active and not long_context:
            long_wave_active = False
            long_wave_blocked = False
        if short_wave_active and not short_context:
            short_wave_active = False
            short_wave_target = 0

        if long_context:
            if not long_wave_active:
                long_wave_active = True
                long_wave_blocked = low10[i] < low_distance
            short_wave_active = False
            short_wave_target = 0
            pending = 0 if long_wave_blocked else 1
        elif short_context:
            if not short_wave_active:
                short_wave_active = True
                short_wave_target = -1 if dd10[i] <= high_drawdown else 1
            long_wave_active = False
            long_wave_blocked = False
            pending = short_wave_target
        else:
            pending = 0

    if current_direction != 0:
        if current_direction == 1:
            ret = closes[-1] / current_entry_price - 1.0
        else:
            ret = current_entry_price / closes[-1] - 1.0
        trade_ret = (ret - fee) * 100.0
        trade_returns.append(trade_ret)
        if current_direction == 1:
            long_returns.append(trade_ret)
        else:
            short_returns.append(trade_ret)

    trade_arr = np.array(trade_returns, dtype=float)
    long_arr = np.array(long_returns, dtype=float)
    short_arr = np.array(short_returns, dtype=float)
    total_return = (equity - 1.0) * 100.0
    max_dd_pct = max_dd * 100.0
    return {
        "csi_long_threshold": csi_long_threshold,
        "csi_short_threshold": csi_short_threshold,
        "hs300_long_filter": hs300_long_filter,
        "hs300_short_filter": hs300_short_filter,
        "csi_long_ratio": csi_long_threshold / 1000.0,
        "csi_short_ratio": -csi_short_threshold / 1000.0,
        "hs300_long_ratio": hs300_long_filter / 300.0,
        "hs300_short_ratio": hs300_short_filter / 300.0,
        "long_volume_multiplier": long_volume_multiplier,
        "short_volume_multiplier": short_volume_multiplier,
        "low_distance": low_distance,
        "high_drawdown": high_drawdown,
        "total_return_pct": total_return,
        "max_drawdown_pct": max_dd_pct,
        "return_to_dd": total_return / abs(max_dd_pct) if max_dd_pct < 0 else np.nan,
        "trade_count": len(trade_arr),
        "win_rate_pct": float((trade_arr > 0).mean() * 100.0) if len(trade_arr) else 0.0,
        "avg_trade_return_pct": float(trade_arr.mean()) if len(trade_arr) else 0.0,
        "median_trade_return_pct": float(np.median(trade_arr)) if len(trade_arr) else 0.0,
        "long_trade_count": len(long_arr),
        "long_avg_return_pct": float(long_arr.mean()) if len(long_arr) else 0.0,
        "long_win_rate_pct": float((long_arr > 0).mean() * 100.0) if len(long_arr) else 0.0,
        "short_trade_count": len(short_arr),
        "short_avg_return_pct": float(short_arr.mean()) if len(short_arr) else 0.0,
        "short_win_rate_pct": float((short_arr > 0).mean() * 100.0) if len(short_arr) else 0.0,
        "long_days": long_days,
        "short_days": short_days,
        "flat_days": flat_days,
    }


def summarize_dimension(results: pd.DataFrame, column: str) -> pd.DataFrame:
    return (
        results.groupby(column)
        .agg(
            combos=("total_return_pct", "count"),
            median_return_pct=("total_return_pct", "median"),
            mean_return_pct=("total_return_pct", "mean"),
            median_max_dd_pct=("max_drawdown_pct", "median"),
            median_return_to_dd=("return_to_dd", "median"),
            profitable_pct=("total_return_pct", lambda s: (s > 0).mean() * 100),
            dd_under_20_pct=("max_drawdown_pct", lambda s: (s >= -20).mean() * 100),
            median_trade_count=("trade_count", "median"),
        )
        .reset_index()
    )


def main() -> int:
    OUT_DIR.mkdir(exist_ok=True)
    df = load_frame()
    arr = {
        "open": df["open"].to_numpy(dtype=float),
        "close": df["close"].to_numpy(dtype=float),
        "csi": df["csi_score_ma3"].to_numpy(dtype=float),
        "hs300": df["hs300_score_ma3"].to_numpy(dtype=float),
        "vol": df["vol_ratio_5_20"].to_numpy(dtype=float),
        "low10": df["price_from_low10"].to_numpy(dtype=float),
        "dd10": df["drawdown_from_high10"].to_numpy(dtype=float),
    }
    arr["valid"] = (
        np.isfinite(arr["csi"])
        & np.isfinite(arr["hs300"])
        & np.isfinite(arr["vol"])
        & np.isfinite(arr["low10"])
        & np.isfinite(arr["dd10"])
    )

    rows = []
    params_iter = itertools.product(
        CSI_LONG_THRESHOLDS,
        CSI_SHORT_THRESHOLDS,
        HS300_LONG_FILTERS,
        HS300_SHORT_FILTERS,
        LONG_VOLUME_MULTIPLIERS,
        SHORT_VOLUME_MULTIPLIERS,
        LOW_DISTANCES,
        HIGH_DRAWDOWNS,
    )
    for params in params_iter:
        rows.append(run_one(arr, params))

    results = pd.DataFrame(rows)
    grid_path = OUT_DIR / "csi1000_split_threshold_stability_grid.csv"
    results.to_csv(grid_path, index=False)

    summaries = []
    for column in (
        "csi_long_threshold",
        "csi_short_threshold",
        "hs300_long_filter",
        "hs300_short_filter",
        "long_volume_multiplier",
        "short_volume_multiplier",
        "low_distance",
        "high_drawdown",
    ):
        summary = summarize_dimension(results, column)
        summary.insert(0, "dimension", column)
        summaries.append(summary.rename(columns={column: "value"}))
    summary_df = pd.concat(summaries, ignore_index=True)
    summary_path = OUT_DIR / "csi1000_split_threshold_stability_summary.csv"
    summary_df.to_csv(summary_path, index=False)

    print(f"rows={len(results)}")
    print(f"grid_csv={grid_path}")
    print(f"summary_csv={summary_path}")
    print("\nTop 20 by return_to_dd:")
    print(
        results.sort_values(["return_to_dd", "total_return_pct"], ascending=False)
        .head(20)
        .to_string(index=False)
    )
    print("\nDimension summary:")
    print(summary_df.to_string(index=False))

    baseline = results[
        (results["csi_long_threshold"] == 20)
        & (results["csi_short_threshold"] == 20)
        & (results["hs300_long_filter"] == -10)
        & (results["hs300_short_filter"] == 10)
        & (results["long_volume_multiplier"] == 1.10)
        & (results["short_volume_multiplier"] == 1.05)
        & (results["low_distance"] == 0.04)
        & (results["high_drawdown"] == 0.05)
    ]
    if not baseline.empty:
        row = baseline.iloc[0]
        print("\nBaseline:")
        print(row.to_string())
        print(
            "baseline_return_rank=",
            int(results["total_return_pct"].rank(ascending=False, method="min")[baseline.index[0]]),
            "baseline_rdd_rank=",
            int(results["return_to_dd"].rank(ascending=False, method="min")[baseline.index[0]]),
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
