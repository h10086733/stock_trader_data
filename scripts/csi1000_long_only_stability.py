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
HS300_LONG_FILTERS = [-30, -20, -10, 0]
LONG_VOLUME_MULTIPLIERS = [1.0, 1.05, 1.1, 1.15]
LOW_DISTANCES = [0.02, 0.04, 0.06]
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
    ):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def run_one(arr: dict[str, np.ndarray], params: tuple[float, ...]) -> dict[str, float]:
    csi_long_threshold, hs300_long_filter, long_volume_multiplier, low_distance = params
    opens = arr["open"]
    closes = arr["close"]
    csi = arr["csi"]
    hs300 = arr["hs300"]
    vol = arr["vol"]
    low10 = arr["low10"]
    valid = arr["valid"]

    fee = FEE_BPS / 10000.0
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    position = 0
    pending = 0
    entry_price = 0.0
    entry_index = 0
    trade_returns: list[float] = []
    hold_days: list[int] = []
    long_days = 0
    flat_days = 0
    wave_active = False
    wave_blocked = False

    for i in range(len(opens) - 1):
        if pending != position:
            open_price = opens[i]
            if position == 1:
                ret = open_price / entry_price - 1.0
                trade_returns.append((ret - fee) * 100.0)
                hold_days.append(i - entry_index)
                equity *= 1.0 - fee
            position = pending
            if position == 1:
                equity *= 1.0 - fee
                entry_price = open_price
                entry_index = i

        if position == 1:
            interval_ret = opens[i + 1] / opens[i] - 1.0
            long_days += 1
        else:
            interval_ret = 0.0
            flat_days += 1
        equity *= 1.0 + interval_ret
        peak = max(peak, equity)
        max_dd = min(max_dd, equity / peak - 1.0)

        long_context = bool(
            valid[i]
            and csi[i] > csi_long_threshold
            and hs300[i] > hs300_long_filter
            and vol[i] > long_volume_multiplier
        )
        if wave_active and not long_context:
            wave_active = False
            wave_blocked = False

        if long_context:
            if not wave_active:
                wave_active = True
                wave_blocked = low10[i] < low_distance
            pending = 0 if wave_blocked else 1
        else:
            pending = 0

    if position == 1:
        ret = closes[-1] / entry_price - 1.0
        trade_returns.append((ret - fee) * 100.0)
        hold_days.append(len(opens) - 1 - entry_index)

    trade_arr = np.array(trade_returns, dtype=float)
    total_return = (equity - 1.0) * 100.0
    max_dd_pct = max_dd * 100.0
    return {
        "csi_long_threshold": csi_long_threshold,
        "hs300_long_filter": hs300_long_filter,
        "long_volume_multiplier": long_volume_multiplier,
        "low_distance": low_distance,
        "total_return_pct": total_return,
        "max_drawdown_pct": max_dd_pct,
        "return_to_dd": total_return / abs(max_dd_pct) if max_dd_pct < 0 else np.nan,
        "trade_count": len(trade_arr),
        "win_rate_pct": float((trade_arr > 0).mean() * 100.0) if len(trade_arr) else 0.0,
        "avg_trade_return_pct": float(trade_arr.mean()) if len(trade_arr) else 0.0,
        "median_trade_return_pct": float(np.median(trade_arr)) if len(trade_arr) else 0.0,
        "avg_hold_days": float(np.mean(hold_days)) if hold_days else 0.0,
        "long_days": long_days,
        "flat_days": flat_days,
    }


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
    }
    arr["valid"] = (
        np.isfinite(arr["csi"])
        & np.isfinite(arr["hs300"])
        & np.isfinite(arr["vol"])
        & np.isfinite(arr["low10"])
    )

    rows = [
        run_one(arr, params)
        for params in itertools.product(
            CSI_LONG_THRESHOLDS,
            HS300_LONG_FILTERS,
            LONG_VOLUME_MULTIPLIERS,
            LOW_DISTANCES,
        )
    ]
    results = pd.DataFrame(rows)
    grid_path = OUT_DIR / "csi1000_long_only_stability_grid.csv"
    results.to_csv(grid_path, index=False)

    summaries = []
    for col in ("csi_long_threshold", "hs300_long_filter", "long_volume_multiplier", "low_distance"):
        summary = (
            results.groupby(col)
            .agg(
                combos=("total_return_pct", "count"),
                median_return_pct=("total_return_pct", "median"),
                mean_return_pct=("total_return_pct", "mean"),
                median_max_dd_pct=("max_drawdown_pct", "median"),
                median_return_to_dd=("return_to_dd", "median"),
                median_win_rate_pct=("win_rate_pct", "median"),
                mean_win_rate_pct=("win_rate_pct", "mean"),
                median_trade_count=("trade_count", "median"),
            )
            .reset_index()
            .rename(columns={col: "value"})
        )
        summary.insert(0, "dimension", col)
        summaries.append(summary)
    summary_df = pd.concat(summaries, ignore_index=True)
    summary_path = OUT_DIR / "csi1000_long_only_stability_summary.csv"
    summary_df.to_csv(summary_path, index=False)

    print(f"rows={len(results)}")
    print(f"grid_csv={grid_path}")
    print(f"summary_csv={summary_path}")
    print("\nTop by return_to_dd:")
    print(results.sort_values(["return_to_dd", "total_return_pct"], ascending=False).head(20).to_string(index=False))
    print("\nLowest drawdown with return floors:")
    floors = []
    for floor in (100, 200, 300, 400, 500):
        sub = results[results.total_return_pct >= floor]
        if sub.empty:
            continue
        best = sub.sort_values(["max_drawdown_pct", "return_to_dd"], ascending=[False, False]).iloc[0]
        floors.append({"return_floor": floor, **best.to_dict(), "eligible": len(sub)})
    print(pd.DataFrame(floors).to_string(index=False))
    print("\nDimension summary:")
    print(summary_df.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
