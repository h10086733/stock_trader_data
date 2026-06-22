from __future__ import annotations

import itertools
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

import sys

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))

import csi1000_timing as timing  # noqa: E402


DB_PATH = BASE_DIR / "stock_data.db"
EXCEL_PATH = BASE_DIR / "data" / "历史新高新低300和1000.xlsx"
OUT_DIR = BASE_DIR / "outputs"


WIDTH_THRESHOLDS = [10, 20, 30]
HS300_FILTERS = [-20, -10, 0]
LONG_VOLUME_MULTIPLIERS = [1.0, 1.05, 1.1, 1.15]
SHORT_VOLUME_MULTIPLIERS = [1.0, 1.05, 1.1, 1.15]
LOW_DISTANCES = [0.02, 0.04, 0.06]
HIGH_DRAWDOWNS = [0.03, 0.05, 0.07]
FEE_BPS = 2.0


def finite(*values: float) -> bool:
    return all(value is not None and not pd.isna(value) and np.isfinite(float(value)) for value in values)


def run_backtest(
    df: pd.DataFrame,
    width_threshold: float,
    hs300_filter: float,
    long_volume_multiplier: float,
    short_volume_multiplier: float,
    low_distance: float,
    high_drawdown: float,
) -> dict:
    fee = FEE_BPS / 10000
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    position = "FLAT"
    pending_target = "FLAT"
    current_trade = None
    trades = []
    daily_positions = []

    long_wave_active = False
    long_wave_blocked = False
    short_wave_active = False
    short_wave_target = None

    for i in range(len(df) - 1):
        row = df.iloc[i]

        if pending_target != position:
            open_price = float(row.open)
            if current_trade:
                ret = (
                    open_price / current_trade["entry_price"] - 1
                    if current_trade["direction"] == "LONG"
                    else current_trade["entry_price"] / open_price - 1
                )
                current_trade["return_pct"] = (ret - fee) * 100
                current_trade["hold_days"] = len(daily_positions) - current_trade["entry_index"]
                trades.append(current_trade)
                equity *= 1 - fee
                current_trade = None

            position = pending_target
            if position in ("LONG", "SHORT"):
                equity *= 1 - fee
                current_trade = {
                    "direction": position,
                    "entry_price": open_price,
                    "entry_index": len(daily_positions),
                }

        today_open = float(row.open)
        next_open = float(df.iloc[i + 1].open)
        if position == "LONG":
            interval_ret = next_open / today_open - 1
        elif position == "SHORT":
            interval_ret = today_open / next_open - 1
        else:
            interval_ret = 0.0
        equity *= 1 + interval_ret
        peak = max(peak, equity)
        max_dd = min(max_dd, equity / peak - 1)

        if finite(
            row.csi_score_ma3,
            row.hs300_score_ma3,
            row.vol_ratio_5_20,
            row.price_from_low10,
            row.drawdown_from_high10,
        ):
            long_context = (
                row.csi_score_ma3 > width_threshold
                and row.hs300_score_ma3 > hs300_filter
                and row.vol_ratio_5_20 > long_volume_multiplier
            )
            short_context = (
                row.csi_score_ma3 < -width_threshold
                and row.hs300_score_ma3 < -hs300_filter
                and row.vol_ratio_5_20 < short_volume_multiplier
            )
        else:
            long_context = False
            short_context = False

        if long_wave_active and not long_context:
            long_wave_active = False
            long_wave_blocked = False
        if short_wave_active and not short_context:
            short_wave_active = False
            short_wave_target = None

        if long_context:
            if not long_wave_active:
                long_wave_active = True
                long_wave_blocked = not (row.price_from_low10 >= low_distance)
            short_wave_active = False
            short_wave_target = None
            target = "FLAT" if long_wave_blocked else "LONG"
        elif short_context:
            if not short_wave_active:
                short_wave_active = True
                short_wave_target = "SHORT" if row.drawdown_from_high10 <= high_drawdown else "LONG"
            long_wave_active = False
            long_wave_blocked = False
            target = short_wave_target
        else:
            target = "FLAT"

        pending_target = target
        daily_positions.append(position)

    final_close = float(df.iloc[-1].close)
    if current_trade:
        ret = (
            final_close / current_trade["entry_price"] - 1
            if current_trade["direction"] == "LONG"
            else current_trade["entry_price"] / final_close - 1
        )
        current_trade["return_pct"] = (ret - fee) * 100
        current_trade["hold_days"] = len(daily_positions) - current_trade["entry_index"]
        trades.append(current_trade)

    trade_returns = pd.Series([trade["return_pct"] for trade in trades], dtype=float)
    long_returns = pd.Series([trade["return_pct"] for trade in trades if trade["direction"] == "LONG"], dtype=float)
    short_returns = pd.Series([trade["return_pct"] for trade in trades if trade["direction"] == "SHORT"], dtype=float)
    positions = pd.Series(daily_positions)

    return {
        "width_threshold": width_threshold,
        "hs300_filter": hs300_filter,
        "long_volume_multiplier": long_volume_multiplier,
        "short_volume_multiplier": short_volume_multiplier,
        "low_distance": low_distance,
        "high_drawdown": high_drawdown,
        "total_return_pct": (equity - 1) * 100,
        "max_drawdown_pct": max_dd * 100,
        "return_to_dd": ((equity - 1) * 100 / abs(max_dd * 100)) if max_dd < 0 else np.nan,
        "trade_count": len(trades),
        "win_rate_pct": (trade_returns.gt(0).mean() * 100) if len(trade_returns) else 0,
        "avg_trade_return_pct": trade_returns.mean() if len(trade_returns) else 0,
        "median_trade_return_pct": trade_returns.median() if len(trade_returns) else 0,
        "long_trade_count": len(long_returns),
        "long_avg_return_pct": long_returns.mean() if len(long_returns) else 0,
        "short_trade_count": len(short_returns),
        "short_avg_return_pct": short_returns.mean() if len(short_returns) else 0,
        "long_days": int((positions == "LONG").sum()),
        "short_days": int((positions == "SHORT").sum()),
        "flat_days": int((positions == "FLAT").sum()),
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

    rows = []
    for params in itertools.product(
        WIDTH_THRESHOLDS,
        HS300_FILTERS,
        LONG_VOLUME_MULTIPLIERS,
        SHORT_VOLUME_MULTIPLIERS,
        LOW_DISTANCES,
        HIGH_DRAWDOWNS,
    ):
        rows.append(run_backtest(df, *params))

    results = pd.DataFrame(rows)
    grid_path = OUT_DIR / "csi1000_param_stability_grid_dual_volume.csv"
    results.to_csv(grid_path, index=False)

    summaries = []
    for column in (
        "width_threshold",
        "hs300_filter",
        "long_volume_multiplier",
        "short_volume_multiplier",
        "low_distance",
        "high_drawdown",
    ):
        summary = summarize_dimension(results, column)
        summary.insert(0, "dimension", column)
        summaries.append(summary.rename(columns={column: "value"}))
    summary_df = pd.concat(summaries, ignore_index=True)
    summary_path = OUT_DIR / "csi1000_param_stability_summary_dual_volume.csv"
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
