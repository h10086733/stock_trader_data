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

SHORT_ENTRY_VOLS = [1.00, 1.03, 1.05]
SHORT_EXIT_VOLS = [1.05, 1.08, 1.10]
SHORT_STOP_1D = [0.02, 0.025, 0.03]
SHORT_STOP_2D = [0.02, 0.03, 0.04]
FEE_BPS = 2.0


def load_frame() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    try:
        df = timing.load_frame_by_source(
            conn,
            "db",
            EXCEL_PATH,
            start="2016-06-20",
            end="2026-06-18",
        )
    finally:
        conn.close()
    if df.empty:
        raise RuntimeError("没有可用的回测数据")
    df = df.sort_values("trade_date").reset_index(drop=True)
    for col in (
        "close",
        "csi_score_ma3",
        "hs300_score_ma3",
        "vol_ratio_5_20",
        "price_from_low10",
        "drawdown_from_high10",
        "pct_1d",
        "pct_2d",
    ):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def run_one(arr: dict[str, np.ndarray], params: tuple[float, float, float, float]) -> dict[str, float]:
    short_entry_vol, short_exit_vol, short_stop_1d, short_stop_2d = params
    close = arr["close"]
    csi = arr["csi"]
    hs300 = arr["hs300"]
    vol = arr["vol"]
    low10 = arr["low10"]
    dd10 = arr["dd10"]
    pct1 = arr["pct1"]
    pct2 = arr["pct2"]
    valid = arr["valid"]

    fee = FEE_BPS / 10000.0
    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    position = 0
    entry_price = 0.0
    entry_index = -1
    entry_source = ""
    trade_returns: list[float] = []
    long_returns: list[float] = []
    short_returns: list[float] = []
    hold_days: list[int] = []
    long_days = 0
    short_days = 0
    flat_days = 0

    long_wave_active = False
    long_wave_blocked = False
    short_wave_active = False
    short_wave_target = 0

    for i in range(len(close)):
        if i > 0:
            if position == 1:
                interval = close[i] / close[i - 1] - 1.0
                long_days += 1
            elif position == -1:
                interval = close[i - 1] / close[i] - 1.0
                short_days += 1
            else:
                interval = 0.0
                flat_days += 1
            equity *= 1.0 + interval
            peak = max(peak, equity)
            max_dd = min(max_dd, equity / peak - 1.0)

        if not valid[i]:
            target = 0
        else:
            long_context = csi[i] > 20 and hs300[i] > -20 and vol[i] > 1.05
            short_width = csi[i] < -10 and hs300[i] < 10
            short_entry_context = short_width and vol[i] <= short_entry_vol
            stop_hit = False
            if position == -1:
                bars = i - entry_index
                stop_2d_gains = []
                if bars <= 2:
                    stop_2d_gains.append(close[i] / entry_price - 1.0)
                elif np.isfinite(pct2[i]):
                    stop_2d_gains.append(pct2[i])
                stop_1d_hit = np.isfinite(pct1[i]) and pct1[i] >= short_stop_1d
                stop_2d_hit = bool(stop_2d_gains and max(stop_2d_gains) >= short_stop_2d)
                stop_hit = stop_1d_hit or stop_2d_hit
                if stop_hit and short_width:
                    short_wave_active = True
                    short_wave_target = 0

            if long_wave_active and not long_context:
                long_wave_active = False
                long_wave_blocked = False
            if short_wave_active and not short_width:
                short_wave_active = False
                short_wave_target = 0

            if stop_hit:
                target = 0
            elif long_context:
                if not long_wave_active:
                    long_wave_active = True
                    long_wave_blocked = low10[i] < 0.06
                short_wave_active = False
                short_wave_target = 0
                target = 0 if long_wave_blocked else 1
            elif short_wave_active and short_width:
                long_wave_active = False
                long_wave_blocked = False
                if short_wave_target == -1 and vol[i] <= short_exit_vol:
                    target = -1
                elif short_wave_target == 1 and vol[i] <= short_entry_vol:
                    target = 1
                else:
                    short_wave_target = 0
                    target = 0
            elif short_entry_context:
                short_wave_active = True
                short_wave_target = -1 if dd10[i] <= 0.05 else 1
                long_wave_active = False
                long_wave_blocked = False
                target = short_wave_target
            elif short_width:
                target = 0
            else:
                long_wave_active = False
                long_wave_blocked = False
                short_wave_active = False
                short_wave_target = 0
                target = 0

        if target != position:
            if position != 0:
                ret = close[i] / entry_price - 1.0 if position == 1 else entry_price / close[i] - 1.0
                trade_ret = (ret - fee) * 100.0
                trade_returns.append(trade_ret)
                if position == 1:
                    long_returns.append(trade_ret)
                else:
                    short_returns.append(trade_ret)
                hold_days.append(i - entry_index)
                equity *= 1.0 - fee
            position = target
            if position != 0 and i < len(close) - 1:
                equity *= 1.0 - fee
                entry_price = close[i]
                entry_index = i
                entry_source = "LONG" if position == 1 else "SHORT"
            elif position != 0:
                position = 0

    if position != 0:
        ret = close[-1] / entry_price - 1.0 if position == 1 else entry_price / close[-1] - 1.0
        trade_ret = (ret - fee) * 100.0
        trade_returns.append(trade_ret)
        if position == 1:
            long_returns.append(trade_ret)
        else:
            short_returns.append(trade_ret)
        hold_days.append(len(close) - 1 - entry_index)
        equity *= 1.0 - fee

    trades = np.array(trade_returns, dtype=float)
    longs = np.array(long_returns, dtype=float)
    shorts = np.array(short_returns, dtype=float)
    total = (equity - 1.0) * 100.0
    max_dd_pct = max_dd * 100.0
    return {
        "short_entry_vol": short_entry_vol,
        "short_exit_vol": short_exit_vol,
        "short_stop_1d": short_stop_1d,
        "short_stop_2d": short_stop_2d,
        "total_return_pct": total,
        "max_drawdown_pct": max_dd_pct,
        "return_to_dd": total / abs(max_dd_pct) if max_dd_pct < 0 else np.nan,
        "trade_count": len(trades),
        "win_rate_pct": float((trades > 0).mean() * 100.0) if len(trades) else 0.0,
        "avg_trade_return_pct": float(trades.mean()) if len(trades) else 0.0,
        "long_trade_count": len(longs),
        "long_win_rate_pct": float((longs > 0).mean() * 100.0) if len(longs) else 0.0,
        "long_avg_return_pct": float(longs.mean()) if len(longs) else 0.0,
        "short_trade_count": len(shorts),
        "short_win_rate_pct": float((shorts > 0).mean() * 100.0) if len(shorts) else 0.0,
        "short_avg_return_pct": float(shorts.mean()) if len(shorts) else 0.0,
        "avg_hold_days": float(np.mean(hold_days)) if hold_days else 0.0,
        "long_days": long_days,
        "short_days": short_days,
        "flat_days": flat_days,
    }


def main() -> int:
    OUT_DIR.mkdir(exist_ok=True)
    df = load_frame()
    arr = {
        "close": df["close"].to_numpy(dtype=float),
        "csi": df["csi_score_ma3"].to_numpy(dtype=float),
        "hs300": df["hs300_score_ma3"].to_numpy(dtype=float),
        "vol": df["vol_ratio_5_20"].to_numpy(dtype=float),
        "low10": df["price_from_low10"].to_numpy(dtype=float),
        "dd10": df["drawdown_from_high10"].to_numpy(dtype=float),
        "pct1": df["pct_1d"].to_numpy(dtype=float),
        "pct2": df["pct_2d"].to_numpy(dtype=float),
    }
    arr["valid"] = (
        np.isfinite(arr["close"])
        & np.isfinite(arr["csi"])
        & np.isfinite(arr["hs300"])
        & np.isfinite(arr["vol"])
        & np.isfinite(arr["low10"])
        & np.isfinite(arr["dd10"])
    )
    rows = [
        run_one(arr, params)
        for params in itertools.product(SHORT_ENTRY_VOLS, SHORT_EXIT_VOLS, SHORT_STOP_1D, SHORT_STOP_2D)
    ]
    results = pd.DataFrame(rows)
    grid_path = OUT_DIR / "csi1000_short_exit_stability_grid.csv"
    results.to_csv(grid_path, index=False)

    summaries = []
    for col in ("short_entry_vol", "short_exit_vol", "short_stop_1d", "short_stop_2d"):
        summary = (
            results.groupby(col)
            .agg(
                combos=("total_return_pct", "count"),
                median_return_pct=("total_return_pct", "median"),
                mean_return_pct=("total_return_pct", "mean"),
                median_max_dd_pct=("max_drawdown_pct", "median"),
                median_return_to_dd=("return_to_dd", "median"),
                median_win_rate_pct=("win_rate_pct", "median"),
                median_trade_count=("trade_count", "median"),
            )
            .reset_index()
            .rename(columns={col: "value"})
        )
        summary.insert(0, "dimension", col)
        summaries.append(summary)
    summary_df = pd.concat(summaries, ignore_index=True)
    summary_path = OUT_DIR / "csi1000_short_exit_stability_summary.csv"
    summary_df.to_csv(summary_path, index=False)

    selected = results[
        (results["short_entry_vol"] == 1.05)
        & (results["short_exit_vol"] == 1.05)
        & (results["short_stop_1d"] == 0.02)
        & (results["short_stop_2d"] == 0.02)
    ]
    print(f"rows={len(results)}")
    print(f"grid_csv={grid_path}")
    print(f"summary_csv={summary_path}")
    print("\nCurrent params:")
    print(selected.to_string(index=False))
    print("\nTop by return_to_dd:")
    print(results.sort_values(["return_to_dd", "total_return_pct"], ascending=False).head(15).to_string(index=False))
    print("\nTop by return:")
    print(results.sort_values(["total_return_pct", "return_to_dd"], ascending=False).head(15).to_string(index=False))
    print("\nDimension summary:")
    print(summary_df.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
