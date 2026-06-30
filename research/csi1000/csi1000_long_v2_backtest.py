from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))

import csi1000_timing as timing  # noqa: E402


DB_PATH = BASE_DIR / "stock_data.db"
OUT_DIR = BASE_DIR / "outputs"

START_DATE = "2016-06-20"
END_DATE = "2026-06-22"
FEE_BPS = 2.0

CSI_MIN = 20.0
HS300_MIN = -10.0
VOL_MIN = 1.10
EXIT_CSI_MIN = -10.0
EXIT_HS300_MIN = -20.0
PROFIT_TRIGGER = 0.09
TRAIL_DRAWDOWN = 0.03


def load_frame() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    try:
        df = timing.load_feature_frame(conn, start=START_DATE, end=END_DATE)
    finally:
        conn.close()
    if df.empty:
        raise RuntimeError("没有可用的中证1000回测数据")
    df = df.dropna(subset=["open", "close"]).sort_values("trade_date").reset_index(drop=True)
    for col in (
        "open",
        "close",
        "csi_score_ma3",
        "hs300_score_ma3",
        "vol_ratio_5_20",
    ):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def base_signal(row: pd.Series) -> bool:
    return bool(
        pd.notna(row["csi_score_ma3"])
        and pd.notna(row["hs300_score_ma3"])
        and pd.notna(row["vol_ratio_5_20"])
        and row["csi_score_ma3"] > CSI_MIN
        and row["hs300_score_ma3"] > HS300_MIN
        and row["vol_ratio_5_20"] > VOL_MIN
    )


def backtest(df: pd.DataFrame) -> tuple[dict[str, float | int | str], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    fee = FEE_BPS / 10000.0
    opens = df["open"].to_numpy(dtype=float)
    closes = df["close"].to_numpy(dtype=float)

    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    position = 0
    pending = 0
    entry_price = 0.0
    entry_date = None
    highest_price = 0.0
    trailing_active = False
    pending_exit_reason = ""

    daily_rows = []
    trades = []

    for i in range(len(df) - 1):
        row = df.iloc[i]
        next_open = opens[i + 1]

        if pending != position:
            open_price = opens[i]
            if position == 1:
                gross_ret = open_price / entry_price - 1.0
                net_ret = gross_ret - fee
                trades.append({
                    "entry_date": entry_date,
                    "exit_date": row["trade_date"],
                    "entry_price": entry_price,
                    "exit_price": open_price,
                    "hold_days": len(daily_rows) - entry_daily_index,
                    "return_pct": net_ret * 100.0,
                    "exit_reason": pending_exit_reason or "signal_off",
                })
                equity *= 1.0 - fee
            position = pending
            if position == 1:
                equity *= 1.0 - fee
                entry_price = open_price
                entry_date = row["trade_date"]
                entry_daily_index = len(daily_rows)
                highest_price = open_price
                trailing_active = False
            pending_exit_reason = ""

        if position == 1:
            highest_price = max(highest_price, opens[i], next_open)
            interval_ret = next_open / opens[i] - 1.0
        else:
            interval_ret = 0.0
        equity *= 1.0 + interval_ret
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity / peak - 1.0)

        entry_ok = base_signal(row)
        action = "持仓" if position == 1 else "空仓"
        exit_reason = ""

        if position == 1:
            target = 1 if entry_ok else 0
            if not entry_ok:
                exit_reason = "entry_condition_off"
            if pd.notna(row["csi_score_ma3"]) and row["csi_score_ma3"] <= EXIT_CSI_MIN:
                target = 0
                exit_reason = "csi_width_exit"
            if pd.notna(row["hs300_score_ma3"]) and row["hs300_score_ma3"] <= EXIT_HS300_MIN:
                target = 0
                exit_reason = "hs300_width_exit"

            trade_profit = highest_price / entry_price - 1.0
            if trade_profit >= PROFIT_TRIGGER:
                trailing_active = True
            trailing_dd = next_open / highest_price - 1.0
            if trailing_active and trailing_dd <= -TRAIL_DRAWDOWN:
                target = 0
                exit_reason = "profit_trailing_exit"
        else:
            target = 1 if entry_ok else 0
            if target == 1:
                action = "开仓信号"

        pending = target
        if position == 1 and target == 0:
            pending_exit_reason = exit_reason or "signal_off"
            action = "平仓信号"

        daily_rows.append({
            "trade_date": row["trade_date"],
            "open": row["open"],
            "close": row["close"],
            "position": "LONG" if position == 1 else "FLAT",
            "target_next": "LONG" if pending == 1 else "FLAT",
            "action": action,
            "exit_reason": exit_reason,
            "equity": equity,
            "interval_ret": interval_ret,
            "drawdown": equity / peak - 1.0,
            "csi_score_ma3": row["csi_score_ma3"],
            "hs300_score_ma3": row["hs300_score_ma3"],
            "vol_ratio_5_20": row["vol_ratio_5_20"],
            "highest_since_entry": highest_price if position == 1 else np.nan,
            "trailing_active": trailing_active if position == 1 else False,
        })

    if position == 1:
        final = df.iloc[-1]
        gross_ret = closes[-1] / entry_price - 1.0
        trades.append({
            "entry_date": entry_date,
            "exit_date": final["trade_date"],
            "entry_price": entry_price,
            "exit_price": closes[-1],
            "hold_days": len(daily_rows) - entry_daily_index,
            "return_pct": (gross_ret - fee) * 100.0,
            "exit_reason": "final_close",
        })

    daily = pd.DataFrame(daily_rows)
    trades_df = pd.DataFrame(trades)
    returns = trades_df["return_pct"] if not trades_df.empty else pd.Series(dtype=float)
    summary = {
        "strategy": "CSI1000_LONG_V2",
        "start": df["trade_date"].min().strftime("%Y-%m-%d"),
        "end": df["trade_date"].max().strftime("%Y-%m-%d"),
        "total_return_pct": (equity - 1.0) * 100.0,
        "max_drawdown_pct": max_drawdown * 100.0,
        "return_to_dd": ((equity - 1.0) / abs(max_drawdown)) if max_drawdown < 0 else np.nan,
        "trade_count": len(trades_df),
        "win_rate_pct": float((returns > 0).mean() * 100.0) if len(returns) else 0.0,
        "avg_trade_return_pct": float(returns.mean()) if len(returns) else 0.0,
        "median_trade_return_pct": float(returns.median()) if len(returns) else 0.0,
        "hold_days": int((daily["position"] == "LONG").sum()) if not daily.empty else 0,
        "flat_days": int((daily["position"] == "FLAT").sum()) if not daily.empty else 0,
        "profit_trailing_exit_count": int((trades_df.get("exit_reason", pd.Series(dtype=str)) == "profit_trailing_exit").sum()),
    }

    annual_rows = []
    for year, part in daily.groupby(daily["trade_date"].dt.year):
        equity_curve = (1.0 + part["interval_ret"].astype(float)).cumprod()
        dd = equity_curve / equity_curve.cummax() - 1.0
        annual_rows.append({
            "year": int(year),
            "return_pct": (equity_curve.iloc[-1] - 1.0) * 100.0,
            "max_drawdown_pct": dd.min() * 100.0,
            "hold_days": int((part["position"] == "LONG").sum()),
            "trade_count": int(((part["position"] == "FLAT") & (part["target_next"] == "LONG")).sum()),
        })
    annual = pd.DataFrame(annual_rows)
    return summary, daily, trades_df, annual


def main() -> int:
    OUT_DIR.mkdir(exist_ok=True)
    df = load_frame()
    summary, daily, trades, annual = backtest(df)

    summary_path = OUT_DIR / "csi1000_long_v2_summary.csv"
    daily_path = OUT_DIR / "csi1000_long_v2_daily.csv"
    trades_path = OUT_DIR / "csi1000_long_v2_trades.csv"
    annual_path = OUT_DIR / "csi1000_long_v2_annual_returns.csv"

    pd.DataFrame([summary]).to_csv(summary_path, index=False)
    daily.to_csv(daily_path, index=False)
    trades.to_csv(trades_path, index=False)
    annual.to_csv(annual_path, index=False)

    print(f"summary_csv={summary_path}")
    print(f"daily_csv={daily_path}")
    print(f"trades_csv={trades_path}")
    print(f"annual_csv={annual_path}")
    print("\nSummary:")
    print(pd.Series(summary).to_string())
    print("\nAnnual:")
    print(annual.to_string(index=False))
    print("\nTrades:")
    print(trades.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
