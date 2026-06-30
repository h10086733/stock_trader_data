from __future__ import annotations

import itertools
import sqlite3
import sys
from dataclasses import dataclass
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


@dataclass(frozen=True)
class ShortRule:
    name: str
    csi_max: float
    hs300_max: float
    vol_max: float
    ma_filter: str
    max_drop_from_high10: float | None
    exit_csi_min: float
    exit_hs300_min: float
    profit_trigger: float
    trail_rebound: float


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
        "high",
        "low",
        "csi_score_ma3",
        "hs300_score_ma3",
        "vol_ratio_5_20",
        "drawdown_from_high10",
    ):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["ma20"] = df["close"].rolling(20, min_periods=20).mean()
    df["ma60"] = df["close"].rolling(60, min_periods=60).mean()
    df["ma20_slope10"] = df["ma20"] / df["ma20"].shift(10) - 1.0
    df["ma60_slope20"] = df["ma60"] / df["ma60"].shift(20) - 1.0
    return df


def ma_ok(row: pd.Series, ma_filter: str) -> bool:
    if ma_filter == "close_lt_ma20":
        return bool(row["close"] < row["ma20"])
    if ma_filter == "close_lt_ma60":
        return bool(row["close"] < row["ma60"])
    if ma_filter == "ma20_lt_ma60":
        return bool(row["ma20"] < row["ma60"])
    if ma_filter == "close_lt_ma20_ma20_lt_ma60":
        return bool(row["close"] < row["ma20"] and row["ma20"] < row["ma60"])
    if ma_filter == "close_lt_ma60_ma60_slope_neg":
        return bool(row["close"] < row["ma60"] and row["ma60_slope20"] < 0)
    raise ValueError(f"未知均线过滤: {ma_filter}")


def entry_signal(row: pd.Series, rule: ShortRule) -> bool:
    if not (
        pd.notna(row["csi_score_ma3"])
        and pd.notna(row["hs300_score_ma3"])
        and pd.notna(row["vol_ratio_5_20"])
        and pd.notna(row["drawdown_from_high10"])
    ):
        return False
    if not (
        row["csi_score_ma3"] < rule.csi_max
        and row["hs300_score_ma3"] < rule.hs300_max
        and row["vol_ratio_5_20"] < rule.vol_max
        and ma_ok(row, rule.ma_filter)
    ):
        return False
    if rule.max_drop_from_high10 is not None and row["drawdown_from_high10"] > rule.max_drop_from_high10:
        return False
    return True


def run_backtest(df: pd.DataFrame, rule: ShortRule) -> tuple[dict[str, float | int | str], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    fee = FEE_BPS / 10000.0
    opens = df["open"].to_numpy(dtype=float)
    closes = df["close"].to_numpy(dtype=float)

    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    position = 0
    pending = 0
    entry_price = 0.0
    lowest_price = 0.0
    trailing_active = False
    entry_date = None
    entry_idx = 0
    pending_exit_reason = ""
    daily_rows = []
    trades = []

    for i in range(len(df) - 1):
        row = df.iloc[i]
        next_open = opens[i + 1]
        if pending != position:
            open_price = opens[i]
            if position == -1:
                gross_ret = entry_price / open_price - 1.0
                trades.append({
                    "entry_date": entry_date,
                    "exit_date": row["trade_date"],
                    "entry_price": entry_price,
                    "exit_price": open_price,
                    "hold_days": len(daily_rows) - entry_idx,
                    "return_pct": (gross_ret - fee) * 100.0,
                    "exit_reason": pending_exit_reason or "signal_off",
                })
                equity *= 1.0 - fee
            position = pending
            if position == -1:
                equity *= 1.0 - fee
                entry_price = open_price
                lowest_price = open_price
                trailing_active = False
                entry_date = row["trade_date"]
                entry_idx = len(daily_rows)
            pending_exit_reason = ""

        if position == -1:
            lowest_price = min(lowest_price, opens[i], next_open)
            interval_ret = opens[i] / next_open - 1.0
        else:
            interval_ret = 0.0
        equity *= 1.0 + interval_ret
        peak = max(peak, equity)
        max_dd = min(max_dd, equity / peak - 1.0)

        entry_ok = entry_signal(row, rule)
        exit_reason = ""
        if position == -1:
            target = -1 if entry_ok else 0
            if not entry_ok:
                exit_reason = "entry_condition_off"
            if pd.notna(row["csi_score_ma3"]) and row["csi_score_ma3"] >= rule.exit_csi_min:
                target = 0
                exit_reason = "csi_width_exit"
            if pd.notna(row["hs300_score_ma3"]) and row["hs300_score_ma3"] >= rule.exit_hs300_min:
                target = 0
                exit_reason = "hs300_width_exit"

            profit = entry_price / lowest_price - 1.0
            if profit >= rule.profit_trigger:
                trailing_active = True
            rebound = next_open / lowest_price - 1.0
            if trailing_active and rebound >= rule.trail_rebound:
                target = 0
                exit_reason = "profit_trailing_exit"
        else:
            target = -1 if entry_ok else 0

        pending = target
        if position == -1 and target == 0:
            pending_exit_reason = exit_reason or "signal_off"

        daily_rows.append({
            "trade_date": row["trade_date"],
            "position": "SHORT" if position == -1 else "FLAT",
            "target_next": "SHORT" if pending == -1 else "FLAT",
            "equity": equity,
            "interval_ret": interval_ret,
            "drawdown": equity / peak - 1.0,
            "exit_reason": exit_reason,
            "csi_score_ma3": row["csi_score_ma3"],
            "hs300_score_ma3": row["hs300_score_ma3"],
            "vol_ratio_5_20": row["vol_ratio_5_20"],
            "drawdown_from_high10": row["drawdown_from_high10"],
            "ma_filter": rule.ma_filter,
        })

    if position == -1:
        gross_ret = entry_price / closes[-1] - 1.0
        trades.append({
            "entry_date": entry_date,
            "exit_date": df.iloc[-1]["trade_date"],
            "entry_price": entry_price,
            "exit_price": closes[-1],
            "hold_days": len(daily_rows) - entry_idx,
            "return_pct": (gross_ret - fee) * 100.0,
            "exit_reason": "final_close",
        })

    daily = pd.DataFrame(daily_rows)
    trades_df = pd.DataFrame(trades)
    returns = trades_df["return_pct"] if not trades_df.empty else pd.Series(dtype=float)
    summary = {
        "rule": rule.name,
        "csi_max": rule.csi_max,
        "hs300_max": rule.hs300_max,
        "vol_max": rule.vol_max,
        "ma_filter": rule.ma_filter,
        "max_drop_from_high10": rule.max_drop_from_high10,
        "total_return_pct": (equity - 1.0) * 100.0,
        "max_drawdown_pct": max_dd * 100.0,
        "return_to_dd": ((equity - 1.0) / abs(max_dd)) if max_dd < 0 else np.nan,
        "trade_count": len(trades_df),
        "win_rate_pct": float((returns > 0).mean() * 100.0) if len(returns) else 0.0,
        "avg_trade_return_pct": float(returns.mean()) if len(returns) else 0.0,
        "median_trade_return_pct": float(returns.median()) if len(returns) else 0.0,
        "short_days": int((daily["position"] == "SHORT").sum()) if not daily.empty else 0,
        "profit_trailing_exit_count": int((trades_df.get("exit_reason", pd.Series(dtype=str)) == "profit_trailing_exit").sum()),
    }
    annual_rows = []
    for year, part in daily.groupby(daily["trade_date"].dt.year):
        eq = (1.0 + part["interval_ret"].astype(float)).cumprod()
        dd = eq / eq.cummax() - 1.0
        annual_rows.append({
            "rule": rule.name,
            "year": int(year),
            "return_pct": (eq.iloc[-1] - 1.0) * 100.0,
            "max_drawdown_pct": dd.min() * 100.0,
            "short_days": int((part["position"] == "SHORT").sum()),
            "trade_count": int(((part["position"] == "FLAT") & (part["target_next"] == "SHORT")).sum()),
        })
    return summary, daily, trades_df, pd.DataFrame(annual_rows)


def candidate_rules() -> list[ShortRule]:
    rules = []
    for csi_max, hs300_max, vol_max, ma_filter, max_drop in itertools.product(
        [-20.0, -30.0],
        [-10.0, 0.0],
        [0.95, 1.00],
        [
            "close_lt_ma20",
            "close_lt_ma60",
            "close_lt_ma20_ma20_lt_ma60",
        ],
        [0.03, 0.05],
    ):
        name = f"csi<{csi_max}_hs<{hs300_max}_vol<{vol_max}_{ma_filter}_drop<{max_drop}"
        rules.append(ShortRule(
            name=name,
            csi_max=csi_max,
            hs300_max=hs300_max,
            vol_max=vol_max,
            ma_filter=ma_filter,
            max_drop_from_high10=max_drop,
            exit_csi_min=10.0,
            exit_hs300_min=20.0,
            profit_trigger=0.09,
            trail_rebound=0.03,
        ))
    return rules


def main() -> int:
    OUT_DIR.mkdir(exist_ok=True)
    df = load_frame()
    summaries = []
    annual_frames = []

    for rule in candidate_rules():
        summary, _daily, trades, annual = run_backtest(df, rule)
        summaries.append(summary)
        annual_frames.append(annual)

    summary_df = pd.DataFrame(summaries)
    summary_df["score"] = (
        summary_df["total_return_pct"]
        + summary_df["return_to_dd"].fillna(-999) * 5.0
        - summary_df["max_drawdown_pct"].abs() * 4.0
    )
    summary_df = summary_df.sort_values(["score", "return_to_dd"], ascending=False)
    annual_df = pd.concat(annual_frames, ignore_index=True)

    top_trade_frames = []
    for rule_name in summary_df.head(10)["rule"]:
        rule = next(item for item in candidate_rules() if item.name == rule_name)
        _summary, _daily, trades, _annual = run_backtest(df, rule)
        if not trades.empty:
            trades = trades.copy()
            trades.insert(0, "rule", rule.name)
            top_trade_frames.append(trades)
    trades_df = pd.concat(top_trade_frames, ignore_index=True) if top_trade_frames else pd.DataFrame()

    summary_path = OUT_DIR / "csi1000_short_v2_research_grid.csv"
    annual_path = OUT_DIR / "csi1000_short_v2_research_annual.csv"
    trades_path = OUT_DIR / "csi1000_short_v2_research_trades.csv"
    selected_path = OUT_DIR / "csi1000_short_v2_research_selected.csv"
    summary_df.to_csv(summary_path, index=False)
    annual_df.to_csv(annual_path, index=False)
    trades_df.to_csv(trades_path, index=False)
    summary_df.head(50).to_csv(selected_path, index=False)

    cols = [
        "rule",
        "total_return_pct",
        "max_drawdown_pct",
        "return_to_dd",
        "trade_count",
        "win_rate_pct",
        "avg_trade_return_pct",
        "short_days",
    ]
    print(f"summary_csv={summary_path}")
    print(f"annual_csv={annual_path}")
    print(f"trades_csv={trades_path}")
    print(f"selected_csv={selected_path}")
    print("\nTop 20:")
    print(summary_df[cols].head(20).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
