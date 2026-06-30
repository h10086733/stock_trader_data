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
FEE_BPS = 2.0
SPLIT_DATE = pd.Timestamp("2022-12-31")


@dataclass(frozen=True)
class Rule:
    name: str
    csi_min: float = 20.0
    hs300_min: float = -10.0
    vol_min: float = 1.10
    exit_csi_min: float = -10.0
    exit_hs300_min: float = -20.0
    regime: str = "none"
    profit_trigger: float | None = None
    trail_drawdown: float | None = None


def load_frame() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    try:
        df = timing.load_feature_frame(conn, start="2016-06-20", end="2026-06-22")
    finally:
        conn.close()
    if df.empty:
        raise RuntimeError("没有可用的数据库回测数据")
    df = df.dropna(subset=["open", "close"]).sort_values("trade_date").reset_index(drop=True)
    for col in (
        "open",
        "high",
        "low",
        "close",
        "csi_score_ma3",
        "hs300_score_ma3",
        "vol_ratio_5_20",
    ):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["ma20"] = df["close"].rolling(20, min_periods=20).mean()
    df["ma60"] = df["close"].rolling(60, min_periods=60).mean()
    df["ma120"] = df["close"].rolling(120, min_periods=120).mean()
    df["ma20_slope10"] = df["ma20"] / df["ma20"].shift(10) - 1.0
    df["ma60_slope20"] = df["ma60"] / df["ma60"].shift(20) - 1.0
    df["ret1"] = df["close"] / df["close"].shift(1) - 1.0
    df["volatility20"] = df["ret1"].rolling(20, min_periods=20).std()
    df["range20"] = df["close"].rolling(20, min_periods=20).max() / df["close"].rolling(20, min_periods=20).min() - 1.0
    df["csi_width_ma5"] = df["csi_score_ma3"].rolling(5, min_periods=5).mean()
    df["csi_width_ma20"] = df["csi_score_ma3"].rolling(20, min_periods=20).mean()
    df["hs300_width_ma5"] = df["hs300_score_ma3"].rolling(5, min_periods=5).mean()
    df["hs300_width_ma20"] = df["hs300_score_ma3"].rolling(20, min_periods=20).mean()
    return df


def regime_mask(df: pd.DataFrame, name: str) -> pd.Series:
    if name == "none":
        return pd.Series(True, index=df.index)
    if name == "price_ma20_gt_ma60":
        return df["ma20"] > df["ma60"]
    if name == "price_close_gt_ma60":
        return df["close"] > df["ma60"]
    if name == "price_ma20_slope_pos":
        return df["ma20_slope10"] > 0
    if name == "price_ma60_slope_pos":
        return df["ma60_slope20"] > 0
    if name == "width_ma5_gt_ma20":
        return df["csi_width_ma5"] > df["csi_width_ma20"]
    if name == "dual_width_ma5_gt_ma20":
        return (df["csi_width_ma5"] > df["csi_width_ma20"]) & (df["hs300_width_ma5"] > df["hs300_width_ma20"])
    if name == "avoid_low_range20":
        return df["range20"] > 0.08
    if name == "trend_or_width":
        return (df["ma20"] > df["ma60"]) | (df["csi_width_ma5"] > df["csi_width_ma20"])
    if name == "trend_and_width":
        return (df["ma20"] > df["ma60"]) & (df["csi_width_ma5"] > df["csi_width_ma20"])
    raise ValueError(f"未知震荡过滤: {name}")


def entry_signal(df: pd.DataFrame, rule: Rule) -> pd.Series:
    signal = base_signal(df, rule) & regime_mask(df, rule.regime)
    return signal.fillna(False)


def base_signal(df: pd.DataFrame, rule: Rule) -> pd.Series:
    signal = (
        (df["csi_score_ma3"] > rule.csi_min)
        & (df["hs300_score_ma3"] > rule.hs300_min)
        & (df["vol_ratio_5_20"] > rule.vol_min)
    )
    return signal.fillna(False)


def run_backtest(df: pd.DataFrame, rule: Rule) -> tuple[dict[str, float | str], pd.DataFrame, pd.DataFrame]:
    fee = FEE_BPS / 10000.0
    signal = entry_signal(df, rule)
    hold_signal = base_signal(df, rule)
    opens = df["open"].to_numpy(dtype=float)
    closes = df["close"].to_numpy(dtype=float)
    dates = df["trade_date"]

    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    position = 0
    pending = 0
    entry_price = 0.0
    entry_idx = 0
    highest_price = 0.0
    trailing_active = False
    pending_exit_reason = ""

    daily_rows: list[dict[str, float | int | str | pd.Timestamp]] = []
    trades: list[dict[str, float | int | str | pd.Timestamp]] = []

    for i in range(len(df) - 1):
        exit_reason = ""
        if pending != position:
            open_price = opens[i]
            if position == 1:
                ret = open_price / entry_price - 1.0
                trades.append({
                    "rule": rule.name,
                    "entry_date": dates.iloc[entry_idx],
                    "exit_date": dates.iloc[i],
                    "entry_price": entry_price,
                    "exit_price": open_price,
                    "hold_days": i - entry_idx,
                    "return_pct": (ret - fee) * 100.0,
                    "exit_reason": pending_exit_reason or "signal",
                })
                equity *= 1.0 - fee
            position = pending
            if position == 1:
                equity *= 1.0 - fee
                entry_price = open_price
                entry_idx = i
                highest_price = open_price
                trailing_active = False
            pending_exit_reason = ""

        if position == 1:
            highest_price = max(highest_price, opens[i], opens[i + 1])
            interval_ret = opens[i + 1] / opens[i] - 1.0
        else:
            interval_ret = 0.0
        equity *= 1.0 + interval_ret
        peak = max(peak, equity)
        max_dd = min(max_dd, equity / peak - 1.0)

        if position == 1:
            target = 1 if hold_signal.iloc[i] else 0
            if target == 0:
                exit_reason = "base_signal_off"
            if df["csi_score_ma3"].iloc[i] <= rule.exit_csi_min:
                target = 0
                exit_reason = "csi_exit"
            if df["hs300_score_ma3"].iloc[i] <= rule.exit_hs300_min:
                target = 0
                exit_reason = "hs300_exit"
            if rule.profit_trigger is not None and rule.trail_drawdown is not None:
                trade_profit = highest_price / entry_price - 1.0
                if trade_profit >= rule.profit_trigger:
                    trailing_active = True
                trailing_dd = opens[i + 1] / highest_price - 1.0
                if trailing_active and trailing_dd <= -rule.trail_drawdown:
                    target = 0
                    exit_reason = "trailing_profit"
        else:
            target = 1 if signal.iloc[i] else 0
        pending = target
        if target == 0 and position == 1 and exit_reason:
            pending_exit_reason = exit_reason

        daily_rows.append({
            "rule": rule.name,
            "trade_date": dates.iloc[i],
            "position": position,
            "equity": equity,
            "interval_ret": interval_ret,
            "drawdown": equity / peak - 1.0,
            "trailing_active": int(trailing_active),
            "exit_reason": exit_reason,
        })

    if position == 1:
        ret = closes[-1] / entry_price - 1.0
        trades.append({
            "rule": rule.name,
            "entry_date": dates.iloc[entry_idx],
            "exit_date": dates.iloc[-1],
            "entry_price": entry_price,
            "exit_price": closes[-1],
            "hold_days": len(df) - 1 - entry_idx,
            "return_pct": (ret - fee) * 100.0,
            "exit_reason": "final",
        })

    daily = pd.DataFrame(daily_rows)
    trade_df = pd.DataFrame(trades)
    returns = trade_df["return_pct"] if not trade_df.empty else pd.Series(dtype=float)
    summary = {
        "rule": rule.name,
        "regime": rule.regime,
        "profit_trigger": rule.profit_trigger,
        "trail_drawdown": rule.trail_drawdown,
        "total_return_pct": (equity - 1.0) * 100.0,
        "max_drawdown_pct": max_dd * 100.0,
        "return_to_dd": ((equity - 1.0) / abs(max_dd)) if max_dd < 0 else np.nan,
        "trade_count": len(trade_df),
        "win_rate_pct": float((returns > 0).mean() * 100.0) if len(returns) else 0.0,
        "avg_trade_return_pct": float(returns.mean()) if len(returns) else 0.0,
        "median_trade_return_pct": float(returns.median()) if len(returns) else 0.0,
        "hold_days": int((daily["position"] == 1).sum()) if not daily.empty else 0,
        "trailing_exit_count": int((trade_df.get("exit_reason", pd.Series(dtype=str)) == "trailing_profit").sum()),
    }
    return summary, daily, trade_df


def period_metrics(daily: pd.DataFrame, start: pd.Timestamp | None, end: pd.Timestamp | None) -> dict[str, float]:
    part = daily.copy()
    if start is not None:
        part = part[part["trade_date"] >= start]
    if end is not None:
        part = part[part["trade_date"] <= end]
    if part.empty:
        return {"return_pct": np.nan, "max_drawdown_pct": np.nan, "hold_days": 0}
    equity = (1.0 + part["interval_ret"].astype(float)).cumprod()
    dd = equity / equity.cummax() - 1.0
    return {
        "return_pct": (equity.iloc[-1] - 1.0) * 100.0,
        "max_drawdown_pct": dd.min() * 100.0,
        "hold_days": int((part["position"] == 1).sum()),
    }


def annual_metrics(daily: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for year, part in daily.groupby(daily["trade_date"].dt.year):
        equity = (1.0 + part["interval_ret"].astype(float)).cumprod()
        dd = equity / equity.cummax() - 1.0
        rows.append({
            "rule": part["rule"].iloc[0],
            "year": int(year),
            "return_pct": (equity.iloc[-1] - 1.0) * 100.0,
            "max_drawdown_pct": dd.min() * 100.0,
            "hold_days": int((part["position"] == 1).sum()),
        })
    return pd.DataFrame(rows)


def candidate_rules() -> list[Rule]:
    regimes = [
        "none",
        "price_ma20_gt_ma60",
        "price_close_gt_ma60",
        "price_ma20_slope_pos",
        "price_ma60_slope_pos",
        "width_ma5_gt_ma20",
        "dual_width_ma5_gt_ma20",
        "avoid_low_range20",
        "trend_or_width",
        "trend_and_width",
    ]
    trailing = [(None, None)]
    trailing.extend(itertools.product([0.08, 0.10, 0.12, 0.15], [0.03, 0.05, 0.07]))

    rules = []
    for regime, (trigger, drawdown) in itertools.product(regimes, trailing):
        parts = [f"regime={regime}"]
        if trigger is not None:
            parts.append(f"trail={trigger:.0%}/{drawdown:.0%}")
        else:
            parts.append("trail=None")
        rules.append(Rule(
            name=";".join(parts),
            regime=regime,
            profit_trigger=trigger,
            trail_drawdown=drawdown,
        ))
    return rules


def main() -> int:
    OUT_DIR.mkdir(exist_ok=True)
    df = load_frame()
    summaries = []
    annual_rows = []
    trade_rows = []

    for rule in candidate_rules():
        summary, daily, trades = run_backtest(df, rule)
        train = period_metrics(daily, None, SPLIT_DATE)
        valid = period_metrics(daily, SPLIT_DATE + pd.Timedelta(days=1), None)
        summary.update({
            "train_return_pct": train["return_pct"],
            "train_max_drawdown_pct": train["max_drawdown_pct"],
            "valid_return_pct": valid["return_pct"],
            "valid_max_drawdown_pct": valid["max_drawdown_pct"],
            "valid_hold_days": valid["hold_days"],
        })
        summaries.append(summary)
        annual_rows.append(annual_metrics(daily))
        if not trades.empty:
            trade_rows.append(trades)

    summary_df = pd.DataFrame(summaries)
    summary_df["valid_return_to_dd"] = summary_df["valid_return_pct"] / summary_df["valid_max_drawdown_pct"].abs()
    summary_df["score"] = (
        summary_df["total_return_pct"]
        + summary_df["valid_return_pct"] * 2.0
        + summary_df["return_to_dd"] * 5.0
        - summary_df["max_drawdown_pct"].abs() * 5.0
    )
    summary_df = summary_df.sort_values(["score", "return_to_dd"], ascending=False)
    annual_df = pd.concat(annual_rows, ignore_index=True)
    trades_df = pd.concat(trade_rows, ignore_index=True) if trade_rows else pd.DataFrame()

    grid_path = OUT_DIR / "csi1000_regime_trailing_validation_grid.csv"
    annual_path = OUT_DIR / "csi1000_regime_trailing_validation_annual.csv"
    trades_path = OUT_DIR / "csi1000_regime_trailing_validation_trades.csv"
    selected_path = OUT_DIR / "csi1000_regime_trailing_validation_selected.csv"
    summary_df.to_csv(grid_path, index=False)
    annual_df.to_csv(annual_path, index=False)
    trades_df.to_csv(trades_path, index=False)

    selected = summary_df[
        (summary_df["valid_return_pct"] > 0)
        & (summary_df["valid_hold_days"] >= 15)
        & (summary_df["max_drawdown_pct"] >= -14)
        & (summary_df["trade_count"].between(15, 120))
    ].head(30)
    selected.to_csv(selected_path, index=False)

    cols = [
        "rule",
        "total_return_pct",
        "max_drawdown_pct",
        "return_to_dd",
        "valid_return_pct",
        "valid_max_drawdown_pct",
        "trade_count",
        "win_rate_pct",
        "hold_days",
        "trailing_exit_count",
    ]
    print(f"sample={df.trade_date.min().date()}..{df.trade_date.max().date()} rows={len(df)}")
    print(f"grid_csv={grid_path}")
    print(f"annual_csv={annual_path}")
    print(f"trades_csv={trades_path}")
    print(f"selected_csv={selected_path}")
    print("\nBaseline and best:")
    baseline = summary_df[summary_df["rule"] == "regime=none;trail=None"]
    print(pd.concat([baseline, summary_df.head(20)])[cols].drop_duplicates().to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
