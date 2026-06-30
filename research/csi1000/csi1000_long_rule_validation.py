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
EXCEL_PATH = BASE_DIR / "data" / "历史新高新低300和1000.xlsx"
OUT_DIR = BASE_DIR / "outputs"

FEE_BPS = 2.0
SPLIT_DATE = pd.Timestamp("2022-12-31")


@dataclass(frozen=True)
class LongRule:
    name: str
    csi_min: float
    hs300_min: float | None = None
    vol_min: float | None = None
    low10_min: float | None = None
    exit_csi_min: float | None = None
    exit_hs300_min: float | None = None
    stop_loss_pct: float | None = None
    take_profit_drawdown_pct: float | None = None


def load_frame() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    try:
        df = timing.load_db_price_excel_width_frame(conn, EXCEL_PATH)
    finally:
        conn.close()
    if df.empty:
        raise RuntimeError("没有可用的中证1000回测数据")

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


def build_base_signal(df: pd.DataFrame, rule: LongRule) -> pd.Series:
    signal = df["csi_score_ma3"] > rule.csi_min
    if rule.hs300_min is not None:
        signal &= df["hs300_score_ma3"] > rule.hs300_min
    if rule.vol_min is not None:
        signal &= df["vol_ratio_5_20"] > rule.vol_min
    if rule.low10_min is not None:
        signal &= df["price_from_low10"] >= rule.low10_min
    return signal.fillna(False)


def run_backtest(df: pd.DataFrame, rule: LongRule) -> tuple[dict[str, float], pd.DataFrame]:
    fee = FEE_BPS / 10000.0
    signal = build_base_signal(df, rule)
    opens = df["open"].to_numpy(dtype=float)
    closes = df["close"].to_numpy(dtype=float)
    dates = df["trade_date"]

    equity = 1.0
    peak = 1.0
    max_dd = 0.0
    position = 0
    entry_price = 0.0
    entry_equity_peak = 1.0
    entry_idx = 0
    pending = 0

    daily_rows: list[dict[str, float | str | pd.Timestamp]] = []
    trades: list[dict[str, float | str | pd.Timestamp | int]] = []

    for i in range(len(df) - 1):
        if pending != position:
            open_price = opens[i]
            if position == 1:
                ret = open_price / entry_price - 1.0
                net_ret = ret - fee
                trades.append({
                    "rule": rule.name,
                    "entry_date": dates.iloc[entry_idx],
                    "exit_date": dates.iloc[i],
                    "entry_price": entry_price,
                    "exit_price": open_price,
                    "hold_days": i - entry_idx,
                    "return_pct": net_ret * 100.0,
                })
                equity *= 1.0 - fee
            position = pending
            if position == 1:
                equity *= 1.0 - fee
                entry_price = open_price
                entry_idx = i
                entry_equity_peak = equity

        if position == 1:
            interval_ret = opens[i + 1] / opens[i] - 1.0
        else:
            interval_ret = 0.0
        equity *= 1.0 + interval_ret
        peak = max(peak, equity)
        max_dd = min(max_dd, equity / peak - 1.0)
        if position == 1:
            entry_equity_peak = max(entry_equity_peak, equity)

        target = 1 if signal.iloc[i] else 0
        if position == 1:
            exit_csi_min = rule.exit_csi_min if rule.exit_csi_min is not None else rule.csi_min
            exit_hs300_min = rule.exit_hs300_min if rule.exit_hs300_min is not None else rule.hs300_min
            if df["csi_score_ma3"].iloc[i] <= exit_csi_min:
                target = 0
            if exit_hs300_min is not None and df["hs300_score_ma3"].iloc[i] <= exit_hs300_min:
                target = 0
            if rule.stop_loss_pct is not None:
                entry_ret = opens[i + 1] / entry_price - 1.0
                if entry_ret <= -rule.stop_loss_pct:
                    target = 0
            if rule.take_profit_drawdown_pct is not None:
                trade_dd = equity / entry_equity_peak - 1.0
                if trade_dd <= -rule.take_profit_drawdown_pct and equity > entry_equity_peak * 0.98:
                    target = 0
        pending = target

        daily_rows.append({
            "rule": rule.name,
            "trade_date": dates.iloc[i],
            "position": position,
            "equity": equity,
            "interval_ret": interval_ret,
            "drawdown": equity / peak - 1.0,
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
        })

    daily = pd.DataFrame(daily_rows)
    trade_df = pd.DataFrame(trades)
    returns = trade_df["return_pct"] if not trade_df.empty else pd.Series(dtype=float)
    summary = {
        "rule": rule.name,
        "start": df["trade_date"].min().strftime("%Y-%m-%d"),
        "end": df["trade_date"].max().strftime("%Y-%m-%d"),
        "total_return_pct": (equity - 1.0) * 100.0,
        "max_drawdown_pct": max_dd * 100.0,
        "return_to_dd": ((equity - 1.0) / abs(max_dd)) if max_dd < 0 else np.nan,
        "trade_count": len(trade_df),
        "win_rate_pct": float((returns > 0).mean() * 100.0) if len(returns) else 0.0,
        "avg_trade_return_pct": float(returns.mean()) if len(returns) else 0.0,
        "median_trade_return_pct": float(returns.median()) if len(returns) else 0.0,
        "hold_days": int((daily["position"] == 1).sum()) if not daily.empty else 0,
        "flat_days": int((daily["position"] == 0).sum()) if not daily.empty else 0,
    }
    return summary, daily


def period_metrics(daily: pd.DataFrame, start: pd.Timestamp | None, end: pd.Timestamp | None) -> dict[str, float]:
    part = daily.copy()
    if start is not None:
        part = part[part["trade_date"] >= start]
    if end is not None:
        part = part[part["trade_date"] <= end]
    if part.empty:
        return {"return_pct": np.nan, "max_drawdown_pct": np.nan, "hold_days": 0}
    rets = part["interval_ret"].astype(float)
    equity = (1.0 + rets).cumprod()
    dd = equity / equity.cummax() - 1.0
    return {
        "return_pct": (equity.iloc[-1] - 1.0) * 100.0,
        "max_drawdown_pct": dd.min() * 100.0,
        "hold_days": int((part["position"] == 1).sum()),
    }


def annual_metrics(daily: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for year, part in daily.groupby(daily["trade_date"].dt.year):
        rets = part["interval_ret"].astype(float)
        equity = (1.0 + rets).cumprod()
        dd = equity / equity.cummax() - 1.0
        rows.append({
            "rule": part["rule"].iloc[0],
            "year": int(year),
            "return_pct": (equity.iloc[-1] - 1.0) * 100.0,
            "max_drawdown_pct": dd.min() * 100.0,
            "hold_days": int((part["position"] == 1).sum()),
        })
    return pd.DataFrame(rows)


def candidate_rules() -> list[LongRule]:
    rules = [
        LongRule("baseline_1000_gt20", 20),
        LongRule("baseline_1000_gt20_300_gt-10", 20, -10),
    ]
    for csi_min, hs300_min, vol_min, low10_min, exit_csi_min, exit_hs300_min, stop in itertools.product(
        [15, 20, 25],
        [-20, -10, 0],
        [None, 1.05, 1.10, 1.15],
        [None, 0.02, 0.04],
        [-10, 0],
        [-20, -10],
        [None, 0.08],
    ):
        if exit_csi_min >= csi_min:
            continue
        if hs300_min is not None and exit_hs300_min >= hs300_min:
            continue
        name = (
            f"csi>{csi_min}_hs>{hs300_min}_vol>{vol_min}_low>{low10_min}"
            f"_exit_csi<={exit_csi_min}_exit_hs<={exit_hs300_min}_stop{stop}"
        )
        rules.append(LongRule(
            name=name,
            csi_min=csi_min,
            hs300_min=hs300_min,
            vol_min=vol_min,
            low10_min=low10_min,
            exit_csi_min=exit_csi_min,
            exit_hs300_min=exit_hs300_min,
            stop_loss_pct=stop,
        ))
    return rules


def main() -> int:
    OUT_DIR.mkdir(exist_ok=True)
    df = load_frame()

    summaries = []
    annual = []
    for rule in candidate_rules():
        summary, daily = run_backtest(df, rule)
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
        annual.append(annual_metrics(daily))

    summary_df = pd.DataFrame(summaries)
    summary_df["valid_return_to_dd"] = (
        summary_df["valid_return_pct"] / summary_df["valid_max_drawdown_pct"].abs()
    )
    summary_df["score"] = (
        summary_df["total_return_pct"]
        + summary_df["valid_return_pct"] * 2.0
        + summary_df["return_to_dd"] * 5.0
        - summary_df["max_drawdown_pct"].abs() * 4.0
    )
    summary_df = summary_df.sort_values(
        ["score", "valid_return_pct", "return_to_dd"],
        ascending=False,
    )

    annual_df = pd.concat(annual, ignore_index=True)
    grid_path = OUT_DIR / "csi1000_long_rule_validation_grid.csv"
    annual_path = OUT_DIR / "csi1000_long_rule_validation_annual.csv"
    selected_path = OUT_DIR / "csi1000_long_rule_validation_selected.csv"
    summary_df.to_csv(grid_path, index=False)
    annual_df.to_csv(annual_path, index=False)

    selected = summary_df[
        (summary_df["valid_return_pct"] > 0)
        & (summary_df["valid_hold_days"] >= 20)
        & (summary_df["max_drawdown_pct"] >= -18)
        & (summary_df["trade_count"].between(20, 160))
    ].head(50)
    selected.to_csv(selected_path, index=False)

    print(f"sample={df.trade_date.min().date()}..{df.trade_date.max().date()} rows={len(df)}")
    print(f"grid_csv={grid_path}")
    print(f"annual_csv={annual_path}")
    print(f"selected_csv={selected_path}")
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
    ]
    print("\nBaselines:")
    print(summary_df[summary_df["rule"].str.startswith("baseline")][cols].to_string(index=False))
    print("\nSelected top 20:")
    print(selected[cols].head(20).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
