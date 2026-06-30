from __future__ import annotations

import itertools
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR))

from scripts.csi1000_regime_trailing_validation import Rule, load_frame, period_metrics, run_backtest  # noqa: E402


OUT_DIR = BASE_DIR / "outputs"


def metric_score(row: pd.Series) -> float:
    return_col = "return_pct" if "return_pct" in row.index else "total_return_pct"
    return (
        float(row[return_col])
        + float(row["return_to_dd"]) * 5.0
        - abs(float(row["max_drawdown_pct"])) * 4.0
    )


def score_values(return_pct: float, return_to_dd: float, max_drawdown_pct: float) -> float:
    return return_pct + return_to_dd * 5.0 - abs(max_drawdown_pct) * 4.0


def make_rule(trigger: float | None, drawdown: float | None, regime: str = "none") -> Rule:
    if trigger is None or drawdown is None:
        name = f"regime={regime};trail=None"
    else:
        name = f"regime={regime};trail={trigger:.1%}/{drawdown:.1%}"
    return Rule(name=name, regime=regime, profit_trigger=trigger, trail_drawdown=drawdown)


def period_summary(daily: pd.DataFrame, start: pd.Timestamp | None, end: pd.Timestamp | None) -> dict[str, float]:
    metrics = period_metrics(daily, start, end)
    ret = metrics["return_pct"]
    dd = metrics["max_drawdown_pct"]
    metrics["return_to_dd"] = ret / abs(dd) if dd < 0 else np.nan
    return metrics


def trade_contribution(trades: pd.DataFrame) -> dict[str, float]:
    if trades.empty:
        return {
            "trade_count": 0,
            "top1_trade_pct": 0.0,
            "top3_trade_pct": 0.0,
            "top5_trade_pct": 0.0,
            "bottom3_trade_pct": 0.0,
        }
    returns = trades["return_pct"].astype(float).sort_values(ascending=False)
    return {
        "trade_count": len(returns),
        "top1_trade_pct": float(returns.head(1).sum()),
        "top3_trade_pct": float(returns.head(3).sum()),
        "top5_trade_pct": float(returns.head(5).sum()),
        "bottom3_trade_pct": float(returns.tail(3).sum()),
    }


def main() -> int:
    OUT_DIR.mkdir(exist_ok=True)
    df = load_frame()

    triggers = [None] + [x / 100.0 for x in range(5, 16)]
    drawdowns = [None] + [x / 1000.0 for x in range(20, 61, 5)]
    rules = [make_rule(None, None)]
    rules.extend(make_rule(t, d) for t, d in itertools.product(triggers[1:], drawdowns[1:]))

    full_rows = []
    annual_rows = []
    trade_rows = []
    daily_by_rule: dict[str, pd.DataFrame] = {}
    trades_by_rule: dict[str, pd.DataFrame] = {}
    for rule in rules:
        summary, daily, trades = run_backtest(df, rule)
        daily_by_rule[rule.name] = daily
        trades_by_rule[rule.name] = trades
        summary.update(trade_contribution(trades))
        full_rows.append(summary)
        for year, part in daily.groupby(daily["trade_date"].dt.year):
            metrics = period_summary(part, None, None)
            annual_rows.append({
                "rule": rule.name,
                "profit_trigger": rule.profit_trigger,
                "trail_drawdown": rule.trail_drawdown,
                "year": int(year),
                "return_pct": metrics["return_pct"],
                "max_drawdown_pct": metrics["max_drawdown_pct"],
                "return_to_dd": metrics["return_to_dd"],
                "hold_days": metrics["hold_days"],
            })
        if not trades.empty:
            trade_rows.append(trades)

    full_df = pd.DataFrame(full_rows)
    full_df["score"] = full_df.apply(metric_score, axis=1)
    full_df = full_df.sort_values(["score", "return_to_dd"], ascending=False)
    annual_df = pd.DataFrame(annual_rows)
    trades_df = pd.concat(trade_rows, ignore_index=True) if trade_rows else pd.DataFrame()

    # Walk-forward: choose the best params on all data before each test year,
    # then evaluate the selected params on that single following year.
    wf_rows = []
    years = sorted(df["trade_date"].dt.year.unique())
    for test_year in years:
        if test_year < 2020:
            continue
        train_end = pd.Timestamp(f"{test_year - 1}-12-31")
        test_start = pd.Timestamp(f"{test_year}-01-01")
        test_end = pd.Timestamp(f"{test_year}-12-31")
        train_scores = []
        for rule in rules:
            daily = daily_by_rule[rule.name]
            metrics = period_summary(daily, None, train_end)
            if metrics["hold_days"] < 30 or pd.isna(metrics["return_to_dd"]):
                continue
            train_scores.append({
                "rule": rule.name,
                "profit_trigger": rule.profit_trigger,
                "trail_drawdown": rule.trail_drawdown,
                "train_return_pct": metrics["return_pct"],
                "train_max_drawdown_pct": metrics["max_drawdown_pct"],
                "train_return_to_dd": metrics["return_to_dd"],
                "train_score": score_values(
                    metrics["return_pct"],
                    metrics["return_to_dd"],
                    metrics["max_drawdown_pct"],
                ),
            })
        if not train_scores:
            continue
        selected = pd.DataFrame(train_scores).sort_values(["train_score", "train_return_to_dd"], ascending=False).iloc[0]
        test_metrics = period_summary(daily_by_rule[selected["rule"]], test_start, test_end)
        baseline_metrics = period_summary(daily_by_rule["regime=none;trail=None"], test_start, test_end)
        wf_rows.append({
            "test_year": test_year,
            "selected_rule": selected["rule"],
            "selected_trigger": selected["profit_trigger"],
            "selected_drawdown": selected["trail_drawdown"],
            "train_return_pct": selected["train_return_pct"],
            "train_max_drawdown_pct": selected["train_max_drawdown_pct"],
            "test_return_pct": test_metrics["return_pct"],
            "test_max_drawdown_pct": test_metrics["max_drawdown_pct"],
            "baseline_test_return_pct": baseline_metrics["return_pct"],
            "baseline_test_max_drawdown_pct": baseline_metrics["max_drawdown_pct"],
            "excess_return_pct": test_metrics["return_pct"] - baseline_metrics["return_pct"],
        })

    wf_df = pd.DataFrame(wf_rows)

    robust = full_df[
        (full_df["profit_trigger"].notna())
        & (full_df["trail_drawdown"].between(0.025, 0.04))
        & (full_df["profit_trigger"].between(0.06, 0.12))
    ].copy()

    paths = {
        "grid": OUT_DIR / "csi1000_trailing_robustness_grid.csv",
        "annual": OUT_DIR / "csi1000_trailing_robustness_annual.csv",
        "walk_forward": OUT_DIR / "csi1000_trailing_robustness_walk_forward.csv",
        "trades": OUT_DIR / "csi1000_trailing_robustness_trades.csv",
        "robust_zone": OUT_DIR / "csi1000_trailing_robustness_zone.csv",
    }
    full_df.to_csv(paths["grid"], index=False)
    annual_df.to_csv(paths["annual"], index=False)
    wf_df.to_csv(paths["walk_forward"], index=False)
    trades_df.to_csv(paths["trades"], index=False)
    robust.to_csv(paths["robust_zone"], index=False)

    cols = [
        "rule",
        "profit_trigger",
        "trail_drawdown",
        "total_return_pct",
        "max_drawdown_pct",
        "return_to_dd",
        "trade_count",
        "trailing_exit_count",
        "top3_trade_pct",
        "bottom3_trade_pct",
    ]
    print(f"sample={df.trade_date.min().date()}..{df.trade_date.max().date()} rows={len(df)}")
    for key, path in paths.items():
        print(f"{key}_csv={path}")
    print("\nTop 20 full sample:")
    print(full_df[cols].head(20).to_string(index=False))
    print("\nRobust zone summary:")
    print(robust[["total_return_pct", "max_drawdown_pct", "return_to_dd", "trailing_exit_count"]].describe().to_string())
    print("\nWalk-forward:")
    print(wf_df.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
