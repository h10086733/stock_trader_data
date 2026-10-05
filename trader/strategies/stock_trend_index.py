"""个股趋势预测聚合为指数趋势的研究策略。

每天只使用当日收盘及以前数据，为每只成分股生成趋势分数，再按指数成分
股聚合为多空信号。该模块独立于中证1000择时生产策略，不写入其交易表。
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from trader.core.config import DB_PATH


@dataclass(frozen=True)
class TrendConfig:
    fast_days: int = 5
    slow_days: int = 20
    momentum_days: int = 60
    long_threshold: float = 0.15
    short_threshold: float = -0.15
    min_stocks: int = 100
    fee_bps: float = 2.0


def load_stock_panel(conn: sqlite3.Connection, index_code: str,
                     start: str, end: str) -> pd.DataFrame:
    """读取指数当前成分股行情。历史成分变化需另行接入。"""
    return pd.read_sql_query(
        """SELECT p.code, p.trade_date, p.open, p.close, p.high, p.low,
                          p.amount, p.pct_change
           FROM daily_prices p
           JOIN index_constituents ic ON ic.stock_code = p.code
          WHERE ic.index_code = ? AND p.trade_date BETWEEN ? AND ?
          ORDER BY p.code, p.trade_date""",
        conn, params=(str(index_code), start, end),
    )


def score_stock_trends(panel: pd.DataFrame, config: TrendConfig) -> pd.DataFrame:
    """为每只股票生成[-1,1]趋势分数，并计算下一交易日收益。"""
    if panel.empty:
        return panel
    df = panel.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    for col in ("close", "high", "low", "amount", "pct_change"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["code", "trade_date", "close"])
    g = df.groupby("code", sort=False)
    df["ret_fast"] = g["close"].pct_change(config.fast_days)
    df["ret_slow"] = g["close"].pct_change(config.slow_days)
    df["ret_momentum"] = g["close"].pct_change(config.momentum_days)
    df["ma_fast"] = g["close"].transform(lambda x: x.rolling(config.fast_days, min_periods=config.fast_days).mean())
    df["ma_slow"] = g["close"].transform(lambda x: x.rolling(config.slow_days, min_periods=config.slow_days).mean())
    df["trend_ma"] = df["ma_fast"] / df["ma_slow"] - 1.0
    # 三个方向信号等权：短期、波段、长期动量。
    df["trend_score"] = (
        np.sign(df["ret_fast"]) + np.sign(df["ret_slow"]) + np.sign(df["ret_momentum"])
        + np.sign(df["trend_ma"])
    ) / 4.0
    df["next_ret"] = g["close"].shift(-1) / df["close"] - 1.0
    # 异常复权/停牌跳变截断，避免单只股票污染指数聚合。
    df["next_ret"] = df["next_ret"].clip(-0.30, 0.30)
    return df


def aggregate_index_trend(scored: pd.DataFrame, config: TrendConfig) -> pd.DataFrame:
    if scored.empty:
        return scored
    valid = scored.dropna(subset=["trend_score", "next_ret"])
    daily = valid.groupby("trade_date").agg(
        trend_score=("trend_score", "mean"),
        breadth=("trend_score", lambda x: float((x > 0).mean())),
        stock_count=("trend_score", "count"),
        next_ret=("next_ret", "mean"),
    ).reset_index().sort_values("trade_date")
    daily["signal"] = np.where(
        (daily["stock_count"] >= config.min_stocks) & (daily["trend_score"] >= config.long_threshold), "LONG",
        np.where((daily["stock_count"] >= config.min_stocks) & (daily["trend_score"] <= config.short_threshold), "SHORT", "FLAT"),
        )
    daily["strategy_ret"] = np.select(
        [daily["signal"].eq("LONG"), daily["signal"].eq("SHORT")],
        [daily["next_ret"], -daily["next_ret"]], default=0.0,
    )
    daily["strategy_ret"] -= np.where(daily["signal"] != daily["signal"].shift(1), config.fee_bps / 10000.0, 0.0)
    daily["equity"] = (1.0 + daily["strategy_ret"].fillna(0)).cumprod()
    daily["peak"] = daily["equity"].cummax()
    daily["drawdown"] = daily["equity"] / daily["peak"] - 1.0
    return daily


def summarize(daily: pd.DataFrame, config: TrendConfig) -> dict[str, Any]:
    if daily.empty:
        return {"days": 0, "config": asdict(config)}
    active = daily[daily["signal"] != "FLAT"]
    return {
        "start": daily.trade_date.min().strftime("%Y-%m-%d"),
        "end": daily.trade_date.max().strftime("%Y-%m-%d"),
        "days": len(daily), "active_days": len(active),
        "total_return_pct": (daily.equity.iloc[-1] - 1.0) * 100.0,
        "buy_hold_proxy_pct": ((1.0 + daily.next_ret.fillna(0)).prod() - 1.0) * 100.0,
        "max_drawdown_pct": daily.drawdown.min() * 100.0,
        "signal_days_long": int((daily.signal == "LONG").sum()),
        "signal_days_short": int((daily.signal == "SHORT").sum()),
        "direction_accuracy_pct": float((
            ((daily.signal == "LONG") & (daily.next_ret > 0)) |
            ((daily.signal == "SHORT") & (daily.next_ret < 0))
        ).sum() / len(active) * 100.0) if len(active) else 0.0,
        "config": asdict(config),
    }


def run(conn: sqlite3.Connection, index_code: str, start: str, end: str,
        config: TrendConfig = TrendConfig()) -> tuple[dict[str, Any], pd.DataFrame]:
    panel = load_stock_panel(conn, index_code, start, end)
    scored = score_stock_trends(panel, config)
    daily = aggregate_index_trend(scored, config)
    return summarize(daily, config), daily


def main() -> int:
    p = argparse.ArgumentParser(description="个股趋势预测聚合指数多空回测")
    p.add_argument("--db", default=str(DB_PATH)); p.add_argument("--index-code", default="000852")
    p.add_argument("--start", default="2016-06-20"); p.add_argument("--end", default="2026-06-22")
    p.add_argument("--long-threshold", type=float, default=0.15); p.add_argument("--short-threshold", type=float, default=-0.15)
    p.add_argument("--min-stocks", type=int, default=100)
    args = p.parse_args(); cfg = TrendConfig(long_threshold=args.long_threshold, short_threshold=args.short_threshold, min_stocks=args.min_stocks)
    conn = sqlite3.connect(args.db)
    try:
        summary, _ = run(conn, args.index_code, args.start, args.end, cfg)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    finally: conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
