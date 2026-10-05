"""20日线确认点聚合策略（研究版）。

可交易定义：
* 收盘价上穿20日均线：确认此前一段下跌；若跌幅达到 ``swing_threshold``，记一只
  个股反弹信号；
* 收盘价下穿20日均线：确认此前一段上涨；若涨幅达到阈值，记一只个股回撤信号；
* 同日个股信号按成分股比例聚合，达到 ``consensus`` 才改变指数仓位；没有新确认
  点时保持原仓位。信号在收盘生成，收益从下一交易日指数收盘开始计算。

历史成分快照尚未接入时，结果是当前成分股回看，只用于研究参数，不代表无生存偏差
的实盘收益。
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass, asdict
from typing import Any

import numpy as np
import pandas as pd

from trader.core.config import DB_PATH


@dataclass(frozen=True)
class ConfirmationConfig:
    ma_days: int = 20
    swing_threshold_pct: float = 10.0
    consensus_pct: float = 10.0
    fee_bps: float = 2.0


def load_stock_panel(conn: sqlite3.Connection, index_code: str,
                     start: str, end: str) -> pd.DataFrame:
    return pd.read_sql_query(
        """SELECT p.code, p.trade_date, p.close
             FROM daily_prices p
             JOIN index_constituents ic ON ic.stock_code = p.code
            WHERE ic.index_code = ? AND p.trade_date BETWEEN ? AND ?
              AND p.close IS NOT NULL AND p.close > 0
            ORDER BY p.code, p.trade_date""",
        conn, params=(str(index_code), start, end),
    )


def stock_confirmation_events(stock: pd.DataFrame, config: ConfirmationConfig) -> list[dict[str, Any]]:
    if stock.empty:
        return []
    d = stock.sort_values("trade_date").copy().reset_index(drop=True)
    d["trade_date"] = pd.to_datetime(d["trade_date"])
    d["close"] = pd.to_numeric(d["close"], errors="coerce")
    d = d.dropna(subset=["close"]).reset_index(drop=True)
    if len(d) < config.ma_days + 2:
        return []
    d["ma"] = d["close"].rolling(config.ma_days, min_periods=config.ma_days).mean()
    d = d.dropna(subset=["ma"]).reset_index(drop=True)
    above = d["close"] > d["ma"]
    prev = above.shift(1, fill_value=False)
    crosses: list[tuple[int, str]] = []
    for i in np.flatnonzero((above & ~prev).to_numpy()):
        crosses.append((int(i), "UP"))
    for i in np.flatnonzero((~above & prev).to_numpy()):
        crosses.append((int(i), "DOWN"))
    crosses.sort()
    # 交替确认点；同方向重复点不会切换波段。
    turns: list[tuple[int, str]] = []
    for item in crosses:
        if turns and turns[-1][1] == item[1]:
            continue
        turns.append(item)
    events: list[dict[str, Any]] = []
    # 当前 UP 用上一 UP 至当前 UP 的完整下跌波段；当前 DOWN 同理。
    for j in range(1, len(turns)):
        cur_i, cur_dir = turns[j]
        prior_same = next((x for x in range(j - 1, -1, -1) if turns[x][1] == cur_dir), None)
        if prior_same is None:
            continue
        start_i = turns[prior_same][0]
        segment = d.iloc[start_i:cur_i + 1]
        if cur_dir == "UP":
            # 先找高点，再找其后的低点，避免把反弹前的旧低点混入。
            low_rel = int(segment["close"].to_numpy().argmin())
            high_rel = int(segment.iloc[:low_rel + 1]["close"].to_numpy().argmax())
            high = float(segment.iloc[high_rel]["close"])
            low = float(segment.iloc[low_rel:]["close"].min())
            swing = (1.0 - low / high) * 100.0 if high > 0 else 0.0
            if swing >= config.swing_threshold_pct:
                events.append({
                    "trade_date": d.loc[cur_i, "trade_date"], "signal": "LONG",
                    "swing_pct": swing, "high": high, "low": low,
                })
        else:
            high_rel = int(segment["close"].to_numpy().argmax())
            low_rel = int(segment.iloc[:high_rel + 1]["close"].to_numpy().argmin())
            low = float(segment.iloc[low_rel]["close"])
            high = float(segment.iloc[high_rel:]["close"].max())
            swing = (high / low - 1.0) * 100.0 if low > 0 else 0.0
            if swing >= config.swing_threshold_pct:
                events.append({
                    "trade_date": d.loc[cur_i, "trade_date"], "signal": "SHORT",
                    "swing_pct": swing, "high": high, "low": low,
                })
    return events


def build_signal_frame(panel: pd.DataFrame, config: ConfirmationConfig) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for code, stock in panel.groupby("code", sort=False):
        for event in stock_confirmation_events(stock, config):
            event["code"] = code
            records.append(event)
    if not records:
        return pd.DataFrame(columns=["trade_date", "long_count", "short_count", "eligible_count", "signal"])
    events = pd.DataFrame(records)
    daily = events.groupby("trade_date").agg(
        long_count=("signal", lambda x: int((x == "LONG").sum())),
        short_count=("signal", lambda x: int((x == "SHORT").sum())),
        eligible_count=("code", "nunique"),
        event_count=("code", "size"),
    ).reset_index().sort_values("trade_date")
    needed = max(1, int(np.ceil(daily["eligible_count"].max() * config.consensus_pct / 100.0)))
    # 以当天实际有事件的股票数作为分母，同时要求绝对数量不少于 consensus 对应的样本量。
    daily["long_ratio_pct"] = daily["long_count"] / daily["eligible_count"] * 100.0
    daily["short_ratio_pct"] = daily["short_count"] / daily["eligible_count"] * 100.0
    daily["signal"] = np.where(
        (daily["long_count"] >= needed) & (daily["long_count"] > daily["short_count"]), "LONG",
        np.where((daily["short_count"] >= needed) & (daily["short_count"] > daily["long_count"]), "SHORT", "FLAT"),
    )
    # 无事件日保持前一仓位；首次信号前为空仓。
    daily["position"] = daily["signal"].replace("FLAT", np.nan).ffill().fillna("FLAT")
    return daily


def backtest_index(conn: sqlite3.Connection, signal_frame: pd.DataFrame,
                   index_code: str, start: str, end: str,
                   config: ConfirmationConfig) -> dict[str, Any]:
    prices = pd.read_sql_query(
        """SELECT trade_date, close FROM index_prices
           WHERE index_code=? AND trade_date BETWEEN ? AND ? ORDER BY trade_date""",
        conn, params=(index_code, start, end),
    )
    if prices.empty or signal_frame.empty:
        return {"days": 0, "config": asdict(config)}
    prices["trade_date"] = pd.to_datetime(prices["trade_date"])
    prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
    d = prices.merge(signal_frame[["trade_date", "signal", "position", "long_count", "short_count"]], on="trade_date", how="left").sort_values("trade_date")
    d["signal"] = d["signal"].fillna("FLAT")
    d["position"] = d["position"].ffill().fillna("FLAT")
    d["next_ret"] = d["close"].shift(-1) / d["close"] - 1.0
    d["strategy_ret"] = np.select([d["position"] == "LONG", d["position"] == "SHORT"], [d["next_ret"], -d["next_ret"]], default=0.0)
    changed = d["position"] != d["position"].shift(1)
    d["strategy_ret"] -= np.where(changed & d["position"].isin(["LONG", "SHORT"]), config.fee_bps / 10000.0, 0.0)
    d["equity"] = (1.0 + d["strategy_ret"].fillna(0)).cumprod()
    peak = d["equity"].cummax()
    d["drawdown"] = d["equity"] / peak - 1.0
    active = d[d["position"].isin(["LONG", "SHORT"]) & d["next_ret"].notna()]
    accuracy = (((active.position == "LONG") & (active.next_ret > 0)) | ((active.position == "SHORT") & (active.next_ret < 0))).mean() * 100 if len(active) else 0.0
    return {
        "start": d.trade_date.min().strftime("%Y-%m-%d"), "end": d.trade_date.max().strftime("%Y-%m-%d"),
        "days": len(d), "active_days": len(active), "total_return_pct": (d.equity.iloc[-1] - 1.0) * 100.0,
        "buy_hold_pct": (d.close.iloc[-1] / d.close.iloc[0] - 1.0) * 100.0,
        "max_drawdown_pct": d.drawdown.min() * 100.0,
        "long_days": int((d.position == "LONG").sum()), "short_days": int((d.position == "SHORT").sum()),
        "switches": int((changed & d.position.isin(["LONG", "SHORT"])).sum()), "direction_accuracy_pct": float(accuracy),
        "config": asdict(config),
    }


def run(conn: sqlite3.Connection, index_code: str, start: str, end: str,
        config: ConfirmationConfig) -> dict[str, Any]:
    # 前置一年用于20日线和首个完整波段，最终回测仍按 start/end 输出。
    panel = load_stock_panel(conn, index_code, "2015-01-01", end)
    signals = build_signal_frame(panel, config)
    result = backtest_index(conn, signals, index_code, start, end, config)
    result["event_days"] = int(len(signals))
    result["event_long_days"] = int((signals.signal == "LONG").sum()) if not signals.empty else 0
    result["event_short_days"] = int((signals.signal == "SHORT").sum()) if not signals.empty else 0
    result["stocks"] = int(panel.code.nunique()) if not panel.empty else 0
    return result


def main() -> int:
    p = argparse.ArgumentParser(description="个股20日线确认点聚合指数多空回测")
    p.add_argument("--db", default=str(DB_PATH)); p.add_argument("--index-code", default="000852")
    p.add_argument("--start", default="2016-06-20"); p.add_argument("--end", default="2026-06-22")
    p.add_argument("--swing-threshold", type=float, default=10.0); p.add_argument("--consensus", type=float, default=10.0)
    args = p.parse_args(); cfg = ConfirmationConfig(swing_threshold_pct=args.swing_threshold, consensus_pct=args.consensus)
    conn = sqlite3.connect(args.db)
    try: print(json.dumps(run(conn, args.index_code, args.start, args.end, cfg), ensure_ascii=False, indent=2))
    finally: conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
