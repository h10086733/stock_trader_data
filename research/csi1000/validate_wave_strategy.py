"""20日线波段策略的可核对审计与次日开盘压力回测。

这个脚本不修改生产策略或交易表。它针对研究版 ``new_wave`` 输出两类证据：

1. 信号审计表：每个指数多空信号的成分股名单、波段极值、MA20确认点，以及
   ``极值右侧4日`` 和确认点的日期。可逐笔检查是否使用了未来数据。
2. 保守成交回测：信号在 D 日收盘后才能得到，因此在 D+1 日开盘切换仓位；
   同时按不同单边总成本（手续费+滑点）压力测试。

注意：当前成分股回看仍有生存偏差；本脚本验证的是信号日期与成交口径，不能消除
历史成分股缺失造成的偏差，也不能单独证明“连续同类极值合并”是因果的。后者必须
使用逐日状态机重新实现和回测，不能以本文件的结果宣称策略已通过未来函数验证。
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from trader.core.config import DB_PATH
from research.csi1000.compare_existing_and_wave import new_target_frame


def load_ohlc(conn: sqlite3.Connection, index_code: str, start: str, end: str) -> pd.DataFrame:
    prices = pd.read_sql_query(
        """SELECT trade_date, open, close
             FROM index_prices
            WHERE index_code=? AND trade_date BETWEEN ? AND ?
            ORDER BY trade_date""",
        conn,
        params=(index_code, start, end),
    )
    prices["trade_date"] = pd.to_datetime(prices["trade_date"])
    for col in ("open", "close"):
        prices[col] = pd.to_numeric(prices[col], errors="coerce")
    return prices.dropna(subset=["open", "close"]).reset_index(drop=True)


def annotate_waves(waves: pd.DataFrame, trade_dates: pd.Series) -> pd.DataFrame:
    """补充极值右侧4日可知日期，并检查每条个股事件的时序。"""
    if waves.empty:
        return waves
    out = waves.copy()
    # end_ready_date 已按个股自身的后4根有效日线生成，保留参数仅兼容早期调用。
    _ = trade_dates
    out["end_ready_date"] = pd.to_datetime(out["end_ready_date"])
    out["confirm_date"] = pd.to_datetime(out["confirm_date"])
    out["execute_date"] = pd.to_datetime(out["execute_date"])
    out["end_date"] = pd.to_datetime(out["end_date"])
    out["timing_ok"] = (
        (out["execute_date"] >= out["end_ready_date"])
        & (out["execute_date"] >= out["confirm_date"])
    )
    out["signal"] = np.where(out["type"].eq("FALL"), "LONG", "SHORT")
    return out


def build_audit_tables(new: pd.DataFrame, waves: pd.DataFrame, index_code: str) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """构造指数信号表以及每个信号对应的个股波段明细。"""
    columns = [
        "trade_date", "long_count", "short_count", "consensus_need", "new_signal", "new_target",
    ]
    daily = new.reindex(columns=columns).copy()
    daily = daily[daily["new_signal"].isin(["LONG", "SHORT"])].copy()
    daily = daily.rename(columns={"trade_date": "signal_date", "new_signal": "signal"})
    daily["signal_date"] = pd.to_datetime(daily["signal_date"])
    daily["next_open_date"] = daily["signal_date"].shift(-1)

    # next_open_date 不能用下一条信号日期，而是指数日线下一交易日。
    all_dates = pd.Series(new["trade_date"].drop_duplicates().sort_values().to_list())
    pos = {pd.Timestamp(d): i for i, d in enumerate(all_dates)}
    daily["next_open_date"] = daily["signal_date"].map(
        lambda d: all_dates.iloc[pos[pd.Timestamp(d)] + 1]
        if pos[pd.Timestamp(d)] + 1 < len(all_dates) else pd.NaT
    )
    daily["index_code"] = index_code
    daily["count_ok"] = np.where(
        daily["signal"].eq("LONG"),
        daily["long_count"] >= daily["consensus_need"],
        daily["short_count"] >= daily["consensus_need"],
    )

    component = waves.merge(
        daily[["signal_date", "signal", "long_count", "short_count", "consensus_need", "next_open_date"]],
        left_on=["execute_date", "signal"],
        right_on=["signal_date", "signal"],
        how="inner",
    )
    component = component.sort_values(["signal_date", "signal", "code"]).reset_index(drop=True)
    selected_columns = [
        "signal_date", "next_open_date", "signal", "long_count", "short_count", "consensus_need",
        "code", "type", "wave_pct", "start_date", "start_price", "confirm_date", "confirm_price",
        "end_date", "end_price", "end_ready_date", "execute_date", "timing_ok",
    ]
    component = component.reindex(columns=selected_columns)
    # 只有实际同向的成分股会出现在该表；每个日期的数量应与计数器完全一致。
    actual = component.groupby(["signal_date", "signal"], as_index=False).agg(component_count=("code", "nunique"))
    daily = daily.merge(actual, on=["signal_date", "signal"], how="left")
    daily["component_count"] = daily["component_count"].fillna(0).astype(int)
    daily["component_count_ok"] = np.where(
        daily["signal"].eq("LONG"),
        daily["component_count"].eq(daily["long_count"]),
        daily["component_count"].eq(daily["short_count"]),
    )
    check = {
        "signal_days": int(len(daily)),
        "component_rows": int(len(component)),
        "all_component_timing_ok": bool(component["timing_ok"].all()) if not component.empty else True,
        "all_signal_counts_ok": bool(daily["count_ok"].all()) if not daily.empty else True,
        "all_component_counts_ok": bool(daily["component_count_ok"].all()) if not daily.empty else True,
    }
    return daily, component, check


def simulate_next_open(prices: pd.DataFrame, targets: pd.DataFrame, cost_bps: float) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    """D日收盘出信号、D+1日开盘成交的多空回测。

    隔夜收益归属于旧仓位；开盘平/开仓后，当日开盘到收盘收益归属于新仓位。
    ``cost_bps`` 是每一次单边成交的总成本，反手等于两次单边成交。
    """
    d = prices.merge(targets[["trade_date", "new_target", "new_signal"]], on="trade_date", how="left")
    d = d.sort_values("trade_date").reset_index(drop=True)
    d["new_target"] = d["new_target"].fillna("FLAT")
    d["new_signal"] = d["new_signal"].fillna("NONE")
    cost = cost_bps / 10000.0
    equity = 1.0
    position = "FLAT"
    pending_target = "FLAT"
    rows: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    total_sides = 0

    for i, r in d.iterrows():
        date = pd.Timestamp(r["trade_date"])
        open_price = float(r["open"])
        close_price = float(r["close"])
        prev_close = float(d.loc[i - 1, "close"]) if i else open_price

        # 昨日收盘到今日开盘，仍是旧仓位。
        overnight_ret = (
            open_price / prev_close - 1.0 if position == "LONG" else
            prev_close / open_price - 1.0 if position == "SHORT" else 0.0
        )
        equity *= 1.0 + overnight_ret

        sides_today = 0
        action = "HOLD"
        # 昨日信号在今日开盘成交。
        if pending_target != position:
            if current is not None:
                gross = (
                    open_price / current["entry_price"] - 1.0
                    if current["direction"] == "LONG"
                    else current["entry_price"] / open_price - 1.0
                )
                current.update(
                    exit_date=date.strftime("%Y-%m-%d"),
                    exit_price=open_price,
                    return_pct=(gross - 2.0 * cost) * 100.0,
                    hold_days=i - current["entry_index"],
                )
                trades.append(current)
                equity *= 1.0 - cost
                total_sides += 1; sides_today += 1
                current = None
            if pending_target in ("LONG", "SHORT"):
                position = pending_target
                equity *= 1.0 - cost
                total_sides += 1; sides_today += 1
                current = {
                    "direction": position, "entry_date": date.strftime("%Y-%m-%d"),
                    "entry_price": open_price, "entry_index": i,
                }
                action = "OPEN" if sides_today == 1 else "REVERSE"
            else:
                position = "FLAT"
                action = "CLOSE"

        intraday_ret = (
            close_price / open_price - 1.0 if position == "LONG" else
            open_price / close_price - 1.0 if position == "SHORT" else 0.0
        )
        equity *= 1.0 + intraday_ret
        # 今日收盘才能产生，明天开盘才执行。
        pending_target = r["new_target"] if r["new_target"] in ("LONG", "SHORT", "FLAT") else "FLAT"
        rows.append({
            "trade_date": date, "open": open_price, "close": close_price, "position_close": position,
            "signal_at_close": r["new_signal"], "pending_target": pending_target, "action_at_open": action,
            "overnight_ret": overnight_ret, "intraday_ret": intraday_ret, "sides": sides_today, "equity": equity,
        })

    # 期末按最后收盘价平仓，便于与已结算交易收益比较。
    if current is not None:
        last = d.iloc[-1]
        close = float(last["close"])
        gross = close / current["entry_price"] - 1.0 if current["direction"] == "LONG" else current["entry_price"] / close - 1.0
        current.update(
            exit_date=pd.Timestamp(last["trade_date"]).strftime("%Y-%m-%d"), exit_price=close,
            return_pct=(gross - 2.0 * cost) * 100.0, hold_days=len(d) - 1 - current["entry_index"],
        )
        trades.append(current)
        equity *= 1.0 - cost
        total_sides += 1

    daily = pd.DataFrame(rows)
    daily["peak"] = daily["equity"].cummax()
    daily["drawdown"] = daily["equity"] / daily["peak"] - 1.0
    trade_df = pd.DataFrame(trades)
    result = {
        "execution": "信号日收盘后生成，下一交易日开盘成交；隔夜收益属于旧仓位",
        "cost_per_side_bps": cost_bps,
        "start": daily.iloc[0]["trade_date"].strftime("%Y-%m-%d"),
        "end": daily.iloc[-1]["trade_date"].strftime("%Y-%m-%d"),
        "total_return_pct": float((equity - 1.0) * 100.0),
        "max_drawdown_pct": float(daily["drawdown"].min() * 100.0),
        "trade_count": int(len(trade_df)),
        "transaction_sides": int(total_sides),
        "win_rate_pct": float((trade_df["return_pct"] > 0).mean() * 100.0) if not trade_df.empty else 0.0,
        "long_days": int((daily["position_close"] == "LONG").sum()),
        "short_days": int((daily["position_close"] == "SHORT").sum()),
        "flat_days": int((daily["position_close"] == "FLAT").sum()),
    }
    return result, daily, trade_df


def main() -> int:
    parser = argparse.ArgumentParser(description="波段策略审计与次日开盘压力回测")
    parser.add_argument("--db", default=str(DB_PATH))
    parser.add_argument("--index-code", default="000852")
    parser.add_argument("--start", default="2016-06-20")
    parser.add_argument("--end", default="2026-06-22")
    parser.add_argument("--min-wave", type=float, default=10.0)
    parser.add_argument("--consensus", type=float, default=2.0)
    parser.add_argument("--out-dir", default="outputs/csi1000_wave_validation")
    args = parser.parse_args()
    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db)
    try:
        new, raw_waves = new_target_frame(conn, args.index_code, args.start, args.end, args.min_wave, args.consensus)
        prices = load_ohlc(conn, args.index_code, args.start, args.end)
    finally:
        conn.close()

    new["trade_date"] = pd.to_datetime(new["trade_date"])
    # 仅保留实际回测窗口，避免为计算均线引入的前置日期出现在审计结果中。
    new = new[(new["trade_date"] >= pd.Timestamp(args.start)) & (new["trade_date"] <= pd.Timestamp(args.end))].copy()
    waves = annotate_waves(raw_waves, pd.Series(new["trade_date"].drop_duplicates().sort_values().to_list()))
    signal_daily, components, checks = build_audit_tables(new, waves, args.index_code)
    signal_daily.to_csv(out_dir / "signal_audit_daily.csv", index=False, encoding="utf-8-sig")
    components.to_csv(out_dir / "signal_audit_components.csv", index=False, encoding="utf-8-sig")

    reports: dict[str, Any] = {
        "meta": {"index_code": args.index_code, "start": args.start, "end": args.end, "min_wave_pct": args.min_wave, "consensus_pct": args.consensus},
        "audit_checks": checks,
        "limitations": [
            "audit_checks 仅验证每条已输出事件的日期排序；不等于连续同类极值合并已通过因果验证。",
            "波段生成仍使用当前成分股回看，缺少历史成分股快照，存在生存偏差。",
            "次日开盘回测使用指数OHLC模拟，未包含实际ETF/期货的基差、展期、融券或冲击成本。",
        ],
        "next_open_backtests": {},
    }
    # 每个情景的逐日权益曲线和逐笔交易都独立落盘，便于检查具体差异。
    for cost_bps in (2.0, 10.0, 20.0):
        result, daily, trades = simulate_next_open(prices, new, cost_bps)
        key = f"cost_{int(cost_bps)}bps_per_side"
        reports["next_open_backtests"][key] = result
        daily.to_csv(out_dir / f"next_open_daily_{int(cost_bps)}bps.csv", index=False, encoding="utf-8-sig")
        trades.to_csv(out_dir / f"next_open_trades_{int(cost_bps)}bps.csv", index=False, encoding="utf-8-sig")
    (out_dir / "validation_summary.json").write_text(json.dumps(reports, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(reports, ensure_ascii=False, indent=2))
    print(f"saved={out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
