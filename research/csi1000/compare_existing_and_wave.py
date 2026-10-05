"""中证1000旧策略与20日线波段确认策略的详细10年对比。

本报告只读数据库，不修改 csi1000 生产信号。新策略使用：
9日局部高低点（前后各4日）、MA20 上下方过滤、同类极值合并、极值交替，
并在局部极值右侧4日完成后才执行确认点信号。组合策略以旧 low_dd 为主，
只在旧策略空仓时用新策略补充，用来判断新策略是否提供边际收益。

注意：该文件依赖全样本的连续极值合并，只保留为离线研究比较；它不能证明
新波段信号可实盘交易。无回头改写的验证请使用 ``causal_wave_strategy.py``。
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
from trader.strategies.csi1000 import timing
from research.csi1000.ma20_wave_strategy_v2 import WaveConfig, collect_waves, load_panel


START = "2016-06-20"
END = "2026-06-22"


def old_target_frame(conn: sqlite3.Connection, start: str, end: str) -> pd.DataFrame:
    df = timing.load_feature_frame(conn, start=start, end=end)
    sig = timing.generate_and_save_signals(conn, df, timing.STRATEGY_PRESETS["low_dd"], persist=False)
    out = df[["trade_date", "close"]].copy()
    out["trade_date"] = pd.to_datetime(out["trade_date"])
    out["old_signal"] = sig["signal"].to_numpy()
    # production backtest treats NO_DATA as an immediate flat target
    out["old_target"] = out["old_signal"].where(out["old_signal"].isin(["LONG", "SHORT", "FLAT"]), "FLAT")
    return out[(out.trade_date >= pd.Timestamp(start)) & (out.trade_date <= pd.Timestamp(end))].reset_index(drop=True)


def new_target_frame(conn: sqlite3.Connection, index_code: str, start: str, end: str,
                     min_wave: float, consensus_pct: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    panel = load_panel(conn, index_code, "2015-01-01", end)
    waves = collect_waves(panel, WaveConfig(min_wave_pct=min_wave, consensus_pct=consensus_pct))
    prices = pd.read_sql_query(
        "SELECT trade_date, close FROM index_prices WHERE index_code=? AND trade_date BETWEEN ? AND ? ORDER BY trade_date",
        conn, params=(index_code, start, end),
    )
    prices["trade_date"] = pd.to_datetime(prices["trade_date"])
    prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
    if waves.empty:
        prices["new_signal"] = None; prices["new_target"] = "FLAT"
        return prices, waves
    waves = waves[(waves.confirm_date >= pd.Timestamp(start)) & (waves.confirm_date <= pd.Timestamp(end))].copy()
    # 局部极值要等该股票右侧4根有效日线走完才能确认；同时不能早于该波段
    # 的20日线确认点。两者取较晚日期，避免在确认点出现前提前交易。
    def execution_date(r: pd.Series) -> pd.Timestamp:
        ready = pd.Timestamp(r.end_ready_date)
        confirm = pd.Timestamp(r.confirm_date)
        return max(ready, confirm)

    waves["execute_date"] = waves.apply(execution_date, axis=1)
    waves = waves.dropna(subset=["execute_date"])
    # 一个交易日同一股票若有重复事件，只保留最后一条，避免重复计票。
    waves = waves.sort_values(["execute_date", "code", "confirm_date"]).drop_duplicates(["execute_date", "code"], keep="last")
    counts = waves.groupby("execute_date").agg(
        long_count=("type", lambda x: int((x == "FALL").sum())),
        short_count=("type", lambda x: int((x == "RISE").sum())),
    ).reset_index()
    total_stocks = max(1, panel.code.nunique())
    need = max(1, int(np.ceil(total_stocks * consensus_pct / 100.0)))
    counts["new_signal"] = np.where(
        (counts.long_count >= need) & (counts.long_count > counts.short_count), "LONG",
        np.where((counts.short_count >= need) & (counts.short_count > counts.long_count), "SHORT", "NONE"),
    )
    out = prices.merge(counts, left_on="trade_date", right_on="execute_date", how="left").sort_values("trade_date").reset_index(drop=True)
    out["new_signal"] = out["new_signal"].fillna("NONE")
    # NONE 表示没有新的确认点，保持上一仓位；不是平仓信号。
    out["new_target"] = out["new_signal"].replace("NONE", np.nan).ffill().fillna("FLAT")
    out["consensus_need"] = need
    return out, waves


def simulate(prices: pd.DataFrame, target_col: str, fee_bps: float = 2.0) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    d = prices[["trade_date", "close", target_col]].copy().sort_values("trade_date").reset_index(drop=True)
    d["target"] = d[target_col].fillna("FLAT")
    fee = fee_bps / 10000.0
    position = "FLAT"; equity = 1.0; peak = 1.0
    rows: list[dict[str, Any]] = []; trades: list[dict[str, Any]] = []; current: dict[str, Any] | None = None
    prev_close = None; fees = 0
    for i, r in d.iterrows():
        date = r.trade_date.strftime("%Y-%m-%d"); close = float(r.close)
        interval_ret = 0.0 if prev_close is None else (
            close / prev_close - 1.0 if position == "LONG" else prev_close / close - 1.0 if position == "SHORT" else 0.0
        )
        equity *= 1.0 + interval_ret
        target = r.target if r.target in ("LONG", "SHORT", "FLAT") else "FLAT"
        if target != position:
            if current is not None:
                entry = current["entry_price"]; direction = current["direction"]
                gross = close / entry - 1.0 if direction == "LONG" else entry / close - 1.0
                current.update(exit_date=date, exit_price=close, return_pct=(gross - fee) * 100.0, hold_days=i - current["entry_index"])
                trades.append(current); equity *= 1.0 - fee; fees += 1; current = None
            if target in ("LONG", "SHORT") and i < len(d) - 1:
                position = target; equity *= 1.0 - fee; fees += 1
                current = {"direction": position, "entry_date": date, "entry_price": close, "entry_index": i}
            else:
                position = "FLAT"
        peak = max(peak, equity)
        rows.append({"trade_date": r.trade_date, "position": position, "target": target, "equity": equity, "interval_ret": interval_ret})
        prev_close = close
    if current is not None:
        close = float(d.iloc[-1].close); date = d.iloc[-1].trade_date.strftime("%Y-%m-%d"); entry = current["entry_price"]
        gross = close / entry - 1.0 if current["direction"] == "LONG" else entry / close - 1.0
        current.update(exit_date=date, exit_price=close, return_pct=(gross - fee) * 100.0, hold_days=len(d) - 1 - current["entry_index"])
        trades.append(current); equity *= 1.0 - fee; fees += 1
    daily = pd.DataFrame(rows); daily["peak"] = daily.equity.cummax(); daily["drawdown"] = daily.equity / daily.peak - 1.0
    tr = pd.DataFrame(trades)
    annual = []
    for year, g in daily.groupby(daily.trade_date.dt.year):
        first = float(g.equity.iloc[0]); last = float(g.equity.iloc[-1]); annual.append({"year": int(year), "return_pct": (last / first - 1.0) * 100.0, "max_drawdown_pct": g.drawdown.min() * 100.0, "long_days": int((g.position == "LONG").sum()), "short_days": int((g.position == "SHORT").sum()), "switches": int((g.position != g.position.shift(1)).sum())})
    active = daily[daily.position.isin(["LONG", "SHORT"]) & daily.interval_ret.notna()]
    result = {"start": daily.trade_date.min().strftime("%Y-%m-%d"), "end": daily.trade_date.max().strftime("%Y-%m-%d"), "days": len(daily), "total_return_pct": (equity - 1.0) * 100.0, "max_drawdown_pct": daily.drawdown.min() * 100.0, "trade_count": len(tr), "win_rate_pct": float((tr.return_pct > 0).mean() * 100.0) if len(tr) else 0.0, "avg_trade_return_pct": float(tr.return_pct.mean()) if len(tr) else 0.0, "long_days": int((daily.position == "LONG").sum()), "short_days": int((daily.position == "SHORT").sum()), "flat_days": int((daily.position == "FLAT").sum()), "fee_events": fees, "direction_accuracy_pct": float((((active.position == "LONG") & (active.interval_ret > 0)) | ((active.position == "SHORT") & (active.interval_ret < 0))).mean() * 100.0) if len(active) else 0.0}
    return result, daily, tr.assign(return_pct=lambda x: x.return_pct.round(4)) if not tr.empty else tr


def main() -> int:
    p = argparse.ArgumentParser(description="旧中证1000策略/20日线波段策略详细10年对比")
    p.add_argument("--db", default=str(DB_PATH)); p.add_argument("--index-code", default="000852"); p.add_argument("--start", default=START); p.add_argument("--end", default=END); p.add_argument("--min-wave", type=float, default=10.0); p.add_argument("--consensus", type=float, default=2.0); p.add_argument("--json-out", default=None); p.add_argument("--trades-csv", default=None)
    args = p.parse_args(); conn = sqlite3.connect(args.db)
    try:
        old = old_target_frame(conn, args.start, args.end)
        new, waves = new_target_frame(conn, args.index_code, args.start, args.end, args.min_wave, args.consensus)
    finally: conn.close()
    base = old.merge(new[["trade_date", "new_signal", "new_target"]], on="trade_date", how="inner").sort_values("trade_date").reset_index(drop=True)
    base["new_target"] = base.new_target.fillna("FLAT")
    # 旧策略优先；新策略只在旧策略空仓时补充。
    base["combo_both"] = np.where(base.old_target.isin(["LONG", "SHORT"]), base.old_target, base.new_target)
    base["combo_long"] = np.where(base.old_target.isin(["LONG", "SHORT"]), base.old_target, np.where(base.new_target == "LONG", "LONG", "FLAT"))
    base["combo_short"] = np.where(base.old_target.isin(["LONG", "SHORT"]), base.old_target, np.where(base.new_target == "SHORT", "SHORT", "FLAT"))
    reports: dict[str, Any] = {"meta": {"index_code": args.index_code, "start": args.start, "end": args.end, "min_wave_pct": args.min_wave, "consensus_pct": args.consensus, "stocks": int(waves.code.nunique()) if not waves.empty else 0, "waves": int(len(waves))}, "strategies": {}, "overlap": {}}
    daily_map: dict[str, pd.DataFrame] = {}
    trade_map: dict[str, pd.DataFrame] = {}
    for name, col in [("old_low_dd", "old_target"), ("new_wave", "new_target"), ("old_plus_new", "combo_both"), ("old_plus_new_long", "combo_long"), ("old_plus_new_short", "combo_short")]:
        result, daily, trades = simulate(base[["trade_date", "close", col]].rename(columns={col: "target"}), "target")
        result["annual"] = []
        daily_map[name] = daily; trade_map[name] = trades
        # 逐年摘要放入结果，避免只看总收益。
        result["annual"] = [{"year": int(y), "return_pct": float((g.equity.iloc[-1] / g.equity.iloc[0] - 1) * 100), "max_drawdown_pct": float(g.drawdown.min() * 100), "long_days": int((g.position == "LONG").sum()), "short_days": int((g.position == "SHORT").sum())} for y, g in daily.groupby(daily.trade_date.dt.year)]
        reports["strategies"][name] = result
    reports["overlap"] = {"old_flat_days": int((base.old_target == "FLAT").sum()), "new_active_days": int(base.new_target.isin(["LONG", "SHORT"]).sum()), "new_long_on_old_flat": int(((base.old_target == "FLAT") & (base.new_target == "LONG")).sum()), "new_short_on_old_flat": int(((base.old_target == "FLAT") & (base.new_target == "SHORT")).sum()), "old_new_same_days": int((base.old_target == base.new_target).sum()), "old_new_conflict_days": int((base.old_target.isin(["LONG", "SHORT"]) & base.new_target.isin(["LONG", "SHORT"]) & (base.old_target != base.new_target)).sum())}
    print(json.dumps(reports, ensure_ascii=False, indent=2))
    if args.json_out: Path(args.json_out).write_text(json.dumps(reports, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.trades_csv:
        rows = []
        for name, tr in trade_map.items():
            if not tr.empty:
                x = tr.copy(); x.insert(0, "strategy", name); rows.append(x)
        if rows: pd.concat(rows, ignore_index=True).to_csv(args.trades_csv, index=False, encoding="utf-8-sig")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
