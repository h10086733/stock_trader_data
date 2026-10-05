"""按 9 日局部极值 + 20 日线确认点定义的波段研究。

定义严格按研究笔记：
* 高点：前后各 4 个交易日（共 9 日）收盘最高，且收盘在 MA20 之上；
* 低点：前后各 4 个交易日（共 9 日）收盘最低，且收盘在 MA20 之下；
* 连续高点合并取最高，连续低点合并取最低，最终高低点交替；
* 相邻极值之间的上穿/下穿只保留第一个确认点。

脚本用于离线事件研究。9 日局部极值需要未来 4 日数据，因此极值标签本身
不是当天可知；若用于实盘，至少要等 4 个交易日确认。

重要：本文件的“连续同类极值合并”按完整历史序列回溯完成，后续极值可能改写
此前的合并结果。因此它只能用于描述性波段研究，不能作为无未来函数的实盘回测。
需要实盘时序验证请使用 ``causal_wave_strategy.py``，该版本不会用未来数据修改
已经发出的历史信号。
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
class WaveConfig:
    ma_days: int = 20
    radius: int = 4
    min_wave_pct: float = 5.0
    consensus_pct: float = 2.0
    fee_bps: float = 2.0


def load_panel(conn: sqlite3.Connection, index_code: str,
               start: str, end: str) -> pd.DataFrame:
    return pd.read_sql_query(
        """SELECT p.code, p.trade_date, p.close, p.high, p.low
             FROM daily_prices p
             JOIN index_constituents ic ON ic.stock_code = p.code
            WHERE ic.index_code=? AND p.trade_date BETWEEN ? AND ?
              AND p.close IS NOT NULL AND p.close > 0
            ORDER BY p.code, p.trade_date""",
        conn, params=(str(index_code), start, end),
    )


def local_points(stock: pd.DataFrame, config: WaveConfig) -> list[dict[str, Any]]:
    """识别9日局部高低点，并合并连续同类点。"""
    d = stock.sort_values("trade_date").reset_index(drop=True).copy()
    if d.empty:
        return []
    d["trade_date"] = pd.to_datetime(d["trade_date"])
    d["close"] = pd.to_numeric(d["close"], errors="coerce")
    for col in ("high", "low"):
        if col not in d:
            d[col] = d["close"]
        d[col] = pd.to_numeric(d[col], errors="coerce").fillna(d["close"])
    d = d.dropna(subset=["close"]).reset_index(drop=True)
    window = config.radius * 2 + 1
    if len(d) < config.ma_days + window:
        return []
    d["ma"] = d["close"].rolling(config.ma_days, min_periods=config.ma_days).mean()
    d["max9"] = d["high"].rolling(window, center=True, min_periods=window).max()
    d["min9"] = d["low"].rolling(window, center=True, min_periods=window).min()
    high_mask = d["high"].eq(d["max9"]) & d["close"].gt(d["ma"])
    low_mask = d["low"].eq(d["min9"]) & d["close"].lt(d["ma"])
    candidates: list[dict[str, Any]] = []
    for i in np.flatnonzero(high_mask.to_numpy()):
        # 局部极值的右侧4日应是该股票自己的后续4根有效日线；个股停牌时
        # 不能简单使用指数日历的第4天，否则会在极值尚未被确认前提前使用信息。
        candidates.append({
            "kind": "HIGH", "index": int(i), "date": d.loc[i, "trade_date"],
            "price": float(d.loc[i, "high"]),
            "available_date": d.loc[i + config.radius, "trade_date"],
        })
    for i in np.flatnonzero(low_mask.to_numpy()):
        candidates.append({
            "kind": "LOW", "index": int(i), "date": d.loc[i, "trade_date"],
            "price": float(d.loc[i, "low"]),
            "available_date": d.loc[i + config.radius, "trade_date"],
        })
    candidates.sort(key=lambda x: x["index"])

    merged: list[dict[str, Any]] = []
    for point in candidates:
        if not merged or merged[-1]["kind"] != point["kind"]:
            merged.append(point)
            continue
        # 同类点连续出现，合并成一个极值点。
        if point["kind"] == "HIGH" and point["price"] > merged[-1]["price"]:
            merged[-1] = point
        elif point["kind"] == "LOW" and point["price"] < merged[-1]["price"]:
            merged[-1] = point
    return merged


def first_cross(d: pd.DataFrame, start_i: int, end_i: int, direction: str) -> dict[str, Any] | None:
    """返回两个相邻极值之间的第一个上穿/下穿确认点。"""
    if end_i <= start_i:
        return None
    above = d["close"] > d["ma"]
    prev = above.shift(1, fill_value=False)
    if direction == "UP":
        mask = above & ~prev
    else:
        mask = ~above & prev
    idx = np.flatnonzero(mask.iloc[start_i + 1:end_i + 1].to_numpy())
    if len(idx) == 0:
        return None
    i = start_i + 1 + int(idx[0])
    return {"index": i, "date": d.loc[i, "trade_date"], "price": float(d.loc[i, "close"]), "kind": direction}


def stock_waves(stock: pd.DataFrame, config: WaveConfig) -> list[dict[str, Any]]:
    """将极值点配成上涨/下跌波段，并挂载第一个确认点。"""
    d = stock.sort_values("trade_date").reset_index(drop=True).copy()
    d["trade_date"] = pd.to_datetime(d["trade_date"])
    d["close"] = pd.to_numeric(d["close"], errors="coerce")
    d["ma"] = d["close"].rolling(config.ma_days, min_periods=config.ma_days).mean()
    d = d.reset_index(drop=True)
    points = local_points(d, config)
    if len(points) < 2:
        return []
    waves: list[dict[str, Any]] = []
    for left, right in zip(points[:-1], points[1:]):
        if left["kind"] == right["kind"]:
            continue
        direction = "UP" if left["kind"] == "LOW" else "DOWN"
        confirm = first_cross(d, left["index"], right["index"], direction)
        if confirm is None:
            continue
        start_price, end_price = left["price"], right["price"]
        if start_price <= 0 or end_price <= 0 or confirm["price"] <= 0:
            continue
        if direction == "UP":
            wave_pct = (end_price / start_price - 1.0) * 100.0
            pre_confirm_pct = (confirm["price"] / start_price - 1.0) * 100.0
            post_confirm_pct = (end_price / confirm["price"] - 1.0) * 100.0
            wave_type = "RISE"
        else:
            wave_pct = (1.0 - end_price / start_price) * 100.0
            pre_confirm_pct = (1.0 - confirm["price"] / start_price) * 100.0
            post_confirm_pct = (1.0 - end_price / confirm["price"]) * 100.0
            wave_type = "FALL"
        if wave_pct < config.min_wave_pct:
            continue
        waves.append({
            "type": wave_type,
            "start_kind": left["kind"], "start_date": left["date"], "start_price": start_price,
            "confirm_kind": confirm["kind"], "confirm_date": confirm["date"], "confirm_price": confirm["price"],
            "end_kind": right["kind"], "end_date": right["date"], "end_price": end_price,
            "end_ready_date": right["available_date"],
            "end_index": right["index"],
            "wave_pct": wave_pct, "pre_confirm_pct": pre_confirm_pct,
            "post_confirm_pct": post_confirm_pct,
            "days_start_confirm": confirm["index"] - left["index"],
            "days_confirm_end": right["index"] - confirm["index"],
            "days_wave": right["index"] - left["index"],
        })
    return waves


def collect_waves(panel: pd.DataFrame, config: WaveConfig) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for code, stock in panel.groupby("code", sort=False):
        for wave in stock_waves(stock, config):
            wave["code"] = code
            rows.append(wave)
    return pd.DataFrame(rows)


def summarize_waves(waves: pd.DataFrame) -> dict[str, Any]:
    if waves.empty:
        return {"events": 0, "rise": [], "fall": [], "prior_relation": []}
    out: dict[str, Any] = {"events": int(len(waves))}
    for kind in ("RISE", "FALL"):
        d = waves[waves.type == kind]
        out["rise" if kind == "RISE" else "fall"] = {
            "events": int(len(d)), "stocks": int(d.code.nunique()),
            "avg_wave_pct": float(d.wave_pct.mean()), "median_wave_pct": float(d.wave_pct.median()),
            "avg_pre_confirm_pct": float(d.pre_confirm_pct.mean()), "median_pre_confirm_pct": float(d.pre_confirm_pct.median()),
            "avg_post_confirm_pct": float(d.post_confirm_pct.mean()), "median_post_confirm_pct": float(d.post_confirm_pct.median()),
            "avg_days_start_confirm": float(d.days_start_confirm.mean()), "avg_days_confirm_end": float(d.days_confirm_end.mean()),
        }
    # 每只股票前后相邻波段关系：暴跌后上涨、上涨后下跌。
    relations: list[dict[str, Any]] = []
    for code, d in waves.sort_values("start_date").groupby("code", sort=False):
        vals = d.to_dict("records")
        for a, b in zip(vals[:-1], vals[1:]):
            relations.append({
                "code": code, "prior_type": a["type"], "prior_wave_pct": a["wave_pct"],
                "next_type": b["type"], "next_wave_pct": b["wave_pct"],
                "next_pre_confirm_pct": b["pre_confirm_pct"], "next_post_confirm_pct": b["post_confirm_pct"],
            })
    rel = pd.DataFrame(relations)
    if rel.empty:
        out["prior_relation"] = []
    else:
        rel["prior_bucket"] = pd.cut(rel["prior_wave_pct"], [-np.inf, 5, 10, 15, 20, 30, np.inf], labels=["<5%", "5-10%", "10-15%", "15-20%", "20-30%", "30%以上"])
        grouped = rel.groupby(["prior_type", "next_type", "prior_bucket"], observed=True).agg(
            events=("code", "size"), avg_next_wave_pct=("next_wave_pct", "mean"),
            median_next_wave_pct=("next_wave_pct", "median"),
            avg_next_pre_confirm_pct=("next_pre_confirm_pct", "mean"),
        ).reset_index()
        out["prior_relation"] = grouped.to_dict("records")
    return out


def confirmation_backtest(waves: pd.DataFrame, prices: pd.DataFrame,
                          index_code: str, start: str, end: str,
                          fee_bps: float = 2.0) -> dict[str, Any]:
    """用指数官方收盘价，把个股确认事件按日聚合为多空仓位。"""
    if waves.empty or prices.empty:
        return {"days": 0, "index_code": index_code}
    p = prices.copy(); p["trade_date"] = pd.to_datetime(p["trade_date"]); p["close"] = pd.to_numeric(p["close"], errors="coerce")
    w = waves[(waves.confirm_date >= pd.Timestamp(start)) & (waves.confirm_date <= pd.Timestamp(end))].copy()
    # 极值点需该股票未来 radius 根有效日线才能确认；确认点也只能在该时点之后执行。
    # ``end_ready_date`` 在个股波段构造时生成，已处理停牌日，不能替换为指数日历+4。
    # 既要等待局部极值右侧4个交易日确认，也要等20日线确认点出现；
    # 若确认点更晚，则以确认点为执行日，不能提前使用未来信息。
    def execution_date(row: pd.Series) -> pd.Timestamp:
        ready = pd.Timestamp(row["end_ready_date"])
        confirm = pd.Timestamp(row["confirm_date"])
        return max(ready, confirm)

    w["execute_date"] = w.apply(execution_date, axis=1)
    w = w.dropna(subset=["execute_date"])
    # 上一段下跌完成后寻找反弹做多；上一段上涨完成后寻找回撤做空。
    counts = w.groupby("execute_date").agg(long_count=("type", lambda x: int((x == "FALL").sum())), short_count=("type", lambda x: int((x == "RISE").sum()))).reset_index()
    total_stocks = waves.code.nunique()
    need = max(1, int(np.ceil(total_stocks * 0.02)))
    counts["signal"] = np.where((counts.long_count >= need) & (counts.long_count > counts.short_count), "LONG", np.where((counts.short_count >= need) & (counts.short_count > counts.long_count), "SHORT", "FLAT"))
    d = p[(p.trade_date >= pd.Timestamp(start)) & (p.trade_date <= pd.Timestamp(end))].merge(counts, left_on="trade_date", right_on="execute_date", how="left").sort_values("trade_date")
    d["signal"] = d.signal.fillna("FLAT"); d["position"] = d.signal.replace("FLAT", np.nan).ffill().fillna("FLAT"); d["next_ret"] = d.close.shift(-1) / d.close - 1.0
    changed = d.position != d.position.shift(1)
    d["strategy_ret"] = np.select([d.position == "LONG", d.position == "SHORT"], [d.next_ret, -d.next_ret], 0.0) - np.where(changed & d.position.isin(["LONG", "SHORT"]), fee_bps / 10000.0, 0.0)
    d["equity"] = (1 + d.strategy_ret.fillna(0)).cumprod(); dd = d.equity / d.equity.cummax() - 1
    active = d[d.position.isin(["LONG", "SHORT"]) & d.next_ret.notna()]
    acc = ((((active.position == "LONG") & (active.next_ret > 0)) | ((active.position == "SHORT") & (active.next_ret < 0))).mean() * 100) if len(active) else 0.0
    return {"index_code": index_code, "start": start, "end": end, "days": len(d), "total_return_pct": float((d.equity.iloc[-1] - 1) * 100), "buy_hold_pct": float((d.close.iloc[-1] / d.close.iloc[0] - 1) * 100), "max_drawdown_pct": float(dd.min() * 100), "long_days": int((d.position == "LONG").sum()), "short_days": int((d.position == "SHORT").sum()), "switches": int((changed & d.position.isin(["LONG", "SHORT"])).sum()), "direction_accuracy_pct": float(acc), "event_days": int(len(counts)), "consensus_stocks": need}


def main() -> int:
    p = argparse.ArgumentParser(description="9日局部高低点/20日线确认点波段研究")
    p.add_argument("--db", default=str(DB_PATH)); p.add_argument("--index-code", default="000852")
    p.add_argument("--start", default="2016-01-01"); p.add_argument("--end", default="2026-06-22")
    p.add_argument("--min-wave", type=float, default=5.0)
    p.add_argument("--backtest", action="store_true", help="按确认点聚合并回测指数多空")
    p.add_argument("--csv", default=None)
    args = p.parse_args(); cfg = WaveConfig(min_wave_pct=args.min_wave)
    conn = sqlite3.connect(args.db)
    try:
        panel = load_panel(conn, args.index_code, "2015-01-01", args.end)
        prices = pd.read_sql_query("SELECT trade_date,close FROM index_prices WHERE index_code=? AND trade_date BETWEEN ? AND ? ORDER BY trade_date", conn, params=(args.index_code, args.start, args.end))
    finally: conn.close()
    waves = collect_waves(panel, cfg); summary = summarize_waves(waves)
    print(json.dumps({"index_code": args.index_code, "stocks": int(panel.code.nunique()) if not panel.empty else 0, "config": asdict(cfg), **summary}, ensure_ascii=False, indent=2, default=str))
    if args.backtest:
        print("\n[confirmation_backtest]")
        print(json.dumps(confirmation_backtest(waves, prices, args.index_code, args.start, args.end, cfg.fee_bps), ensure_ascii=False, indent=2))
    if args.csv:
        waves.to_csv(args.csv, index=False, encoding="utf-8-sig")
        print(f"saved={args.csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
