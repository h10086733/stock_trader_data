"""20 日线四点法：个股高点/低点/上穿确认/下穿确认事件研究。

该脚本只做事件统计，不修改现有中证1000生产择时策略。每只股票按收盘价与
20 日均线的上穿/下穿划分波段，再统计：上涨波段后的回撤，以及下跌波段后的
反弹。由于数据库目前保存的是当前成分股，结果属于当前成分股回看研究。
"""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from trader.core.config import DB_PATH


RISE_BUCKETS = ((0, 10, "0-10%"), (10, 20, "10-20%"), (20, float("inf"), "20%以上"))
FALL_BUCKETS = ((0, 10, "10%以下"), (10, 20, "10-20%"), (20, 30, "20-30%"), (30, float("inf"), "30%以上"))


def bucket(value_pct: float, ranges: tuple[tuple[float, float, str], ...]) -> str | None:
    for low, high, label in ranges:
        if low <= value_pct < high:
            return label
    return None


def load_prices(conn: sqlite3.Connection, index_code: str, start: str, end: str) -> pd.DataFrame:
    return pd.read_sql_query(
        """SELECT p.code, p.trade_date, p.close
             FROM daily_prices p
             JOIN index_constituents ic ON ic.stock_code = p.code
            WHERE ic.index_code = ? AND p.trade_date BETWEEN ? AND ?
              AND p.close IS NOT NULL AND p.close > 0
            ORDER BY p.code, p.trade_date""",
        conn, params=(index_code, start, end),
    )


def detect_stock_events(stock: pd.DataFrame, ma_days: int = 20) -> list[dict[str, Any]]:
    """返回一个股票完整上涨/下跌波段的四点事件记录。"""
    if len(stock) < ma_days + 5:
        return []
    d = stock.sort_values("trade_date").reset_index(drop=True).copy()
    d["trade_date"] = pd.to_datetime(d["trade_date"])
    d["close"] = pd.to_numeric(d["close"], errors="coerce")
    d["ma20"] = d["close"].rolling(ma_days, min_periods=ma_days).mean()
    d = d.dropna(subset=["close", "ma20"]).reset_index(drop=True)
    if len(d) < 5:
        return []
    above = d["close"] > d["ma20"]
    cross = np.full(len(d), "", dtype=object)
    cross[(above & ~above.shift(1, fill_value=False)).to_numpy()] = "UP"
    down = (~above) & above.shift(1, fill_value=False)
    cross[down.to_numpy()] = "DOWN"
    cross_idx = np.flatnonzero(cross != "")
    if len(cross_idx) < 3:
        return []

    events: list[dict[str, Any]] = []
    # 只保留交替确认点；同方向重复点不应形成新波段。
    turns: list[tuple[int, str]] = []
    for i in cross_idx:
        direction = str(cross[i])
        if turns and turns[-1][1] == direction:
            continue
        turns.append((int(i), direction))
    for j in range(1, len(turns) - 1):
        prev_i, prev_dir = turns[j - 1]
        cur_i, cur_dir = turns[j]
        next_i, next_dir = turns[j + 1]
        if prev_dir == "DOWN" and cur_dir == "UP" and next_dir == "DOWN":
            # 低点在上穿确认前的下跌段，高点在本次上穿至下穿之间。
            low_slice = d.iloc[prev_i:cur_i + 1]
            high_slice = d.iloc[cur_i:next_i + 1]
            low_pos = int(low_slice["close"].idxmin())
            high_pos = int(high_slice["close"].idxmax())
            low = float(d.loc[low_pos, "close"])
            high = float(d.loc[high_pos, "close"])
            rise = (high / low - 1.0) * 100.0
            if rise >= 0:
                events.append({
                    "type": "RISE", "low_date": d.loc[low_pos, "trade_date"],
                    "up_confirm_date": d.loc[cur_i, "trade_date"],
                    "high_date": d.loc[high_pos, "trade_date"],
                    "down_confirm_date": d.loc[next_i, "trade_date"],
                    "low": low, "high": high, "swing_pct": rise,
                })
        elif prev_dir == "UP" and cur_dir == "DOWN" and next_dir == "UP":
            high_slice = d.iloc[prev_i:cur_i + 1]
            low_slice = d.iloc[cur_i:next_i + 1]
            high_pos = int(high_slice["close"].idxmax())
            low_pos = int(low_slice["close"].idxmin())
            high = float(d.loc[high_pos, "close"])
            low = float(d.loc[low_pos, "close"])
            fall = (1.0 - low / high) * 100.0
            if fall >= 0:
                events.append({
                    "type": "FALL", "high_date": d.loc[high_pos, "trade_date"],
                    "down_confirm_date": d.loc[cur_i, "trade_date"],
                    "low_date": d.loc[low_pos, "trade_date"],
                    "up_confirm_date": d.loc[next_i, "trade_date"],
                    "high": high, "low": low, "swing_pct": fall,
                })
    return events


def attach_following_move(events: list[dict[str, Any]], stock: pd.DataFrame) -> list[dict[str, Any]]:
    """在高点/低点之后，计算到下一反向确认前的实际反向幅度。"""
    if not events:
        return events
    d = stock.sort_values("trade_date").copy()
    d["trade_date"] = pd.to_datetime(d["trade_date"])
    d["close"] = pd.to_numeric(d["close"], errors="coerce")
    for event in events:
        if event["type"] == "RISE":
            # 高点后的下一段下跌，止于下一个上穿确认点。
            next_up = d[d.trade_date > event["down_confirm_date"]]
            end = next_up.trade_date.iloc[0] if not next_up.empty else d.trade_date.iloc[-1]
            after = d[(d.trade_date >= event["high_date"]) & (d.trade_date <= end)]
            if not after.empty:
                trough = float(after.close.min())
                event["following_move_pct"] = (trough / event["high"] - 1.0) * 100.0
                event["following_extreme"] = trough
        else:
            after = d[d.trade_date >= event["low_date"]]
            next_down = d[d.trade_date > event["up_confirm_date"]]
            end = next_down.trade_date.iloc[0] if not next_down.empty else d.trade_date.iloc[-1]
            after = after[after.trade_date <= end]
            if not after.empty:
                rebound = float(after.close.max())
                event["following_move_pct"] = (rebound / event["low"] - 1.0) * 100.0
                event["following_extreme"] = rebound
    return events


def collect_events(panel: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for code, stock in panel.groupby("code", sort=False):
        events = detect_stock_events(stock)
        # 先识别波段，再用下一反向波段的极值计算后续实际涨跌幅。
        events = attach_following_move(events, stock)
        events = sorted(events, key=lambda x: x.get("high_date", x.get("low_date")))
        for i, event in enumerate(events):
            if event["type"] == "RISE":
                nxt = next((x for x in events[i + 1:] if x["type"] == "FALL" and x["high_date"] >= event["high_date"]), None)
                if nxt:
                    event["following_move_pct"] = -float(nxt["swing_pct"])
            else:
                nxt = next((x for x in events[i + 1:] if x["type"] == "RISE" and x["low_date"] >= event["low_date"]), None)
                if nxt:
                    event["following_move_pct"] = float(nxt["swing_pct"])
        for event in events:
            event["code"] = code
            records.append(event)
    return pd.DataFrame(records)


def summarize(events: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if events.empty:
        return {"rise": pd.DataFrame(), "fall": pd.DataFrame(), "conditional": pd.DataFrame()}
    rise = events[events.type == "RISE"].copy()
    fall = events[events.type == "FALL"].copy()
    rise["bucket"] = rise.swing_pct.map(lambda x: bucket(float(x), RISE_BUCKETS))
    fall["bucket"] = fall.swing_pct.map(lambda x: bucket(float(x), FALL_BUCKETS))
    def table(frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame()
        out = frame.groupby("bucket", dropna=True).agg(
            events=("code", "size"), stocks=("code", "nunique"),
            avg_swing_pct=("swing_pct", "mean"), median_swing_pct=("swing_pct", "median"),
            avg_following_pct=("following_move_pct", "mean"), median_following_pct=("following_move_pct", "median"),
            favorable_rate_pct=("following_move_pct", lambda x: float((x > 0).mean() * 100)),
        ).reset_index()
        return out
    conditional = fall.merge(
        rise[["code", "high_date", "swing_pct"]].rename(columns={"high_date": "rise_high_date", "swing_pct": "prior_rise_pct"}),
        left_on=["code", "high_date"], right_on=["code", "rise_high_date"], how="left",
    )
    conditional["prior_rise_bucket"] = conditional.prior_rise_pct.map(lambda x: bucket(float(x), RISE_BUCKETS) if pd.notna(x) else None)
    cond = conditional.groupby(["bucket", "prior_rise_bucket"], dropna=True).agg(
        events=("code", "size"), avg_rebound_pct=("following_move_pct", "mean"),
        median_rebound_pct=("following_move_pct", "median"),
        rebound_positive_pct=("following_move_pct", lambda x: float((x > 0).mean() * 100)),
    ).reset_index()
    return {"rise": table(rise), "fall": table(fall), "conditional": cond}


def main() -> int:
    p = argparse.ArgumentParser(description="20日线四点法个股涨跌事件统计")
    p.add_argument("--db", default=str(DB_PATH)); p.add_argument("--index-code", default="000852")
    p.add_argument("--start", default="2015-01-01"); p.add_argument("--end", default="2026-06-22")
    p.add_argument("--csv", default=None, help="可选：保存完整事件明细CSV")
    args = p.parse_args()
    conn = sqlite3.connect(args.db)
    try:
        panel = load_prices(conn, args.index_code, args.start, args.end)
    finally:
        conn.close()
    events = collect_events(panel)
    result = summarize(events)
    print(f"index={args.index_code} stocks={panel.code.nunique() if not panel.empty else 0} events={len(events)}")
    for name in ("rise", "fall", "conditional"):
        print(f"\n[{name}]\n{result[name].to_string(index=False) if not result[name].empty else '无数据'}")
    if args.csv:
        Path(args.csv).parent.mkdir(parents=True, exist_ok=True)
        events.to_csv(args.csv, index=False, encoding="utf-8-sig")
        print(f"\nsaved={args.csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
