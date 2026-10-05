"""不回改历史的20日线波段策略验证版。

与 ``ma20_wave_strategy_v2`` 的全样本极值合并不同，本文件按时间顺序处理每只
股票的局部极值：一个极值只会在其自身后续4根有效日线完成后出现。之后即使有更高
的高点或更低的低点，也只影响之后的判断，绝不删除过去已经发出的交易信号。

这是对“能否按实盘方式执行”的保守验证，而不是生产策略。
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
from research.csi1000.ma20_wave_strategy_v2 import WaveConfig, load_panel
from research.csi1000.validate_wave_strategy import load_ohlc, simulate_next_open


def first_cross(d: pd.DataFrame, start_i: int, end_i: int, direction: str) -> dict[str, Any] | None:
    """只在两个已知极值原始日期之间寻找首个MA20交叉。"""
    above = d["close"] > d["ma"]
    prev = above.shift(1, fill_value=False)
    mask = (above & ~prev) if direction == "UP" else (~above & prev)
    found = np.flatnonzero(mask.iloc[start_i + 1:end_i + 1].to_numpy())
    if not len(found):
        return None
    i = start_i + 1 + int(found[0])
    return {"index": i, "date": d.loc[i, "trade_date"], "price": float(d.loc[i, "close"]), "kind": direction}


def causal_stock_waves(stock: pd.DataFrame, cfg: WaveConfig) -> list[dict[str, Any]]:
    """逐日处理局部极值；已输出的波段不会被未来极值合并或改写。"""
    d = stock.sort_values("trade_date").reset_index(drop=True).copy()
    d["trade_date"] = pd.to_datetime(d["trade_date"])
    for col in ("close", "high", "low"):
        d[col] = pd.to_numeric(d[col], errors="coerce")
    d["high"] = d["high"].fillna(d["close"])
    d["low"] = d["low"].fillna(d["close"])
    d = d.dropna(subset=["close", "high", "low"]).reset_index(drop=True)
    window = cfg.radius * 2 + 1
    if len(d) < cfg.ma_days + window:
        return []
    d["ma"] = d["close"].rolling(cfg.ma_days, min_periods=cfg.ma_days).mean()
    d["max9"] = d["high"].rolling(window, center=True, min_periods=window).max()
    d["min9"] = d["low"].rolling(window, center=True, min_periods=window).min()
    high_mask = d["high"].eq(d["max9"]) & d["close"].gt(d["ma"])
    low_mask = d["low"].eq(d["min9"]) & d["close"].lt(d["ma"])

    # 候选点在 original_index + radius 这一天收盘后才可知；以此时序处理。
    candidates: list[dict[str, Any]] = []
    for i in np.flatnonzero(high_mask.to_numpy()):
        candidates.append({"kind": "HIGH", "index": int(i), "date": d.loc[i, "trade_date"], "price": float(d.loc[i, "high"]), "available_date": d.loc[i + cfg.radius, "trade_date"]})
    for i in np.flatnonzero(low_mask.to_numpy()):
        candidates.append({"kind": "LOW", "index": int(i), "date": d.loc[i, "trade_date"], "price": float(d.loc[i, "low"]), "available_date": d.loc[i + cfg.radius, "trade_date"]})
    candidates.sort(key=lambda x: (x["available_date"], x["index"], x["kind"]))

    last: dict[str, Any] | None = None
    waves: list[dict[str, Any]] = []
    for point in candidates:
        if last is None:
            last = point
            continue
        if point["kind"] == last["kind"]:
            # 此时尚未形成反向极值，因此只更新正在观察的候选点；绝不修改已输出波段。
            better = (point["kind"] == "HIGH" and point["price"] > last["price"]) or (point["kind"] == "LOW" and point["price"] < last["price"])
            if better:
                last = point
            continue

        left, right = last, point
        direction = "UP" if left["kind"] == "LOW" else "DOWN"
        confirm = first_cross(d, left["index"], right["index"], direction)
        if confirm is not None and left["price"] > 0 and right["price"] > 0:
            if direction == "UP":
                wave_pct = (right["price"] / left["price"] - 1.0) * 100.0
                wave_type = "RISE"
            else:
                wave_pct = (1.0 - right["price"] / left["price"]) * 100.0
                wave_type = "FALL"
            if wave_pct >= cfg.min_wave_pct:
                # right point becomes可知时才发信号；交叉点若更晚，取更晚者。
                execute = max(pd.Timestamp(right["available_date"]), pd.Timestamp(confirm["date"]))
                waves.append({
                    "type": wave_type, "start_date": left["date"], "start_price": left["price"],
                    "confirm_date": confirm["date"], "confirm_price": confirm["price"],
                    "end_date": right["date"], "end_price": right["price"],
                    "end_ready_date": right["available_date"], "execute_date": execute,
                    "wave_pct": wave_pct,
                })
        # 反向点已经在今天可知，后续波段从它继续；此前输出的事件不再修改。
        last = point
    return waves


def collect_causal_waves(panel: pd.DataFrame, cfg: WaveConfig) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for code, stock in panel.groupby("code", sort=False):
        for wave in causal_stock_waves(stock, cfg):
            wave["code"] = code
            rows.append(wave)
    return pd.DataFrame(rows)


def signal_frame(waves: pd.DataFrame, prices: pd.DataFrame, consensus_pct: float, stocks: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    d = prices[["trade_date", "open", "close"]].copy().sort_values("trade_date")
    dates = pd.Series(d["trade_date"].to_list())
    # 如果个股可知日不是指数行情日，使用之后第一个指数交易日的收盘信号。
    def index_day(value: Any) -> pd.Timestamp:
        i = int(dates.searchsorted(pd.Timestamp(value), side="left"))
        return dates.iloc[i] if i < len(dates) else pd.NaT

    w = waves.copy()
    if w.empty:
        d["new_signal"] = "NONE"; d["new_target"] = "FLAT"
        return d, w
    w["signal_date"] = w["execute_date"].map(index_day)
    w = w.dropna(subset=["signal_date"])
    w["signal"] = np.where(w["type"].eq("FALL"), "LONG", "SHORT")
    # 同一股票同日只保留最终可知的一条波段，防止重复计数。
    w = w.sort_values(["signal_date", "code", "end_date"]).drop_duplicates(["signal_date", "code"], keep="last")
    counts = w.groupby("signal_date").agg(
        long_count=("signal", lambda x: int((x == "LONG").sum())),
        short_count=("signal", lambda x: int((x == "SHORT").sum())),
    ).reset_index()
    need = max(1, int(np.ceil(stocks * consensus_pct / 100.0)))
    counts["consensus_need"] = need
    counts["new_signal"] = np.where(
        (counts["long_count"] >= need) & (counts["long_count"] > counts["short_count"]), "LONG",
        np.where((counts["short_count"] >= need) & (counts["short_count"] > counts["long_count"]), "SHORT", "NONE"),
    )
    out = d.merge(counts, left_on="trade_date", right_on="signal_date", how="left")
    out["new_signal"] = out["new_signal"].fillna("NONE")
    out["new_target"] = out["new_signal"].replace("NONE", np.nan).ffill().fillna("FLAT")
    return out, w


def main() -> int:
    p = argparse.ArgumentParser(description="因果版20日线波段策略验证")
    p.add_argument("--db", default=str(DB_PATH)); p.add_argument("--index-code", default="000852")
    p.add_argument("--start", default="2016-06-20"); p.add_argument("--end", default="2026-06-22")
    p.add_argument("--min-wave", type=float, default=10.0); p.add_argument("--consensus", type=float, default=2.0)
    p.add_argument("--out-dir", default="outputs/csi1000_causal_wave_validation")
    args = p.parse_args(); out = Path(args.out_dir); out.mkdir(parents=True, exist_ok=True)
    cfg = WaveConfig(min_wave_pct=args.min_wave, consensus_pct=args.consensus)
    conn = sqlite3.connect(args.db)
    try:
        panel = load_panel(conn, args.index_code, "2015-01-01", args.end)
        prices = load_ohlc(conn, args.index_code, args.start, args.end)
    finally:
        conn.close()
    waves = collect_causal_waves(panel, cfg)
    waves = waves[(pd.to_datetime(waves["execute_date"]) >= pd.Timestamp(args.start)) & (pd.to_datetime(waves["execute_date"]) <= pd.Timestamp(args.end))].copy() if not waves.empty else waves
    signals, components = signal_frame(waves, prices, args.consensus, int(panel.code.nunique()))
    reports: dict[str, Any] = {"meta": {"index_code": args.index_code, "start": args.start, "end": args.end, "min_wave_pct": args.min_wave, "consensus_pct": args.consensus, "stocks": int(panel.code.nunique()), "waves": int(len(waves))}, "backtests": {}}
    signals.to_csv(out / "causal_signal_daily.csv", index=False, encoding="utf-8-sig")
    components.to_csv(out / "causal_signal_components.csv", index=False, encoding="utf-8-sig")
    for cost in (2.0, 10.0, 20.0):
        result, daily, trades = simulate_next_open(prices, signals, cost)
        reports["backtests"][f"cost_{int(cost)}bps_per_side"] = result
        daily.to_csv(out / f"causal_next_open_daily_{int(cost)}bps.csv", index=False, encoding="utf-8-sig")
        trades.to_csv(out / f"causal_next_open_trades_{int(cost)}bps.csv", index=False, encoding="utf-8-sig")
    (out / "causal_validation_summary.json").write_text(json.dumps(reports, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(reports, ensure_ascii=False, indent=2)); print(f"saved={out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
