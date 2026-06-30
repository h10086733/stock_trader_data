"""技术指标计算（纯函数，无网络）。

来源：indicator_enrich.py。这是项目唯一的指标计算实现，被
patterns.scanner、strategies.high_confidence、strategies.surge 等复用。

DEFAULT_DB / DEFAULT_OUTPUT_DIR 为向后兼容保留，实际取自 core.config。
"""
from __future__ import annotations

import sqlite3

import numpy as np
import pandas as pd

from trader.core import config

DEFAULT_DB = config.DB_PATH
DEFAULT_OUTPUT_DIR = config.OUTPUT_DIR


def wma(values: pd.Series, period: int) -> pd.Series:
    """加权移动平均（权重 1..period）。"""
    arr = values.to_numpy(dtype="float64")
    result = np.full(len(arr), np.nan)
    if len(arr) < period:
        return pd.Series(result, index=values.index)
    weights = np.arange(1, period + 1, dtype="float64")
    windows = np.lib.stride_tricks.sliding_window_view(arr, period)
    result[period - 1:] = windows @ (weights / weights.sum())
    return pd.Series(result, index=values.index)


def hma(values: pd.Series, period: int) -> pd.Series:
    """Hull 移动平均。"""
    half = max(1, period // 2)
    root = max(1, int(period ** 0.5))
    raw = 2 * wma(values, half) - wma(values, period)
    return wma(raw, root)


def add_indicators(hist: pd.DataFrame) -> pd.DataFrame:
    """给单只股票的日K历史补 MA/HMA/MACD/量比 等指标。"""
    hist = hist.sort_values("trade_date").copy()
    close = hist["close"]
    high = hist["high"]
    low = hist["low"]
    volume = hist["volume"]

    for period in (5, 10, 20, 30, 60):
        ma = close.rolling(period).mean()
        hist[f"ma{period}"] = ma
        hist[f"ma{period}_slope_pct"] = (ma / ma.shift(1) - 1) * 100
        hist[f"close_ma{period}_dist_pct"] = (close / ma - 1) * 100
        hist[f"close_above_ma{period}"] = close >= ma

    for period in (20, 30, 60):
        line = hma(close, period)
        hist[f"hma{period}"] = line
        hist[f"hma{period}_slope_pct"] = (line / line.shift(1) - 1) * 100
        hist[f"close_hma{period}_dist_pct"] = (close / line - 1) * 100
        hist[f"close_above_hma{period}"] = close >= line

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    macd = 2 * (dif - dea)
    hist["macd_dif"] = dif
    hist["macd_dea"] = dea
    hist["macd"] = macd
    hist["macd_gt0"] = macd > 0
    hist["macd_dif_gt_dea"] = dif > dea
    hist["macd_cross_up"] = (dif > dea) & (dif.shift(1) <= dea.shift(1))
    hist["macd_cross_down"] = (dif < dea) & (dif.shift(1) >= dea.shift(1))
    hist["macd_rising"] = macd > macd.shift(1)
    hist["macd_dif_gt0"] = dif > 0
    hist["macd_dea_gt0"] = dea > 0

    hist["amount_ma20"] = hist["amount"].rolling(20).mean()
    hist["amount_ratio20"] = hist["amount"] / hist["amount_ma20"]
    hist["volume_ma20"] = volume.rolling(20).mean()
    hist["volume_ratio20"] = volume / hist["volume_ma20"]
    hist["close_position_day_pct"] = (close - low) / (high - low).replace(0, pd.NA) * 100
    return hist


def load_histories(conn: sqlite3.Connection, codes: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """批量读取多只股票区间日K（按 500 分块避免 SQL 参数过多）。"""
    frames = []
    for chunk_start in range(0, len(codes), 500):
        chunk = codes[chunk_start:chunk_start + 500]
        placeholders = ",".join("?" for _ in chunk)
        frame = pd.read_sql_query(
            f"""
            SELECT code, trade_date, open, high, low, close, volume, amount,
                   pct_change, turnover
            FROM daily_prices
            WHERE code IN ({placeholders})
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY code, trade_date
            """,
            conn,
            params=chunk + [start_date, end_date],
        )
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
