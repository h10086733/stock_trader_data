"""
中证1000择时策略：指数行情、信号提醒与历史回测。

核心思想来自 1000判断 文档：
  1. 中证1000是资金推动型资产，做多要求价格上涨并放量。
  2. 做空要求大小盘宽度同时偏弱，且中证1000缩量。
  3. 下跌过深后不追空，默认空仓，也可选择反手做多。

常用命令：
  python csi1000_timing.py --init-db
  python csi1000_timing.py --fetch-index-prices --start 20100101
  python csi1000_timing.py --backtest --start 2026-02-03
  python csi1000_timing.py --signal
  python csi1000_timing.py --signals --limit 20
  python csi1000_timing.py --trades --limit 20
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
import time
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "stock_data.db"
KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
SINA_PRICE_URL = "https://hq.sinajs.cn/list="
QUOTE_URLS = [
    "https://push2delay.eastmoney.com/api/qt/ulist.np/get",
    "https://push2.eastmoney.com/api/qt/ulist.np/get",
]


INDEX_PRICE_SOURCES = {
    # 东方财富 secid：沪深/中证指数通常是 1.xxxxxx。
    "000300": {"name": "沪深300", "market": "1"},
    "000852": {"name": "中证1000", "market": "1"},
}


SINA_INDEX_SYMBOLS = {
    "000300": "sh000300",
    "000852": "sh000852",
}


BAOSTOCK_INDEX_CODES = {
    "000300": "sh.000300",
    "000852": "sh.000852",
}


@dataclass(frozen=True)
class StrategyConfig:
    long_csi_score_min: float = 10.0
    long_hs300_score_min: float = -10.0
    long_vol_ratio_min: float = 1.10
    long_price_from_low_min: float = 0.04

    short_csi_score_max: float = -10.0
    short_hs300_score_max: float = 10.0
    short_score_ma_days: int = 3
    short_vol_ratio_max: float = 1.05
    short_drawdown_from_high_max: float = 0.05
    short_stop_2d_gain: float = 0.02

    reverse_deep_short: bool = False
    fee_bps: float = 2.0


STRATEGY_PRESETS: dict[str, StrategyConfig] = {
    "original": StrategyConfig(
        long_csi_score_min=20.0,
        long_hs300_score_min=-10.0,
        long_vol_ratio_min=1.10,
        long_price_from_low_min=0.04,
        short_csi_score_max=-20.0,
        short_hs300_score_max=10.0,
        short_vol_ratio_max=1.05,
        short_drawdown_from_high_max=0.05,
        reverse_deep_short=True,
    ),
    "low_dd": StrategyConfig(
        long_csi_score_min=20.0,
        long_hs300_score_min=-20.0,
        long_vol_ratio_min=1.05,
        long_price_from_low_min=0.06,
        short_csi_score_max=-10.0,
        short_hs300_score_max=10.0,
        short_vol_ratio_max=1.05,
        short_drawdown_from_high_max=0.05,
        reverse_deep_short=True,
    ),
    "aggressive": StrategyConfig(
        long_csi_score_min=20.0,
        long_hs300_score_min=-30.0,
        long_vol_ratio_min=1.10,
        long_price_from_low_min=0.04,
        short_csi_score_max=-10.0,
        short_hs300_score_max=10.0,
        short_vol_ratio_max=1.00,
        short_drawdown_from_high_max=0.05,
        reverse_deep_short=True,
    ),
    "best_rdd": StrategyConfig(
        long_csi_score_min=10.0,
        long_hs300_score_min=-10.0,
        long_vol_ratio_min=1.10,
        long_price_from_low_min=0.02,
        short_csi_score_max=-10.0,
        short_hs300_score_max=10.0,
        short_vol_ratio_max=1.00,
        short_drawdown_from_high_max=0.05,
        reverse_deep_short=True,
    ),
}


@dataclass
class SignalContextState:
    long_wave_active: bool = False
    long_wave_blocked: bool = False
    short_wave_active: bool = False
    short_wave_target: str = "FLAT"
    trade_position: str = "FLAT"
    trade_entry_price: float | None = None
    trade_entry_index: int | None = None

    def reset_long(self) -> None:
        self.long_wave_active = False
        self.long_wave_blocked = False

    def reset_short(self) -> None:
        self.short_wave_active = False
        self.short_wave_target = "FLAT"

    def block_short_wave(self) -> None:
        self.short_wave_active = True
        self.short_wave_target = "FLAT"

    def set_trade_position(self, position: str, entry_price: float | None,
                           entry_index: int | None) -> None:
        self.trade_position = position
        self.trade_entry_price = entry_price
        self.trade_entry_index = entry_index

    def clear_trade_position(self) -> None:
        self.trade_position = "FLAT"
        self.trade_entry_price = None
        self.trade_entry_index = None


def connect(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS index_prices (
            index_code  TEXT NOT NULL,
            trade_date  DATE NOT NULL,
            open        REAL,
            close       REAL,
            high        REAL,
            low         REAL,
            volume      REAL,
            amount      REAL,
            pct_change  REAL,
            turnover    REAL,
            PRIMARY KEY (index_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_index_prices_date
        ON index_prices(trade_date)
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS csi1000_timing_signals (
            trade_date      DATE PRIMARY KEY,
            signal          TEXT NOT NULL,
            trade_state     TEXT,
            action          TEXT NOT NULL,
            reason          TEXT,
            csi_close       REAL,
            csi_score       REAL,
            hs300_score     REAL,
            csi_score_ma3   REAL,
            hs300_score_ma3 REAL,
            vol_ratio_5_20  REAL,
            price_from_low10 REAL,
            drawdown_from_high10 REAL,
            pct_2d          REAL,
            payload_json    TEXT,
            created_at      DATETIME DEFAULT (datetime('now','localtime')),
            updated_at      DATETIME DEFAULT (datetime('now','localtime'))
        )
    """)
    signal_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(csi1000_timing_signals)").fetchall()
    }
    if "trade_state" not in signal_cols:
        conn.execute("ALTER TABLE csi1000_timing_signals ADD COLUMN trade_state TEXT")
    conn.execute("""
        UPDATE csi1000_timing_signals
        SET trade_state = CASE
            WHEN signal = 'LONG' THEN '多1000'
            WHEN signal = 'SHORT' THEN '空1000'
            ELSE '空仓'
        END
        WHERE trade_state IS NULL OR trade_state = ''
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS csi1000_timing_trades (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_key         TEXT NOT NULL,
            direction       TEXT NOT NULL,
            entry_date      DATE NOT NULL,
            entry_price     REAL NOT NULL,
            exit_date       DATE,
            exit_price      REAL,
            exit_reason     TEXT,
            hold_days       INTEGER,
            return_pct      REAL,
            signal_date     DATE,
            entry_reason    TEXT,
            created_at      DATETIME DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_csi1000_trades_run_exit
        ON csi1000_timing_trades(run_key, exit_date)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_csi1000_trades_run_entry
        ON csi1000_timing_trades(run_key, entry_date)
    """)
    conn.commit()


def init_width_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS index_daily_stats (
            index_code   TEXT  NOT NULL,
            trade_date   DATE  NOT NULL,
            score_sum    REAL,
            high_count   INTEGER,
            low_count    INTEGER,
            valid_count  INTEGER,
            total_count  INTEGER,
            net_value    REAL,
            PRIMARY KEY (index_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ids_date
        ON index_daily_stats(trade_date)
    """)
    conn.commit()


def safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, "", "-"):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_baostock_date(value: str | None) -> str | None:
    if not value:
        return None
    text = value.strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text


def fetch_index_kline(index_code: str, start: str, end: str | None = None) -> list[dict[str, Any]]:
    source = INDEX_PRICE_SOURCES[index_code]
    end = end or datetime.today().strftime("%Y%m%d")
    params = {
        "secid": f"{source['market']}.{index_code}",
        "ut": "8dec03ba335b81bf4ebdf7b29ec27d15",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": 101,
        "fqt": 1,
        "beg": start,
        "end": end,
        "lmt": 10000,
    }
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 Chrome/120 Safari/537.36"
        ),
        "Referer": "https://quote.eastmoney.com/",
    }
    last_error: Exception | None = None
    data = {}
    for attempt in range(5):
        try:
            resp = requests.get(KLINE_URL, params=params, timeout=20, headers=headers)
            resp.raise_for_status()
            data = resp.json().get("data") or {}
            break
        except requests.RequestException as exc:
            last_error = exc
            if attempt == 4:
                raise
            time.sleep(2 * (attempt + 1))
    if not data and last_error:
        raise last_error
    rows = []
    for item in data.get("klines") or []:
        parts = item.split(",")
        if len(parts) < 11:
            continue
        rows.append({
            "index_code": index_code,
            "trade_date": parts[0],
            "open": safe_float(parts[1]),
            "close": safe_float(parts[2]),
            "high": safe_float(parts[3]),
            "low": safe_float(parts[4]),
            "volume": safe_float(parts[5]),
            "amount": safe_float(parts[6]),
            "pct_change": safe_float(parts[8]),
            "turnover": safe_float(parts[10]),
        })
    return rows


def fetch_index_kline_baostock(index_code: str, start: str, end: str | None = None) -> list[dict[str, Any]]:
    baostock_code = BAOSTOCK_INDEX_CODES.get(index_code)
    if not baostock_code:
        raise RuntimeError(f"baostock 不支持指数 {index_code}")
    try:
        import baostock as bs
    except ImportError as exc:
        raise RuntimeError("未安装 baostock，无法使用历史K线兜底") from exc

    try:
        login = bs.login()
    except Exception as exc:
        raise RuntimeError(f"baostock 登录异常: {exc}") from exc
    if login.error_code != "0":
        raise RuntimeError(f"baostock 登录失败: {login.error_code} {login.error_msg}")
    fields = "date,open,high,low,close,preclose,volume,amount,pctChg"
    rows = []
    try:
        rs = bs.query_history_k_data_plus(
            baostock_code,
            fields,
            start_date=normalize_baostock_date(start),
            end_date=normalize_baostock_date(end),
            frequency="d",
            adjustflag="3",
        )
        if rs.error_code != "0":
            raise RuntimeError(f"baostock 查询失败: {rs.error_code} {rs.error_msg}")
        columns = rs.fields
        while rs.next():
            data = dict(zip(columns, rs.get_row_data()))
            close = safe_float(data.get("close"))
            if close is None or close <= 0:
                continue
            rows.append({
                "index_code": index_code,
                "trade_date": data.get("date"),
                "open": safe_float(data.get("open")),
                "close": close,
                "high": safe_float(data.get("high")),
                "low": safe_float(data.get("low")),
                "volume": safe_float(data.get("volume")),
                "amount": safe_float(data.get("amount")),
                "pct_change": safe_float(data.get("pctChg")),
                "turnover": None,
            })
    finally:
        bs.logout()
    return rows


def fetch_index_realtime_sina(trade_date: str | None = None) -> list[dict[str, Any]]:
    trade_date = trade_date or datetime.today().strftime("%Y-%m-%d")
    symbol_to_code = {symbol: code for code, symbol in SINA_INDEX_SYMBOLS.items()}
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 Chrome/120 Safari/537.36"
        ),
        "Referer": "https://finance.sina.com.cn/",
    }
    last_error: Exception | None = None
    for attempt in range(5):
        try:
            resp = requests.get(
                SINA_PRICE_URL + ",".join(SINA_INDEX_SYMBOLS.values()),
                timeout=15,
                headers=headers,
            )
            resp.raise_for_status()
            rows = []
            for line in resp.text.strip().splitlines():
                if "=" not in line or '"' not in line:
                    continue
                prefix, payload = line.split("=", 1)
                symbol = prefix.rsplit("_", 1)[-1]
                index_code = symbol_to_code.get(symbol)
                if not index_code:
                    continue
                parts = payload.strip().strip('";').split(",")
                if len(parts) < 10:
                    continue
                close = safe_float(parts[3])
                if close is None or close <= 0:
                    continue
                prev_close = safe_float(parts[2])
                quote_date = parts[30] if len(parts) > 30 and parts[30] else trade_date
                rows.append({
                    "index_code": index_code,
                    "trade_date": quote_date,
                    "open": safe_float(parts[1]),
                    "close": close,
                    "high": safe_float(parts[4]),
                    "low": safe_float(parts[5]),
                    "volume": safe_float(parts[8]),
                    "amount": safe_float(parts[9]),
                    "pct_change": ((close - prev_close) / prev_close * 100.0 if prev_close else None),
                    "turnover": None,
                })
            if rows:
                return rows
        except requests.RequestException as exc:
            last_error = exc
        time.sleep(2 * (attempt + 1))
    if last_error:
        raise last_error
    return []


def fetch_index_realtime_quotes(trade_date: str | None = None) -> list[dict[str, Any]]:
    """用实时行情接口兜底写入当日 000300/000852 指数价格。"""
    trade_date = trade_date or datetime.today().strftime("%Y-%m-%d")
    secids = ",".join(
        f"{source['market']}.{index_code}"
        for index_code, source in INDEX_PRICE_SOURCES.items()
    )
    params = {
        "fltt": 2,
        "invt": 2,
        "secids": secids,
        "fields": "f12,f14,f2,f3,f5,f6,f15,f16,f17,f18",
    }
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 Chrome/120 Safari/537.36"
        ),
        "Referer": "https://quote.eastmoney.com/",
    }
    last_error: Exception | None = None
    for attempt in range(5):
        for url in QUOTE_URLS:
            try:
                resp = requests.get(url, params=params, timeout=15, headers=headers)
                resp.raise_for_status()
                diff = (resp.json().get("data") or {}).get("diff") or []
                rows = []
                for item in diff:
                    index_code = str(item.get("f12") or "").strip()
                    if index_code not in INDEX_PRICE_SOURCES:
                        continue
                    close = safe_float(item.get("f2"))
                    if close is None or close <= 0:
                        continue
                    prev_close = safe_float(item.get("f18"))
                    pct_change = safe_float(item.get("f3"))
                    rows.append({
                        "index_code": index_code,
                        "trade_date": trade_date,
                        "open": safe_float(item.get("f17")),
                        "close": close,
                        "high": safe_float(item.get("f15")),
                        "low": safe_float(item.get("f16")),
                        "volume": safe_float(item.get("f5")),
                        "amount": safe_float(item.get("f6")),
                        "pct_change": (
                            pct_change if pct_change is not None
                            else ((close - prev_close) / prev_close * 100.0 if prev_close else None)
                        ),
                        "turnover": None,
                    })
                if rows:
                    return rows
            except requests.RequestException as exc:
                last_error = exc
        time.sleep(2 * (attempt + 1))
    if last_error:
        raise last_error
    return []


def save_index_prices(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    conn.executemany("""
        INSERT INTO index_prices (
            index_code, trade_date, open, close, high, low,
            volume, amount, pct_change, turnover
        )
        VALUES (
            :index_code, :trade_date, :open, :close, :high, :low,
            :volume, :amount, :pct_change, :turnover
        )
        ON CONFLICT(index_code, trade_date) DO UPDATE SET
            open = excluded.open,
            close = excluded.close,
            high = excluded.high,
            low = excluded.low,
            volume = excluded.volume,
            amount = excluded.amount,
            pct_change = excluded.pct_change,
            turnover = excluded.turnover
    """, rows)
    conn.commit()
    return len(rows)


def cmd_fetch_index_prices(conn: sqlite3.Connection, start: str, end: str | None) -> None:
    init_db(conn)
    for index_code, source in INDEX_PRICE_SOURCES.items():
        provider = "eastmoney"
        try:
            rows = fetch_index_kline(index_code, start=start, end=end)
        except (requests.RequestException, RuntimeError) as exc:
            print(f"[{index_code}] {source['name']} 东方财富失败，改用 baostock: {exc}")
            rows = fetch_index_kline_baostock(index_code, start=start, end=end)
            provider = "baostock"
        saved = save_index_prices(conn, rows)
        if rows:
            print(
                f"[{index_code}] {source['name']} 写入 {saved} 条："
                f"{rows[0]['trade_date']} ~ {rows[-1]['trade_date']} source={provider}"
            )
        else:
            print(f"[{index_code}] {source['name']} 未获取到行情")
        time.sleep(0.5)


def backfill_width_fast(conn: sqlite3.Connection, index_code: str, start: str,
                        end: str | None = None, force: bool = False) -> dict[str, Any]:
    init_width_table(conn)
    end = end or datetime.today().strftime("%Y-%m-%d")
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    query_start = (start_dt - timedelta(days=90)).strftime("%Y-%m-%d")

    total_count_row = conn.execute("""
        SELECT COUNT(*) FROM index_constituents WHERE index_code = ?
    """, (index_code,)).fetchone()
    total_count = int(total_count_row[0] or 0)
    if total_count == 0:
        raise RuntimeError(f"指数 {index_code} 没有成分股，请先运行 index.py 更新成分股")

    prices = pd.read_sql_query("""
        SELECT p.code, p.trade_date, p.close
        FROM daily_prices p
        JOIN index_constituents ic
          ON ic.stock_code = p.code
         AND ic.index_code = ?
        WHERE p.trade_date >= ?
          AND p.trade_date <= ?
          AND p.close IS NOT NULL
        ORDER BY p.code, p.trade_date
    """, conn, params=(index_code, query_start, end))
    if prices.empty:
        raise RuntimeError(f"指数 {index_code} 没有可用成分股日线")

    prices["trade_date"] = pd.to_datetime(prices["trade_date"])
    grouped = prices.groupby("code", sort=False)["close"]
    prices["high_20"] = grouped.transform(lambda s: s.rolling(20, min_periods=20).max())
    prices["low_20"] = grouped.transform(lambda s: s.rolling(20, min_periods=20).min())
    prices = prices[prices["trade_date"] >= pd.to_datetime(start)].copy()
    prices = prices[prices["high_20"].notna() & prices["low_20"].notna()]
    if prices.empty:
        raise RuntimeError(f"指数 {index_code} 在 {start} 之后不足20日历史，无法计算宽度")

    prices["score"] = 0
    prices.loc[prices["close"] >= prices["high_20"], "score"] = 1
    prices.loc[prices["close"] <= prices["low_20"], "score"] = -1
    prices["is_high"] = (prices["score"] == 1).astype(int)
    prices["is_low"] = (prices["score"] == -1).astype(int)

    stats = prices.groupby("trade_date").agg(
        score_sum=("score", "sum"),
        high_count=("is_high", "sum"),
        low_count=("is_low", "sum"),
        valid_count=("score", "count"),
    ).reset_index()
    stats["index_code"] = index_code
    stats["total_count"] = total_count
    stats["net_value"] = (stats["score_sum"] / stats["valid_count"]).round(6)
    stats["trade_date"] = stats["trade_date"].dt.strftime("%Y-%m-%d")

    if not force:
        existing = pd.read_sql_query("""
            SELECT trade_date
            FROM index_daily_stats
            WHERE index_code = ?
              AND trade_date >= ?
              AND trade_date <= ?
        """, conn, params=(index_code, start, end))
        if not existing.empty:
            stats = stats[~stats["trade_date"].isin(set(existing["trade_date"]))]

    rows = stats[[
        "index_code", "trade_date", "score_sum", "high_count", "low_count",
        "valid_count", "total_count", "net_value",
    ]].to_dict("records")
    if rows:
        conn.executemany("""
            INSERT OR REPLACE INTO index_daily_stats (
                index_code, trade_date, score_sum, high_count, low_count,
                valid_count, total_count, net_value
            )
            VALUES (
                :index_code, :trade_date, :score_sum, :high_count, :low_count,
                :valid_count, :total_count, :net_value
            )
        """, rows)
        conn.commit()

    return {
        "index_code": index_code,
        "start": start,
        "end": end,
        "source_constituents": total_count,
        "computed_rows": len(rows),
        "first": rows[0]["trade_date"] if rows else None,
        "last": rows[-1]["trade_date"] if rows else None,
        "force": force,
    }


def cmd_backfill_width(conn: sqlite3.Connection, start: str, end: str | None,
                       force: bool) -> None:
    for index_code in ("000300", "000852"):
        result = backfill_width_fast(conn, index_code, start, end=end, force=force)
        print(json.dumps(result, ensure_ascii=False))


def load_feature_frame(conn: sqlite3.Connection, start: str | None = None,
                       end: str | None = None) -> pd.DataFrame:
    price = pd.read_sql_query("""
        SELECT trade_date, open, close, high, low, volume, amount, pct_change
        FROM index_prices
        WHERE index_code = '000852'
          AND trade_date >= COALESCE(?, '1900-01-01')
          AND trade_date <= COALESCE(?, '2999-12-31')
        ORDER BY trade_date
    """, conn, params=(start, end))
    if price.empty:
        return price
    price["trade_date"] = pd.to_datetime(price["trade_date"])

    width = pd.read_sql_query("""
        SELECT trade_date,
               MAX(CASE WHEN index_code='000852' THEN score_sum END) AS csi_score,
               MAX(CASE WHEN index_code='000300' THEN score_sum END) AS hs300_score
        FROM index_daily_stats
        WHERE index_code IN ('000852','000300')
          AND trade_date >= COALESCE(?, '1900-01-01')
          AND trade_date <= COALESCE(?, '2999-12-31')
        GROUP BY trade_date
        ORDER BY trade_date
    """, conn, params=(start, end))
    if not width.empty:
        width["trade_date"] = pd.to_datetime(width["trade_date"])
        width = width.sort_values("trade_date").reset_index(drop=True)
        width["csi_score_ma3"] = width["csi_score"].rolling(3, min_periods=3).mean()
        width["hs300_score_ma3"] = width["hs300_score"].rolling(3, min_periods=3).mean()

    df = price.merge(width, how="left", on="trade_date")
    df = df.sort_values("trade_date").reset_index(drop=True)

    df["amount_ma5"] = df["amount"].rolling(5, min_periods=5).mean()
    df["amount_ma20"] = df["amount"].rolling(20, min_periods=20).mean()
    df["vol_ratio_5_20"] = df["amount_ma5"] / df["amount_ma20"]
    df["low10"] = df["close"].rolling(10, min_periods=10).min()
    df["high10"] = df["close"].rolling(10, min_periods=10).max()
    df["price_from_low10"] = df["close"] / df["low10"] - 1.0
    df["drawdown_from_high10"] = 1.0 - df["close"] / df["high10"]
    df["pct_1d"] = df["close"] / df["close"].shift(1) - 1.0
    df["pct_2d"] = df["close"] / df["close"].shift(2) - 1.0
    return df


def load_excel_feature_frame(path: str | Path, start: str | None = None,
                             end: str | None = None) -> pd.DataFrame:
    """
    直接使用历史 Excel 中的宽度和中证1000行情。

    Excel 结构：
      - 测算：A-E 为中证1000 OHLC，I-K 为成交额/5日均额/20日均额。
      - 以前300和1000统计：C 为1000宽度3日均值，G 为300宽度3日均值。
        D/E/H/I 分别为当日新高/新低数，可用于还原当日 score。
    """
    path = Path(path)
    calc = pd.read_excel(path, sheet_name="测算", header=None)
    stats = pd.read_excel(path, sheet_name="以前300和1000统计", header=None)

    calc = calc.iloc[1:].copy()
    calc = calc.iloc[:, :11]
    calc.columns = [
        "trade_date", "open", "high", "low", "close",
        "_c5", "_c6", "_c7", "amount", "amount_ma5", "amount_ma20",
    ]
    calc["trade_date"] = pd.to_datetime(calc["trade_date"], errors="coerce")
    for col in ("open", "high", "low", "close", "amount", "amount_ma5", "amount_ma20"):
        calc[col] = pd.to_numeric(calc[col], errors="coerce")
    calc = calc[calc["trade_date"].notna() & calc["close"].notna()]

    stats = stats.iloc[1:].copy()
    stats = stats.iloc[:, :9]
    stats.columns = [
        "trade_date", "_blank1", "csi_score_ma3", "csi_high_count", "csi_low_count",
        "_blank2", "hs300_score_ma3", "hs300_high_count", "hs300_low_count",
    ]
    stats["trade_date"] = pd.to_datetime(stats["trade_date"], errors="coerce")
    for col in (
        "csi_score_ma3", "csi_high_count", "csi_low_count",
        "hs300_score_ma3", "hs300_high_count", "hs300_low_count",
    ):
        stats[col] = pd.to_numeric(stats[col], errors="coerce")
    stats = stats[stats["trade_date"].notna()]
    stats["csi_score"] = stats["csi_high_count"] - stats["csi_low_count"]
    stats["hs300_score"] = stats["hs300_high_count"] - stats["hs300_low_count"]

    df = calc.merge(
        stats[["trade_date", "csi_score", "hs300_score", "csi_score_ma3", "hs300_score_ma3"]],
        on="trade_date",
        how="left",
    )
    df = df.sort_values("trade_date").reset_index(drop=True)

    df["vol_ratio_5_20"] = df["amount_ma5"] / df["amount_ma20"]
    df["low10"] = df["close"].rolling(10, min_periods=10).min()
    df["high10"] = df["close"].rolling(10, min_periods=10).max()
    df["price_from_low10"] = df["close"] / df["low10"] - 1.0
    df["drawdown_from_high10"] = 1.0 - df["close"] / df["high10"]
    df["pct_1d"] = df["close"] / df["close"].shift(1) - 1.0
    df["pct_2d"] = df["close"] / df["close"].shift(2) - 1.0

    if start:
        df = df[df["trade_date"] >= pd.to_datetime(start)]
    if end:
        df = df[df["trade_date"] <= pd.to_datetime(end)]
    return df.reset_index(drop=True)


def load_excel_width_frame(path: str | Path) -> pd.DataFrame:
    """读取 Excel 中的 1000/300 宽度，不读取行情和量能。"""
    stats = pd.read_excel(path, sheet_name="以前300和1000统计", header=None)
    stats = stats.iloc[1:].copy()
    stats = stats.iloc[:, :9]
    stats.columns = [
        "trade_date", "_blank1", "csi_score_ma3", "csi_high_count", "csi_low_count",
        "_blank2", "hs300_score_ma3", "hs300_high_count", "hs300_low_count",
    ]
    stats["trade_date"] = pd.to_datetime(stats["trade_date"], errors="coerce")
    for col in (
        "csi_score_ma3", "csi_high_count", "csi_low_count",
        "hs300_score_ma3", "hs300_high_count", "hs300_low_count",
    ):
        stats[col] = pd.to_numeric(stats[col], errors="coerce")
    stats = stats[stats["trade_date"].notna()].copy()
    stats["csi_score"] = stats["csi_high_count"] - stats["csi_low_count"]
    stats["hs300_score"] = stats["hs300_high_count"] - stats["hs300_low_count"]
    return stats[[
        "trade_date", "csi_score", "hs300_score", "csi_score_ma3", "hs300_score_ma3",
    ]].sort_values("trade_date").reset_index(drop=True)


def load_db_price_excel_width_frame(conn: sqlite3.Connection, excel_path: str | Path,
                                    start: str | None = None,
                                    end: str | None = None) -> pd.DataFrame:
    """本地指数行情/量能 + Excel 日期范围/宽度。"""
    width = load_excel_width_frame(excel_path)
    if width.empty:
        return width
    excel_start = width["trade_date"].min().strftime("%Y-%m-%d")
    excel_end = width["trade_date"].max().strftime("%Y-%m-%d")
    start = start or excel_start
    end = end or excel_end

    df = load_feature_frame(conn, start=start, end=end)
    if df.empty:
        return df
    df = df.drop(columns=[
        col for col in ("csi_score", "hs300_score", "csi_score_ma3", "hs300_score_ma3")
        if col in df.columns
    ])
    width = width[(width["trade_date"] >= pd.to_datetime(start)) & (width["trade_date"] <= pd.to_datetime(end))]
    df = df.merge(width, on="trade_date", how="inner")
    return df.sort_values("trade_date").reset_index(drop=True)


def finite(value: Any) -> bool:
    return value is not None and not pd.isna(value) and math.isfinite(float(value))


def classify_signal(row: pd.Series, config: StrategyConfig,
                    current_position: str = "FLAT",
                    state: SignalContextState | None = None,
                    row_index: int | None = None) -> tuple[str, str, str]:
    needed = [
        row.get("csi_score_ma3"), row.get("hs300_score_ma3"),
        row.get("vol_ratio_5_20"), row.get("price_from_low10"),
        row.get("drawdown_from_high10"),
    ]
    if not all(finite(v) for v in needed):
        return "NO_DATA", "数据不足", "缺少指数行情、20日量能或宽度指标"

    if current_position == "SHORT":
        short_stop_gain = short_stop_gain_since_entry(row, state, row_index)
        if short_stop_gain is not None and short_stop_gain >= config.short_stop_2d_gain:
            if state is not None:
                state.block_short_wave()
            return "FLAT", "空单止损", "中证1000持仓后反弹超过空单止损线，本轮空头波段作废"

    long_context = (
        row["csi_score_ma3"] > config.long_csi_score_min
        and row["hs300_score_ma3"] > config.long_hs300_score_min
        and row["vol_ratio_5_20"] > config.long_vol_ratio_min
    )
    short_context = (
        row["csi_score_ma3"] < config.short_csi_score_max
        and row["hs300_score_ma3"] < config.short_hs300_score_max
        and row["vol_ratio_5_20"] <= config.short_vol_ratio_max
    )
    short_width_context = (
        row["csi_score_ma3"] < config.short_csi_score_max
        and row["hs300_score_ma3"] < config.short_hs300_score_max
    )

    if state is None:
        return classify_signal_stateless(row, config, long_context, short_context)

    if state.long_wave_active and not long_context:
        state.reset_long()
    if state.short_wave_active and not short_width_context:
        state.reset_short()

    if long_context:
        if not state.long_wave_active:
            state.long_wave_active = True
            state.long_wave_blocked = row["price_from_low10"] < config.long_price_from_low_min
        state.reset_short()
        if state.long_wave_blocked:
            return "FLAT", "做多波段作废", "多头环境成立，但波段开始时价格未脱离10日低点"
        return "LONG", "做多", "1000宽度MA3转强，300宽度MA3不弱，5/20日成交额放大，价格脱离10日低点"

    if short_context:
        if not state.short_wave_active:
            state.short_wave_active = True
            if row["drawdown_from_high10"] <= config.short_drawdown_from_high_max:
                state.short_wave_target = "SHORT"
            elif config.reverse_deep_short:
                state.short_wave_target = "LONG"
            else:
                state.short_wave_target = "FLAT"
        state.reset_long()
        if state.short_wave_target == "SHORT":
            return "SHORT", "做空", "大小盘3日宽度偏弱，1000缩量，且未从10日高点跌太深"
        if state.short_wave_target == "LONG":
            return "LONG", "跌深反做多", "空头环境已从10日高点跌超阈值，按文档反向做多假设处理"
        return "FLAT", "跌深不追空", "空头环境成立但已从10日高点跌超阈值，等待反弹或新机会"

    if state.short_wave_active and short_width_context:
        state.block_short_wave()
        state.reset_long()
        return "FLAT", "空头波段作废", "本轮空头宽度波段已退出或条件中断，后续不再开仓，等待下一波段"

    state.reset_long()
    state.reset_short()
    return "FLAT", "空仓", "量价和宽度未形成可交易共振"


def short_stop_gain_since_entry(row: pd.Series, state: SignalContextState | None,
                                row_index: int | None) -> float | None:
    gains = []
    if finite(row.get("pct_1d")):
        gains.append(float(row["pct_1d"]))
    if (
        state is not None
        and state.trade_position == "SHORT"
        and state.trade_entry_price
        and state.trade_entry_index is not None
        and row_index is not None
    ):
        bars_since_entry = row_index - state.trade_entry_index
        if bars_since_entry <= 2:
            gains.append(float(row["close"]) / state.trade_entry_price - 1.0)
            return max(gains) if gains else None
    if finite(row.get("pct_2d")):
        gains.append(float(row["pct_2d"]))
    return max(gains) if gains else None


def classify_signal_stateless(row: pd.Series, config: StrategyConfig,
                              long_context: bool, short_context: bool) -> tuple[str, str, str]:
    if long_context:
        if row["price_from_low10"] < config.long_price_from_low_min:
            return "FLAT", "做多波段作废", "多头环境成立，但价格未脱离10日低点"
        return "LONG", "做多", "1000宽度MA3转强，300宽度MA3不弱，5/20日成交额放大，价格脱离10日低点"
    if short_context:
        if row["drawdown_from_high10"] <= config.short_drawdown_from_high_max:
            return "SHORT", "做空", "大小盘3日宽度偏弱，1000缩量，且未从10日高点跌太深"
        if config.reverse_deep_short:
            return "LONG", "跌深反做多", "空头环境已从10日高点跌超阈值，按文档反向做多假设处理"
        return "FLAT", "跌深不追空", "空头环境成立但已从10日高点跌超阈值，等待反弹或新机会"
    return "FLAT", "空仓", "量价和宽度未形成可交易共振"


def trade_state_from_signal(signal: str) -> str:
    if signal == "LONG":
        return "多1000"
    if signal == "SHORT":
        return "空1000"
    return "空仓"


def row_payload(row: pd.Series) -> dict[str, Any]:
    keys = [
        "trade_date", "open", "close", "amount", "csi_score", "hs300_score",
        "csi_score_ma3", "hs300_score_ma3", "vol_ratio_5_20",
        "price_from_low10", "drawdown_from_high10", "pct_1d", "pct_2d",
    ]
    payload = {}
    for key in keys:
        value = row.get(key)
        if isinstance(value, pd.Timestamp):
            payload[key] = value.strftime("%Y-%m-%d")
        elif pd.isna(value):
            payload[key] = None
        elif isinstance(value, float):
            payload[key] = round(value, 6)
        else:
            payload[key] = value
    return payload


def save_signal(conn: sqlite3.Connection, row: pd.Series, signal: str,
                action: str, reason: str) -> None:
    payload = row_payload(row)
    trade_state = trade_state_from_signal(signal)
    conn.execute("""
        INSERT INTO csi1000_timing_signals (
            trade_date, signal, trade_state, action, reason, csi_close,
            csi_score, hs300_score, csi_score_ma3, hs300_score_ma3,
            vol_ratio_5_20, price_from_low10, drawdown_from_high10,
            pct_2d, payload_json, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))
        ON CONFLICT(trade_date) DO UPDATE SET
            signal = excluded.signal,
            trade_state = excluded.trade_state,
            action = excluded.action,
            reason = excluded.reason,
            csi_close = excluded.csi_close,
            csi_score = excluded.csi_score,
            hs300_score = excluded.hs300_score,
            csi_score_ma3 = excluded.csi_score_ma3,
            hs300_score_ma3 = excluded.hs300_score_ma3,
            vol_ratio_5_20 = excluded.vol_ratio_5_20,
            price_from_low10 = excluded.price_from_low10,
            drawdown_from_high10 = excluded.drawdown_from_high10,
            pct_2d = excluded.pct_2d,
            payload_json = excluded.payload_json,
            updated_at = datetime('now','localtime')
    """, (
        payload["trade_date"], signal, trade_state, action, reason, payload.get("close"),
        payload.get("csi_score"), payload.get("hs300_score"),
        payload.get("csi_score_ma3"), payload.get("hs300_score_ma3"),
        payload.get("vol_ratio_5_20"), payload.get("price_from_low10"),
        payload.get("drawdown_from_high10"), payload.get("pct_2d"),
        json.dumps(payload, ensure_ascii=False),
    ))
    conn.commit()


def generate_and_save_signals(conn: sqlite3.Connection, df: pd.DataFrame,
                              config: StrategyConfig) -> pd.DataFrame:
    rows = []
    position = "FLAT"
    state = SignalContextState()
    for i, row in df.iterrows():
        signal, action, reason = classify_signal(row, config, position, state, i)
        if signal in ("LONG", "SHORT", "FLAT"):
            if signal != position:
                if signal in ("LONG", "SHORT"):
                    state.set_trade_position(signal, float(row["close"]), i)
                else:
                    state.clear_trade_position()
            position = signal
        save_signal(conn, row, signal, action, reason)
        item = row_payload(row)
        item.update({
            "signal": signal,
            "trade_state": trade_state_from_signal(signal),
            "action": action,
            "reason": reason,
        })
        rows.append(item)
    return pd.DataFrame(rows)


def backtest(conn: sqlite3.Connection, df: pd.DataFrame,
             config: StrategyConfig, run_key: str) -> dict[str, Any]:
    if len(df) < 30:
        raise RuntimeError("可回测数据不足，至少需要30个交易日")

    fee = config.fee_bps / 10000.0
    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    position = "FLAT"
    trades: list[dict[str, Any]] = []
    current_trade: dict[str, Any] | None = None
    daily_rows = []
    state = SignalContextState()
    prev_close: float | None = None

    for i in range(len(df)):
        row = df.iloc[i]
        today = row["trade_date"].strftime("%Y-%m-%d")
        close_price = float(row["close"])

        if prev_close is None:
            interval_ret = 0.0
        elif position == "LONG":
            interval_ret = close_price / prev_close - 1.0
        elif position == "SHORT":
            interval_ret = prev_close / close_price - 1.0
        else:
            interval_ret = 0.0
        equity *= (1.0 + interval_ret)
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity / peak - 1.0)

        signal, action, reason = classify_signal(row, config, position, state, i)
        target = "FLAT" if signal == "NO_DATA" else signal

        if target != position:
            if current_trade:
                direction = current_trade["direction"]
                ret = (
                    close_price / current_trade["entry_price"] - 1.0
                    if direction == "LONG"
                    else current_trade["entry_price"] / close_price - 1.0
                )
                current_trade.update({
                    "exit_date": today,
                    "exit_price": close_price,
                    "exit_reason": action or "signal_change",
                    "hold_days": len(daily_rows) - current_trade["entry_index"],
                    "return_pct": (ret - fee) * 100.0,
                })
                trades.append(current_trade)
                equity *= (1.0 - fee)
                current_trade = None

            if target in ("LONG", "SHORT") and i < len(df) - 1:
                position = target
                equity *= (1.0 - fee)
                state.set_trade_position(position, close_price, i)
                current_trade = {
                    "run_key": run_key,
                    "direction": position,
                    "entry_date": today,
                    "entry_price": close_price,
                    "signal_date": today,
                    "entry_reason": action,
                    "entry_index": len(daily_rows),
                }
            else:
                position = "FLAT"
                state.clear_trade_position()

        daily_rows.append({
            "trade_date": today,
            "position": position,
            "position_state": trade_state_from_signal(position),
            "signal": signal,
            "trade_state": trade_state_from_signal(signal),
            "action": action,
            "equity": equity,
            "interval_ret": interval_ret,
        })
        prev_close = close_price

    final_row = df.iloc[-1]
    final_date = final_row["trade_date"].strftime("%Y-%m-%d")
    final_close = float(final_row["close"])
    if current_trade:
        direction = current_trade["direction"]
        ret = (
            final_close / current_trade["entry_price"] - 1.0
            if direction == "LONG"
            else current_trade["entry_price"] / final_close - 1.0
        )
        current_trade.update({
            "exit_date": final_date,
            "exit_price": final_close,
            "exit_reason": "end_of_data",
            "hold_days": len(daily_rows) - current_trade["entry_index"],
            "return_pct": (ret - fee) * 100.0,
        })
        trades.append(current_trade)
        equity *= (1.0 - fee)
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity / peak - 1.0)

    conn.execute("DELETE FROM csi1000_timing_trades WHERE run_key=?", (run_key,))
    conn.executemany("""
        INSERT INTO csi1000_timing_trades (
            run_key, direction, entry_date, entry_price, exit_date, exit_price,
            exit_reason, hold_days, return_pct, signal_date, entry_reason
        )
        VALUES (
            :run_key, :direction, :entry_date, :entry_price, :exit_date, :exit_price,
            :exit_reason, :hold_days, :return_pct, :signal_date, :entry_reason
        )
    """, [
        {k: v for k, v in trade.items() if k != "entry_index"}
        for trade in trades
    ])
    conn.commit()

    daily = pd.DataFrame(daily_rows)
    trade_returns = [t["return_pct"] for t in trades if t.get("return_pct") is not None]
    wins = [r for r in trade_returns if r > 0]
    long_days = int((daily["position"] == "LONG").sum()) if not daily.empty else 0
    short_days = int((daily["position"] == "SHORT").sum()) if not daily.empty else 0

    return {
        "run_key": run_key,
        "start": df.iloc[0]["trade_date"].strftime("%Y-%m-%d"),
        "end": final_date,
        "config": asdict(config),
        "days": len(daily),
        "total_return_pct": (equity - 1.0) * 100.0,
        "max_drawdown_pct": max_drawdown * 100.0,
        "trade_count": len(trades),
        "win_rate_pct": len(wins) / len(trade_returns) * 100.0 if trade_returns else 0.0,
        "avg_trade_return_pct": sum(trade_returns) / len(trade_returns) if trade_returns else 0.0,
        "long_days": long_days,
        "short_days": short_days,
        "flat_days": len(daily) - long_days - short_days,
    }


def format_pct(value: Any) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value) * 100:.2f}%"


def format_num(value: Any, digits: int = 3) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.{digits}f}"


def print_signal(row: pd.Series | sqlite3.Row) -> None:
    if isinstance(row, pd.Series):
        data = row.to_dict()
    elif isinstance(row, sqlite3.Row):
        data = dict(row)
    else:
        data = dict(row)

    close = data.get("csi_close")
    if close is None or pd.isna(close):
        close = data.get("close")

    trade_state = data.get("trade_state") or trade_state_from_signal(data.get("signal"))
    print(
        f"{data.get('trade_date')}  {data.get('action')}  "
        f"做单状态={trade_state}  signal={data.get('signal')}"
    )
    print(f"原因: {data.get('reason')}")
    print(
        "指标: "
        f"收盘={format_num(close, 2)}  "
        f"1000宽度={data.get('csi_score')}  300宽度={data.get('hs300_score')}  "
        f"量比5/20={format_num(data.get('vol_ratio_5_20'), 3)}  "
        f"离10日低点={format_pct(data.get('price_from_low10'))}  "
        f"离10日高点回撤={format_pct(data.get('drawdown_from_high10'))}"
    )


def load_frame_by_source(conn: sqlite3.Connection, source: str, excel_path: str | Path,
                         start: str | None = None, end: str | None = None) -> pd.DataFrame:
    if source == "excel":
        return load_excel_feature_frame(excel_path, start=start, end=end)
    if source == "excel_width":
        return load_db_price_excel_width_frame(conn, excel_path, start=start, end=end)
    return load_feature_frame(conn, start=start, end=end)


def cmd_signal(conn: sqlite3.Connection, config: StrategyConfig, date: str | None,
               source: str, excel_path: str | Path) -> None:
    init_db(conn)
    df = load_frame_by_source(conn, source, excel_path)
    if df.empty:
        raise RuntimeError("没有可用行情/宽度数据")
    signals = generate_and_save_signals(conn, df, config)
    if date:
        selected = signals[signals["trade_date"] == date]
    else:
        selected = signals.tail(1)
    if selected.empty:
        raise RuntimeError(f"没有找到信号日期: {date}")
    print_signal(selected.iloc[-1])


def cmd_backtest(conn: sqlite3.Connection, config: StrategyConfig,
                 start: str | None, end: str | None, run_key: str,
                 source: str, excel_path: str | Path) -> None:
    init_db(conn)
    df = load_frame_by_source(conn, source, excel_path, start=start, end=end)
    if df.empty:
        raise RuntimeError("没有可用行情/宽度数据")
    generate_and_save_signals(conn, df, config)
    result = backtest(conn, df, config, run_key)
    print(json.dumps(result, ensure_ascii=False, indent=2))


def cmd_list_signals(conn: sqlite3.Connection, limit: int, include_unchanged: bool = False) -> None:
    query_limit = limit + 1 if include_unchanged else 10000
    rows = conn.execute("""
        SELECT trade_date, signal, trade_state, action, reason, csi_close, csi_score,
               hs300_score, csi_score_ma3, hs300_score_ma3, vol_ratio_5_20,
               price_from_low10, drawdown_from_high10, pct_2d
        FROM csi1000_timing_signals
        ORDER BY trade_date DESC
        LIMIT ?
    """, (query_limit,)).fetchall()
    chronological = list(reversed(rows))
    if include_unchanged and len(chronological) > limit:
        chronological = chronological[1:]
    previous_state = None
    if rows and len(rows) > limit:
        older = rows[-1]
        previous_state = older["trade_state"] or trade_state_from_signal(older["signal"])

    detailed_rows = []
    for row in chronological:
        trade_state = row["trade_state"] or trade_state_from_signal(row["signal"])
        exec_date = row["trade_date"]
        if previous_state is None:
            instruction = "初始状态"
        else:
            instruction = execution_instruction(previous_state, trade_state)
        detailed_rows.append((row, previous_state, trade_state, exec_date, instruction))
        previous_state = trade_state

    if not include_unchanged:
        operation_rows = [
            item for item in detailed_rows
            if item[1] is not None and not item[4].startswith("维持")
        ]
        execution_prices = load_index_close_prices(conn, [item[3] for item in operation_rows])
        annotated_rows = annotate_operation_returns(operation_rows, execution_prices)
        for item in reversed(annotated_rows[-limit:]):
            row, _previous_state, trade_state, exec_date, instruction, return_pct = item
            suffix = f" 收益={return_pct:.2f}%" if return_pct is not None else ""
            if return_pct is not None:
                suffix += f" 平仓原因={row['action']}"
            print(f"{exec_date} 收盘 {instruction}{suffix}")
            print(f"  {row['trade_date']} 收盘信号={trade_state} 原因={row['action']}")
            print(
                "  "
                f"close={format_num(row['csi_close'], 2)} "
                f"ma3_1000={format_num(row['csi_score_ma3'], 1)} "
                f"ma3_300={format_num(row['hs300_score_ma3'], 1)} "
                f"vol={format_num(row['vol_ratio_5_20'], 3)} "
                f"low10={format_pct(row['price_from_low10'])} "
                f"dd10={format_pct(row['drawdown_from_high10'])}"
            )
        return

    for row, previous_state, trade_state, exec_date, instruction in reversed(detailed_rows):
        print(
            f"{row['trade_date']} 收盘信号={trade_state:<6} "
            f"前状态={previous_state or '-':<6} 执行={exec_date}收盘 {instruction} "
            f"原因={row['action']}"
        )
        print(
            "  "
            f"close={format_num(row['csi_close'], 2)} "
            f"w1000={row['csi_score']} w300={row['hs300_score']} "
            f"ma3_1000={format_num(row['csi_score_ma3'], 1)} "
            f"ma3_300={format_num(row['hs300_score_ma3'], 1)} "
            f"vol={format_num(row['vol_ratio_5_20'], 3)} "
            f"low10={format_pct(row['price_from_low10'])} "
            f"dd10={format_pct(row['drawdown_from_high10'])} "
            f"pct2d={format_pct(row['pct_2d'])}"
        )
        print(f"  说明: {row['reason']}")


def load_index_close_prices(conn: sqlite3.Connection, trade_dates: list[str]) -> dict[str, float]:
    dates = sorted({d for d in trade_dates if d and d != "下一交易日"})
    if not dates:
        return {}
    placeholders = ",".join("?" for _ in dates)
    rows = conn.execute(f"""
        SELECT trade_date, close
        FROM index_prices
        WHERE index_code = '000852' AND trade_date IN ({placeholders})
    """, dates).fetchall()
    return {row["trade_date"]: float(row["close"]) for row in rows if row["close"] is not None}


def annotate_operation_returns(
    operation_rows: list[tuple[sqlite3.Row, str | None, str, str, str]],
    open_prices: dict[str, float],
) -> list[tuple[sqlite3.Row, str | None, str, str, str, float | None]]:
    active_state: str | None = None
    entry_price: float | None = None
    annotated = []
    for row, previous_state, trade_state, exec_date, instruction in operation_rows:
        exec_price = open_prices.get(exec_date)
        return_pct = None
        if exec_price is not None and active_state and entry_price:
            if active_state == "多1000" and trade_state != "多1000":
                return_pct = (exec_price / entry_price - 1.0) * 100.0
            elif active_state == "空1000" and trade_state != "空1000":
                return_pct = (entry_price / exec_price - 1.0) * 100.0

        if trade_state in ("多1000", "空1000") and exec_price is not None:
            active_state = trade_state
            entry_price = exec_price
        elif trade_state == "空仓":
            active_state = None
            entry_price = None

        annotated.append((row, previous_state, trade_state, exec_date, instruction, return_pct))
    return annotated


def execution_instruction(previous_state: str, trade_state: str) -> str:
    if previous_state == trade_state:
        return f"维持{trade_state}"
    if previous_state == "空仓" and trade_state == "多1000":
        return "开多1000"
    if previous_state == "空仓" and trade_state == "空1000":
        return "开空1000"
    if previous_state == "多1000" and trade_state == "空仓":
        return "平多1000"
    if previous_state == "空1000" and trade_state == "空仓":
        return "平空1000"
    if previous_state == "多1000" and trade_state == "空1000":
        return "平多1000并开空1000"
    if previous_state == "空1000" and trade_state == "多1000":
        return "平空1000并开多1000"
    return f"{previous_state} -> {trade_state}"


def cmd_list_trades(conn: sqlite3.Connection, limit: int) -> None:
    rows = conn.execute("""
        SELECT run_key, direction, entry_date, entry_price, exit_date, exit_price,
               exit_reason, hold_days, return_pct, entry_reason
        FROM csi1000_timing_trades
        ORDER BY id DESC
        LIMIT ?
    """, (limit,)).fetchall()
    for row in rows:
        print(
            f"{row['run_key']} {row['direction']:<5} "
            f"{row['entry_date']}@{row['entry_price']:.2f} -> "
            f"{row['exit_date']}@{row['exit_price']:.2f} "
            f"{row['return_pct']:.2f}% {row['exit_reason']} "
            f"hold={row['hold_days']}d reason={row['entry_reason']}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="中证1000择时策略回测与每日信号")
    parser.add_argument("--db", default=str(DB_PATH), help="SQLite 数据库路径")
    parser.add_argument("--init-db", action="store_true", help="初始化策略表")
    parser.add_argument("--fetch-index-prices", action="store_true", help="从东方财富同步沪深300/中证1000指数日线")
    parser.add_argument("--backfill-width", action="store_true", help="用当前成分股快速补算沪深300/中证1000宽度指标")
    parser.add_argument("--start", default=None, help="开始日期；抓行情用 YYYYMMDD，回测用 YYYY-MM-DD 或 YYYYMMDD")
    parser.add_argument("--end", default=None, help="结束日期；抓行情用 YYYYMMDD，回测用 YYYY-MM-DD 或 YYYYMMDD")
    parser.add_argument("--backtest", action="store_true", help="运行回测并保存交易明细")
    parser.add_argument("--signal", action="store_true", help="生成并显示最新信号")
    parser.add_argument("--date", default=None, help="配合 --signal 查看指定日期 YYYY-MM-DD")
    parser.add_argument("--signals", action="store_true", help="查看已保存的近期信号")
    parser.add_argument("--include-unchanged", action="store_true",
                        help="配合 --signals 显示每日维持仓位记录；默认只显示操作变化")
    parser.add_argument("--trades", action="store_true", help="查看已保存的近期交易")
    parser.add_argument("--limit", type=int, default=20, help="列表数量")
    parser.add_argument("--run-key", default="default", help="回测交易保存批次名")
    parser.add_argument("--data-source", choices=["db", "excel", "excel_width"], default="db",
                        help="数据来源：db=数据库行情+宽度，excel=Excel行情+宽度，excel_width=数据库行情+Excel宽度")
    parser.add_argument("--excel-path", default=str(BASE_DIR / "data" / "历史新高新低300和1000.xlsx"),
                        help="--data-source excel 时读取的 Excel 路径")
    parser.add_argument("--preset", choices=sorted(STRATEGY_PRESETS), default="low_dd",
                        help="策略参数预设，默认 low_dd=稳定低回撤版本")
    parser.add_argument("--reverse-deep-short", action="store_true",
                        help="强制开启跌深不追空时反手做多；预设默认值已包含该设置")
    parser.add_argument("--no-reverse-deep-short", action="store_true",
                        help="关闭跌深反手做多，只做跌深不追空")
    parser.add_argument("--fee-bps", type=float, default=2.0, help="单边交易成本，默认2bp")
    parser.add_argument("--force", action="store_true", help="补宽度时覆盖已存在日期")
    return parser.parse_args()


def build_config(args: argparse.Namespace) -> StrategyConfig:
    config = STRATEGY_PRESETS[args.preset]
    if args.reverse_deep_short:
        config = replace(config, reverse_deep_short=True)
    if args.no_reverse_deep_short:
        config = replace(config, reverse_deep_short=False)
    return replace(config, fee_bps=args.fee_bps)


def normalize_dash_date(value: str | None) -> str | None:
    if not value:
        return None
    text = value.strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text


def normalize_plain_date(value: str | None) -> str | None:
    if not value:
        return None
    return value.replace("-", "")


def main() -> int:
    args = parse_args()
    config = build_config(args)
    conn = connect(args.db)
    try:
        if args.init_db:
            init_db(conn)
            print("策略表已初始化")
        elif args.fetch_index_prices:
            cmd_fetch_index_prices(
                conn,
                start=normalize_plain_date(args.start) or "20100101",
                end=normalize_plain_date(args.end),
            )
        elif args.backfill_width:
            cmd_backfill_width(
                conn,
                start=normalize_dash_date(args.start) or "2016-06-20",
                end=normalize_dash_date(args.end),
                force=args.force,
            )
        elif args.backtest:
            cmd_backtest(
                conn,
                config,
                start=normalize_dash_date(args.start),
                end=normalize_dash_date(args.end),
                run_key=args.run_key,
                source=args.data_source,
                excel_path=args.excel_path,
            )
        elif args.signal:
            cmd_signal(conn, config, normalize_dash_date(args.date), args.data_source, args.excel_path)
        elif args.signals:
            init_db(conn)
            cmd_list_signals(conn, args.limit, include_unchanged=args.include_unchanged)
        elif args.trades:
            init_db(conn)
            cmd_list_trades(conn, args.limit)
        else:
            print("请指定命令。示例：python csi1000_timing.py --signal")
            return 2
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
