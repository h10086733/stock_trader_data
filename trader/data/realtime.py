"""实时行情与分钟 K 线：新浪/东方财富报价抓取、5 分钟/1 分钟 K 线、内存+DB 双层缓存。

来源：app.py 中以下函数段（已从 Flask 运行时解耦）：
  - fetch_sina_quotes / fetch_eastmoney_quotes / fetch_realtime_quotes（~2912-3086）
  - fetch_eastmoney_5m_kline / fetch_eastmoney_1m_kline_for_date（~2690-2840）
  - fetch_baostock_5m_kline_uncached / fetch_baostock_5m_klines_parallel（~2638-2912）
  - get_cached_kline / set_cached_kline / ensure_kline_cache_table（~2789-2840）
  - load_stock_universe / load_daily_metrics（~2513-3145）
  - infer_market / to_sina_symbol / to_baostock_code（~2460-2512）
  - score_candidate / build_sparkline（~3223-3320）
  - daily_price_coverage_threshold / latest_daily_trade_date / recent_market_trade_dates（~3283-3350）
"""
from __future__ import annotations

import json
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

import requests

from trader.core import config
from trader.core.db import connect_existing
from trader.core.utils import chunked, clamp, to_float

try:
    import baostock as _bs_module
except ImportError:
    _bs_module = None

# ── 运行时状态（模块级单例） ─────────────────────────────────────────────
BAOSTOCK_LOCK = threading.Lock()
BAOSTOCK_LOGGED_IN = False

KLINE_CACHE: dict = {}
KLINE_CACHE_LOCK = threading.Lock()
KLINE_CACHE_DB_READY = False

# ── 内部工具 ─────────────────────────────────────────────────────────────

def _default_scan_trade_date() -> str:
    """返回今天日期（YYYY-MM-DD）作为扫描交易日默认值。"""
    return datetime.today().strftime("%Y-%m-%d")


def _is_current_scan_date(trade_date: str) -> bool:
    return trade_date == _default_scan_trade_date()


def _get_json_with_retry(url, params, headers, timeout=4, retries=2):
    last_error = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_error = exc
            time.sleep(0.25 * (attempt + 1))
    raise last_error


# ── 股票代码工具 ─────────────────────────────────────────────────────────

def to_sina_symbol(code: str) -> str:
    if code.startswith("92"):
        return "bj" + code
    return ("sh" if code.startswith(("5", "6", "9")) else "sz") + code


def infer_market(code: str, market: Optional[str] = None) -> str:
    """返回东方财富 secid 的市场前缀（'1'=沪, '0'=深/北）。"""
    if market in ("0", "1"):
        return market
    return "1" if code.startswith(("5", "6", "9")) else "0"


def to_baostock_code(code: str) -> str:
    return ("sh." if code.startswith(("5", "6", "9")) else "sz.") + code


# ── 股票池 ───────────────────────────────────────────────────────────────

def load_stock_universe(conn, pool: str = "all", index_code: str = "") -> list[dict]:
    """从数据库读取股票池，过滤 ST/退市，补全 market 字段。"""
    if pool == "sector":
        rows = conn.execute("""
            SELECT DISTINCT ss.stock_code AS code,
                   COALESCE(s.name, ss.stock_name) AS name,
                   s.market
            FROM sector_stocks ss
            LEFT JOIN stocks s ON s.code = ss.stock_code
            WHERE COALESCE(s.is_delisted, 0) = 0
            ORDER BY ss.stock_code
        """).fetchall()
    elif pool == "index" and index_code:
        rows = conn.execute("""
            SELECT DISTINCT ic.stock_code AS code,
                   COALESCE(s.name, ic.stock_name) AS name,
                   s.market
            FROM index_constituents ic
            LEFT JOIN stocks s ON s.code = ic.stock_code
            WHERE ic.index_code = ?
              AND COALESCE(s.is_delisted, 0) = 0
            ORDER BY ic.stock_code
        """, (index_code,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT code, name, market
            FROM stocks
            WHERE COALESCE(is_delisted, 0) = 0
              AND history_end IS NOT NULL
            ORDER BY code
        """).fetchall()

    stocks = []
    for row in rows:
        code = row["code"]
        name = row["name"] or ""
        if not code or "ST" in name.upper() or "退" in name:
            continue
        stocks.append({
            "code": code,
            "name": name,
            "market": infer_market(code, row["market"]),
        })
    return stocks


# ── 日线指标 ─────────────────────────────────────────────────────────────

def load_daily_metrics(conn, codes: list[str]) -> dict:
    """批量读取最近 80 根日 K 线，计算 MA5/MA20/高低区间/量比等供实时扫描用。"""
    metrics = {}
    for batch in chunked(codes, 600):
        placeholders = ",".join("?" for _ in batch)
        rows = conn.execute(f"""
            SELECT code, trade_date, close, high, low, volume, amount, pct_change
            FROM (
                SELECT code, trade_date, close, high, low, volume, amount, pct_change,
                       ROW_NUMBER() OVER (
                           PARTITION BY code ORDER BY trade_date DESC
                       ) AS rn
                FROM daily_prices
                WHERE code IN ({placeholders})
            )
            WHERE rn <= 80
            ORDER BY code, trade_date DESC
        """, batch).fetchall()

        grouped: dict[str, list] = {}
        for row in rows:
            grouped.setdefault(row["code"], []).append(row)

        for code, series_desc in grouped.items():
            series = list(reversed(series_desc[:80]))
            closes = [r["close"] for r in series if r["close"] is not None]
            volumes = [r["volume"] for r in series[-20:] if r["volume"] is not None]
            if not closes:
                continue
            ma5 = sum(closes[-5:]) / min(len(closes), 5)
            ma20 = sum(closes[-20:]) / min(len(closes), 20)
            high20 = max((r["high"] for r in series[-20:] if r["high"] is not None), default=None)
            low20 = min((r["low"] for r in series[-20:] if r["low"] is not None), default=None)
            avg_volume20 = sum(volumes) / len(volumes) if volumes else None
            last = series[-1]
            prev_ma5 = (sum(closes[-6:-1]) / 5) if len(closes) >= 6 else None
            today = datetime.today().strftime("%Y-%m-%d")
            prev_bar = series[-2] if len(series) >= 2 else None
            prev_low = (prev_bar["low"] if last["trade_date"] == today and prev_bar
                        else last["low"])
            metrics[code] = {
                "last_trade_date": last["trade_date"],
                "last_close": last["close"],
                "prev_low": prev_low,
                "ma5": ma5,
                "ma5_prev": prev_ma5,
                "ma5_up": bool(prev_ma5 is not None and ma5 > prev_ma5),
                "ma20": ma20,
                "high20": high20,
                "low20": low20,
                "avg_volume20": avg_volume20,
            }
    return metrics


def daily_price_coverage_threshold(conn) -> int:
    row = conn.execute("""
        SELECT COUNT(*) AS count FROM stocks
        WHERE COALESCE(is_delisted, 0) = 0
    """).fetchone()
    total = row["count"] if row and row["count"] else 0
    return max(1, math.floor(total * 0.8))


def latest_daily_trade_date(conn) -> str:
    min_count = daily_price_coverage_threshold(conn)
    row = conn.execute("""
        SELECT trade_date FROM daily_prices
        GROUP BY trade_date
        HAVING COUNT(DISTINCT code) >= ?
        ORDER BY trade_date DESC LIMIT 1
    """, (min_count,)).fetchone()
    return row["trade_date"] if row and row["trade_date"] else _default_scan_trade_date()


def recent_market_trade_dates(conn, trade_date: str, count: int) -> list[str]:
    min_count = daily_price_coverage_threshold(conn)
    rows = conn.execute("""
        SELECT trade_date FROM daily_prices
        WHERE trade_date <= ?
        GROUP BY trade_date
        HAVING COUNT(DISTINCT code) >= ?
        ORDER BY trade_date DESC LIMIT ?
    """, (trade_date, min_count, count)).fetchall()
    return list(reversed([r["trade_date"] for r in rows]))


# ── 实时报价 ─────────────────────────────────────────────────────────────

def fetch_sina_quotes(codes: list[str]) -> dict:
    quotes = {}
    for batch in chunked(codes, 700):
        symbols = ",".join(to_sina_symbol(code) for code in batch)
        resp = None
        for attempt in range(3):
            try:
                resp = requests.get(
                    config.SINA_PRICE_URL + symbols,
                    headers=config.HTTP_HEADERS,
                    timeout=12,
                )
                resp.raise_for_status()
                resp.encoding = resp.apparent_encoding or "gbk"
                if "var hq_str_" in resp.text:
                    break
            except requests.RequestException:
                resp = None
            time.sleep(0.3 * (attempt + 1))
        if resp is None or "var hq_str_" not in resp.text:
            continue

        for line in resp.text.strip().splitlines():
            if "=" not in line or '"' not in line:
                continue
            prefix, payload = line.split("=", 1)
            symbol = prefix.rsplit("_", 1)[-1]
            code = symbol[2:]
            fields = payload.strip().strip('";').split(",")
            if len(fields) < 32:
                continue
            prev_close = to_float(fields[2])
            price = to_float(fields[3])
            if not prev_close or not price or price <= 0:
                continue
            volume_hands = (to_float(fields[8], 0) or 0) / 100.0
            amount = to_float(fields[9], 0) or 0
            quotes[code] = {
                "name": fields[0],
                "open": to_float(fields[1]),
                "prev_close": prev_close,
                "price": price,
                "high": to_float(fields[4]),
                "low": to_float(fields[5]),
                "volume": volume_hands,
                "amount": amount,
                "trade_date": fields[30],
                "quote_time": fields[31],
                "pct": (price - prev_close) / prev_close * 100.0,
            }
    return quotes


def fetch_eastmoney_quotes(codes: list[str]) -> dict:
    quotes = {}
    fields = "f12,f14,f2,f3,f5,f6,f15,f16,f17,f18"
    for batch in chunked(codes, 50):
        params = {
            "fltt": 2,
            "invt": 2,
            "fields": fields,
            "secids": ",".join(f"{infer_market(code)}.{code}" for code in batch),
        }
        diff = []
        em_headers = {
            "User-Agent": config.HTTP_HEADERS["User-Agent"],
            "Referer": "https://quote.eastmoney.com/",
        }
        for url in config.EASTMONEY_QUOTE_URLS:
            for attempt in range(2):
                try:
                    resp = requests.get(url, params=params, headers=em_headers, timeout=8)
                    resp.raise_for_status()
                    data = resp.json().get("data") or {}
                    diff = data.get("diff") or []
                    if diff:
                        break
                except Exception:
                    time.sleep(0.25 * (attempt + 1))
            if diff:
                break
        if not diff:
            continue
        for item in diff:
            code = str(item.get("f12") or "")
            price = to_float(item.get("f2"))
            prev_close = to_float(item.get("f18"))
            pct = to_float(item.get("f3"))
            if not price or not prev_close or price <= 0:
                continue
            if pct is None:
                pct = (price - prev_close) / prev_close * 100.0
            quotes[code] = {
                "name": item.get("f14") or "",
                "open": to_float(item.get("f17")),
                "prev_close": prev_close,
                "price": price,
                "high": to_float(item.get("f15")),
                "low": to_float(item.get("f16")),
                "volume": to_float(item.get("f5"), 0) or 0,
                "amount": to_float(item.get("f6"), 0) or 0,
                "trade_date": datetime.today().strftime("%Y-%m-%d"),
                "quote_time": datetime.now().strftime("%H:%M:%S"),
                "pct": pct,
            }
    return quotes


def fetch_realtime_quotes(codes: list[str]) -> dict:
    """优先新浪（覆盖率≥70% 时直接返回），否则合并东方财富补全。"""
    quotes = fetch_sina_quotes(codes)
    if len(quotes) >= len(codes) * 0.7:
        return quotes
    eastmoney_quotes = fetch_eastmoney_quotes(codes)
    if not quotes:
        return eastmoney_quotes
    quotes.update({code: q for code, q in eastmoney_quotes.items() if code not in quotes})
    return quotes


# ── K 线缓存 ─────────────────────────────────────────────────────────────

def _ensure_kline_cache_table(conn):
    global KLINE_CACHE_DB_READY
    if KLINE_CACHE_DB_READY:
        return
    conn.execute("""
        CREATE TABLE IF NOT EXISTS intraday_5m_cache (
            code        TEXT NOT NULL,
            trade_date  DATE NOT NULL,
            cutoff      TEXT NOT NULL,
            bars_json   TEXT NOT NULL,
            source      TEXT,
            created_at  DATETIME DEFAULT (datetime('now','localtime')),
            PRIMARY KEY (code, trade_date, cutoff)
        )
    """)
    conn.commit()
    KLINE_CACHE_DB_READY = True


def get_cached_kline(code: str, cutoff_text: str, trade_date: Optional[str] = None) -> list:
    today = trade_date or _default_scan_trade_date()
    key = (today, code, cutoff_text)
    with KLINE_CACHE_LOCK:
        bars = KLINE_CACHE.get(key)
        if bars:
            return bars
    try:
        conn = connect_existing()
        _ensure_kline_cache_table(conn)
        row = conn.execute("""
            SELECT bars_json FROM intraday_5m_cache
            WHERE code = ? AND trade_date = ? AND cutoff = ?
        """, (code, today, cutoff_text)).fetchone()
        conn.close()
        if row:
            bars = json.loads(row["bars_json"])
            with KLINE_CACHE_LOCK:
                KLINE_CACHE[key] = bars
            return bars
    except Exception:
        pass
    return []


def set_cached_kline(code: str, cutoff_text: str, bars: list,
                     source: str = "baostock", trade_date: Optional[str] = None):
    if not bars:
        return
    today = trade_date or _default_scan_trade_date()
    key = (today, code, cutoff_text)
    with KLINE_CACHE_LOCK:
        KLINE_CACHE[key] = bars
    try:
        conn = connect_existing()
        _ensure_kline_cache_table(conn)
        conn.execute("""
            INSERT INTO intraday_5m_cache
                (code, trade_date, cutoff, bars_json, source, created_at)
            VALUES (?, ?, ?, ?, ?, datetime('now','localtime'))
            ON CONFLICT(code, trade_date, cutoff) DO UPDATE SET
                bars_json = excluded.bars_json,
                source    = excluded.source,
                created_at= datetime('now','localtime')
        """, (code, today, cutoff_text, json.dumps(bars, ensure_ascii=False), source))
        conn.commit()
        conn.close()
    except Exception:
        pass


# ── 分钟 K 线 HTTP ────────────────────────────────────────────────────────

def _aggregate_to_5m_bars(bars: list) -> list:
    result = []
    for group in chunked(bars, 5):
        valid = [b for b in group if b.get("close") is not None]
        if not valid:
            continue
        result.append({
            "time": valid[-1]["time"],
            "open": valid[0]["open"],
            "close": valid[-1]["close"],
            "high": max((b["high"] for b in valid if b["high"] is not None), default=None),
            "low": min((b["low"] for b in valid if b["low"] is not None), default=None),
            "volume": sum(b.get("volume") or 0 for b in valid),
            "amount": sum(b.get("amount") or 0 for b in valid),
        })
    return result


def fetch_eastmoney_5m_kline(stock: dict, cutoff_text: str) -> list:
    secid = f"{infer_market(stock['code'], stock.get('market'))}.{stock['code']}"
    params = {
        "secid": secid,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "ndays": 1,
        "iscr": 0,
        "iscca": 0,
    }
    em_headers = {
        "User-Agent": config.HTTP_HEADERS["User-Agent"],
        "Referer": "https://quote.eastmoney.com/",
    }
    for url in config.EASTMONEY_TRENDS_URLS:
        try:
            trends = (_get_json_with_retry(url, params, em_headers, timeout=5, retries=1
                      ).get("data") or {}).get("trends") or []
        except Exception:
            continue
        one_minute = []
        for item in trends:
            parts = item.split(",")
            if len(parts) < 7:
                continue
            hhmm = parts[0][-5:]
            if hhmm > cutoff_text:
                continue
            one_minute.append({
                "time": hhmm,
                "open": to_float(parts[1]),
                "close": to_float(parts[2]),
                "high": to_float(parts[3]),
                "low": to_float(parts[4]),
                "volume": to_float(parts[5], 0) or 0,
                "amount": to_float(parts[6], 0) or 0,
            })
        if one_minute:
            return _aggregate_to_5m_bars(one_minute)
    return []


def fetch_eastmoney_1m_kline_for_date(stock: dict, cutoff_text: str, trade_date: str) -> list:
    secid = f"{infer_market(stock['code'], stock.get('market'))}.{stock['code']}"
    today = trade_date.replace("-", "")
    params = {
        "secid": secid,
        "ut": "8dec03ba335b81bf4ebdf7b29ec27d15",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": 1,
        "fqt": 1,
        "beg": today,
        "end": today,
        "lmt": 360,
    }
    em_headers = {
        "User-Agent": config.HTTP_HEADERS["User-Agent"],
        "Referer": "https://quote.eastmoney.com/",
    }
    try:
        klines = (_get_json_with_retry(
            config.KLINE_URL, params, em_headers, timeout=4, retries=1,
        ).get("data") or {}).get("klines") or []
    except Exception:
        return []

    bars = []
    for item in klines:
        parts = item.split(",")
        if len(parts) < 7:
            continue
        hhmm = parts[0][-5:]
        if hhmm > cutoff_text:
            continue
        bars.append({
            "time": hhmm,
            "open": to_float(parts[1]),
            "close": to_float(parts[2]),
            "high": to_float(parts[3]),
            "low": to_float(parts[4]),
            "volume": to_float(parts[5], 0) or 0,
            "amount": to_float(parts[6], 0) or 0,
        })
    return bars


# ── Baostock 分钟 K 线 ────────────────────────────────────────────────────

def _ensure_baostock_login() -> bool:
    global BAOSTOCK_LOGGED_IN
    if _bs_module is None:
        return False
    if BAOSTOCK_LOGGED_IN:
        return True
    lg = _bs_module.login()
    if getattr(lg, "error_code", "1") == "0":
        BAOSTOCK_LOGGED_IN = True
        return True
    return False


def _parse_baostock_row(row: list, cutoff_text: str) -> Optional[dict]:
    if len(row) < 9:
        return None
    hhmm = f"{row[1][8:10]}:{row[1][10:12]}"
    if hhmm > cutoff_text:
        return None
    return {
        "time": hhmm,
        "open": to_float(row[3]),
        "high": to_float(row[4]),
        "low": to_float(row[5]),
        "close": to_float(row[6]),
        "volume": (to_float(row[7], 0) or 0) / 100.0,  # 股→手
        "amount": to_float(row[8], 0) or 0,
    }


def _query_baostock_5m_rows(bs_module, code: str, cutoff_text: str, today: str) -> list:
    fields = "date,time,code,open,high,low,close,volume,amount"
    rs = bs_module.query_history_k_data_plus(
        to_baostock_code(code), fields,
        start_date=today, end_date=today,
        frequency="5", adjustflag="2",
    )
    if getattr(rs, "error_code", "1") != "0":
        return []
    bars = []
    while rs.next():
        bar = _parse_baostock_row(rs.get_row_data(), cutoff_text)
        if bar:
            bars.append(bar)
    return bars


def fetch_baostock_5m_kline_uncached(stock: dict, cutoff_text: str,
                                     trade_date: Optional[str] = None) -> list:
    if _bs_module is None:
        return []
    today = trade_date or _default_scan_trade_date()
    with BAOSTOCK_LOCK:
        try:
            if not _ensure_baostock_login():
                return []
            return _query_baostock_5m_rows(_bs_module, stock["code"], cutoff_text, today)
        except Exception:
            global BAOSTOCK_LOGGED_IN
            BAOSTOCK_LOGGED_IN = False
            return []


def _fetch_baostock_5m_batch_worker(stock_codes: list, cutoff_text: str, today: str) -> dict:
    try:
        import baostock as worker_bs
        lg = worker_bs.login()
        if getattr(lg, "error_code", "1") != "0":
            return {}
        result = {}
        for code in stock_codes:
            try:
                bars = _query_baostock_5m_rows(worker_bs, code, cutoff_text, today)
                if bars:
                    result[code] = bars
            except Exception:
                continue
        worker_bs.logout()
        return result
    except Exception:
        return {}


# ── 批量分钟 K 线（带缓存） ───────────────────────────────────────────────

def fetch_minute_kline(stock: dict, cutoff_text: str,
                       trade_date: Optional[str] = None) -> list:
    """单只股票分钟 K 线，带内存+DB 双层缓存。"""
    scan_date = trade_date or _default_scan_trade_date()
    bars = get_cached_kline(stock["code"], cutoff_text, scan_date)
    if bars:
        return bars

    if _is_current_scan_date(scan_date):
        bars = fetch_eastmoney_5m_kline(stock, cutoff_text)
        if bars:
            set_cached_kline(stock["code"], cutoff_text, bars,
                             source="eastmoney", trade_date=scan_date)
            return bars
    else:
        bars = fetch_eastmoney_1m_kline_for_date(stock, cutoff_text, scan_date)
        if bars:
            set_cached_kline(stock["code"], cutoff_text, bars,
                             source="eastmoney-history", trade_date=scan_date)
            return bars
        return []

    bars = fetch_baostock_5m_kline_uncached(stock, cutoff_text, scan_date)
    if bars:
        set_cached_kline(stock["code"], cutoff_text, bars,
                         source="baostock", trade_date=scan_date)
        return bars

    # 兜底：东方财富分时 trend 接口（1 分钟聚合）
    secid = f"{infer_market(stock['code'], stock.get('market'))}.{stock['code']}"
    if _is_current_scan_date(scan_date):
        em_headers = {
            "User-Agent": config.HTTP_HEADERS["User-Agent"],
            "Referer": "https://quote.eastmoney.com/",
        }
        trend_params = {
            "secid": secid,
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
            "ndays": 1, "iscr": 0, "iscca": 0,
        }
        try:
            trends = (_get_json_with_retry(
                config.EASTMONEY_TRENDS_URLS[-1], trend_params, em_headers,
                timeout=4, retries=2,
            ).get("data") or {}).get("trends") or []
            bars = []
            for item in trends:
                parts = item.split(",")
                if len(parts) < 7:
                    continue
                hhmm = parts[0][-5:]
                if hhmm > cutoff_text:
                    continue
                bars.append({
                    "time": hhmm,
                    "open": to_float(parts[1]),
                    "close": to_float(parts[2]),
                    "high": to_float(parts[3]),
                    "low": to_float(parts[4]),
                    "volume": to_float(parts[5], 0) or 0,
                    "amount": to_float(parts[6], 0) or 0,
                })
            if bars:
                return bars
        except Exception:
            pass

    return fetch_eastmoney_1m_kline_for_date(stock, cutoff_text, scan_date)


def fetch_baostock_5m_klines_parallel(stocks: list, cutoff_text: str,
                                      max_workers: int = 4,
                                      trade_date: Optional[str] = None) -> tuple[dict, int]:
    """并行拉取多只股票分钟 K 线，返回 (code→bars, cache命中数)。"""
    scan_date = trade_date or _default_scan_trade_date()
    cached: dict = {}
    missing: list = []
    for stock in stocks:
        bars = get_cached_kline(stock["code"], cutoff_text, scan_date)
        if bars:
            cached[stock["code"]] = bars
        else:
            missing.append(stock)

    result = dict(cached)
    if not missing:
        return result, len(cached)

    if not _is_current_scan_date(scan_date):
        with ThreadPoolExecutor(max_workers=min(max_workers, 10)) as executor:
            futures = {
                executor.submit(fetch_eastmoney_1m_kline_for_date,
                                stock, cutoff_text, scan_date): stock
                for stock in missing
            }
            for future in as_completed(futures):
                stock = futures[future]
                try:
                    bars = future.result()
                except Exception:
                    bars = []
                if bars:
                    result[stock["code"]] = bars
                    set_cached_kline(stock["code"], cutoff_text, bars,
                                     source="eastmoney-history", trade_date=scan_date)
        return result, len(cached)

    # 实时：先东方财富批量，少量失败项用 baostock 兜底
    still_missing: list = []
    with ThreadPoolExecutor(max_workers=min(max_workers, 10)) as executor:
        futures = {
            executor.submit(fetch_eastmoney_5m_kline, stock, cutoff_text): stock
            for stock in missing
        }
        for future in as_completed(futures):
            stock = futures[future]
            try:
                bars = future.result()
            except Exception:
                bars = []
            if bars:
                result[stock["code"]] = bars
                set_cached_kline(stock["code"], cutoff_text, bars,
                                 source="eastmoney", trade_date=scan_date)
            else:
                still_missing.append(stock)

    for stock in still_missing[:5]:
        bars = fetch_baostock_5m_kline_uncached(stock, cutoff_text, scan_date)
        if bars:
            result[stock["code"]] = bars
            set_cached_kline(stock["code"], cutoff_text, bars,
                             source="baostock", trade_date=scan_date)
    return result, len(cached)


# ── 评分/可视化工具 ──────────────────────────────────────────────────────

def position_in_range(value, low, high) -> Optional[float]:
    if value is None or low is None or high is None or high <= low:
        return None
    return clamp((value - low) / (high - low), 0.0, 1.0)


def score_candidate(quote: dict, daily: dict, minute: dict) -> float:
    """综合评分：涨幅+量比+收盘强度+趋势+流动性。"""
    pct = quote["pct"]
    volume_ratio = minute["volume_ratio"]
    close_pos = minute["close_position"]
    amount_yi = quote["amount"] / 100000000
    ma5 = daily.get("ma5")
    ma20 = daily.get("ma20")
    price = minute["price"]

    gain_score = 20 - abs(pct - 4.8) * 3.0
    gain_score = clamp(gain_score, 0, 20)
    volume_score = clamp((volume_ratio - 1.0) / 1.8 * 25, 0, 25)
    trend_score = 0.0
    if close_pos is not None:
        trend_score += close_pos * 11
    if minute["above_vwap"]:
        trend_score += 7
    if minute["afternoon_pct"] is not None:
        trend_score += clamp((minute["afternoon_pct"] + 0.5) / 2.0, 0, 1) * 7
    strength_score = 0.0
    if ma5 and price >= ma5:
        strength_score += 7
    if ma20 and price >= ma20:
        strength_score += 6
    high20 = daily.get("high20")
    low20 = daily.get("low20")
    pos20 = position_in_range(price, low20, high20)
    if pos20 is not None:
        strength_score += pos20 * 7
    liquidity_score = clamp(amount_yi / 3.0 * 10, 0, 10)
    return round(gain_score + volume_score + trend_score + strength_score + liquidity_score, 1)


def build_sparkline(bars: list, width: int = 132, height: int = 34) -> str:
    closes = [b["close"] for b in bars if b.get("close") is not None]
    if len(closes) < 2:
        return ""
    if len(closes) > 80:
        step = math.ceil(len(closes) / 80)
        closes = closes[::step]
        if closes[-1] != bars[-1].get("close"):
            closes.append(bars[-1]["close"])
    low, high = min(closes), max(closes)
    span = high - low or 1
    points = []
    for i, close in enumerate(closes):
        x = i / (len(closes) - 1) * width
        y = height - ((close - low) / span * (height - 4) + 2)
        points.append(f"{x:.1f},{y:.1f}")
    color = "#ff4d6a" if closes[-1] >= closes[0] else "#00c97a"
    return (
        f'<svg viewBox="0 0 {width} {height}" width="{width}" height="{height}" '
        f'aria-hidden="true"><polyline fill="none" stroke="{color}" '
        f'stroke-width="2" points="{" ".join(points)}"/></svg>'
    )
