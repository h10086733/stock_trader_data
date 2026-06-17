"""
行业宽度指标查询页面 — app.py
==============================
启动：
  pip install flask
  python app.py
访问：http://localhost:5000
"""

from flask import Flask, jsonify, render_template_string, request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, time as dt_time
import argparse
import json
import math
import sys
import threading
import time
import sqlite3
import requests

try:
    import baostock as bs
except ImportError:
    bs = None

app = Flask(__name__)
DB_PATH = "stock_data.db"
EASTMONEY_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
EASTMONEY_TRENDS_URLS = [
    "https://push2delay.eastmoney.com/api/qt/stock/trends2/get",
    "https://push2.eastmoney.com/api/qt/stock/trends2/get",
]
SINA_PRICE_URL = "https://hq.sinajs.cn/list="
EASTMONEY_QUOTE_URLS = [
    "https://push2delay.eastmoney.com/api/qt/ulist.np/get",
    "https://push2.eastmoney.com/api/qt/ulist.np/get",
]
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Referer": "https://finance.sina.com.cn/",
}
BAOSTOCK_LOCK = threading.Lock()
BAOSTOCK_LOGGED_IN = False
SCAN_LOCK = threading.Lock()
KLINE_CACHE = {}
KLINE_CACHE_LOCK = threading.Lock()
KLINE_CACHE_DB_READY = False


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_kline_cache_table(conn):
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


def ensure_momentum_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS momentum_scan_runs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date      DATE NOT NULL,
            cutoff          TEXT NOT NULL,
            pool            TEXT NOT NULL,
            index_code      TEXT,
            min_gain        REAL,
            max_gain        REAL,
            min_vol_ratio   REAL,
            min_amount_wan  REAL,
            limit_count     INTEGER,
            verify_limit    INTEGER,
            workers         INTEGER,
            universe        INTEGER,
            quoted          INTEGER,
            prefiltered     INTEGER,
            verified        INTEGER,
            minute_success  INTEGER,
            minute_failed   INTEGER,
            cache_hits      INTEGER,
            elapsed_s       REAL,
            row_count       INTEGER,
            status          TEXT,
            error           TEXT,
            params_json     TEXT,
            created_at      DATETIME DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS momentum_picks (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id             INTEGER,
            trade_date         DATE NOT NULL,
            cutoff             TEXT NOT NULL,
            pool               TEXT NOT NULL,
            index_code         TEXT,
            code               TEXT NOT NULL,
            name               TEXT,
            buy_price          REAL,
            buy_pct            REAL,
            score              REAL,
            amount_yi          REAL,
            volume_ratio       REAL,
            volume_full_ratio  REAL,
            close_position     REAL,
            pullback_pct       REAL,
            high_time          TEXT,
            reasons            TEXT,
            row_json           TEXT,
            created_at         DATETIME DEFAULT (datetime('now','localtime')),
            updated_at         DATETIME DEFAULT (datetime('now','localtime')),
            UNIQUE(trade_date, cutoff, pool, index_code, code)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS momentum_pick_returns (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            pick_id        INTEGER NOT NULL UNIQUE,
            buy_date       DATE NOT NULL,
            sell_date      DATE NOT NULL,
            code           TEXT NOT NULL,
            name           TEXT,
            buy_price      REAL,
            sell_price     REAL,
            return_pct     REAL,
            sell_cutoff    TEXT NOT NULL,
            sell_time      TEXT,
            status         TEXT NOT NULL,
            error          TEXT,
            created_at     DATETIME DEFAULT (datetime('now','localtime')),
            updated_at     DATETIME DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mom_picks_date ON momentum_picks(trade_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mom_returns_date ON momentum_pick_returns(sell_date)")
    conn.commit()


def clamp(value, low, high):
    return max(low, min(high, value))


def to_float(value, default=None):
    try:
        if value in (None, "", "-"):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int_arg(name, default, min_value=None, max_value=None):
    value = request.args.get(name, default)
    try:
        value = int(value)
    except (TypeError, ValueError):
        value = default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def to_float_arg(name, default, min_value=None, max_value=None):
    value = request.args.get(name, default)
    try:
        value = float(value)
    except (TypeError, ValueError):
        value = default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def coerce_int(value, default, min_value=None, max_value=None):
    try:
        value = int(value)
    except (TypeError, ValueError):
        value = default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def coerce_float(value, default, min_value=None, max_value=None):
    try:
        value = float(value)
    except (TypeError, ValueError):
        value = default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def get_source_value(source, *names, default=None):
    for name in names:
        value = source.get(name)
        if value is not None:
            return value
    return default


def build_momentum_params(source=None):
    source = source or {}
    pool = source.get("pool", "all")
    index_code = get_source_value(source, "indexCode", "index_code", default="") or ""
    if pool == "index" and not index_code:
        pool = "all"
    cutoff_text = source.get("cutoff", "14:30")
    cutoff = parse_cutoff_time(cutoff_text)
    cutoff_text = cutoff.strftime("%H:%M")
    min_gain = coerce_float(get_source_value(source, "minGain", "min_gain"),
                            2.0, -5, 15)
    max_gain = coerce_float(get_source_value(source, "maxGain", "max_gain"),
                            7.5, min_gain, 20)
    return {
        "pool": pool,
        "index_code": index_code,
        "cutoff": cutoff_text,
        "trade_date": (
            source.get("tradeDate")
            or source.get("trade_date")
            or default_scan_trade_date()
        ),
        "min_gain": min_gain,
        "max_gain": max_gain,
        "min_vol_ratio": coerce_float(
            get_source_value(source, "minVolRatio", "min_vol_ratio"),
            1.5, 0.2, 10,
        ),
        "min_amount_wan": coerce_float(
            get_source_value(source, "minAmount", "min_amount"),
            8000, 0, 1000000,
        ),
        "limit": coerce_int(source.get("limit"), 80, 1, 300),
        "verify_limit": coerce_int(
            get_source_value(source, "verifyLimit", "verify_limit"),
            50, 1, 1000,
        ),
        "workers": coerce_int(source.get("workers"), 6, 2, 12),
    }


def to_sina_symbol(code):
    if code.startswith("92"):
        return "bj" + code
    return ("sh" if code.startswith(("5", "6", "9")) else "sz") + code


def infer_market(code, market=None):
    if market in ("0", "1"):
        return market
    return "1" if code.startswith(("5", "6", "9")) else "0"


def to_baostock_code(code):
    return ("sh." if code.startswith(("5", "6", "9")) else "sz.") + code


def parse_cutoff_time(value):
    try:
        hour, minute = [int(x) for x in value.split(":", 1)]
        return dt_time(hour, minute)
    except Exception:
        return dt_time(14, 30)


def default_scan_trade_date():
    now = datetime.now()
    if now.time() < dt_time(6, 0):
        now = now - timedelta(days=1)
    return now.strftime("%Y-%m-%d")


def is_current_scan_date(trade_date):
    return (trade_date or default_scan_trade_date()) == default_scan_trade_date()


def trade_elapsed_ratio(cutoff):
    morning_start = dt_time(9, 30)
    morning_end = dt_time(11, 30)
    afternoon_start = dt_time(13, 0)
    afternoon_end = dt_time(15, 0)

    def minutes_between(start, end):
        return (datetime.combine(datetime.today(), end)
                - datetime.combine(datetime.today(), start)).seconds / 60

    elapsed = 0
    if cutoff > morning_start:
        elapsed += minutes_between(morning_start, min(cutoff, morning_end))
    if cutoff > afternoon_start:
        elapsed += minutes_between(afternoon_start, min(cutoff, afternoon_end))
    return clamp(elapsed / 240.0, 0.05, 1.0)


def load_stock_universe(conn, pool="all", index_code=""):
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


def load_indices(conn):
    return [dict(r) for r in conn.execute(
        "SELECT code, name FROM indices ORDER BY code"
    ).fetchall()]


def chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def get_json_with_retry(url, params, headers, timeout=4, retries=2):
    last_error = None
    for attempt in range(retries):
        try:
            resp = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_error = exc
            time.sleep(0.25 * (attempt + 1))
    raise last_error


def ensure_baostock_login():
    global BAOSTOCK_LOGGED_IN
    if bs is None:
        return False
    if BAOSTOCK_LOGGED_IN:
        return True
    lg = bs.login()
    if getattr(lg, "error_code", "1") == "0":
        BAOSTOCK_LOGGED_IN = True
        return True
    return False


def parse_baostock_row(row, cutoff_text):
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
        # baostock 分钟成交量单位是股；daily_prices.volume 单位是手。
        "volume": (to_float(row[7], 0) or 0) / 100.0,
        "amount": to_float(row[8], 0) or 0,
    }


def query_baostock_5m_rows(bs_module, code, cutoff_text, today):
    fields = "date,time,code,open,high,low,close,volume,amount"
    rs = bs_module.query_history_k_data_plus(
        to_baostock_code(code),
        fields,
        start_date=today,
        end_date=today,
        frequency="5",
        adjustflag="2",
    )
    if getattr(rs, "error_code", "1") != "0":
        return []
    bars = []
    while rs.next():
        bar = parse_baostock_row(rs.get_row_data(), cutoff_text)
        if bar:
            bars.append(bar)
    return bars


def fetch_baostock_5m_kline_uncached(stock, cutoff_text, trade_date=None):
    if bs is None:
        return []
    today = trade_date or default_scan_trade_date()
    with BAOSTOCK_LOCK:
        try:
            if not ensure_baostock_login():
                return []
            return query_baostock_5m_rows(bs, stock["code"], cutoff_text, today)
        except Exception:
            BAOSTOCK_LOGGED_IN = False
            return []


def fetch_baostock_5m_batch_worker(stock_codes, cutoff_text, today):
    try:
        import baostock as worker_bs
        lg = worker_bs.login()
        if getattr(lg, "error_code", "1") != "0":
            return {}
        result = {}
        for code in stock_codes:
            try:
                bars = query_baostock_5m_rows(worker_bs, code, cutoff_text, today)
                if bars:
                    result[code] = bars
            except Exception:
                continue
        worker_bs.logout()
        return result
    except Exception:
        return {}


def aggregate_to_5m_bars(bars):
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


def fetch_eastmoney_5m_kline(stock, cutoff_text):
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
    headers = {
        "User-Agent": HTTP_HEADERS["User-Agent"],
        "Referer": "https://quote.eastmoney.com/",
    }
    for url in EASTMONEY_TRENDS_URLS:
        try:
            trends = (get_json_with_retry(
                url,
                params,
                headers,
                timeout=5,
                retries=1,
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
                # 东方财富分时成交量按手计，与 daily_prices.volume 口径一致。
                "volume": to_float(parts[5], 0) or 0,
                "amount": to_float(parts[6], 0) or 0,
            })
        if one_minute:
            return aggregate_to_5m_bars(one_minute)
    return []


def fetch_eastmoney_1m_kline_for_date(stock, cutoff_text, trade_date):
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
    headers = {
        "User-Agent": HTTP_HEADERS["User-Agent"],
        "Referer": "https://quote.eastmoney.com/",
    }
    try:
        klines = (get_json_with_retry(
            EASTMONEY_KLINE_URL,
            params,
            headers,
            timeout=4,
            retries=1,
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


def get_cached_kline(code, cutoff_text, trade_date=None):
    today = trade_date or default_scan_trade_date()
    key = (today, code, cutoff_text)
    with KLINE_CACHE_LOCK:
        bars = KLINE_CACHE.get(key)
        if bars:
            return bars

    try:
        conn = get_db()
        ensure_kline_cache_table(conn)
        row = conn.execute("""
            SELECT bars_json
            FROM intraday_5m_cache
            WHERE code = ? AND trade_date = ? AND cutoff = ?
        """, (code, today, cutoff_text)).fetchone()
        conn.close()
        if not row:
            return None
        bars = json.loads(row["bars_json"])
        with KLINE_CACHE_LOCK:
            KLINE_CACHE[key] = bars
        return bars
    except Exception:
        return None


def set_cached_kline(code, cutoff_text, bars, source="baostock", trade_date=None):
    if not bars:
        return
    today = trade_date or default_scan_trade_date()
    key = (today, code, cutoff_text)
    with KLINE_CACHE_LOCK:
        KLINE_CACHE[key] = bars
    try:
        conn = get_db()
        ensure_kline_cache_table(conn)
        conn.execute("""
            INSERT INTO intraday_5m_cache
                (code, trade_date, cutoff, bars_json, source, created_at)
            VALUES (?, ?, ?, ?, ?, datetime('now','localtime'))
            ON CONFLICT(code, trade_date, cutoff) DO UPDATE SET
                bars_json = excluded.bars_json,
                source = excluded.source,
                created_at = datetime('now','localtime')
        """, (code, today, cutoff_text, json.dumps(bars, ensure_ascii=False), source))
        conn.commit()
        conn.close()
    except Exception:
        pass


def fetch_baostock_5m_klines_parallel(stocks, cutoff_text, max_workers=4,
                                      trade_date=None):
    scan_date = trade_date or default_scan_trade_date()
    cached = {}
    missing = []
    for stock in stocks:
        bars = get_cached_kline(stock["code"], cutoff_text, scan_date)
        if bars:
            cached[stock["code"]] = bars
        else:
            missing.append(stock)

    result = dict(cached)
    if not missing:
        return result, len(cached)

    if not is_current_scan_date(scan_date):
        with ThreadPoolExecutor(max_workers=min(max_workers, 10)) as executor:
            futures = {
                executor.submit(
                    fetch_eastmoney_1m_kline_for_date,
                    stock,
                    cutoff_text,
                    scan_date,
                ): stock
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

    still_missing = missing
    if is_current_scan_date(scan_date):
        still_missing = []
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

    # baostock 很慢：实时页面只兜底少量失败项；历史回填需要完整获取候选项。
    fallback_limit = len(still_missing) if not is_current_scan_date(scan_date) else 5
    for stock in still_missing[:fallback_limit]:
        bars = fetch_baostock_5m_kline_uncached(stock, cutoff_text, scan_date)
        if bars:
            result[stock["code"]] = bars
            set_cached_kline(stock["code"], cutoff_text, bars,
                             source="baostock", trade_date=scan_date)
    return result, len(cached)


def fetch_sina_quotes(codes):
    quotes = {}
    for batch in chunked(codes, 700):
        symbols = ",".join(to_sina_symbol(code) for code in batch)
        resp = None
        for attempt in range(3):
            try:
                resp = requests.get(
                    SINA_PRICE_URL + symbols,
                    headers=HTTP_HEADERS,
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


def fetch_eastmoney_quotes(codes):
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
        for url in EASTMONEY_QUOTE_URLS:
            for attempt in range(2):
                try:
                    resp = requests.get(
                        url,
                        params=params,
                        headers={
                            "User-Agent": HTTP_HEADERS["User-Agent"],
                            "Referer": "https://quote.eastmoney.com/",
                        },
                        timeout=8,
                    )
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
                # 东方财富 f5 对 A 股是手，和 daily_prices.volume 口径一致。
                "volume": to_float(item.get("f5"), 0) or 0,
                "amount": to_float(item.get("f6"), 0) or 0,
                "trade_date": datetime.today().strftime("%Y-%m-%d"),
                "quote_time": datetime.now().strftime("%H:%M:%S"),
                "pct": pct,
            }
    return quotes


def fetch_realtime_quotes(codes):
    quotes = fetch_sina_quotes(codes)
    if len(quotes) >= len(codes) * 0.7:
        return quotes

    eastmoney_quotes = fetch_eastmoney_quotes(codes)
    if not quotes:
        return eastmoney_quotes
    quotes.update({code: quote for code, quote in eastmoney_quotes.items()
                   if code not in quotes})
    return quotes


def load_daily_metrics(conn, codes):
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

        grouped = {}
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
            high20 = max((r["high"] for r in series[-20:] if r["high"] is not None),
                         default=None)
            low20 = min((r["low"] for r in series[-20:] if r["low"] is not None),
                        default=None)
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


def fetch_minute_kline(stock, cutoff_text, trade_date=None):
    scan_date = trade_date or default_scan_trade_date()
    bars = get_cached_kline(stock["code"], cutoff_text, scan_date)
    if bars:
        return bars

    if is_current_scan_date(scan_date):
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

    secid = f"{infer_market(stock['code'], stock.get('market'))}.{stock['code']}"
    if is_current_scan_date(scan_date):
        trend_params = {
            "secid": secid,
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
            "ndays": 1,
            "iscr": 0,
            "iscca": 0,
        }
        headers = {
            "User-Agent": HTTP_HEADERS["User-Agent"],
            "Referer": "https://quote.eastmoney.com/",
        }
        try:
            trends = (get_json_with_retry(
                EASTMONEY_TRENDS_URLS[-1],
                trend_params,
                headers,
                timeout=4,
                retries=2,
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


def position_in_range(value, low, high):
    if value is None or low is None or high is None or high <= low:
        return None
    return clamp((value - low) / (high - low), 0, 1)


def score_candidate(quote, daily, minute):
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
    trend_score = 0
    if close_pos is not None:
        trend_score += close_pos * 11
    if minute["above_vwap"]:
        trend_score += 7
    if minute["afternoon_pct"] is not None:
        trend_score += clamp((minute["afternoon_pct"] + 0.5) / 2.0, 0, 1) * 7
    strength_score = 0
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


def build_sparkline(bars, width=132, height=34):
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


def evaluate_candidate_with_bars(stock, quote, daily, cutoff_text, elapsed_ratio, bars):
    if not bars:
        return None
    price = bars[-1]["close"] or quote["price"]
    high = max((b["high"] for b in bars if b["high"] is not None), default=quote["high"])
    low = min((b["low"] for b in bars if b["low"] is not None), default=quote["low"])
    volume = sum(b["volume"] for b in bars)
    amount = sum(b["amount"] for b in bars)
    vwap = amount / (volume * 100.0) if volume else None
    high_time = None
    if high is not None:
        high_times = [b["time"] for b in bars if b.get("high") == high]
        high_time = max(high_times) if high_times else None
    afternoon_bars = [b for b in bars if b["time"] >= "13:00" and b.get("close")]
    afternoon_pct = None
    if afternoon_bars and afternoon_bars[0]["close"]:
        afternoon_pct = (price - afternoon_bars[0]["close"]) / afternoon_bars[0]["close"] * 100

    avg_volume20 = daily.get("avg_volume20")
    expected_volume = avg_volume20 * elapsed_ratio if avg_volume20 else None
    volume_ratio = volume / expected_volume if expected_volume else 0
    volume_full_ratio = volume / avg_volume20 if avg_volume20 else 0
    close_position = position_in_range(price, low, high)
    pullback_pct = ((high - price) / price * 100) if high and price else None
    above_vwap = bool(vwap and price >= vwap)
    trend_above_ma5 = bool(daily.get("ma5") and price > daily["ma5"])
    ma5_up = bool(daily.get("ma5_up"))
    prev_low = daily.get("prev_low")
    not_break_prev_low = bool(prev_low and low and low >= prev_low)
    high_after_14 = bool(high_time and high_time >= "14:00")
    close_strong = bool(
        (close_position is not None and close_position >= 0.80)
        or (pullback_pct is not None and pullback_pct <= 1.0)
    )

    minute = {
        "price": price,
        "volume_ratio": volume_ratio,
        "close_position": close_position,
        "pullback_pct": pullback_pct,
        "above_vwap": above_vwap,
        "afternoon_pct": afternoon_pct,
    }
    score = score_candidate(quote, daily, minute)

    reasons = []
    if trend_above_ma5:
        reasons.append("强于5日线")
    if ma5_up:
        reasons.append("5日线向上")
    if not_break_prev_low:
        reasons.append("未破前低")
    if volume_full_ratio >= 1.0 or volume_ratio >= 1.5:
        reasons.append("放量")
    if above_vwap:
        reasons.append("站上VWAP")
    if high_after_14:
        reasons.append("14点后高点")
    if close_strong:
        reasons.append("收盘强")

    return {
        "code": stock["code"],
        "name": stock["name"] or quote.get("name") or "",
        "price": round(price, 3),
        "pct": round((price - quote["prev_close"]) / quote["prev_close"] * 100, 2),
        "amount_yi": round((amount or quote["amount"]) / 100000000, 2),
        "volume_ratio": round(volume_ratio, 2),
        "volume_full_ratio": round(volume_full_ratio, 2),
        "close_position": round(close_position * 100, 1) if close_position is not None else None,
        "pullback_pct": round(pullback_pct, 2) if pullback_pct is not None else None,
        "afternoon_pct": round(afternoon_pct, 2) if afternoon_pct is not None else None,
        "above_vwap": above_vwap,
        "trend_above_ma5": trend_above_ma5,
        "ma5_up": ma5_up,
        "not_break_prev_low": not_break_prev_low,
        "high_time": high_time,
        "high_after_14": high_after_14,
        "close_strong": close_strong,
        "has_minute": True,
        "score": score,
        "reasons": " / ".join(reasons),
        "sparkline": build_sparkline(bars),
        "quote_time": quote.get("quote_time"),
        "trade_date": quote.get("trade_date"),
    }


def evaluate_candidate(stock, quote, daily, cutoff_text, elapsed_ratio,
                       trade_date=None):
    bars = fetch_minute_kline(stock, cutoff_text, trade_date)
    return evaluate_candidate_with_bars(
        stock, quote, daily, cutoff_text, elapsed_ratio, bars
    )


def passes_momentum_filters(row, min_gain, max_gain, min_vol_ratio):
    if row["pct"] < min_gain or row["pct"] > max_gain:
        return False
    # A层：趋势过滤，防止做空头反弹。
    if not row.get("trend_above_ma5"):
        return False
    if not row.get("ma5_up"):
        return False
    if not row.get("not_break_prev_low"):
        return False

    # B层：日内资金结构。
    volume_ok = (
        row.get("volume_full_ratio", 0) >= 1.0
        or row.get("volume_ratio", 0) >= min_vol_ratio
    )
    if not volume_ok:
        return False
    if not row.get("above_vwap"):
        return False
    if not row.get("high_after_14"):
        return False

    # C层：收盘强度。
    if not row.get("close_strong"):
        return False
    return True


# ─────────────────────────────────────────────────────────────────
# HTML 模板
# ─────────────────────────────────────────────────────────────────
HTML = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>行业宽度指标</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Noto+Sans+SC:wght@300;400;500;700&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:        #0d0f14;
    --surface:   #141720;
    --surface2:  #1a1e2e;
    --border:    #252a3a;
    --text:      #c8cdd8;
    --text-dim:  #5a6070;
    --text-head: #8892a4;
    --red:       #ff4d6a;
    --red-dim:   rgba(255,77,106,0.12);
    --green:     #00c97a;
    --green-dim: rgba(0,201,122,0.10);
    --accent:    #3d7fff;
  }
  * { margin:0; padding:0; box-sizing:border-box; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'Noto Sans SC', sans-serif;
    font-size: 13px;
    min-height: 100vh;
    padding: 32px 24px;
  }

  /* 顶部 */
  .header {
    display: flex;
    align-items: flex-end;
    justify-content: space-between;
    margin-bottom: 28px;
    padding-bottom: 20px;
    border-bottom: 1px solid var(--border);
  }
  .header h1 { font-size:20px; font-weight:500; letter-spacing:.08em; color:#fff; }
  .header p  { margin-top:4px; font-size:11px; color:var(--text-dim); letter-spacing:.05em; }
  .header-right { display:flex; align-items:center; gap:12px; }

  .days-select {
    background: var(--surface2);
    border: 1px solid var(--border);
    color: var(--text);
    padding: 6px 12px;
    border-radius: 6px;
    font-size: 12px;
    font-family: inherit;
    cursor: pointer;
    outline: none;
    transition: border-color .2s;
  }
  .days-select:hover { border-color: var(--accent); }

  .refresh-btn {
    background: var(--accent);
    color: #fff;
    border: none;
    padding: 6px 16px;
    border-radius: 6px;
    font-size: 12px;
    font-family: inherit;
    cursor: pointer;
    transition: opacity .2s;
  }
  .refresh-btn:hover { opacity:.85; }
  .nav-link {
    color: var(--text);
    text-decoration: none;
    border: 1px solid var(--border);
    background: var(--surface2);
    padding: 6px 12px;
    border-radius: 6px;
    font-size: 12px;
  }
  .nav-link:hover { border-color: var(--accent); }

  /* 状态栏 */
  .status-bar {
    display: flex;
    gap: 24px;
    margin-bottom: 20px;
    padding: 12px 16px;
    background: var(--surface);
    border-radius: 8px;
    border: 1px solid var(--border);
  }
  .status-item { display:flex; flex-direction:column; gap:2px; }
  .status-label { font-size:10px; color:var(--text-dim); letter-spacing:.06em; text-transform:uppercase; }
  .status-value { font-family:'DM Mono',monospace; font-size:13px; color:var(--text); }

  /* 表格 */
  .table-wrap {
    overflow-x: auto;
    border-radius: 10px;
    border: 1px solid var(--border);
  }
  table { width:100%; border-collapse:collapse; }
  thead tr { background: var(--surface2); }
  th {
    padding: 12px 16px;
    font-size: 11px;
    font-weight: 500;
    color: var(--text-head);
    letter-spacing: .06em;
    text-transform: uppercase;
    text-align: center;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }
  th:first-child { text-align:left; min-width:130px; }

  .date-header { display:flex; flex-direction:column; align-items:center; gap:2px; }
  .date-month  { font-size:10px; color:var(--text-dim); }
  .date-day    { font-size:13px; font-family:'DM Mono',monospace; color:var(--text); }

  tbody tr {
    border-bottom: 1px solid var(--border);
    transition: background .15s;
  }
  tbody tr:last-child { border-bottom:none; }
  tbody tr:hover { background: rgba(255,255,255,0.025); }

  td.name-cell {
    padding: 14px 16px;
    font-size: 13px;
    color: var(--text);
    white-space: nowrap;
  }
  .index-btn {
    display:block;
    width:100%;
    border:0;
    background:transparent;
    color:inherit;
    font:inherit;
    text-align:left;
    cursor:pointer;
  }
  .index-btn:hover { color:#fff; }
  .idx-code {
    display: block;
    font-size: 10px;
    font-family: 'DM Mono', monospace;
    color: var(--text-dim);
    margin-top: 2px;
  }

  td.val-cell {
    padding: 8px 6px;
    text-align: center;
  }
  .val-inner {
    display: inline-flex;
    flex-direction: column;
    align-items: center;
    gap: 4px;
    padding: 8px 14px;
    border-radius: 6px;
    min-width: 76px;
  }

  .val-inner.positive { background: var(--red-dim); }
  .val-inner.negative { background: var(--green-dim); }
  .val-inner.zero     { background: transparent; }

  .val-number {
    font-family: 'DM Mono', monospace;
    font-size: 15px;
    font-weight: 500;
  }
  .positive .val-number { color: var(--red); }
  .negative .val-number { color: var(--green); }
  .zero     .val-number { color: var(--text-dim); }

  .val-bar {
    width: 100%;
    height: 2px;
    border-radius: 1px;
    background: var(--border);
    position: relative;
    overflow: hidden;
  }
  .val-bar-fill {
    position: absolute;
    top: 0;
    height: 100%;
    border-radius: 1px;
  }
  .positive .val-bar-fill { background:var(--red);   left:50%; }
  .negative .val-bar-fill { background:var(--green); right:50%; }

  .empty-cell { font-size:12px; color:var(--text-dim); }
  .metric-btn {
    border: 0;
    font: inherit;
    color: inherit;
  }

  .modal-backdrop {
    position: fixed;
    inset: 0;
    display: none;
    align-items: center;
    justify-content: center;
    padding: 24px;
    background: rgba(0,0,0,.58);
    z-index: 20;
  }
  .modal-backdrop.open { display:flex; }
  .modal {
    width: min(620px, 100%);
    max-height: calc(100vh - 48px);
    overflow: auto;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    box-shadow: 0 24px 80px rgba(0,0,0,.42);
  }
  .modal-head {
    display:flex;
    align-items:flex-start;
    justify-content:space-between;
    gap:16px;
    padding:18px 20px 14px;
    border-bottom:1px solid var(--border);
  }
  .modal-title { color:#fff; font-size:16px; font-weight:500; }
  .modal-subtitle { margin-top:4px; color:var(--text-dim); font-size:11px; font-family:'DM Mono',monospace; }
  .modal-close {
    width:30px;
    height:30px;
    border:1px solid var(--border);
    border-radius:6px;
    background:var(--surface2);
    color:var(--text);
    cursor:pointer;
    font-size:18px;
    line-height:1;
  }
  .modal-close:hover { border-color:var(--accent); }
  .modal-body { padding:16px 20px 20px; }
  .weight-table th:first-child { min-width:52px; text-align:center; }
  .weight-table th:nth-child(2), .weight-table td:nth-child(2) { text-align:left; }
  .weight-table td { padding:11px 12px; border-bottom:1px solid var(--border); text-align:center; }
  .weight-table tr:last-child td { border-bottom:0; }
  .stock-code { display:block; margin-top:2px; color:var(--text-dim); font-size:10px; font-family:'DM Mono',monospace; }
  .weight-value { color:var(--red); font-family:'DM Mono',monospace; font-weight:500; }
  .weight-value.empty { color:var(--text-dim); }

  /* Loading */
  .loading {
    display: flex;
    align-items: center;
    justify-content: center;
    height: 200px;
    color: var(--text-dim);
    gap: 10px;
  }
  .spinner {
    width:16px; height:16px;
    border: 2px solid var(--border);
    border-top-color: var(--accent);
    border-radius: 50%;
    animation: spin .8s linear infinite;
  }
  @keyframes spin { to { transform:rotate(360deg); } }
</style>
</head>
<body>

<div class="header">
  <div>
    <h1>行业宽度指标</h1>
  </div>
  <div class="header-right">
    <a class="nav-link" href="/momentum">14:30 选股</a>
    <select class="days-select" id="daysSelect" onchange="loadData()">
      <option value="5">最近 5 日</option>
      <option value="10">最近 10 日</option>
      <option value="20">最近 20 日</option>
      <option value="60">最近 60 日</option>
    </select>
    <button class="refresh-btn" onclick="loadData()">↻ 刷新</button>
  </div>
</div>

<div class="status-bar">
  <div class="status-item">
    <span class="status-label">最新交易日</span>
    <span class="status-value" id="statDate">—</span>
  </div>
  <div class="status-item">
    <span class="status-label">指数数量</span>
    <span class="status-value" id="statCount">—</span>
  </div>
  <div class="status-item">
    <span class="status-label">页面更新</span>
    <span class="status-value" id="statTime">—</span>
  </div>
</div>

<div class="table-wrap" id="tableWrap">
  <div class="loading"><div class="spinner"></div>加载中…</div>
</div>

<div class="modal-backdrop" id="weightModal" onclick="onModalBackdropClick(event)">
  <div class="modal" role="dialog" aria-modal="true" aria-labelledby="weightModalTitle">
    <div class="modal-head">
      <div>
        <div class="modal-title" id="weightModalTitle">成分股权重 Top 10</div>
        <div class="modal-subtitle" id="weightModalMeta">—</div>
      </div>
      <button class="modal-close" onclick="closeWeightModal()" aria-label="关闭">×</button>
    </div>
    <div class="modal-body" id="weightModalBody"></div>
  </div>
</div>

<script>
function escapeHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, ch => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;'
  }[ch]));
}

function fmtDateHeader(s) {
  const d = new Date(s);
  const m = String(d.getMonth()+1).padStart(2,'0');
  return { month: d.getFullYear()+'/'+m, day: String(d.getDate()).padStart(2,'0') };
}

function buildTable(data) {
  const { dates, indices } = data;
  const displayDates = [...dates].reverse();

  if (!displayDates.length || !indices.length) {
    document.getElementById('tableWrap').innerHTML =
      '<div class="loading">暂无数据，请先运行 index_stats.py</div>';
    return;
  }

  document.getElementById('statDate').textContent  = displayDates[0];
  document.getElementById('statCount').textContent = indices.length + ' 个';
  document.getElementById('statTime').textContent  = new Date().toLocaleTimeString('zh-CN');

  let html = '<table><thead><tr><th>行业指数</th>';
  displayDates.forEach(d => {
    const {month, day} = fmtDateHeader(d);
    html += `<th><div class="date-header">
      <span class="date-month">${month}</span>
      <span class="date-day">${day}</span>
    </div></th>`;
  });
  html += '</tr></thead><tbody>';

  indices.forEach(idx => {
    html += `<tr><td class="name-cell">
      <button class="index-btn" onclick="showConstituents('${escapeHtml(idx.code)}')" title="查看成分股">
        ${escapeHtml(idx.name)}<span class="idx-code">${escapeHtml(idx.code)}</span>
      </button>
    </td>`;
    displayDates.forEach(d => {
      const v = idx.ma3[d];
      if (v === undefined || v === null) {
        html += '<td class="val-cell"><span class="empty-cell">—</span></td>';
        return;
      }
      const pct = (v * 100).toFixed(2);
      const cls = v >  0.001 ? 'positive' : v < -0.001 ? 'negative' : 'zero';
      const barW = Math.min(Math.abs(v) * 100, 50);
      html += `<td class="val-cell">
        <div class="val-inner metric-btn ${cls}">
          <span class="val-number">${pct}</span>
          <div class="val-bar"><div class="val-bar-fill" style="width:${barW}%"></div></div>
        </div></td>`;
    });
    html += '</tr>';
  });

  html += '</tbody></table>';
  document.getElementById('tableWrap').innerHTML = html;
}

function openWeightModal(title, meta, bodyHtml) {
  document.getElementById('weightModalTitle').textContent = title;
  document.getElementById('weightModalMeta').textContent = meta;
  document.getElementById('weightModalBody').innerHTML = bodyHtml;
  document.getElementById('weightModal').classList.add('open');
}

function closeWeightModal() {
  document.getElementById('weightModal').classList.remove('open');
}

function onModalBackdropClick(event) {
  if (event.target.id === 'weightModal') closeWeightModal();
}

async function showConstituents(code) {
  openWeightModal(`${code} 成分股`, code, '<div class="loading"><div class="spinner"></div>加载中…</div>');
  try {
    const res = await fetch('/api/index-constituents?code=' + encodeURIComponent(code) + '&limit=10');
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || '加载失败');

    document.getElementById('weightModalTitle').textContent =
      data.weight_count ? `${data.name} 成分股权重 Top 10` : `${data.name} 成分股前 10`;
    const updated = data.updated_at ? ` · 成分股更新 ${data.updated_at}` : '';
    const modeText = data.weight_count ? '按权重排序' : '暂无权重，显示成分股前 10';
    const weightDate = data.weight_date ? ` · 权重日期 ${data.weight_date}` : '';
    document.getElementById('weightModalMeta').textContent =
      `${data.code} · 共 ${data.total_count} 只 · 有权重 ${data.weight_count} 只 · ${modeText}${weightDate}${updated}`;

    if (!data.rows.length) {
      document.getElementById('weightModalBody').innerHTML =
        '<div class="loading">该指数暂无成分股数据</div>';
      return;
    }

    let html = '<table class="weight-table"><thead><tr><th>排名</th><th>成分股</th><th>交易所</th><th>权重</th></tr></thead><tbody>';
    data.rows.forEach((row, i) => {
      html += `<tr>
        <td>${i + 1}</td>
        <td>${escapeHtml(row.name || '')}<span class="stock-code">${escapeHtml(row.code)}</span></td>
        <td>${escapeHtml(row.exchange || '-')}</td>
        <td class="weight-value ${row.weight === null || row.weight === undefined ? 'empty' : ''}">${row.weight === null || row.weight === undefined ? '—' : Number(row.weight).toFixed(2) + '%'}</td>
      </tr>`;
    });
    html += '</tbody></table>';
    document.getElementById('weightModalBody').innerHTML = html;
  } catch (err) {
    document.getElementById('weightModalBody').innerHTML =
      `<div class="loading">加载失败：${escapeHtml(err.message)}</div>`;
  }
}

document.addEventListener('keydown', event => {
  if (event.key === 'Escape') closeWeightModal();
});

async function loadData() {
  const days = document.getElementById('daysSelect').value;
  document.getElementById('tableWrap').innerHTML =
    '<div class="loading"><div class="spinner"></div>加载中…</div>';
  try {
    const res  = await fetch('/api/stats?days=' + days);
    const data = await res.json();
    buildTable(data);
  } catch(err) {
    document.getElementById('tableWrap').innerHTML =
      `<div class="loading">加载失败：${err.message}</div>`;
  }
}

loadData();
</script>
</body>
</html>
"""


MOMENTUM_HTML = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>14:30 强势放量选股</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Noto+Sans+SC:wght@300;400;500;700&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:#0d0f14;
    --surface:#141720;
    --surface2:#1a1e2e;
    --border:#252a3a;
    --text:#c8cdd8;
    --text-dim:#697082;
    --head:#8d96a9;
    --red:#ff4d6a;
    --green:#00c97a;
    --accent:#3d7fff;
    --amber:#f5b84b;
  }
  * { box-sizing:border-box; margin:0; padding:0; }
  body {
    min-height:100vh;
    background:var(--bg);
    color:var(--text);
    font-family:'Noto Sans SC', sans-serif;
    font-size:13px;
    padding:28px 24px;
  }
  .header {
    display:flex;
    justify-content:space-between;
    align-items:flex-end;
    gap:16px;
    padding-bottom:18px;
    margin-bottom:18px;
    border-bottom:1px solid var(--border);
  }
  h1 { font-size:20px; color:#fff; font-weight:500; letter-spacing:.04em; }
  .sub { margin-top:5px; color:var(--text-dim); font-size:11px; }
  .nav-link {
    color:var(--text);
    text-decoration:none;
    border:1px solid var(--border);
    background:var(--surface2);
    padding:7px 12px;
    border-radius:6px;
    white-space:nowrap;
  }
  .toolbar {
    display:grid;
    grid-template-columns: repeat(8, minmax(92px, 1fr));
    gap:10px;
    align-items:end;
    padding:14px;
    margin-bottom:16px;
    background:var(--surface);
    border:1px solid var(--border);
    border-radius:8px;
  }
  label { display:flex; flex-direction:column; gap:5px; color:var(--text-dim); font-size:10px; }
  input, select {
    height:34px;
    background:var(--surface2);
    color:var(--text);
    border:1px solid var(--border);
    border-radius:6px;
    padding:0 10px;
    font:12px 'Noto Sans SC', sans-serif;
    outline:none;
  }
  input:focus, select:focus { border-color:var(--accent); }
  button {
    height:34px;
    border:0;
    border-radius:6px;
    background:var(--accent);
    color:white;
    cursor:pointer;
    font:500 12px 'Noto Sans SC', sans-serif;
  }
  button:disabled { opacity:.55; cursor:default; }
  .status {
    display:flex;
    flex-wrap:wrap;
    gap:10px;
    margin-bottom:14px;
  }
  .pill {
    display:flex;
    gap:8px;
    align-items:center;
    min-height:32px;
    padding:7px 10px;
    border:1px solid var(--border);
    border-radius:6px;
    background:var(--surface);
    color:var(--text-dim);
  }
  .pill b {
    color:var(--text);
    font-family:'DM Mono', monospace;
    font-weight:500;
  }
  .profit-panel {
    margin-bottom:16px;
    border:1px solid var(--border);
    border-radius:8px;
    background:var(--surface);
    overflow:hidden;
  }
  .profit-head {
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:12px;
    padding:12px 14px;
    border-bottom:1px solid var(--border);
    background:rgba(255,255,255,.018);
  }
  .profit-title { color:#fff; font-size:13px; font-weight:500; }
  .profit-range { margin-left:8px; color:var(--text-dim); font:11px 'DM Mono', monospace; }
  .ghost-btn {
    width:auto;
    min-width:64px;
    padding:0 12px;
    border:1px solid var(--border);
    background:var(--surface2);
    color:var(--text);
  }
  .profit-grid {
    display:grid;
    grid-template-columns: repeat(6, minmax(92px, 1fr));
    gap:1px;
    background:var(--border);
  }
  .profit-stat {
    min-height:68px;
    padding:12px;
    background:var(--surface);
  }
  .profit-label { color:var(--text-dim); font-size:10px; margin-bottom:7px; }
  .profit-value { color:var(--text); font:500 18px 'DM Mono', monospace; }
  .profit-value.up { color:var(--red); }
  .profit-value.down { color:var(--green); }
  .profit-body {
    display:grid;
    grid-template-columns: minmax(360px, 1fr) minmax(420px, 1.15fr);
    gap:14px;
    padding:14px;
  }
  .mini-title {
    color:var(--head);
    font-size:11px;
    margin-bottom:8px;
  }
  .profit-days {
    display:flex;
    flex-direction:column;
    gap:6px;
  }
  .day-row {
    display:grid;
    grid-template-columns: 86px 1fr 70px 58px;
    align-items:center;
    gap:10px;
    min-height:24px;
    color:var(--text-dim);
    font-size:11px;
  }
  .bar-track {
    height:6px;
    border-radius:999px;
    background:var(--surface2);
    overflow:hidden;
  }
  .bar-fill {
    height:100%;
    width:0;
    border-radius:999px;
    background:var(--text-dim);
  }
  .bar-fill.up { background:var(--red); }
  .bar-fill.down { background:var(--green); }
  .recent-list {
    display:flex;
    flex-direction:column;
    gap:6px;
  }
  .recent-row {
    display:grid;
    grid-template-columns: 72px 70px 1fr 70px 58px;
    gap:8px;
    align-items:center;
    min-height:24px;
    color:var(--text-dim);
    font-size:11px;
  }
  .recent-code { font-family:'DM Mono', monospace; color:var(--text); }
  .recent-name { overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .table-wrap {
    overflow-x:auto;
    border:1px solid var(--border);
    border-radius:8px;
  }
  table { width:100%; border-collapse:collapse; min-width:1120px; }
  thead tr { background:var(--surface2); }
  th {
    color:var(--head);
    font-size:11px;
    font-weight:500;
    text-align:right;
    padding:11px 12px;
    border-bottom:1px solid var(--border);
    white-space:nowrap;
  }
  th:first-child, th:nth-child(2), th:last-child { text-align:left; }
  td {
    padding:10px 12px;
    border-bottom:1px solid var(--border);
    text-align:right;
    white-space:nowrap;
  }
  tbody tr:hover { background:rgba(255,255,255,.025); }
  tbody tr:last-child td { border-bottom:0; }
  .code {
    font-family:'DM Mono', monospace;
    color:var(--text-dim);
    text-align:left;
  }
  .name { color:#fff; text-align:left; }
  .num { font-family:'DM Mono', monospace; }
  .up { color:var(--red); }
  .down { color:var(--green); }
  .score {
    display:inline-flex;
    justify-content:center;
    min-width:46px;
    padding:3px 8px;
    border-radius:999px;
    background:rgba(61,127,255,.14);
    color:#8eb1ff;
    font-family:'DM Mono', monospace;
  }
  .spark { width:142px; text-align:left; }
  .reason { text-align:left; color:var(--text-dim); max-width:220px; overflow:hidden; text-overflow:ellipsis; }
  .loading {
    min-height:220px;
    display:flex;
    align-items:center;
    justify-content:center;
    color:var(--text-dim);
  }
  .spinner {
    width:16px;
    height:16px;
    margin-right:10px;
    border:2px solid var(--border);
    border-top-color:var(--accent);
    border-radius:50%;
    animation:spin .8s linear infinite;
  }
  @keyframes spin { to { transform:rotate(360deg); } }
  @media (max-width: 1100px) {
    .toolbar { grid-template-columns: repeat(4, minmax(92px, 1fr)); }
    .profit-grid { grid-template-columns: repeat(3, minmax(92px, 1fr)); }
    .profit-body { grid-template-columns: 1fr; }
  }
  @media (max-width: 640px) {
    body { padding:20px 14px; }
    .header { align-items:flex-start; flex-direction:column; }
    .toolbar { grid-template-columns: repeat(2, minmax(92px, 1fr)); }
    .profit-grid { grid-template-columns: repeat(2, minmax(92px, 1fr)); }
    .day-row { grid-template-columns: 78px 1fr 62px; }
    .day-row .day-win { display:none; }
    .recent-row { grid-template-columns: 66px 1fr 58px; }
    .recent-row .recent-date, .recent-row .recent-status { display:none; }
  }
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>14:30 强势放量选股</h1>
    <div class="sub">涨幅适中、量能放大、日内位置强，默认次日 10:00 前卖出观察</div>
  </div>
  <a class="nav-link" href="/">行业宽度</a>
</div>

<div class="toolbar">
  <label>股票池
    <select id="pool" onchange="syncIndexWithPool()">
      <option value="all">全市场</option>
      <option value="sector">行业池</option>
      <option value="index">指数成分</option>
    </select>
  </label>
  <label>指数
    <select id="indexCode" onchange="syncPoolWithIndex()"></select>
  </label>
  <label>截止
    <input id="cutoff" value="14:30" inputmode="numeric">
  </label>
  <label>最低涨幅%
    <input id="minGain" type="number" value="2" step="0.1">
  </label>
  <label>最高涨幅%
    <input id="maxGain" type="number" value="7.5" step="0.1">
  </label>
  <label>量比
    <input id="minVolRatio" type="number" value="1.5" step="0.1">
  </label>
  <label>成交额万元
    <input id="minAmount" type="number" value="8000" step="500">
  </label>
  <label>验证数量
    <input id="verifyLimit" type="number" value="50" step="10" min="5" max="300">
  </label>
  <button id="scanBtn" onclick="scan()">开始扫描</button>
</div>

<div class="status">
  <div class="pill">报价 <b id="quoted">—</b></div>
  <div class="pill">预筛 <b id="prefiltered">—</b></div>
  <div class="pill">验证 <b id="verified">—</b></div>
  <div class="pill">5分钟K <b id="minuteStats">—</b></div>
  <div class="pill">缓存 <b id="cacheHits">—</b></div>
  <div class="pill">入选 <b id="matched">—</b></div>
  <div class="pill">耗时 <b id="elapsed">—</b></div>
  <div class="pill">时间 <b id="scanTime">—</b></div>
</div>

<div class="profit-panel">
  <div class="profit-head">
    <div>
      <span class="profit-title">最近一个月收益</span>
      <span class="profit-range" id="profitRange">—</span>
    </div>
    <button class="ghost-btn" onclick="loadProfit()">刷新</button>
  </div>
  <div class="profit-grid">
    <div class="profit-stat">
      <div class="profit-label">平均收益</div>
      <div class="profit-value" id="profitAvg">—</div>
    </div>
    <div class="profit-stat">
      <div class="profit-label">胜率</div>
      <div class="profit-value" id="profitWin">—</div>
    </div>
    <div class="profit-stat">
      <div class="profit-label">成交记录</div>
      <div class="profit-value" id="profitSold">—</div>
    </div>
    <div class="profit-stat">
      <div class="profit-label">未结算/失败</div>
      <div class="profit-value" id="profitFailed">—</div>
    </div>
    <div class="profit-stat">
      <div class="profit-label">最好</div>
      <div class="profit-value" id="profitBest">—</div>
    </div>
    <div class="profit-stat">
      <div class="profit-label">最差</div>
      <div class="profit-value" id="profitWorst">—</div>
    </div>
  </div>
  <div class="profit-body">
    <div>
      <div class="mini-title">按买入日</div>
      <div class="profit-days" id="profitDays"><div class="loading">加载中…</div></div>
    </div>
    <div>
      <div class="mini-title">最近记录</div>
      <div class="recent-list" id="profitRecent"><div class="loading">加载中…</div></div>
    </div>
  </div>
</div>

<div class="table-wrap" id="tableWrap">
  <div class="loading">等待扫描</div>
</div>

<script>
const fmt = (value, digits=2) => value === null || value === undefined ? '—' : Number(value).toFixed(digits);
const esc = value => String(value ?? '').replace(/[&<>"']/g, ch => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
}[ch]));
const pctText = (value, digits=2) => value === null || value === undefined ? '—' : `${Number(value).toFixed(digits)}%`;
const signedCls = value => Number(value || 0) > 0 ? 'up' : Number(value || 0) < 0 ? 'down' : '';

function setProfitValue(id, value, suffix='', digits=2) {
  const el = document.getElementById(id);
  const cls = signedCls(value);
  el.className = `profit-value ${cls}`;
  el.textContent = value === null || value === undefined ? '—' : `${Number(value).toFixed(digits)}${suffix}`;
}

async function loadIndices() {
  const res = await fetch('/api/indices');
  const data = await res.json();
  const select = document.getElementById('indexCode');
  select.innerHTML = data.indices.map(x =>
    `<option value="${esc(x.code)}">${esc(x.name)} ${esc(x.code)}</option>`
  ).join('');
}

function syncPoolWithIndex() {
  const indexCode = document.getElementById('indexCode').value;
  document.getElementById('pool').value = indexCode ? 'index' : 'all';
}

function syncIndexWithPool() {
  const pool = document.getElementById('pool').value;
  if (pool !== 'index') {
    document.getElementById('indexCode').value = '';
  }
}

function params() {
  const p = new URLSearchParams();
  ['pool','indexCode','cutoff','minGain','maxGain','minVolRatio','minAmount','verifyLimit'].forEach(id => {
    p.set(id, document.getElementById(id).value);
  });
  p.set('limit', '80');
  p.set('workers', '8');
  return p.toString();
}

function renderRows(rows) {
  if (!rows.length) {
    document.getElementById('tableWrap').innerHTML = '<div class="loading">暂无符合条件的股票</div>';
    return;
  }
  let html = `<table><thead><tr>
    <th>代码</th><th>名称</th><th>现价</th><th>涨幅</th><th>量比</th>
    <th>成交额</th><th>高位</th><th>高点回撤</th><th>高点</th><th>分时</th><th>评分</th><th>要点</th>
  </tr></thead><tbody>`;
  rows.forEach(r => {
    html += `<tr>
      <td class="code">${esc(r.code)}</td>
      <td class="name">${esc(r.name)}</td>
      <td class="num">${fmt(r.price, 2)}</td>
      <td class="num ${r.pct >= 0 ? 'up' : 'down'}">${fmt(r.pct, 2)}%</td>
      <td class="num">${fmt(r.volume_ratio, 2)}</td>
      <td class="num">${fmt(r.amount_yi, 2)}亿</td>
      <td class="num">${r.close_position === null ? '—' : fmt(r.close_position, 1) + '%'}</td>
      <td class="num">${r.pullback_pct === null ? '—' : fmt(r.pullback_pct, 2) + '%'}</td>
      <td class="num">${esc(r.high_time || '—')}</td>
      <td class="spark">${r.sparkline || '—'}</td>
      <td><span class="score">${fmt(r.score, 1)}</span></td>
      <td class="reason" title="${esc(r.reasons)}">${esc(r.reasons)}</td>
    </tr>`;
  });
  html += '</tbody></table>';
  document.getElementById('tableWrap').innerHTML = html;
}

function renderProfit(data) {
  const summary = data.summary || {};
  document.getElementById('profitRange').textContent =
    data.start_date && data.end_date ? `${data.start_date} ~ ${data.end_date}` : '暂无记录';
  setProfitValue('profitAvg', summary.avg_return_pct, '%', 2);
  setProfitValue('profitWin', summary.win_rate_pct, '%', 1);
  document.getElementById('profitSold').textContent = summary.sold_count ?? 0;
  document.getElementById('profitFailed').textContent = summary.failed_count ?? 0;
  setProfitValue('profitBest', summary.max_return_pct, '%', 2);
  setProfitValue('profitWorst', summary.min_return_pct, '%', 2);

  const days = data.by_date || [];
  const maxAbs = Math.max(1, ...days.map(x => Math.abs(Number(x.avg_return_pct || 0))));
  if (!days.length) {
    document.getElementById('profitDays').innerHTML = '<div class="loading">暂无收益记录</div>';
  } else {
    document.getElementById('profitDays').innerHTML = days.slice(0, 12).map(day => {
      const avg = Number(day.avg_return_pct || 0);
      const width = Math.max(2, Math.abs(avg) / maxAbs * 100);
      const cls = signedCls(avg);
      return `<div class="day-row">
        <span class="num">${esc(day.buy_date)}</span>
        <span class="bar-track"><span class="bar-fill ${cls}" style="width:${width}%"></span></span>
        <span class="num ${cls}">${pctText(day.avg_return_pct, 2)}</span>
        <span class="day-win">${day.sold_count || 0}笔 / ${pctText(day.win_rate_pct, 0)}</span>
      </div>`;
    }).join('');
  }

  const recent = data.recent || [];
  if (!recent.length) {
    document.getElementById('profitRecent').innerHTML = '<div class="loading">暂无最近记录</div>';
  } else {
    document.getElementById('profitRecent').innerHTML = recent.map(row => {
      const cls = signedCls(row.return_pct);
      const status = row.status === 'sold' ? (row.error === 'daily_open_fallback' ? '日线' : '分钟') : '失败';
      return `<div class="recent-row">
        <span class="recent-date num">${esc(row.buy_date)}</span>
        <span class="recent-code">${esc(row.code)}</span>
        <span class="recent-name">${esc(row.name || '')}</span>
        <span class="num ${cls}">${pctText(row.return_pct, 2)}</span>
        <span class="recent-status">${esc(status)}</span>
      </div>`;
    }).join('');
  }
}

async function loadProfit() {
  try {
    const res = await fetch('/api/momentum/profit?days=30');
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.error || '收益加载失败');
    }
    renderProfit(data);
  } catch (err) {
    document.getElementById('profitRange').textContent = '加载失败';
    document.getElementById('profitDays').innerHTML = `<div class="loading">${esc(err.message)}</div>`;
    document.getElementById('profitRecent').innerHTML = `<div class="loading">${esc(err.message)}</div>`;
  }
}

async function scan() {
  const btn = document.getElementById('scanBtn');
  btn.disabled = true;
  document.getElementById('tableWrap').innerHTML =
    '<div class="loading"><span class="spinner"></span>扫描中…</div>';
  try {
    const res = await fetch('/api/momentum/scan?' + params());
    const data = await res.json();
    if (data.meta) {
      document.getElementById('quoted').textContent = data.meta.quoted;
      document.getElementById('prefiltered').textContent = data.meta.prefiltered;
      document.getElementById('verified').textContent = data.meta.verified;
      document.getElementById('minuteStats').textContent =
        `${data.meta.minute_success ?? 0}/${data.meta.verified ?? 0}`;
      document.getElementById('cacheHits').textContent = data.meta.cache_hits ?? 0;
      document.getElementById('elapsed').textContent = data.meta.elapsed_s + 's';
      document.getElementById('scanTime').textContent = new Date().toLocaleTimeString('zh-CN');
    }
    if (!res.ok) {
      document.getElementById('matched').textContent = '0';
      throw new Error(data.error || '扫描失败');
    }
    document.getElementById('matched').textContent = data.rows.length;
    renderRows(data.rows);
  } catch (err) {
    document.getElementById('tableWrap').innerHTML = `<div class="loading">加载失败：${err.message}</div>`;
  } finally {
    btn.disabled = false;
  }
}

loadIndices();
loadProfit();
</script>
</body>
</html>
"""


# ─────────────────────────────────────────────────────────────────
# 路由
# ─────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/momentum")
def momentum_page():
    return render_template_string(MOMENTUM_HTML)


@app.route("/api/indices")
def api_indices():
    conn = get_db()
    try:
        indices = [{"code": "", "name": "全部市场"}] + load_indices(conn)
        return jsonify({"indices": indices})
    finally:
        conn.close()


@app.route("/api/index-constituents")
def api_index_constituents():
    code = (request.args.get("code") or "").strip()
    limit = to_int_arg("limit", 10, 1, 50)
    if not code:
        return jsonify({"error": "缺少指数代码"}), 400

    conn = get_db()
    try:
        idx = conn.execute(
            "SELECT code, name FROM indices WHERE code = ?",
            (code,)
        ).fetchone()
        if not idx:
            return jsonify({"error": "指数不存在"}), 404

        summary = conn.execute("""
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN weight IS NOT NULL THEN 1 ELSE 0 END) AS weight_count,
                MAX(weight_date) AS weight_date,
                MAX(updated_at) AS updated_at
            FROM index_constituents
            WHERE index_code = ?
        """, (code,)).fetchone()

        weight_count = summary["weight_count"] or 0
        if weight_count:
            rows = conn.execute("""
                SELECT
                    ic.stock_code,
                    COALESCE(s.name, ic.stock_name) AS stock_name,
                    ic.exchange,
                    ic.weight,
                    ic.weight_date
                FROM index_constituents ic
                LEFT JOIN stocks s ON s.code = ic.stock_code
                WHERE ic.index_code = ?
                  AND ic.weight IS NOT NULL
                ORDER BY ic.weight DESC, ic.stock_code ASC
                LIMIT ?
            """, (code, limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT
                    ic.stock_code,
                    COALESCE(s.name, ic.stock_name) AS stock_name,
                    ic.exchange,
                    ic.weight,
                    ic.weight_date
                FROM index_constituents ic
                LEFT JOIN stocks s ON s.code = ic.stock_code
                WHERE ic.index_code = ?
                ORDER BY ic.stock_code ASC
                LIMIT ?
            """, (code, limit)).fetchall()

        return jsonify({
            "code": idx["code"],
            "name": idx["name"],
            "total_count": summary["total_count"] or 0,
            "weight_count": weight_count,
            "weight_date": summary["weight_date"],
            "updated_at": summary["updated_at"],
            "rows": [
                {
                    "code": r["stock_code"],
                    "name": r["stock_name"],
                    "exchange": r["exchange"],
                    "weight": r["weight"],
                    "weight_date": r["weight_date"],
                }
                for r in rows
            ],
        })
    finally:
        conn.close()


@app.route("/api/momentum/profit")
def api_momentum_profit():
    days = to_int_arg("days", 30, 1, 250)
    conn = get_db()
    try:
        return jsonify(load_momentum_profit_summary(conn, days=days))
    finally:
        conn.close()


@app.route("/api/momentum/scan")
def api_momentum_scan():
    started_at = time.time()
    if not SCAN_LOCK.acquire(blocking=False):
        return jsonify({
            "error": "已有扫描任务进行中，请等待上一次扫描结束",
            "meta": {
                "quoted": 0,
                "prefiltered": 0,
                "verified": 0,
                "minute_success": 0,
                "minute_failed": 0,
                "cache_hits": 0,
                "elapsed_s": 0,
            },
            "rows": [],
        }), 429
    try:
        payload, status = perform_momentum_scan(
            build_momentum_params(request.args),
            started_at=started_at,
        )
        return jsonify(payload), status
    finally:
        SCAN_LOCK.release()


def perform_momentum_scan(params, started_at=None):
    started_at = started_at or time.time()
    pool = params["pool"]
    index_code = params["index_code"]
    cutoff_text = params["cutoff"]
    scan_trade_date = params["trade_date"]
    min_gain = params["min_gain"]
    max_gain = params["max_gain"]
    min_vol_ratio = params["min_vol_ratio"]
    min_amount_yuan = params["min_amount_wan"] * 10000
    limit = params["limit"]
    verify_limit = params["verify_limit"]
    max_workers = params["workers"]
    cutoff = parse_cutoff_time(cutoff_text)
    elapsed_ratio = trade_elapsed_ratio(cutoff)

    conn = get_db()
    try:
        stocks = load_stock_universe(conn, pool=pool, index_code=index_code)
        if not stocks:
            return {"error": "股票池为空", "meta": build_empty_scan_meta(params)}, 400

        stock_by_code = {s["code"]: s for s in stocks}
        codes = list(stock_by_code.keys())
        quotes = fetch_realtime_quotes(codes)
        if not quotes:
            return {
                "error": "实时行情获取失败：新浪和东方财富均无有效返回",
                "meta": build_empty_scan_meta(params, universe=len(stocks)),
                "rows": [],
            }, 502

        valid_codes = [code for code in codes if code in quotes]
        daily_metrics = load_daily_metrics(conn, valid_codes)
    finally:
        conn.close()

    prefiltered = []
    for code in valid_codes:
        quote = quotes[code]
        daily = daily_metrics.get(code)
        if not daily:
            continue
        pct = quote["pct"]
        if pct < min_gain or pct > max_gain:
            continue
        if quote["amount"] < min_amount_yuan:
            continue
        avg_volume20 = daily.get("avg_volume20")
        if not avg_volume20:
            continue
        live_volume_ratio = quote["volume"] / (avg_volume20 * elapsed_ratio)
        if live_volume_ratio < min_vol_ratio * 0.75:
            continue
        ma5 = daily.get("ma5")
        ma20 = daily.get("ma20")
        price = quote["price"]
        if not ma5 or price <= ma5:
            continue
        if not daily.get("ma5_up"):
            continue
        if ma20 and price < ma20 * 0.97:
            continue
        pre_score = (
            pct * 5
            + min(live_volume_ratio, 4) * 12
            + min(quote["amount"] / 100000000, 5) * 4
        )
        prefiltered.append((pre_score, stock_by_code[code], quote, daily))

    prefiltered.sort(key=lambda x: x[0], reverse=True)
    verify_items = prefiltered[:verify_limit]

    rows = []
    minute_success = 0
    minute_failed = 0

    stock_items = [stock for _, stock, _, _ in verify_items]
    kline_map, cache_hits = fetch_baostock_5m_klines_parallel(
        stock_items,
        cutoff_text,
        max_workers=max_workers,
        trade_date=scan_trade_date,
    )

    probe_items = verify_items[:min(5, len(verify_items))]
    for _, stock, quote, daily in probe_items:
        bars = kline_map.get(stock["code"])
        row = evaluate_candidate_with_bars(
            stock, quote, daily, cutoff_text, elapsed_ratio, bars
        )
        if not row:
            minute_failed += 1
            continue
        minute_success += 1
        if passes_momentum_filters(row, min_gain, max_gain, min_vol_ratio):
            rows.append(row)

    if probe_items and minute_success == 0:
        meta = {
            "pool": pool,
            "cutoff": cutoff_text,
            "trade_date": scan_trade_date,
            "index_code": index_code,
            "universe": len(stocks),
            "quoted": len(quotes),
            "prefiltered": len(prefiltered),
            "verified": len(probe_items),
            "minute_success": minute_success,
            "minute_failed": minute_failed,
            "cache_hits": cache_hits,
            "elapsed_s": round(time.time() - started_at, 1),
        }
        return {
            "error": "分钟线接口暂不可用，候选股无法做14:30分时验证",
            "meta": meta,
            "rows": [],
        }, 503

    remaining_items = verify_items[len(probe_items):]
    for _, stock, quote, daily in remaining_items:
        bars = kline_map.get(stock["code"])
        row = evaluate_candidate_with_bars(
            stock, quote, daily, cutoff_text, elapsed_ratio, bars
        )
        if not row:
            minute_failed += 1
            continue
        minute_success += 1
        if not passes_momentum_filters(row, min_gain, max_gain, min_vol_ratio):
            continue
        rows.append(row)

    rows.sort(key=lambda r: (r["score"], r["volume_ratio"], r["amount_yi"]),
              reverse=True)
    rows = rows[:limit]
    meta = {
        "pool": pool,
        "cutoff": cutoff_text,
        "trade_date": scan_trade_date,
        "index_code": index_code,
        "universe": len(stocks),
        "quoted": len(quotes),
        "prefiltered": len(prefiltered),
        "verified": len(verify_items),
        "minute_success": minute_success,
        "minute_failed": minute_failed,
        "cache_hits": cache_hits,
        "elapsed_s": round(time.time() - started_at, 1),
    }
    if verify_items and minute_success == 0:
        return {
            "error": "分钟线接口暂不可用，候选股无法做14:30分时验证",
            "meta": meta,
            "rows": [],
        }, 503
    return {"meta": meta, "rows": rows}, 200


def load_daily_metrics_before(conn, codes, trade_date):
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
                  AND trade_date < ?
            )
            WHERE rn <= 80
            ORDER BY code, trade_date DESC
        """, batch + [trade_date]).fetchall()

        grouped = {}
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
            high20 = max((r["high"] for r in series[-20:] if r["high"] is not None),
                         default=None)
            low20 = min((r["low"] for r in series[-20:] if r["low"] is not None),
                        default=None)
            avg_volume20 = sum(volumes) / len(volumes) if volumes else None
            prev_ma5 = (sum(closes[-6:-1]) / 5) if len(closes) >= 6 else None
            last = series[-1]
            metrics[code] = {
                "last_trade_date": last["trade_date"],
                "last_close": last["close"],
                "prev_low": last["low"],
                "ma5": ma5,
                "ma5_prev": prev_ma5,
                "ma5_up": bool(prev_ma5 is not None and ma5 > prev_ma5),
                "ma20": ma20,
                "high20": high20,
                "low20": low20,
                "avg_volume20": avg_volume20,
            }
    return metrics


def load_historical_daily_quotes(conn, codes, trade_date, daily_metrics):
    quotes = {}
    for batch in chunked(codes, 600):
        placeholders = ",".join("?" for _ in batch)
        rows = conn.execute(f"""
            SELECT code, open, close, high, low, volume, amount, pct_change
            FROM daily_prices
            WHERE code IN ({placeholders})
              AND trade_date = ?
        """, batch + [trade_date]).fetchall()
        for row in rows:
            daily = daily_metrics.get(row["code"])
            prev_close = daily.get("last_close") if daily else None
            close = row["close"]
            if not prev_close or not close:
                continue
            pct = row["pct_change"]
            if pct is None:
                pct = (close - prev_close) / prev_close * 100.0
            quotes[row["code"]] = {
                "open": row["open"],
                "prev_close": prev_close,
                "price": close,
                "high": row["high"],
                "low": row["low"],
                "volume": row["volume"] or 0,
                "amount": row["amount"] or 0,
                "trade_date": trade_date,
                "quote_time": "15:00:00",
                "pct": pct,
            }
    return quotes


def perform_historical_momentum_scan(params, started_at=None):
    started_at = started_at or time.time()
    pool = params["pool"]
    index_code = params["index_code"]
    cutoff_text = params["cutoff"]
    scan_trade_date = params["trade_date"]
    min_gain = params["min_gain"]
    max_gain = params["max_gain"]
    min_vol_ratio = params["min_vol_ratio"]
    min_amount_yuan = params["min_amount_wan"] * 10000
    limit = params["limit"]
    verify_limit = params["verify_limit"]
    max_workers = params["workers"]
    cutoff = parse_cutoff_time(cutoff_text)
    elapsed_ratio = trade_elapsed_ratio(cutoff)

    conn = get_db()
    try:
        stocks = load_stock_universe(conn, pool=pool, index_code=index_code)
        if not stocks:
            return {"error": "股票池为空", "meta": build_empty_scan_meta(params)}, 400
        stock_by_code = {s["code"]: s for s in stocks}
        codes = list(stock_by_code.keys())
        daily_metrics = load_daily_metrics_before(conn, codes, scan_trade_date)
        quotes = load_historical_daily_quotes(
            conn, codes, scan_trade_date, daily_metrics
        )
    finally:
        conn.close()

    valid_codes = [code for code in codes if code in quotes]
    prefiltered = []
    for code in valid_codes:
        quote = quotes[code]
        daily = daily_metrics.get(code)
        if not daily:
            continue
        pct = quote["pct"]
        if pct < min_gain - 2.5 or pct > max_gain + 3.0:
            continue
        if quote["amount"] < min_amount_yuan * 0.45:
            continue
        avg_volume20 = daily.get("avg_volume20")
        if not avg_volume20:
            continue
        day_volume_ratio = quote["volume"] / avg_volume20
        if day_volume_ratio < min_vol_ratio * 0.35:
            continue
        ma5 = daily.get("ma5")
        ma20 = daily.get("ma20")
        price = quote["price"]
        if not ma5 or price <= ma5 * 0.96:
            continue
        if not daily.get("ma5_up"):
            continue
        if ma20 and price < ma20 * 0.94:
            continue
        pre_score = (
            pct * 5
            + min(day_volume_ratio, 4) * 12
            + min(quote["amount"] / 100000000, 5) * 4
        )
        prefiltered.append((pre_score, stock_by_code[code], quote, daily))

    prefiltered.sort(key=lambda x: x[0], reverse=True)
    verify_items = prefiltered[:verify_limit]
    stock_items = [stock for _, stock, _, _ in verify_items]
    kline_map, cache_hits = fetch_baostock_5m_klines_parallel(
        stock_items,
        cutoff_text,
        max_workers=max_workers,
        trade_date=scan_trade_date,
    )

    rows = []
    minute_success = 0
    minute_failed = 0
    for _, stock, quote, daily in verify_items:
        bars = kline_map.get(stock["code"])
        row = evaluate_candidate_with_bars(
            stock, quote, daily, cutoff_text, elapsed_ratio, bars
        )
        if not row:
            minute_failed += 1
            continue
        minute_success += 1
        if not passes_momentum_filters(row, min_gain, max_gain, min_vol_ratio):
            continue
        rows.append(row)

    rows.sort(key=lambda r: (r["score"], r["volume_ratio"], r["amount_yi"]),
              reverse=True)
    rows = rows[:limit]
    meta = {
        "pool": pool,
        "cutoff": cutoff_text,
        "trade_date": scan_trade_date,
        "index_code": index_code,
        "universe": len(stocks),
        "quoted": len(quotes),
        "prefiltered": len(prefiltered),
        "verified": len(verify_items),
        "minute_success": minute_success,
        "minute_failed": minute_failed,
        "cache_hits": cache_hits,
        "elapsed_s": round(time.time() - started_at, 1),
        "historical": True,
    }
    if verify_items and minute_success == 0:
        return {
            "error": "历史分钟线接口暂不可用，候选股无法做14:30分时验证",
            "meta": meta,
            "rows": [],
        }, 503
    return {"meta": meta, "rows": rows}, 200


def perform_daily_fallback_momentum_scan(params, started_at=None):
    started_at = started_at or time.time()
    pool = params["pool"]
    index_code = params["index_code"]
    scan_trade_date = params["trade_date"]
    min_gain = params["min_gain"]
    max_gain = params["max_gain"]
    min_vol_ratio = params["min_vol_ratio"]
    min_amount_yuan = params["min_amount_wan"] * 10000
    limit = params["limit"]

    conn = get_db()
    try:
        stocks = load_stock_universe(conn, pool=pool, index_code=index_code)
        if not stocks:
            return {"error": "股票池为空", "meta": build_empty_scan_meta(params)}, 400
        stock_by_code = {s["code"]: s for s in stocks}
        codes = list(stock_by_code.keys())
        daily_metrics = load_daily_metrics_before(conn, codes, scan_trade_date)
        quotes = load_historical_daily_quotes(
            conn, codes, scan_trade_date, daily_metrics
        )
    finally:
        conn.close()

    rows = []
    for code, quote in quotes.items():
        stock = stock_by_code.get(code)
        daily = daily_metrics.get(code)
        if not stock or not daily:
            continue
        pct = quote["pct"]
        price = quote["price"]
        avg_volume20 = daily.get("avg_volume20")
        if pct < min_gain or pct > max_gain:
            continue
        if quote["amount"] < min_amount_yuan:
            continue
        if not avg_volume20:
            continue
        volume_ratio = quote["volume"] / avg_volume20
        if volume_ratio < min_vol_ratio:
            continue
        ma5 = daily.get("ma5")
        if not ma5 or price <= ma5:
            continue
        if not daily.get("ma5_up"):
            continue
        prev_low = daily.get("prev_low")
        if prev_low and quote.get("low") and quote["low"] < prev_low:
            continue
        ma20 = daily.get("ma20")
        if ma20 and price < ma20 * 0.97:
            continue

        close_position = position_in_range(price, quote.get("low"), quote.get("high"))
        pullback_pct = (
            (quote["high"] - price) / price * 100.0
            if quote.get("high") and price else None
        )
        if close_position is not None and close_position < 0.65:
            continue
        if pullback_pct is not None and pullback_pct > 3.0:
            continue

        score = round(
            clamp(20 - abs(pct - 4.8) * 3.0, 0, 20)
            + clamp((volume_ratio - 1.0) / 1.8 * 25, 0, 25)
            + clamp((quote["amount"] or 0) / 100000000 / 3.0 * 10, 0, 10)
            + (close_position or 0) * 15
            + 10,
            1,
        )
        reasons = ["日线回退", "强于5日线", "5日线向上", "放量"]
        rows.append({
            "code": code,
            "name": stock["name"] or "",
            "price": round(price, 3),
            "pct": round(pct, 2),
            "amount_yi": round((quote["amount"] or 0) / 100000000, 2),
            "volume_ratio": round(volume_ratio, 2),
            "volume_full_ratio": round(volume_ratio, 2),
            "close_position": round(close_position * 100, 1) if close_position is not None else None,
            "pullback_pct": round(pullback_pct, 2) if pullback_pct is not None else None,
            "afternoon_pct": None,
            "above_vwap": None,
            "trend_above_ma5": True,
            "ma5_up": True,
            "not_break_prev_low": True,
            "high_time": "15:00",
            "high_after_14": True,
            "close_strong": True,
            "has_minute": False,
            "historical_fallback": "daily_close_buy_next_open_sell",
            "score": score,
            "reasons": " / ".join(reasons),
            "sparkline": "",
            "quote_time": "15:00:00",
            "trade_date": scan_trade_date,
        })

    rows.sort(key=lambda r: (r["score"], r["volume_ratio"], r["amount_yi"]),
              reverse=True)
    rows = rows[:limit]
    meta = {
        "pool": pool,
        "cutoff": params["cutoff"],
        "trade_date": scan_trade_date,
        "index_code": index_code,
        "universe": len(stocks),
        "quoted": len(quotes),
        "prefiltered": len(rows),
        "verified": len(rows),
        "minute_success": 0,
        "minute_failed": 0,
        "cache_hits": 0,
        "elapsed_s": round(time.time() - started_at, 1),
        "historical": True,
        "fallback": "daily",
    }
    return {"meta": meta, "rows": rows}, 200


def metric_from_previous_series(series):
    closes = [r["close"] for r in series if r["close"] is not None]
    volumes = [r["volume"] for r in series[-20:] if r["volume"] is not None]
    if not closes:
        return None
    ma5 = sum(closes[-5:]) / min(len(closes), 5)
    ma20 = sum(closes[-20:]) / min(len(closes), 20)
    prev_ma5 = (sum(closes[-6:-1]) / 5) if len(closes) >= 6 else None
    last = series[-1]
    return {
        "last_trade_date": last["trade_date"],
        "last_close": last["close"],
        "prev_low": last["low"],
        "ma5": ma5,
        "ma5_prev": prev_ma5,
        "ma5_up": bool(prev_ma5 is not None and ma5 > prev_ma5),
        "ma20": ma20,
        "high20": max((r["high"] for r in series[-20:] if r["high"] is not None),
                      default=None),
        "low20": min((r["low"] for r in series[-20:] if r["low"] is not None),
                     default=None),
        "avg_volume20": sum(volumes) / len(volumes) if volumes else None,
    }


def build_daily_fallback_row(stock, quote, daily, params):
    min_gain = params["min_gain"]
    max_gain = params["max_gain"]
    min_vol_ratio = params["min_vol_ratio"]
    min_amount_yuan = params["min_amount_wan"] * 10000
    pct = quote["pct"]
    price = quote["price"]
    avg_volume20 = daily.get("avg_volume20")
    if pct < min_gain or pct > max_gain:
        return None
    if quote["amount"] < min_amount_yuan:
        return None
    if not avg_volume20:
        return None
    volume_ratio = quote["volume"] / avg_volume20
    if volume_ratio < min_vol_ratio:
        return None
    ma5 = daily.get("ma5")
    if not ma5 or price <= ma5:
        return None
    if not daily.get("ma5_up"):
        return None
    prev_low = daily.get("prev_low")
    if prev_low and quote.get("low") and quote["low"] < prev_low:
        return None
    ma20 = daily.get("ma20")
    if ma20 and price < ma20 * 0.97:
        return None

    close_position = position_in_range(price, quote.get("low"), quote.get("high"))
    pullback_pct = (
        (quote["high"] - price) / price * 100.0
        if quote.get("high") and price else None
    )
    if close_position is not None and close_position < 0.65:
        return None
    if pullback_pct is not None and pullback_pct > 3.0:
        return None

    score = round(
        clamp(20 - abs(pct - 4.8) * 3.0, 0, 20)
        + clamp((volume_ratio - 1.0) / 1.8 * 25, 0, 25)
        + clamp((quote["amount"] or 0) / 100000000 / 3.0 * 10, 0, 10)
        + (close_position or 0) * 15
        + 10,
        1,
    )
    return {
        "code": stock["code"],
        "name": stock["name"] or "",
        "price": round(price, 3),
        "pct": round(pct, 2),
        "amount_yi": round((quote["amount"] or 0) / 100000000, 2),
        "volume_ratio": round(volume_ratio, 2),
        "volume_full_ratio": round(volume_ratio, 2),
        "close_position": round(close_position * 100, 1) if close_position is not None else None,
        "pullback_pct": round(pullback_pct, 2) if pullback_pct is not None else None,
        "afternoon_pct": None,
        "above_vwap": None,
        "trend_above_ma5": True,
        "ma5_up": True,
        "not_break_prev_low": True,
        "high_time": "15:00",
        "high_after_14": True,
        "close_strong": True,
        "has_minute": False,
        "historical_fallback": "daily_close_buy_next_open_sell",
        "score": score,
        "reasons": "日线回退 / 强于5日线 / 5日线向上 / 放量",
        "sparkline": "",
        "quote_time": "15:00:00",
        "trade_date": quote["trade_date"],
    }


def load_daily_history_for_backfill(conn, codes, start_date, end_date):
    histories = {code: [] for code in codes}
    start_dt = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=180)
    history_start = start_dt.strftime("%Y-%m-%d")
    code_set = set(codes)
    rows = conn.execute("""
        SELECT code, trade_date, open, close, high, low, volume, amount, pct_change
        FROM daily_prices
        WHERE trade_date >= ?
          AND trade_date <= ?
        ORDER BY code, trade_date
    """, (history_start, end_date)).fetchall()
    for row in rows:
        code = row["code"]
        if code in code_set:
            histories[code].append(row)
    return histories


def build_daily_fallback_payload_from_history(params, stocks, histories,
                                              trade_date, started_at=None):
    started_at = started_at or time.time()
    rows = []
    quoted = 0
    for stock in stocks:
        series = histories.get(stock["code"]) or []
        idx = None
        for i in range(len(series) - 1, -1, -1):
            if series[i]["trade_date"] == trade_date:
                idx = i
                break
        if idx is None or idx == 0:
            continue
        prev_series = series[max(0, idx - 80):idx]
        daily = metric_from_previous_series(prev_series)
        if not daily or not daily.get("last_close"):
            continue
        current = series[idx]
        close = current["close"]
        if not close:
            continue
        pct = current["pct_change"]
        if pct is None:
            pct = (close - daily["last_close"]) / daily["last_close"] * 100.0
        quote = {
            "trade_date": trade_date,
            "price": close,
            "pct": pct,
            "open": current["open"],
            "high": current["high"],
            "low": current["low"],
            "volume": current["volume"] or 0,
            "amount": current["amount"] or 0,
        }
        quoted += 1
        row = build_daily_fallback_row(stock, quote, daily, params)
        if row:
            rows.append(row)

    rows.sort(key=lambda r: (r["score"], r["volume_ratio"], r["amount_yi"]),
              reverse=True)
    rows = rows[:params["limit"]]
    meta = {
        "pool": params["pool"],
        "cutoff": params["cutoff"],
        "trade_date": trade_date,
        "index_code": params["index_code"],
        "universe": len(stocks),
        "quoted": quoted,
        "prefiltered": len(rows),
        "verified": len(rows),
        "minute_success": 0,
        "minute_failed": 0,
        "cache_hits": 0,
        "elapsed_s": round(time.time() - started_at, 1),
        "historical": True,
        "fallback": "daily-fast",
    }
    return {"meta": meta, "rows": rows}, 200


def build_empty_scan_meta(params, universe=0):
    return {
        "pool": params["pool"],
        "cutoff": params["cutoff"],
        "trade_date": params["trade_date"],
        "index_code": params["index_code"],
        "universe": universe,
        "quoted": 0,
        "prefiltered": 0,
        "verified": 0,
        "minute_success": 0,
        "minute_failed": 0,
        "cache_hits": 0,
        "elapsed_s": 0,
    }


def save_momentum_scan_result(conn, params, payload, status_code):
    ensure_momentum_tables(conn)
    meta = payload.get("meta") or build_empty_scan_meta(params)
    rows = payload.get("rows") or []
    status = "ok" if status_code == 200 else "error"
    cur = conn.execute("""
        INSERT INTO momentum_scan_runs (
            trade_date, cutoff, pool, index_code,
            min_gain, max_gain, min_vol_ratio, min_amount_wan,
            limit_count, verify_limit, workers,
            universe, quoted, prefiltered, verified,
            minute_success, minute_failed, cache_hits, elapsed_s,
            row_count, status, error, params_json
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        params["trade_date"], params["cutoff"], params["pool"], params["index_code"],
        params["min_gain"], params["max_gain"], params["min_vol_ratio"],
        params["min_amount_wan"], params["limit"], params["verify_limit"],
        params["workers"], meta.get("universe"), meta.get("quoted"),
        meta.get("prefiltered"), meta.get("verified"), meta.get("minute_success"),
        meta.get("minute_failed"), meta.get("cache_hits"), meta.get("elapsed_s"),
        len(rows), status, payload.get("error"),
        json.dumps(params, ensure_ascii=False, sort_keys=True),
    ))
    run_id = cur.lastrowid

    for row in rows:
        conn.execute("""
            INSERT INTO momentum_picks (
                run_id, trade_date, cutoff, pool, index_code,
                code, name, buy_price, buy_pct, score, amount_yi,
                volume_ratio, volume_full_ratio, close_position,
                pullback_pct, high_time, reasons, row_json,
                created_at, updated_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now','localtime'),datetime('now','localtime'))
            ON CONFLICT(trade_date, cutoff, pool, index_code, code) DO UPDATE SET
                run_id = excluded.run_id,
                name = excluded.name,
                buy_price = excluded.buy_price,
                buy_pct = excluded.buy_pct,
                score = excluded.score,
                amount_yi = excluded.amount_yi,
                volume_ratio = excluded.volume_ratio,
                volume_full_ratio = excluded.volume_full_ratio,
                close_position = excluded.close_position,
                pullback_pct = excluded.pullback_pct,
                high_time = excluded.high_time,
                reasons = excluded.reasons,
                row_json = excluded.row_json,
                updated_at = datetime('now','localtime')
        """, (
            run_id, params["trade_date"], params["cutoff"], params["pool"],
            params["index_code"], row.get("code"), row.get("name"),
            row.get("price"), row.get("pct"), row.get("score"),
            row.get("amount_yi"), row.get("volume_ratio"),
            row.get("volume_full_ratio"), row.get("close_position"),
            row.get("pullback_pct"), row.get("high_time"), row.get("reasons"),
            json.dumps(row, ensure_ascii=False, sort_keys=True),
        ))
    conn.commit()
    return run_id, len(rows)


def latest_pick_trade_date_before(conn, sell_date):
    row = conn.execute("""
        SELECT MAX(trade_date) AS trade_date
        FROM momentum_picks
        WHERE trade_date < ?
    """, (sell_date,)).fetchone()
    return row["trade_date"] if row and row["trade_date"] else None


def load_momentum_picks_for_settlement(conn, buy_date, sell_date):
    return conn.execute("""
        SELECT p.*, CASE
                   WHEN p.code LIKE '5%' OR p.code LIKE '6%' OR p.code LIKE '9%' THEN '1'
                   ELSE '0'
               END AS market
        FROM momentum_picks p
        LEFT JOIN momentum_pick_returns r ON r.pick_id = p.id
        WHERE p.trade_date = ?
          AND (r.id IS NULL OR r.status != 'sold')
        ORDER BY p.score DESC, p.code
    """, (buy_date,)).fetchall()


def get_daily_open_price(conn, code, trade_date):
    row = conn.execute("""
        SELECT open
        FROM daily_prices
        WHERE code = ?
          AND trade_date = ?
    """, (code, trade_date)).fetchone()
    return row["open"] if row and row["open"] else None


def settle_momentum_picks(conn, sell_date=None, sell_cutoff="10:00", buy_date=None,
                          allow_daily_fallback=False):
    sell_date = sell_date or default_scan_trade_date()
    sell_cutoff = parse_cutoff_time(sell_cutoff).strftime("%H:%M")
    ensure_momentum_tables(conn)
    buy_date = buy_date or latest_pick_trade_date_before(conn, sell_date)
    if not buy_date:
        return {
            "buy_date": None,
            "sell_date": sell_date,
            "sell_cutoff": sell_cutoff,
            "settled": 0,
            "failed": 0,
            "message": "没有找到待结算的前一交易日选股记录",
            "rows": [],
        }

    picks = load_momentum_picks_for_settlement(conn, buy_date, sell_date)
    settled = 0
    failed = 0
    result_rows = []
    for pick in picks:
        stock = {
            "code": pick["code"],
            "name": pick["name"] or "",
            "market": infer_market(pick["code"], pick["market"]),
        }
        buy_price = pick["buy_price"]
        status = "sold"
        error = None
        sell_price = None
        sell_time = None
        return_pct = None

        if not buy_price or buy_price <= 0:
            status = "invalid_buy_price"
            error = "买入价为空"
        else:
            bars = fetch_minute_kline(stock, sell_cutoff, trade_date=sell_date)
            sell_bars = [b for b in bars if b.get("close") and b.get("time") <= sell_cutoff]
            if not sell_bars:
                fallback_open = (
                    get_daily_open_price(conn, pick["code"], sell_date)
                    if allow_daily_fallback else None
                )
                if fallback_open:
                    sell_price = fallback_open
                    sell_time = "09:30*"
                    return_pct = (sell_price - buy_price) / buy_price * 100.0
                    error = "daily_open_fallback"
                else:
                    status = "no_sell_kline"
                    error = "10点前分钟线为空"
            else:
                bar = sell_bars[-1]
                sell_price = bar["close"]
                sell_time = bar["time"]
                return_pct = (sell_price - buy_price) / buy_price * 100.0

        if status == "sold":
            settled += 1
        else:
            failed += 1

        conn.execute("""
            INSERT INTO momentum_pick_returns (
                pick_id, buy_date, sell_date, code, name,
                buy_price, sell_price, return_pct,
                sell_cutoff, sell_time, status, error,
                created_at, updated_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,datetime('now','localtime'),datetime('now','localtime'))
            ON CONFLICT(pick_id) DO UPDATE SET
                sell_date = excluded.sell_date,
                buy_price = excluded.buy_price,
                sell_price = excluded.sell_price,
                return_pct = excluded.return_pct,
                sell_cutoff = excluded.sell_cutoff,
                sell_time = excluded.sell_time,
                status = excluded.status,
                error = excluded.error,
                updated_at = datetime('now','localtime')
        """, (
            pick["id"], buy_date, sell_date, pick["code"], pick["name"],
            buy_price, sell_price,
            round(return_pct, 4) if return_pct is not None else None,
            sell_cutoff, sell_time, status, error,
        ))
        result_rows.append({
            "code": pick["code"],
            "name": pick["name"],
            "buy_price": buy_price,
            "sell_price": sell_price,
            "return_pct": round(return_pct, 4) if return_pct is not None else None,
            "sell_time": sell_time,
            "status": status,
            "error": error,
        })

    conn.commit()
    sold_returns = [r["return_pct"] for r in result_rows if r["return_pct"] is not None]
    avg_return = sum(sold_returns) / len(sold_returns) if sold_returns else None
    return {
        "buy_date": buy_date,
        "sell_date": sell_date,
        "sell_cutoff": sell_cutoff,
        "settled": settled,
        "failed": failed,
        "avg_return_pct": round(avg_return, 4) if avg_return is not None else None,
        "rows": result_rows,
    }


def run_momentum_daily_job(params, sell_date=None, sell_cutoff="10:00",
                           settle_buy_date=None):
    conn = get_db()
    try:
        settlement = settle_momentum_picks(
            conn,
            sell_date=sell_date or params["trade_date"],
            sell_cutoff=sell_cutoff,
            buy_date=settle_buy_date,
        )
    finally:
        conn.close()

    payload, status_code = perform_momentum_scan(params, started_at=time.time())

    conn = get_db()
    try:
        run_id, saved = save_momentum_scan_result(conn, params, payload, status_code)
    finally:
        conn.close()

    return {
        "scan_status": status_code,
        "run_id": run_id,
        "saved": saved,
        "scan": payload,
        "settlement": settlement,
    }


def get_backfill_trade_dates(conn, start_date=None, end_date=None, days=30):
    if end_date is None:
        row = conn.execute("SELECT MAX(trade_date) FROM daily_prices").fetchone()
        end_date = row[0] if row and row[0] else default_scan_trade_date()
    if start_date is None:
        start_dt = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=days)
        start_date = start_dt.strftime("%Y-%m-%d")
    rows = conn.execute("""
        SELECT DISTINCT trade_date
        FROM daily_prices
        WHERE trade_date >= ?
          AND trade_date <= ?
        ORDER BY trade_date
    """, (start_date, end_date)).fetchall()
    return [r["trade_date"] for r in rows]


def summarize_backfill_returns(conn, start_date, end_date):
    row = conn.execute("""
        SELECT COUNT(*) AS n,
               AVG(return_pct) AS avg_return,
               SUM(CASE WHEN return_pct > 0 THEN 1 ELSE 0 END) AS win_count,
               MIN(return_pct) AS min_return,
               MAX(return_pct) AS max_return
        FROM momentum_pick_returns
        WHERE buy_date >= ?
          AND buy_date <= ?
          AND status = 'sold'
    """, (start_date, end_date)).fetchone()
    n = row["n"] if row else 0
    return {
        "count": n,
        "avg_return_pct": round(row["avg_return"], 4) if row and row["avg_return"] is not None else None,
        "win_rate_pct": round(row["win_count"] / n * 100.0, 2) if n else None,
        "min_return_pct": round(row["min_return"], 4) if row and row["min_return"] is not None else None,
        "max_return_pct": round(row["max_return"], 4) if row and row["max_return"] is not None else None,
    }


def exact_return_clause():
    return """
        AND status = 'sold'
        AND COALESCE(error, '') != 'daily_open_fallback'
        AND sell_time = '10:00'
    """


def load_momentum_profit_summary(conn, days=30, exact_only=True):
    ensure_momentum_tables(conn)
    exact_filter = exact_return_clause() if exact_only else ""
    row = conn.execute("""
        SELECT MAX(buy_date) AS end_date
        FROM momentum_pick_returns
        WHERE 1 = 1
        """ + exact_filter + """
    """).fetchone()
    end_date = row["end_date"] if row and row["end_date"] else None
    if not end_date:
        return {
            "start_date": None,
            "end_date": None,
            "days": days,
            "summary": {
                "sold_count": 0,
                "failed_count": 0,
                "avg_return_pct": None,
                "win_rate_pct": None,
                "min_return_pct": None,
                "max_return_pct": None,
            },
            "by_date": [],
            "recent": [],
            "exact_only": exact_only,
        }

    start_dt = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=days)
    start_date = start_dt.strftime("%Y-%m-%d")
    summary_row = conn.execute("""
        SELECT
            SUM(CASE WHEN status = 'sold' THEN 1 ELSE 0 END) AS sold_count,
            SUM(CASE WHEN status != 'sold' THEN 1 ELSE 0 END) AS failed_count,
            AVG(CASE WHEN status = 'sold' THEN return_pct END) AS avg_return,
            SUM(CASE WHEN status = 'sold' AND return_pct > 0 THEN 1 ELSE 0 END) AS win_count,
            MIN(CASE WHEN status = 'sold' THEN return_pct END) AS min_return,
            MAX(CASE WHEN status = 'sold' THEN return_pct END) AS max_return
        FROM momentum_pick_returns
        WHERE buy_date >= ?
          AND buy_date <= ?
        """ + exact_filter + """
    """, (start_date, end_date)).fetchone()
    sold_count = summary_row["sold_count"] or 0
    failed_count = summary_row["failed_count"] or 0
    summary = {
        "sold_count": sold_count,
        "failed_count": failed_count,
        "avg_return_pct": (
            round(summary_row["avg_return"], 4)
            if summary_row["avg_return"] is not None else None
        ),
        "win_rate_pct": (
            round((summary_row["win_count"] or 0) / sold_count * 100.0, 2)
            if sold_count else None
        ),
        "min_return_pct": (
            round(summary_row["min_return"], 4)
            if summary_row["min_return"] is not None else None
        ),
        "max_return_pct": (
            round(summary_row["max_return"], 4)
            if summary_row["max_return"] is not None else None
        ),
    }

    by_date = []
    for row in conn.execute("""
        SELECT buy_date,
               COUNT(*) AS total_count,
               SUM(CASE WHEN status = 'sold' THEN 1 ELSE 0 END) AS sold_count,
               SUM(CASE WHEN status != 'sold' THEN 1 ELSE 0 END) AS failed_count,
               AVG(CASE WHEN status = 'sold' THEN return_pct END) AS avg_return,
               SUM(CASE WHEN status = 'sold' AND return_pct > 0 THEN 1 ELSE 0 END) AS win_count
        FROM momentum_pick_returns
        WHERE buy_date >= ?
          AND buy_date <= ?
          """ + exact_filter + """
        GROUP BY buy_date
        ORDER BY buy_date DESC
    """, (start_date, end_date)).fetchall():
        day_sold = row["sold_count"] or 0
        by_date.append({
            "buy_date": row["buy_date"],
            "total_count": row["total_count"] or 0,
            "sold_count": day_sold,
            "failed_count": row["failed_count"] or 0,
            "avg_return_pct": (
                round(row["avg_return"], 4)
                if row["avg_return"] is not None else None
            ),
            "win_rate_pct": (
                round((row["win_count"] or 0) / day_sold * 100.0, 2)
                if day_sold else None
            ),
        })

    recent = []
    for row in conn.execute("""
        SELECT buy_date, sell_date, code, name,
               buy_price, sell_price, return_pct,
               sell_time, status, error
        FROM momentum_pick_returns
        WHERE buy_date >= ?
          AND buy_date <= ?
          """ + exact_filter + """
        ORDER BY buy_date DESC, return_pct DESC
        LIMIT 12
    """, (start_date, end_date)).fetchall():
        recent.append({
            "buy_date": row["buy_date"],
            "sell_date": row["sell_date"],
            "code": row["code"],
            "name": row["name"],
            "buy_price": row["buy_price"],
            "sell_price": row["sell_price"],
            "return_pct": row["return_pct"],
            "sell_time": row["sell_time"],
            "status": row["status"],
            "error": row["error"],
        })

    return {
        "start_date": start_date,
        "end_date": end_date,
        "days": days,
        "summary": summary,
        "by_date": by_date,
        "recent": recent,
        "exact_only": exact_only,
    }


def run_momentum_backfill(params, start_date=None, end_date=None, days=30,
                          sell_cutoff="10:00", progress=None,
                          use_daily_fallback=True,
                          daily_fallback_only=False):
    conn = get_db()
    try:
        ensure_momentum_tables(conn)
        trade_dates = get_backfill_trade_dates(
            conn,
            start_date=start_date,
            end_date=end_date,
            days=days,
        )
        fast_stocks = None
        fast_histories = None
        if daily_fallback_only and trade_dates:
            fast_stocks = load_stock_universe(
                conn,
                pool=params["pool"],
                index_code=params["index_code"],
            )
            fast_histories = load_daily_history_for_backfill(
                conn,
                [s["code"] for s in fast_stocks],
                trade_dates[0],
                trade_dates[-1],
            )
    finally:
        conn.close()

    results = []
    for i, trade_date in enumerate(trade_dates):
        day_params = dict(params)
        day_params["trade_date"] = trade_date
        next_trade_date = trade_dates[i + 1] if i + 1 < len(trade_dates) else None

        if daily_fallback_only:
            payload, status_code = build_daily_fallback_payload_from_history(
                day_params,
                fast_stocks or [],
                fast_histories or {},
                trade_date,
                started_at=time.time(),
            )
            fallback_used = True
        else:
            payload, status_code = perform_historical_momentum_scan(
                day_params,
                started_at=time.time(),
            )
            fallback_used = False
            if use_daily_fallback and (
                status_code != 200
                or not payload.get("rows")
                or (payload.get("meta") or {}).get("minute_success", 0) == 0
            ):
                payload, status_code = perform_daily_fallback_momentum_scan(
                    day_params,
                    started_at=time.time(),
                )
                fallback_used = True
        conn = get_db()
        try:
            run_id, saved = save_momentum_scan_result(
                conn, day_params, payload, status_code
            )
            settlement = None
            if next_trade_date:
                settlement = settle_momentum_picks(
                    conn,
                    sell_date=next_trade_date,
                    sell_cutoff=sell_cutoff,
                    buy_date=trade_date,
                    allow_daily_fallback=(use_daily_fallback or fallback_used),
                )
        finally:
            conn.close()

        item = {
            "trade_date": trade_date,
            "sell_date": next_trade_date,
            "status": status_code,
            "run_id": run_id,
            "saved": saved,
            "picked": len(payload.get("rows") or []),
            "error": payload.get("error"),
            "meta": payload.get("meta") or {},
            "fallback_used": fallback_used,
            "settlement": settlement,
        }
        results.append(item)
        if progress:
            progress(item, i + 1, len(trade_dates))

    summary = {}
    if trade_dates:
        conn = get_db()
        try:
            summary = summarize_backfill_returns(conn, trade_dates[0], trade_dates[-1])
        finally:
            conn.close()

    return {
        "start_date": trade_dates[0] if trade_dates else start_date,
        "end_date": trade_dates[-1] if trade_dates else end_date,
        "trade_dates": trade_dates,
        "days": len(trade_dates),
        "summary": summary,
        "results": results,
    }


@app.route("/api/stats")
def api_stats():
    days = int(request.args.get("days", 5))
    days = min(max(days, 1), 250)

    conn = get_db()
    cur  = conn.cursor()

    # 取最近 N 个交易日
    cur.execute("""
        SELECT DISTINCT trade_date
        FROM index_daily_stats
        ORDER BY trade_date DESC
        LIMIT ?
    """, (days,))
    dates     = [r["trade_date"] for r in cur.fetchall()]
    dates_asc = list(reversed(dates))

    # 取所有指数
    cur.execute("SELECT code, name FROM indices ORDER BY code")
    indices = cur.fetchall()

    result = []
    for idx in indices:
        code = idx["code"]

        if not dates_asc:
            result.append({"code": code, "name": idx["name"],
                           "ma3": {}, "details": {}})
            continue

        # 多取2天历史数据用于计算MA3（最早显示日期往前2天）
        cur.execute("""
            SELECT trade_date, net_value, high_count, low_count, valid_count, total_count
            FROM index_daily_stats
            WHERE index_code = ?
              AND trade_date <= ?
              AND trade_date >= (
                  SELECT trade_date FROM index_daily_stats
                  WHERE index_code = ?
                    AND trade_date <= ?
                  ORDER BY trade_date ASC
                  LIMIT 1 OFFSET 0
              )
            ORDER BY trade_date ASC
        """, (code, dates_asc[-1], code, dates_asc[0]))

        # 用 Python 算滑动均值，不依赖窗口函数
        all_rows = cur.fetchall()

        # 先建完整的 net_value 时间序列
        nv_series = {r["trade_date"]: r["net_value"] for r in all_rows}
        detail_map = {r["trade_date"]: r for r in all_rows}

        # 取所有日期排序（包括比显示窗口更早的）
        all_dates_sorted = sorted(nv_series.keys())

        # 计算每个日期的 MA3
        ma3_map = {}
        for i, td in enumerate(all_dates_sorted):
            window = [nv_series[all_dates_sorted[j]]
                      for j in range(max(0, i-2), i+1)
                      if nv_series.get(all_dates_sorted[j]) is not None]
            ma3_map[td] = round(sum(window) / len(window), 6) if window else None

        # 只输出用户要看的日期
        ma3    = {}
        details= {}
        for td in dates_asc:
            if td in ma3_map:
                ma3[td] = ma3_map[td]
            if td in detail_map:
                r = detail_map[td]
                details[td] = {
                    "net_value":   r["net_value"],
                    "ma3":         ma3_map.get(td),
                    "high_count":  r["high_count"],
                    "low_count":   r["low_count"],
                    "valid_count": r["valid_count"],
                    "total_count": r["total_count"],
                }

        result.append({
            "code":    code,
            "name":    idx["name"],
            "ma3":     ma3,
            "details": details,
        })

    conn.close()
    return jsonify({"dates": dates_asc, "indices": result})


def build_cli_momentum_params(args):
    return build_momentum_params({
        "pool": args.pool,
        "index_code": args.index_code,
        "cutoff": args.cutoff,
        "trade_date": args.trade_date,
        "min_gain": args.min_gain,
        "max_gain": args.max_gain,
        "min_vol_ratio": args.min_vol_ratio,
        "min_amount": args.min_amount,
        "limit": args.limit,
        "verify_limit": args.verify_limit,
        "workers": args.workers,
    })


def print_settlement_summary(result):
    print(
        f"收益结算: buy_date={result.get('buy_date') or '-'} "
        f"sell_date={result['sell_date']} cutoff={result['sell_cutoff']} "
        f"sold={result['settled']} failed={result['failed']} "
        f"avg={result.get('avg_return_pct') if result.get('avg_return_pct') is not None else '-'}%"
    )
    for row in result.get("rows", [])[:20]:
        ret = row["return_pct"] if row["return_pct"] is not None else "-"
        print(
            f"  {row['code']} {row.get('name') or ''} "
            f"buy={row.get('buy_price') or '-'} sell={row.get('sell_price') or '-'} "
            f"ret={ret}% status={row['status']}"
        )
    if len(result.get("rows", [])) > 20:
        print(f"  ... 还有 {len(result['rows']) - 20} 条")
    if result.get("message"):
        print(result["message"])


def print_scan_summary(payload, status_code, run_id=None, saved=None):
    meta = payload.get("meta") or {}
    print(
        f"扫描保存: status={status_code} run_id={run_id or '-'} "
        f"trade_date={meta.get('trade_date')} cutoff={meta.get('cutoff')} "
        f"quoted={meta.get('quoted', 0)} prefiltered={meta.get('prefiltered', 0)} "
        f"verified={meta.get('verified', 0)} picked={len(payload.get('rows') or [])} "
        f"saved={saved if saved is not None else '-'} elapsed={meta.get('elapsed_s', 0)}s"
    )
    if payload.get("error"):
        print(f"错误: {payload['error']}")
    for row in (payload.get("rows") or [])[:20]:
        print(
            f"  {row['code']} {row.get('name') or ''} "
            f"price={row.get('price')} pct={row.get('pct')}% "
            f"score={row.get('score')} reasons={row.get('reasons') or ''}"
        )
    if len(payload.get("rows") or []) > 20:
        print(f"  ... 还有 {len(payload['rows']) - 20} 条")


def print_recent_returns(limit=30):
    conn = get_db()
    try:
        ensure_momentum_tables(conn)
        rows = conn.execute("""
            SELECT r.buy_date, r.sell_date, r.code, r.name,
                   r.buy_price, r.sell_price, r.return_pct,
                   r.sell_time, r.status, r.error
            FROM momentum_pick_returns r
            ORDER BY r.sell_date DESC, r.return_pct DESC
            LIMIT ?
        """, (limit,)).fetchall()
    finally:
        conn.close()
    if not rows:
        print("暂无收益记录")
        return
    for row in rows:
        ret = row["return_pct"] if row["return_pct"] is not None else "-"
        print(
            f"{row['buy_date']} -> {row['sell_date']} "
            f"{row['code']} {row['name'] or ''} "
            f"buy={row['buy_price'] or '-'} sell={row['sell_price'] or '-'} "
            f"time={row['sell_time'] or '-'} ret={ret}% "
            f"status={row['status']} {row['error'] or ''}"
        )


def print_backfill_progress(item, index, total):
    settlement = item.get("settlement") or {}
    avg_return = settlement.get("avg_return_pct")
    avg_text = f"{avg_return}%" if avg_return is not None else "-"
    fallback = " fallback=daily" if item.get("fallback_used") else ""
    print(
        f"[{index}/{total}] {item['trade_date']} "
        f"picked={item['picked']} saved={item['saved']} "
        f"sold={settlement.get('settled', 0)} failed={settlement.get('failed', 0)} "
        f"avg={avg_text} elapsed={item.get('meta', {}).get('elapsed_s', 0)}s"
        f"{fallback}"
    )
    if item.get("error"):
        print(f"  error: {item['error']}")


def print_backfill_summary(result):
    summary = result.get("summary") or {}
    print(
        f"回填完成: {result.get('start_date')} -> {result.get('end_date')} "
        f"交易日={result.get('days', 0)} "
        f"成交记录={summary.get('count', 0)} "
        f"平均收益={summary.get('avg_return_pct') if summary.get('avg_return_pct') is not None else '-'}% "
        f"胜率={summary.get('win_rate_pct') if summary.get('win_rate_pct') is not None else '-'}% "
        f"最差={summary.get('min_return_pct') if summary.get('min_return_pct') is not None else '-'}% "
        f"最好={summary.get('max_return_pct') if summary.get('max_return_pct') is not None else '-'}%"
    )


def parse_cli_args():
    parser = argparse.ArgumentParser(description="行业宽度与14:30动量选股服务")
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--serve", action="store_true", help="启动 Web 服务")
    actions.add_argument("--momentum-daily", action="store_true",
                         help="结算前一交易日选股收益，并扫描保存今日14:30选股")
    actions.add_argument("--momentum-scan-save", action="store_true",
                         help="只扫描并保存选股")
    actions.add_argument("--momentum-settle", action="store_true",
                         help="只结算前一交易日选股收益")
    actions.add_argument("--momentum-report", action="store_true",
                         help="查看最近收益记录")
    actions.add_argument("--momentum-backfill", action="store_true",
                         help="回填历史14:30选股并按下一交易日10:00前卖出统计收益")

    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--pool", default="all", choices=["all", "sector", "index"])
    parser.add_argument("--index-code", default="")
    parser.add_argument("--cutoff", default="14:30")
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--sell-date", default=None)
    parser.add_argument("--sell-cutoff", default="10:00")
    parser.add_argument("--settle-buy-date", default=None)
    parser.add_argument("--min-gain", type=float, default=2.0)
    parser.add_argument("--max-gain", type=float, default=7.5)
    parser.add_argument("--min-vol-ratio", type=float, default=1.5)
    parser.add_argument("--min-amount", type=float, default=8000,
                        help="最低成交额，单位万元")
    parser.add_argument("--limit", type=int, default=80)
    parser.add_argument("--verify-limit", type=int, default=50)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--report-limit", type=int, default=30)
    parser.add_argument("--backfill-days", type=int, default=30)
    parser.add_argument("--backfill-start", default=None)
    parser.add_argument("--backfill-end", default=None)
    parser.add_argument("--no-daily-fallback", action="store_true",
                        help="历史分钟线不可用时不使用日线近似回退")
    parser.add_argument("--daily-fallback-only", action="store_true",
                        help="历史回填直接使用日线近似口径，不尝试分钟线")
    return parser.parse_args()


def main():
    args = parse_cli_args()
    if len(sys.argv) == 1 or args.serve:
        app.run(debug=args.debug, host=args.host, port=args.port)
        return

    params = build_cli_momentum_params(args)
    if args.momentum_daily:
        result = run_momentum_daily_job(
            params,
            sell_date=args.sell_date or params["trade_date"],
            sell_cutoff=args.sell_cutoff,
            settle_buy_date=args.settle_buy_date,
        )
        print_settlement_summary(result["settlement"])
        print_scan_summary(
            result["scan"],
            result["scan_status"],
            run_id=result["run_id"],
            saved=result["saved"],
        )
    elif args.momentum_scan_save:
        payload, status_code = perform_momentum_scan(params, started_at=time.time())
        conn = get_db()
        try:
            run_id, saved = save_momentum_scan_result(conn, params, payload, status_code)
        finally:
            conn.close()
        print_scan_summary(payload, status_code, run_id=run_id, saved=saved)
    elif args.momentum_settle:
        conn = get_db()
        try:
            result = settle_momentum_picks(
                conn,
                sell_date=args.sell_date or params["trade_date"],
                sell_cutoff=args.sell_cutoff,
                buy_date=args.settle_buy_date,
            )
        finally:
            conn.close()
        print_settlement_summary(result)
    elif args.momentum_report:
        print_recent_returns(args.report_limit)
    elif args.momentum_backfill:
        result = run_momentum_backfill(
            params,
            start_date=args.backfill_start,
            end_date=args.backfill_end,
            days=args.backfill_days,
            sell_cutoff=args.sell_cutoff,
            progress=print_backfill_progress,
            use_daily_fallback=not args.no_daily_fallback,
            daily_fallback_only=args.daily_fallback_only,
        )
        print_backfill_summary(result)


# ─────────────────────────────────────────────────────────────────
# 启动
# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
