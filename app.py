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
import json
import math
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

    # baostock 很慢，只兜底少量失败项，避免页面长时间卡住。
    for stock in still_missing[:5]:
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

    bars = fetch_eastmoney_5m_kline(stock, cutoff_text)
    if bars:
        set_cached_kline(stock["code"], cutoff_text, bars,
                         source="eastmoney", trade_date=scan_date)
        return bars

    bars = fetch_baostock_5m_kline_uncached(stock, cutoff_text, scan_date)
    if bars:
        set_cached_kline(stock["code"], cutoff_text, bars,
                         source="baostock", trade_date=scan_date)
        return bars

    secid = f"{infer_market(stock['code'], stock.get('market'))}.{stock['code']}"
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

    today = scan_date.replace("-", "")
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
    try:
        klines = (get_json_with_retry(
            EASTMONEY_KLINE_URL,
            params,
            headers,
            timeout=3,
            retries=1,
        ).get("data") or {}).get("klines") or []
    except Exception:
        return []

    bars = []
    for item in klines:
        parts = item.split(",")
        if len(parts) < 7:
            continue
        dt_text = parts[0]
        hhmm = dt_text[-5:]
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

<script>
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
    html += `<tr><td class="name-cell">${idx.name}<span class="idx-code">${idx.code}</span></td>`;
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
        <div class="val-inner ${cls}">
          <span class="val-number">${pct}</span>
          <div class="val-bar"><div class="val-bar-fill" style="width:${barW}%"></div></div>
        </div></td>`;
    });
    html += '</tr>';
  });

  html += '</tbody></table>';
  document.getElementById('tableWrap').innerHTML = html;
}

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
  }
  @media (max-width: 640px) {
    body { padding:20px 14px; }
    .header { align-items:flex-start; flex-direction:column; }
    .toolbar { grid-template-columns: repeat(2, minmax(92px, 1fr)); }
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

<div class="table-wrap" id="tableWrap">
  <div class="loading">等待扫描</div>
</div>

<script>
const fmt = (value, digits=2) => value === null || value === undefined ? '—' : Number(value).toFixed(digits);
const esc = value => String(value ?? '').replace(/[&<>"']/g, ch => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
}[ch]));

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
        return run_momentum_scan(started_at)
    finally:
        SCAN_LOCK.release()


def run_momentum_scan(started_at):
    pool = request.args.get("pool", "all")
    index_code = request.args.get("indexCode", "")
    if pool == "index" and not index_code:
        pool = "all"
    cutoff_text = request.args.get("cutoff", "14:30")
    cutoff = parse_cutoff_time(cutoff_text)
    cutoff_text = cutoff.strftime("%H:%M")
    scan_trade_date = request.args.get("tradeDate") or default_scan_trade_date()

    min_gain = to_float_arg("minGain", 2.0, -5, 15)
    max_gain = to_float_arg("maxGain", 7.5, min_gain, 20)
    min_vol_ratio = to_float_arg("minVolRatio", 1.5, 0.2, 10)
    min_amount_yuan = to_float_arg("minAmount", 8000, 0, 1000000) * 10000
    limit = to_int_arg("limit", 80, 1, 300)
    verify_limit = to_int_arg("verifyLimit", 50, 1, 1000)
    max_workers = to_int_arg("workers", 6, 2, 12)
    elapsed_ratio = trade_elapsed_ratio(cutoff)

    conn = get_db()
    try:
        stocks = load_stock_universe(conn, pool=pool, index_code=index_code)
        if not stocks:
            return jsonify({"error": "股票池为空"}), 400

        stock_by_code = {s["code"]: s for s in stocks}
        codes = list(stock_by_code.keys())
        quotes = fetch_realtime_quotes(codes)
        if not quotes:
            return jsonify({"error": "实时行情获取失败：新浪和东方财富均无有效返回"}), 502

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
            "universe": len(stocks),
            "quoted": len(quotes),
            "prefiltered": len(prefiltered),
            "verified": len(probe_items),
            "minute_success": minute_success,
            "minute_failed": minute_failed,
            "cache_hits": cache_hits,
            "elapsed_s": round(time.time() - started_at, 1),
        }
        return jsonify({
            "error": "分钟线接口暂不可用，候选股无法做14:30分时验证",
            "meta": meta,
            "rows": [],
        }), 503

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
        return jsonify({
            "error": "分钟线接口暂不可用，候选股无法做14:30分时验证",
            "meta": meta,
            "rows": [],
        }), 503
    return jsonify({"meta": meta, "rows": rows})


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


# ─────────────────────────────────────────────────────────────────
# 启动
# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
