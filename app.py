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
import os
import sys
import threading
import time
import sqlite3
import requests
from werkzeug.exceptions import HTTPException

try:
    import baostock as bs
except ImportError:
    bs = None

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = (
    os.environ.get("STOCK_TRADER_DB_PATH")
    or os.environ.get("STOCK_DB_PATH")
    or os.path.join(BASE_DIR, "stock_data.db")
)
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
PATTERN_PROGRESS_COLUMNS = [
    "job_key", "job_type", "status", "started_at", "updated_at", "trade_date",
    "current_index", "total", "picked", "matched_rows", "matched_days",
    "elapsed_s", "message", "params_json", "result_json", "error",
]


def get_db():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"数据库文件不存在: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.errorhandler(Exception)
def handle_error(error):
    if isinstance(error, HTTPException):
        status_code = error.code or 500
        message = error.description
    else:
        status_code = 500
        message = str(error) or error.__class__.__name__
        app.logger.exception("Unhandled exception while handling %s", request.path)

    if request.path.startswith("/api/"):
        return jsonify({
            "error": message,
            "type": error.__class__.__name__,
            "status": status_code,
        }), status_code

    if isinstance(error, HTTPException):
        return error
    return render_template_string(
        "<h1>Internal Server Error</h1><p>{{ message }}</p>",
        message=message,
    ), status_code


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


def ensure_pattern_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pattern_scan_runs (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date           DATE NOT NULL,
            pool                 TEXT NOT NULL,
            index_code           TEXT,
            lookback_days        INTEGER,
            chart_bars           INTEGER,
            min_amount_wan       REAL,
            min_turnover         REAL,
            max_body_pct         REAL,
            max_body_range_pct   REAL,
            max_amp_pct          REAL,
            doji_body_pct        REAL,
            max_ma40_distance    REAL,
            universe             INTEGER,
            scanned              INTEGER,
            row_count            INTEGER,
            elapsed_s            REAL,
            status               TEXT,
            error                TEXT,
            params_json          TEXT,
            created_at           DATETIME DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pattern_picks (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id         INTEGER NOT NULL,
            trade_date     DATE NOT NULL,
            code           TEXT NOT NULL,
            name           TEXT,
            close_price    REAL,
            pct_change     REAL,
            amount_yi      REAL,
            turnover       REAL,
            score          REAL,
            reasons        TEXT,
            row_json       TEXT,
            bars_json      TEXT,
            created_at     DATETIME DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pattern_scan_progress (
            job_key       TEXT PRIMARY KEY,
            job_type      TEXT,
            status        TEXT,
            started_at    DATETIME,
            updated_at    DATETIME,
            trade_date    DATE,
            current_index INTEGER,
            total         INTEGER,
            picked        INTEGER,
            matched_rows  INTEGER,
            matched_days  INTEGER,
            elapsed_s     REAL,
            message       TEXT,
            params_json   TEXT,
            result_json   TEXT,
            error         TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pattern_runs_date ON pattern_scan_runs(trade_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pattern_picks_run ON pattern_picks(run_id)")
    conn.commit()


def local_now_text():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def save_pattern_progress(job_key="pattern_backfill", **updates):
    now = local_now_text()
    conn = get_db()
    try:
        ensure_pattern_tables(conn)
        row = conn.execute(
            "SELECT * FROM pattern_scan_progress WHERE job_key = ?",
            (job_key,),
        ).fetchone()
        data = {column: None for column in PATTERN_PROGRESS_COLUMNS}
        data["job_key"] = job_key
        if row:
            data.update(dict(row))
        else:
            data["started_at"] = now
        for key, value in updates.items():
            if key in data:
                data[key] = value
        data["updated_at"] = now
        if updates.get("status") in ("running", "queued") and not updates.get("started_at") and not row:
            data["started_at"] = now
        placeholders = ",".join("?" for _ in PATTERN_PROGRESS_COLUMNS)
        columns = ",".join(PATTERN_PROGRESS_COLUMNS)
        conn.execute(
            f"REPLACE INTO pattern_scan_progress ({columns}) VALUES ({placeholders})",
            [data[column] for column in PATTERN_PROGRESS_COLUMNS],
        )
        conn.commit()
    finally:
        conn.close()


def load_pattern_progress(job_key="pattern_backfill"):
    conn = get_db()
    try:
        ensure_pattern_tables(conn)
        row = conn.execute(
            "SELECT * FROM pattern_scan_progress WHERE job_key = ?",
            (job_key,),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return {"job_key": job_key, "status": "idle"}
    data = dict(row)
    for key in ("params_json", "result_json"):
        raw = data.pop(key, None)
        plain_key = key[:-5]
        if raw:
            try:
                data[plain_key] = json.loads(raw)
            except (TypeError, ValueError):
                data[plain_key] = raw
        else:
            data[plain_key] = None
    return data


def ensure_daily_price_indexes(conn):
    table = conn.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name = 'daily_prices'
    """).fetchone()
    if not table:
        return
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dp_date ON daily_prices(trade_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dp_code_date ON daily_prices(code, trade_date)")
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


def normalize_trade_date(value, default=None):
    if value in (None, ""):
        return default
    text = str(value).strip()
    if not text:
        return default
    for fmt in ("%Y-%m-%d", "%Y-%m-%e", "%Y/%m/%d", "%Y/%m/%e"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    parts = text.replace("/", "-").split("-")
    if len(parts) == 3:
        try:
            year, month, day = (int(part) for part in parts)
            return datetime(year, month, day).strftime("%Y-%m-%d")
        except (TypeError, ValueError):
            pass
    return text


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
        "trade_date": normalize_trade_date(
            get_source_value(source, "tradeDate", "trade_date"),
            default_scan_trade_date(),
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


def build_pattern_params(source=None):
    source = source or {}
    pool = source.get("pool", "all")
    index_code = get_source_value(source, "indexCode", "index_code", default="") or ""
    if pool == "index" and not index_code:
        pool = "all"
    pattern_type = get_source_value(
        source, "patternType", "pattern_type", default="bottom_reversal"
    )
    if pattern_type not in ("four_pin", "bottom_reversal"):
        pattern_type = "bottom_reversal"
    return {
        "pattern_type": pattern_type,
        "pool": pool,
        "index_code": index_code,
        "trade_date": normalize_trade_date(
            get_source_value(source, "tradeDate", "trade_date"),
            None,
        ),
        "lookback_days": coerce_int(
            get_source_value(source, "lookbackDays", "lookback_days"),
            120, 60, 260,
        ),
        "chart_bars": coerce_int(
            get_source_value(source, "chartBars", "chart_bars"),
            70, 40, 140,
        ),
        "min_amount_wan": coerce_float(
            get_source_value(source, "minAmount", "min_amount"),
            0, 0, 1000000,
        ),
        "min_turnover": coerce_float(
            get_source_value(source, "minTurnover", "min_turnover"),
            0, 0, 100,
        ),
        "min_market_cap_yi": coerce_float(
            get_source_value(source, "minMarketCapYi", "min_market_cap_yi"),
            100, 0, 100000,
        ),
        "max_body_pct": coerce_float(
            get_source_value(source, "maxBodyPct", "max_body_pct"),
            1.05, 0.1, 10,
        ),
        "max_body_range_pct": coerce_float(
            get_source_value(source, "maxBodyRangePct", "max_body_range_pct"),
            35, 5, 95,
        ),
        "max_amp_pct": coerce_float(
            get_source_value(source, "maxAmpPct", "max_amp_pct"),
            6.0, 0.5, 20,
        ),
        "doji_body_pct": coerce_float(
            get_source_value(source, "dojiBodyPct", "doji_body_pct"),
            1.05, 0.1, 5,
        ),
        "max_ma40_distance": coerce_float(
            get_source_value(source, "maxMa40Distance", "max_ma40_distance"),
            0.0, 0, 50,
        ),
        "max_pair_distance": coerce_float(
            get_source_value(source, "maxPairDistance", "max_pair_distance"),
            0.5, 0.1, 10,
        ),
        "max_close_pair_distance": coerce_float(
            get_source_value(source, "maxClosePairDistance", "max_close_pair_distance"),
            1.0, 0.1, 10,
        ),
        "min_level_gap": coerce_float(
            get_source_value(source, "minLevelGap", "min_level_gap"),
            0.8, 0.0, 10,
        ),
        "min_shadow_pct": coerce_float(
            get_source_value(source, "minShadowPct", "min_shadow_pct"),
            1.0, 0.0, 50,
        ),
        "max_shadowless_count": coerce_int(
            get_source_value(source, "maxShadowlessCount", "max_shadowless_count"),
            0, 0, 4,
        ),
        "bottom_lookback_days": coerce_int(
            get_source_value(source, "bottomLookbackDays", "bottom_lookback_days"),
            60, 20, 160,
        ),
        "max_bottom_position": coerce_float(
            get_source_value(source, "maxBottomPosition", "max_bottom_position"),
            35, 5, 90,
        ),
        "min_prior_drop_pct": coerce_float(
            get_source_value(source, "minPriorDropPct", "min_prior_drop_pct"),
            10.0, 0, 60,
        ),
        "bottom_max_body_pct": coerce_float(
            get_source_value(source, "bottomMaxBodyPct", "bottom_max_body_pct"),
            3.0, 0.2, 12,
        ),
        "min_bottom_volume_ratio": coerce_float(
            get_source_value(source, "minBottomVolumeRatio", "min_bottom_volume_ratio"),
            1.2, 0, 10,
        ),
        "min_bottom_rebound_pct": coerce_float(
            get_source_value(source, "minBottomReboundPct", "min_bottom_rebound_pct"),
            2.0, 0, 30,
        ),
        "min_bottom_pct_change": coerce_float(
            get_source_value(source, "minBottomPctChange", "min_bottom_pct_change"),
            2.0, -20, 20,
        ),
        "min_bottom_strong_gain_pct": coerce_float(
            get_source_value(source, "minBottomStrongGainPct", "min_bottom_strong_gain_pct"),
            3.0, 0, 20,
        ),
        "require_bottom_confirm": coerce_int(
            get_source_value(source, "requireBottomConfirm", "require_bottom_confirm"),
            1, 0, 1,
        ),
        "min_bottom_close_position": coerce_float(
            get_source_value(source, "minBottomClosePosition", "min_bottom_close_position"),
            55.0, 0, 100,
        ),
        "require_bottom_close_above_prev": coerce_int(
            get_source_value(source, "requireBottomCloseAbovePrev", "require_bottom_close_above_prev"),
            1, 0, 1,
        ),
        "limit": coerce_int(source.get("limit"), 10 if pattern_type == "bottom_reversal" else 80, 1, 300),
    }


def public_pattern_params(params):
    return {k: v for k, v in params.items() if not str(k).startswith("_")}


def saved_pattern_filters_enabled(source):
    value = get_source_value(source, "strict", "filterSaved", "filter_saved")
    return str(value).lower() in ("1", "true", "yes", "on")


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


def fetch_eastmoney_market_caps(codes):
    caps = {}
    if not codes:
        return caps
    fields = "f12,f20,f21"
    for batch in chunked(list(dict.fromkeys(codes)), 80):
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
        for item in diff:
            code = str(item.get("f12") or "")
            total_cap = to_float(item.get("f20"))
            float_cap = to_float(item.get("f21"))
            caps[code] = {
                "market_cap_yi": total_cap / 100000000.0 if total_cap else None,
                "float_market_cap_yi": float_cap / 100000000.0 if float_cap else None,
            }
    return caps


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


def latest_daily_trade_date(conn):
    row = conn.execute("SELECT MAX(trade_date) AS trade_date FROM daily_prices").fetchone()
    return row["trade_date"] if row and row["trade_date"] else default_scan_trade_date()


def recent_market_trade_dates(conn, trade_date, count):
    rows = conn.execute("""
        SELECT DISTINCT trade_date
        FROM daily_prices
        WHERE trade_date <= ?
        ORDER BY trade_date DESC
        LIMIT ?
    """, (trade_date, count)).fetchall()
    return list(reversed([r["trade_date"] for r in rows]))


def candle_metrics(row):
    open_price = row["open"]
    close_price = row["close"]
    high = row["high"]
    low = row["low"]
    if not open_price or not close_price or not high or not low:
        return None
    base = close_price or open_price
    span = max(high - low, 0)
    body = abs(close_price - open_price)
    body_pct = body / base * 100.0 if base else None
    amp_pct = span / base * 100.0 if base else None
    body_range_pct = body / span * 100.0 if span else 0.0
    upper = high - max(open_price, close_price)
    lower = min(open_price, close_price) - low
    upper_pct = upper / span * 100.0 if span else 0.0
    lower_pct = lower / span * 100.0 if span else 0.0
    return {
        "body_pct": body_pct,
        "amp_pct": amp_pct,
        "body_range_pct": body_range_pct,
        "upper_pct": upper_pct,
        "lower_pct": lower_pct,
    }


def is_small_body_candle(row, params):
    metrics = candle_metrics(row)
    if not metrics:
        return False
    return (
        metrics["body_pct"] <= params["max_body_pct"]
        and metrics["body_range_pct"] <= params["max_body_range_pct"]
        and metrics["amp_pct"] <= params["max_amp_pct"]
    )


def is_doji_candle(row, params):
    metrics = candle_metrics(row)
    if not metrics:
        return False
    body_limit = min(params["doji_body_pct"], params["max_body_pct"])
    return (
        metrics["body_pct"] <= body_limit
        and metrics["body_range_pct"] <= params["max_body_range_pct"]
        and metrics["amp_pct"] <= params["max_amp_pct"]
    )


def has_balanced_shadows(row, params):
    metrics = candle_metrics(row)
    if not metrics:
        return False
    min_shadow_pct = params.get("min_shadow_pct", 6.0)
    return (
        metrics["upper_pct"] >= min_shadow_pct
        and metrics["lower_pct"] >= min_shadow_pct
    )


def candle_body_mid(row):
    if not row["open"] or not row["close"]:
        return None
    return (row["open"] + row["close"]) / 2.0


def pct_distance(a, b, base):
    if a is None or b is None or not base:
        return None
    return abs(a - b) / base * 100.0


def four_pin_levels(rows, params):
    mids = [candle_body_mid(row) for row in rows]
    if any(mid is None for mid in mids):
        return None
    base = rows[-1]["close"]
    first_third_gap = pct_distance(mids[0], mids[2], base)
    second_fourth_gap = pct_distance(mids[1], mids[3], base)
    first_third_close_gap = pct_distance(rows[0]["close"], rows[2]["close"], base)
    second_fourth_close_gap = pct_distance(rows[1]["close"], rows[3]["close"], base)
    high_level = (mids[0] + mids[2]) / 2.0
    low_level = (mids[1] + mids[3]) / 2.0
    level_gap = (high_level - low_level) / base * 100.0 if base else None
    if (
        first_third_gap is None
        or second_fourth_gap is None
        or first_third_close_gap is None
        or second_fourth_close_gap is None
        or level_gap is None
    ):
        return None
    if first_third_gap > params["max_pair_distance"]:
        return None
    if second_fourth_gap > params["max_pair_distance"]:
        return None
    if first_third_close_gap > params["max_close_pair_distance"]:
        return None
    if second_fourth_close_gap > params["max_close_pair_distance"]:
        return None
    if level_gap < params["min_level_gap"]:
        return None
    if max(mids[1], mids[3]) >= min(mids[0], mids[2]):
        return None
    return {
        "first_third_gap": first_third_gap,
        "second_fourth_gap": second_fourth_gap,
        "first_third_close_gap": first_third_close_gap,
        "second_fourth_close_gap": second_fourth_close_gap,
        "level_gap": level_gap,
        "high_level": high_level,
        "low_level": low_level,
    }


def build_candlestick_chart(series, highlight=5, width=360, height=172):
    bars = [r for r in series if r["open"] and r["close"] and r["high"] and r["low"]]
    if len(bars) < 2:
        return ""

    pad_l, pad_r, pad_t, pad_b = 34, 8, 10, 22
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    lows = [r["low"] for r in bars]
    highs = [r["high"] for r in bars]
    low, high = min(lows), max(highs)
    span = high - low or 1

    def x_at(i):
        return pad_l + (i + 0.5) / len(bars) * plot_w

    def y_at(price):
        return pad_t + (high - price) / span * plot_h

    ma20 = []
    ma40 = []
    closes = [r["close"] for r in bars]
    for i in range(len(bars)):
        if i >= 19:
            ma20.append((x_at(i), y_at(sum(closes[i - 19:i + 1]) / 20)))
        if i >= 39:
            ma40.append((x_at(i), y_at(sum(closes[i - 39:i + 1]) / 40)))

    def polyline(points, color):
        if len(points) < 2:
            return ""
        pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
        return f'<polyline fill="none" stroke="{color}" stroke-width="1.5" points="{pts}"/>'

    candle_w = clamp(plot_w / len(bars) * 0.58, 2.0, 7.0)
    start_highlight = max(0, len(bars) - highlight)
    parts = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" aria-hidden="true">',
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="transparent"/>',
    ]
    for tick in (0.25, 0.5, 0.75):
        y = pad_t + plot_h * tick
        parts.append(f'<line x1="{pad_l}" y1="{y:.1f}" x2="{width - pad_r}" y2="{y:.1f}" stroke="#252a3a" stroke-width="1"/>')
    parts.append(polyline(ma20, "#d7a53f"))
    parts.append(polyline(ma40, "#5c84e8"))

    for i, row in enumerate(bars):
        open_y = y_at(row["open"])
        close_y = y_at(row["close"])
        high_y = y_at(row["high"])
        low_y = y_at(row["low"])
        x = x_at(i)
        up = row["close"] >= row["open"]
        color = "#ff4d6a" if up else "#00c97a"
        if i >= start_highlight:
            parts.append(
                f'<rect x="{x - candle_w * .82:.1f}" y="{pad_t:.1f}" '
                f'width="{candle_w * 1.64:.1f}" height="{plot_h:.1f}" '
                f'fill="rgba(61,127,255,.10)"/>'
            )
        parts.append(f'<line x1="{x:.1f}" y1="{high_y:.1f}" x2="{x:.1f}" y2="{low_y:.1f}" stroke="{color}" stroke-width="1.2"/>')
        y = min(open_y, close_y)
        h = max(abs(close_y - open_y), 1.4)
        fill = color if not up else "transparent"
        parts.append(
            f'<rect x="{x - candle_w / 2:.1f}" y="{y:.1f}" width="{candle_w:.1f}" '
            f'height="{h:.1f}" fill="{fill}" stroke="{color}" stroke-width="1.2"/>'
        )

    last_date = bars[-1]["trade_date"][5:].replace("-", "/")
    first_date = bars[0]["trade_date"][5:].replace("-", "/")
    parts.append(f'<text x="{pad_l}" y="{height - 6}" fill="#697082" font-size="10">{first_date}</text>')
    parts.append(f'<text x="{width - pad_r}" y="{height - 6}" fill="#697082" font-size="10" text-anchor="end">{last_date}</text>')
    parts.append(f'<text x="{pad_l}" y="9" fill="#d7a53f" font-size="9">MA20</text>')
    parts.append(f'<text x="{pad_l + 34}" y="9" fill="#5c84e8" font-size="9">MA40</text>')
    parts.append("</svg>")
    return "".join(parts)


def load_daily_histories_for_pattern(conn, codes, trade_date, lookback_days):
    histories = {code: [] for code in codes}
    start_dt = datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=lookback_days)
    start_date = start_dt.strftime("%Y-%m-%d")
    for batch in chunked(codes, 600):
        placeholders = ",".join("?" for _ in batch)
        rows = conn.execute(f"""
            SELECT code, trade_date, open, close, high, low, volume, amount,
                   pct_change, turnover
            FROM daily_prices
            WHERE code IN ({placeholders})
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY code, trade_date
        """, batch + [start_date, trade_date]).fetchall()
        for row in rows:
            histories[row["code"]].append(row)
    return histories


def load_daily_histories_for_pattern_range(conn, codes, start_date, end_date,
                                           lookback_days, progress=None):
    histories = {code: [] for code in codes}
    start_dt = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=lookback_days)
    history_start = start_dt.strftime("%Y-%m-%d")
    batches = list(chunked(codes, 600))
    for batch_index, batch in enumerate(batches, 1):
        placeholders = ",".join("?" for _ in batch)
        rows = conn.execute(f"""
            SELECT code, trade_date, open, close, high, low, volume, amount,
                   pct_change, turnover
            FROM daily_prices
            WHERE code IN ({placeholders})
              AND trade_date >= ?
              AND trade_date <= ?
            ORDER BY code, trade_date
        """, batch + [history_start, end_date]).fetchall()
        for row in rows:
            histories[row["code"]].append(row)
        if progress:
            progress(batch_index, len(batches), len(rows))
    return histories


def evaluate_four_pin_candidate(stock, series, params):
    if len(series) < max(45, params["chart_bars"] // 2):
        return None
    if series[-1]["trade_date"] != params["trade_date"]:
        return None
    last_four = series[-4:]
    if len(last_four) < 4:
        return None
    required_dates = params.get("required_pattern_dates") or []
    if required_dates and [row["trade_date"] for row in last_four] != required_dates:
        return None
    if not all(is_doji_candle(row, params) for row in last_four):
        return None
    shadowless_count = sum(
        1 for row in last_four if not has_balanced_shadows(row, params)
    )
    if shadowless_count > params["max_shadowless_count"]:
        return None
    levels = four_pin_levels(last_four, params)
    if not levels:
        return None

    last = last_four[-1]
    if (last["amount"] or 0) < params["min_amount_wan"] * 10000:
        return None
    if params["min_turnover"] and (last["turnover"] is None or last["turnover"] < params["min_turnover"]):
        return None

    closes = [r["close"] for r in series if r["close"] is not None]
    volumes = [r["volume"] for r in series[-20:] if r["volume"] is not None]
    if len(closes) < 40:
        return None
    ma20 = sum(closes[-20:]) / 20
    ma40 = sum(closes[-40:]) / 40
    ma40_distance = abs(last["close"] - ma40) / ma40 * 100.0 if ma40 else None
    if params["max_ma40_distance"] and ma40_distance is not None:
        if ma40_distance > params["max_ma40_distance"]:
            return None

    metrics = [candle_metrics(row) for row in last_four]
    if any(m is None for m in metrics):
        return None
    avg_body = sum(m["body_pct"] for m in metrics) / len(metrics)
    avg_amp = sum(m["amp_pct"] for m in metrics) / len(metrics)
    max_body = max(m["body_pct"] for m in metrics)
    avg_volume20 = sum(volumes) / len(volumes) if volumes else None
    pattern_volume = sum((r["volume"] or 0) for r in last_four) / 4
    volume_ratio = pattern_volume / avg_volume20 if avg_volume20 else None
    high4 = max(r["high"] for r in last_four if r["high"] is not None)
    low4 = min(r["low"] for r in last_four if r["low"] is not None)
    range4_pct = (high4 - low4) / last["close"] * 100.0 if last["close"] else None
    closes20 = closes[-20:]
    position20 = position_in_range(last["close"], min(closes20), max(closes20))
    ma40_score = (
        10
        if not params["max_ma40_distance"]
        else clamp((params["max_ma40_distance"] - (ma40_distance or 0)) / max(params["max_ma40_distance"], 1) * 10, 0, 10)
    )

    score = (
        clamp((params["doji_body_pct"] - max_body) / params["doji_body_pct"] * 22, 0, 22)
        + clamp((params["max_amp_pct"] - avg_amp) / params["max_amp_pct"] * 14, 0, 14)
        + clamp((params["max_pair_distance"] - levels["first_third_gap"]) / params["max_pair_distance"] * 18, 0, 18)
        + clamp((params["max_pair_distance"] - levels["second_fourth_gap"]) / params["max_pair_distance"] * 18, 0, 18)
        + clamp((levels["level_gap"] - params["min_level_gap"]) / 1.8 * 10, 0, 10)
        + ma40_score
        + clamp((1.25 - (volume_ratio or 1.25)) / 1.25 * 8, 0, 8)
    )

    reasons = [
        "1/3在上2/4在下",
        f"1/3偏差{levels['first_third_gap']:.2f}%",
        f"2/4偏差{levels['second_fourth_gap']:.2f}%",
        f"1/3收差{levels['first_third_close_gap']:.2f}%",
        f"2/4收差{levels['second_fourth_close_gap']:.2f}%",
        f"高低差{levels['level_gap']:.2f}%",
    ]
    if shadowless_count:
        reasons.append(f"影线不完整{shadowless_count}根")
    if ma40_distance is not None:
        reasons.append(f"MA40距离{ma40_distance:.1f}%")
    if volume_ratio is not None and volume_ratio <= 0.85:
        reasons.append("缩量整理")
    if last["close"] >= ma20:
        reasons.append("收在MA20上方")

    chart_series = series[-params["chart_bars"]:]
    return {
        "code": stock["code"],
        "name": stock["name"] or "",
        "trade_date": last["trade_date"],
        "close": round(last["close"], 3) if last["close"] is not None else None,
        "pct": round(last["pct_change"], 2) if last["pct_change"] is not None else None,
        "amount_yi": round((last["amount"] or 0) / 100000000, 2),
        "turnover": round(last["turnover"], 2) if last["turnover"] is not None else None,
        "avg_body_pct": round(avg_body, 2),
        "doji_body_pct": round(max_body, 2),
        "avg_amp_pct": round(avg_amp, 2),
        "range5_pct": round(range4_pct, 2) if range4_pct is not None else None,
        "volume_ratio": round(volume_ratio, 2) if volume_ratio is not None else None,
        "first_third_gap": round(levels["first_third_gap"], 2),
        "second_fourth_gap": round(levels["second_fourth_gap"], 2),
        "first_third_close_gap": round(levels["first_third_close_gap"], 2),
        "second_fourth_close_gap": round(levels["second_fourth_close_gap"], 2),
        "level_gap": round(levels["level_gap"], 2),
        "shadowless_count": shadowless_count,
        "ma20": round(ma20, 3),
        "ma40": round(ma40, 3),
        "ma40_distance": round(ma40_distance, 2) if ma40_distance is not None else None,
        "score": round(score, 1),
        "reasons": " / ".join(reasons),
        "chart": build_candlestick_chart(chart_series, highlight=4),
        "bars": [
            {
                "trade_date": r["trade_date"],
                "open": r["open"],
                "close": r["close"],
                "high": r["high"],
                "low": r["low"],
                "volume": r["volume"],
                "amount": r["amount"],
                "pct_change": r["pct_change"],
                "turnover": r["turnover"],
            }
            for r in chart_series
        ],
    }


def candle_shape(row):
    open_price = row["open"]
    close_price = row["close"]
    high = row["high"]
    low = row["low"]
    if not open_price or not close_price or not high or not low or high < low:
        return None
    span = high - low
    body = abs(close_price - open_price)
    upper = high - max(open_price, close_price)
    lower = min(open_price, close_price) - low
    base = close_price or open_price
    return {
        "open": open_price,
        "close": close_price,
        "high": high,
        "low": low,
        "span": span,
        "body": body,
        "upper": max(upper, 0),
        "lower": max(lower, 0),
        "body_pct": body / base * 100.0 if base else 0.0,
        "body_range_pct": body / span * 100.0 if span else 0.0,
        "mid": (open_price + close_price) / 2.0,
        "bull": close_price > open_price,
        "bear": close_price < open_price,
    }


def is_long_body(shape, min_body_pct=0.8):
    return shape and shape["body_pct"] >= min_body_pct and shape["body_range_pct"] >= 45


def bottom_reversal_context(series, pattern_len, params):
    if len(series) < max(25, pattern_len + 10):
        return None
    last = series[-1]
    lookback = min(params["bottom_lookback_days"], len(series))
    window = series[-lookback:]
    highs = [r["high"] for r in window if r["high"] is not None]
    lows = [r["low"] for r in window if r["low"] is not None]
    closes = [r["close"] for r in window if r["close"] is not None]
    if not highs or not lows or not closes or last["close"] is None:
        return None
    range_low = min(lows)
    range_high = max(highs)
    bottom_position = position_in_range(last["close"], range_low, range_high)
    if bottom_position is None:
        return None
    bottom_position_pct = bottom_position * 100.0
    if bottom_position_pct > params["max_bottom_position"]:
        return None

    prior_window = series[max(0, len(series) - lookback):-pattern_len]
    pattern_rows = series[-pattern_len:]
    prior_highs = [r["high"] for r in prior_window if r["high"] is not None]
    pattern_lows = [r["low"] for r in pattern_rows if r["low"] is not None]
    if not prior_highs or not pattern_lows:
        return None
    prior_high = max(prior_highs)
    pattern_low = min(pattern_lows)
    prior_drop_pct = (prior_high - pattern_low) / prior_high * 100.0 if prior_high else 0.0
    if prior_drop_pct < params["min_prior_drop_pct"]:
        return None

    return {
        "bottom_position_pct": bottom_position_pct,
        "prior_drop_pct": prior_drop_pct,
        "range_low": range_low,
        "range_high": range_high,
    }


def detect_bottom_reversal(series, params):
    if len(series) < 3:
        return None
    s1 = candle_shape(series[-1])
    s2 = candle_shape(series[-2])
    s3 = candle_shape(series[-3])
    if not s1:
        return None

    body_limit = params["bottom_max_body_pct"]
    patterns = []

    if s1["span"] > 0 and s1["body_pct"] <= body_limit:
        body_ref = max(s1["body"], s1["close"] * 0.002)
        lower_ratio = s1["lower"] / body_ref if body_ref else 0
        upper_share = s1["upper"] / s1["span"] * 100.0
        lower_share = s1["lower"] / s1["span"] * 100.0
        if lower_ratio >= 2.0 and lower_share >= 45 and upper_share <= 25:
            patterns.append({
                "name": "锤头线",
                "days": 1,
                "score": 66,
                "reasons": [
                    "底部锤头线",
                    f"下影占比{lower_share:.0f}%",
                    f"实体{s1['body_pct']:.2f}%",
                ],
            })

        upper_ratio = s1["upper"] / body_ref if body_ref else 0
        if upper_ratio >= 2.0 and upper_share >= 45 and lower_share <= 25:
            patterns.append({
                "name": "倒锤头线",
                "days": 1,
                "score": 62,
                "reasons": [
                    "底部倒锤头线",
                    f"上影占比{upper_share:.0f}%",
                    f"实体{s1['body_pct']:.2f}%",
                ],
            })

    if s1 and s2 and s2["bear"] and s1["bull"]:
        if (
            is_long_body(s2)
            and s1["body_pct"] >= 0.8
            and s1["open"] <= s2["close"] * 1.006
            and s1["close"] >= s2["open"] * 0.994
            and s1["body"] >= s2["body"] * 0.95
        ):
            patterns.append({
                "name": "看涨吞没",
                "days": 2,
                "score": 78,
                "reasons": [
                    "底部看涨吞没",
                    f"前阴实体{s2['body_pct']:.2f}%",
                    f"后阳实体{s1['body_pct']:.2f}%",
                ],
            })
        if (
            is_long_body(s2)
            and s1["close"] > s2["mid"]
            and s1["close"] < s2["open"] * 1.01
            and s1["open"] <= s2["close"] * 1.015
        ):
            patterns.append({
                "name": "曙光初现",
                "days": 2,
                "score": 72,
                "reasons": [
                    "底部曙光初现",
                    "阳线收复前阴半分位",
                    f"后阳实体{s1['body_pct']:.2f}%",
                ],
            })

    if s1 and s2 and s3 and s3["bear"] and s1["bull"]:
        if (
            is_long_body(s3)
            and s2["body_pct"] <= body_limit
            and s2["body_range_pct"] <= 45
            and s1["close"] >= s3["mid"]
            and s1["body_pct"] >= 0.8
            and s2["low"] <= min(s3["close"], s1["open"]) * 1.02
        ):
            patterns.append({
                "name": "早晨之星",
                "days": 3,
                "score": 84,
                "reasons": [
                    "底部早晨之星",
                    "第三根阳线收复首阴半分位",
                    f"中间小实体{s2['body_pct']:.2f}%",
                ],
            })

    if not patterns:
        return None
    return max(patterns, key=lambda item: item["score"])


def bottom_reversal_confirmation(series, pattern, context, low_pattern,
                                 volume_ratio, ma20, params):
    last = series[-1]
    prev = series[-2] if len(series) >= 2 else None
    pct_change = last["pct_change"] if last["pct_change"] is not None else 0
    close_price = last["close"]
    if close_price is None or not close_price:
        return None

    rebound_pct = (close_price - low_pattern) / close_price * 100.0 if close_price else 0
    close_position_pct = position_in_range(close_price, last["low"], last["high"])
    if close_position_pct is None:
        return None
    close_position_pct *= 100.0
    if close_position_pct < params["min_bottom_close_position"]:
        return None
    close_above_prev = bool(prev and prev["close"] is not None and close_price > prev["close"])
    if params.get("require_bottom_close_above_prev") and not close_above_prev:
        return None
    if pct_change < params.get("min_bottom_pct_change", -20.0):
        return None
    if rebound_pct < params.get("min_bottom_rebound_pct", 0.0):
        return None
    min_volume_ratio = params.get("min_bottom_volume_ratio") or 0
    if min_volume_ratio and (volume_ratio is None or volume_ratio < min_volume_ratio):
        return None

    above_ma20 = ma20 is not None and close_price >= ma20
    strong_patterns = ("看涨吞没", "曙光初现", "早晨之星")
    single_pin_patterns = ("锤头线", "倒锤头线")
    if pattern["name"] in strong_patterns:
        if pct_change < params.get("min_bottom_strong_gain_pct", 0.0):
            return None
        if pattern["name"] == "早晨之星":
            if close_position_pct < 60:
                return None
        elif pattern["name"] == "看涨吞没":
            if close_position_pct < 60:
                return None
        elif pattern["name"] == "曙光初现":
            if close_position_pct < 65:
                return None
    elif pattern["name"] in single_pin_patterns:
        if not (close_position_pct >= 70 and above_ma20):
            return None

    confirm_reasons = [
        f"反弹{rebound_pct:.1f}%",
        f"当日涨幅{pct_change:.1f}%",
        f"收盘位{close_position_pct:.0f}%",
    ]
    if close_above_prev:
        confirm_reasons.append("高于前收")
    if volume_ratio is not None:
        confirm_reasons.append(f"量比{volume_ratio:.2f}")

    if params.get("require_bottom_confirm"):
        bullish_pattern = pattern["name"] in strong_patterns
        high_close = close_position_pct >= 70
        if not (above_ma20 or bullish_pattern or high_close):
            return None
        if above_ma20:
            confirm_reasons.append("收在MA20上方")
        elif bullish_pattern:
            confirm_reasons.append("组合反转确认")
        else:
            confirm_reasons.append("高位收盘确认")

    return {
        "rebound_pct": rebound_pct,
        "close_position_pct": close_position_pct,
        "close_above_prev": close_above_prev,
        "reasons": confirm_reasons,
    }


def evaluate_bottom_reversal_candidate(stock, series, params):
    if len(series) < max(45, params["chart_bars"] // 2):
        return None
    if series[-1]["trade_date"] != params["trade_date"]:
        return None
    last = series[-1]
    if (last["amount"] or 0) < params["min_amount_wan"] * 10000:
        return None
    if params["min_turnover"] and (last["turnover"] is None or last["turnover"] < params["min_turnover"]):
        return None

    pattern = detect_bottom_reversal(series, params)
    if not pattern:
        return None
    context = bottom_reversal_context(series, pattern["days"], params)
    if not context:
        return None

    closes = [r["close"] for r in series if r["close"] is not None]
    volumes = [r["volume"] for r in series[-20:] if r["volume"] is not None]
    if len(closes) < 40:
        return None
    ma20 = sum(closes[-20:]) / 20
    ma40 = sum(closes[-40:]) / 40
    ma40_distance = abs(last["close"] - ma40) / ma40 * 100.0 if ma40 else None
    if params["max_ma40_distance"] and ma40_distance is not None:
        if ma40_distance > params["max_ma40_distance"]:
            return None

    pattern_rows = series[-pattern["days"]:]
    metrics = [candle_metrics(row) for row in pattern_rows]
    if any(m is None for m in metrics):
        return None
    max_body = max(m["body_pct"] for m in metrics)
    avg_amp = sum(m["amp_pct"] for m in metrics) / len(metrics)
    avg_volume20 = sum(volumes) / len(volumes) if volumes else None
    pattern_volume = sum((r["volume"] or 0) for r in pattern_rows) / len(pattern_rows)
    volume_ratio = pattern_volume / avg_volume20 if avg_volume20 else None
    low_pattern = min(r["low"] for r in pattern_rows if r["low"] is not None)
    high_pattern = max(r["high"] for r in pattern_rows if r["high"] is not None)
    range_pct = (high_pattern - low_pattern) / last["close"] * 100.0 if last["close"] else None
    confirmation = bottom_reversal_confirmation(
        series, pattern, context, low_pattern, volume_ratio, ma20, params
    )
    if not confirmation:
        return None
    bottom_bonus = clamp(
        (params["max_bottom_position"] - context["bottom_position_pct"])
        / max(params["max_bottom_position"], 1) * 12,
        0,
        12,
    )
    drop_bonus = clamp((context["prior_drop_pct"] - params["min_prior_drop_pct"]) / 15 * 10, 0, 10)
    volume_bonus = clamp(((volume_ratio or 1.0) - 1.0) / 1.0 * 8, 0, 8)
    ma_bonus = 5 if last["close"] >= ma20 else 0
    score = pattern["score"] + bottom_bonus + drop_bonus + volume_bonus + ma_bonus

    reasons = list(pattern["reasons"])
    reasons.append(f"近{params['bottom_lookback_days']}日低位{context['bottom_position_pct']:.0f}%")
    reasons.append(f"前期回撤{context['prior_drop_pct']:.1f}%")
    reasons.extend(confirmation["reasons"])
    if last["close"] >= ma20:
        reasons.append("收在MA20上方")
    if ma40_distance is not None:
        reasons.append(f"MA40距离{ma40_distance:.1f}%")

    chart_series = series[-params["chart_bars"]:]
    return {
        "pattern_type": "bottom_reversal",
        "pattern_name": pattern["name"],
        "pattern_days": pattern["days"],
        "code": stock["code"],
        "name": stock["name"] or "",
        "trade_date": last["trade_date"],
        "close": round(last["close"], 3) if last["close"] is not None else None,
        "pct": round(last["pct_change"], 2) if last["pct_change"] is not None else None,
        "amount_yi": round((last["amount"] or 0) / 100000000, 2),
        "turnover": round(last["turnover"], 2) if last["turnover"] is not None else None,
        "avg_body_pct": round(sum(m["body_pct"] for m in metrics) / len(metrics), 2),
        "doji_body_pct": round(max_body, 2),
        "avg_amp_pct": round(avg_amp, 2),
        "range5_pct": round(range_pct, 2) if range_pct is not None else None,
        "volume_ratio": round(volume_ratio, 2) if volume_ratio is not None else None,
        "first_third_gap": None,
        "second_fourth_gap": None,
        "first_third_close_gap": None,
        "second_fourth_close_gap": None,
        "level_gap": None,
        "shadowless_count": None,
        "bottom_position_pct": round(context["bottom_position_pct"], 1),
        "prior_drop_pct": round(context["prior_drop_pct"], 2),
        "rebound_pct": round(confirmation["rebound_pct"], 2),
        "close_position_pct": round(confirmation["close_position_pct"], 1),
        "close_above_prev": confirmation["close_above_prev"],
        "ma20": round(ma20, 3),
        "ma40": round(ma40, 3),
        "ma40_distance": round(ma40_distance, 2) if ma40_distance is not None else None,
        "score": round(score, 1),
        "reasons": " / ".join(reasons),
        "chart": build_candlestick_chart(chart_series, highlight=pattern["days"]),
        "bars": [
            {
                "trade_date": r["trade_date"],
                "open": r["open"],
                "close": r["close"],
                "high": r["high"],
                "low": r["low"],
                "volume": r["volume"],
                "amount": r["amount"],
                "pct_change": r["pct_change"],
                "turnover": r["turnover"],
            }
            for r in chart_series
        ],
    }


def evaluate_pattern_candidate(stock, series, params):
    if params.get("pattern_type") == "bottom_reversal":
        return evaluate_bottom_reversal_candidate(stock, series, params)
    return evaluate_four_pin_candidate(stock, series, params)


def apply_market_cap_filter(rows, params):
    min_cap = params.get("min_market_cap_yi") or 0
    if not min_cap or not rows:
        return rows, {"market_cap_checked": 0, "market_cap_missing": 0, "market_cap_filtered": 0}

    cached_caps = params.get("_market_caps")
    if cached_caps is None:
        caps = fetch_eastmoney_market_caps([row["code"] for row in rows])
        cap_source = "request"
    else:
        caps = cached_caps
        cap_source = "cache"
    kept = []
    missing = 0
    filtered = 0
    for row in rows:
        cap = caps.get(row["code"]) or {}
        market_cap = cap.get("market_cap_yi")
        float_cap = cap.get("float_market_cap_yi")
        if market_cap is None:
            missing += 1
            filtered += 1
            continue
        row["market_cap_yi"] = round(market_cap, 2)
        row["float_market_cap_yi"] = round(float_cap, 2) if float_cap is not None else None
        if market_cap < min_cap:
            filtered += 1
            continue
        kept.append(row)
    return kept, {
        "market_cap_checked": len(rows),
        "market_cap_missing": missing,
        "market_cap_filtered": filtered,
        "market_cap_source": cap_source,
    }


def perform_pattern_scan(params, started_at=None):
    started_at = started_at or time.time()
    conn = get_db()
    try:
        if not params.get("trade_date"):
            params["trade_date"] = latest_daily_trade_date(conn)
        params["required_pattern_dates"] = recent_market_trade_dates(
            conn,
            params["trade_date"],
            4,
        )
        stocks = load_stock_universe(conn, pool=params["pool"], index_code=params["index_code"])
        if not stocks:
            return {"error": "股票池为空", "meta": build_empty_pattern_meta(params)}, 400
        codes = [s["code"] for s in stocks]
        histories = load_daily_histories_for_pattern(
            conn, codes, params["trade_date"], params["lookback_days"]
        )
    finally:
        conn.close()

    rows = []
    scanned = 0
    for stock in stocks:
        series = histories.get(stock["code"]) or []
        if not series:
            continue
        scanned += 1
        row = evaluate_pattern_candidate(stock, series, params)
        if row:
            rows.append(row)

    pre_cap_matched = len(rows)
    rows, cap_meta = apply_market_cap_filter(rows, params)
    rows.sort(key=lambda r: (r["score"], r["amount_yi"]), reverse=True)
    rows = rows[:params["limit"]]
    meta = {
        "pool": params["pool"],
        "trade_date": params["trade_date"],
        "index_code": params["index_code"],
        "universe": len(stocks),
        "scanned": scanned,
        "pre_cap_matched": pre_cap_matched,
        "matched": len(rows),
        "elapsed_s": round(time.time() - started_at, 1),
        **cap_meta,
        "params": {
            "pattern_type": params["pattern_type"],
            "max_body_pct": params["max_body_pct"],
            "doji_body_pct": params["doji_body_pct"],
            "max_amp_pct": params["max_amp_pct"],
            "max_ma40_distance": params["max_ma40_distance"],
            "max_pair_distance": params["max_pair_distance"],
            "max_close_pair_distance": params["max_close_pair_distance"],
            "min_level_gap": params["min_level_gap"],
            "min_shadow_pct": params["min_shadow_pct"],
            "max_shadowless_count": params["max_shadowless_count"],
            "bottom_lookback_days": params["bottom_lookback_days"],
            "max_bottom_position": params["max_bottom_position"],
            "min_prior_drop_pct": params["min_prior_drop_pct"],
            "bottom_max_body_pct": params["bottom_max_body_pct"],
            "min_bottom_volume_ratio": params["min_bottom_volume_ratio"],
            "min_bottom_rebound_pct": params["min_bottom_rebound_pct"],
            "min_bottom_pct_change": params["min_bottom_pct_change"],
            "min_bottom_strong_gain_pct": params["min_bottom_strong_gain_pct"],
            "require_bottom_confirm": params["require_bottom_confirm"],
            "min_bottom_close_position": params["min_bottom_close_position"],
            "require_bottom_close_above_prev": params["require_bottom_close_above_prev"],
            "min_amount_wan": params["min_amount_wan"],
            "min_turnover": params["min_turnover"],
            "min_market_cap_yi": params["min_market_cap_yi"],
        },
    }
    return {"meta": meta, "rows": rows}, 200


def perform_pattern_scan_with_histories(params, stocks, histories, market_dates,
                                        started_at=None):
    started_at = started_at or time.time()
    trade_date = params["trade_date"]
    if trade_date not in market_dates:
        return {"meta": build_empty_pattern_meta(params, universe=len(stocks)), "rows": []}, 200
    date_index = market_dates.index(trade_date)
    if date_index < 3:
        return {"meta": build_empty_pattern_meta(params, universe=len(stocks)), "rows": []}, 200
    params["required_pattern_dates"] = market_dates[date_index - 3:date_index + 1]

    rows = []
    scanned = 0
    for stock in stocks:
        series = histories.get(stock["code"]) or []
        if not series:
            continue
        idx = None
        for i in range(len(series) - 1, -1, -1):
            if series[i]["trade_date"] == trade_date:
                idx = i
                break
            if series[i]["trade_date"] < trade_date:
                break
        if idx is None:
            continue
        scanned += 1
        window = series[max(0, idx - params["lookback_days"]):idx + 1]
        row = evaluate_pattern_candidate(stock, window, params)
        if row:
            rows.append(row)

    pre_cap_matched = len(rows)
    rows, cap_meta = apply_market_cap_filter(rows, params)
    rows.sort(key=lambda r: (r["score"], r["amount_yi"]), reverse=True)
    rows = rows[:params["limit"]]
    meta = {
        "pool": params["pool"],
        "trade_date": trade_date,
        "index_code": params["index_code"],
        "universe": len(stocks),
        "scanned": scanned,
        "pre_cap_matched": pre_cap_matched,
        "matched": len(rows),
        "elapsed_s": round(time.time() - started_at, 1),
        **cap_meta,
        "params": {
            "pattern_type": params["pattern_type"],
            "max_body_pct": params["max_body_pct"],
            "doji_body_pct": params["doji_body_pct"],
            "max_amp_pct": params["max_amp_pct"],
            "max_ma40_distance": params["max_ma40_distance"],
            "max_pair_distance": params["max_pair_distance"],
            "max_close_pair_distance": params["max_close_pair_distance"],
            "min_level_gap": params["min_level_gap"],
            "min_shadow_pct": params["min_shadow_pct"],
            "max_shadowless_count": params["max_shadowless_count"],
            "bottom_lookback_days": params["bottom_lookback_days"],
            "max_bottom_position": params["max_bottom_position"],
            "min_prior_drop_pct": params["min_prior_drop_pct"],
            "bottom_max_body_pct": params["bottom_max_body_pct"],
            "min_bottom_volume_ratio": params["min_bottom_volume_ratio"],
            "min_bottom_rebound_pct": params["min_bottom_rebound_pct"],
            "min_bottom_pct_change": params["min_bottom_pct_change"],
            "min_bottom_strong_gain_pct": params["min_bottom_strong_gain_pct"],
            "require_bottom_confirm": params["require_bottom_confirm"],
            "min_bottom_close_position": params["min_bottom_close_position"],
            "require_bottom_close_above_prev": params["require_bottom_close_above_prev"],
            "min_amount_wan": params["min_amount_wan"],
            "min_turnover": params["min_turnover"],
            "min_market_cap_yi": params["min_market_cap_yi"],
        },
    }
    return {"meta": meta, "rows": rows}, 200


def default_pattern_backfill_days(params):
    return 365 if (params or {}).get("pattern_type") == "four_pin" else 30


def get_pattern_backfill_trade_dates(conn, end_date=None, days=30):
    end_date = end_date or latest_daily_trade_date(conn)
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


def run_pattern_backfill(params, days=None, end_date=None, progress=None):
    started_at = time.time()
    days = days or default_pattern_backfill_days(params)
    conn = get_db()
    try:
        ensure_pattern_tables(conn)
        if progress:
            progress({
                "phase": "indexes",
                "trade_date": end_date or params.get("trade_date"),
                "picked": 0,
                "saved": 0,
                "matched_rows_so_far": 0,
                "matched_days_so_far": 0,
                "message": "检查历史K线索引",
            }, 0, 0)
        ensure_daily_price_indexes(conn)
        trade_dates = get_pattern_backfill_trade_dates(
            conn,
            end_date=end_date or params.get("trade_date"),
            days=days,
        )
        if not trade_dates:
            return {
                "start_date": None,
                "end_date": end_date,
                "days": 0,
                "matched_days": 0,
                "matched_rows": 0,
                "elapsed_s": 0,
                "results": [],
            }
        if progress:
            progress({
                "phase": "trade_dates",
                "trade_date": trade_dates[0],
                "picked": 0,
                "saved": 0,
                "matched_rows_so_far": 0,
                "matched_days_so_far": 0,
                "message": f"找到 {len(trade_dates)} 个交易日，准备加载股票池",
            }, 0, len(trade_dates))
        stocks = load_stock_universe(conn, pool=params["pool"], index_code=params["index_code"])
        if progress:
            progress({
                "phase": "stocks",
                "trade_date": trade_dates[0],
                "picked": 0,
                "saved": 0,
                "matched_rows_so_far": 0,
                "matched_days_so_far": 0,
                "message": f"股票池 {len(stocks)} 只，正在加载历史K线",
            }, 0, len(trade_dates))

        market_caps = None
        if params.get("min_market_cap_yi"):
            if progress:
                progress({
                    "phase": "market_caps",
                    "trade_date": trade_dates[0],
                    "picked": 0,
                    "saved": 0,
                    "matched_rows_so_far": 0,
                    "matched_days_so_far": 0,
                    "message": f"正在加载 {len(stocks)} 只股票总市值",
                }, 0, len(trade_dates))
            market_caps = fetch_eastmoney_market_caps([s["code"] for s in stocks])
            params["_market_caps"] = market_caps
            if progress:
                progress({
                    "phase": "market_caps_done",
                    "trade_date": trade_dates[0],
                    "picked": 0,
                    "saved": 0,
                    "matched_rows_so_far": 0,
                    "matched_days_so_far": 0,
                    "message": f"总市值加载完成 {len(market_caps)}/{len(stocks)}",
                }, 0, len(trade_dates))

        def history_progress(batch_index, batch_total, row_count):
            if progress:
                progress({
                    "phase": "history",
                    "trade_date": trade_dates[0],
                    "picked": 0,
                    "saved": 0,
                    "matched_rows_so_far": 0,
                    "matched_days_so_far": 0,
                    "message": (
                        f"正在加载历史K线 {batch_index}/{batch_total} 批，"
                        f"本批 {row_count} 条"
                    ),
                }, 0, len(trade_dates))

        histories = load_daily_histories_for_pattern_range(
            conn,
            [s["code"] for s in stocks],
            trade_dates[0],
            trade_dates[-1],
            params["lookback_days"],
            progress=history_progress,
        )
        if progress:
            progress({
                "phase": "history_done",
                "trade_date": trade_dates[0],
                "picked": 0,
                "saved": 0,
                "matched_rows_so_far": 0,
                "matched_days_so_far": 0,
                "message": "历史K线加载完成，开始逐日扫描",
            }, 0, len(trade_dates))
    finally:
        conn.close()

    results = []
    matched_rows = 0
    matched_days = 0
    for i, trade_date in enumerate(trade_dates, 1):
        day_params = dict(params)
        day_params["trade_date"] = trade_date
        payload, status_code = perform_pattern_scan_with_histories(
            day_params,
            stocks,
            histories,
            trade_dates,
            started_at=time.time(),
        )
        conn = get_db()
        try:
            run_id, saved = save_pattern_scan_result(
                conn,
                day_params,
                payload,
                status_code,
            )
        finally:
            conn.close()
        picked = len(payload.get("rows") or [])
        matched_rows += picked
        if picked > 0:
            matched_days += 1
        item = {
            "trade_date": trade_date,
            "status": status_code,
            "run_id": run_id,
            "saved": saved,
            "picked": picked,
            "matched_rows_so_far": matched_rows,
            "matched_days_so_far": matched_days,
            "meta": payload.get("meta") or {},
        }
        results.append(item)
        if progress:
            progress(item, i, len(trade_dates))

    return {
        "start_date": trade_dates[0],
        "end_date": trade_dates[-1],
        "days": len(trade_dates),
        "matched_days": matched_days,
        "matched_rows": matched_rows,
        "elapsed_s": round(time.time() - started_at, 1),
        "results": results,
    }


def run_pattern_backfill_job(params, days=None, end_date=None, job_key="pattern_backfill"):
    started_at = time.time()
    days = days or default_pattern_backfill_days(params)
    save_pattern_progress(
        job_key,
        job_type="backfill",
        status="running",
        started_at=local_now_text(),
        trade_date=end_date or params.get("trade_date"),
        current_index=0,
        total=0,
        picked=0,
        matched_rows=0,
        matched_days=0,
        elapsed_s=0,
        message="准备回扫",
        params_json=json.dumps(params, ensure_ascii=False, sort_keys=True),
        result_json=None,
        error=None,
    )

    def on_progress(item, index, total):
        picked = item.get("picked", 0)
        message = item.get("message") or f"正在回扫 {item.get('trade_date')}，当天命中 {picked} 条"
        save_pattern_progress(
            job_key,
            job_type="backfill",
            status="running",
            trade_date=item.get("trade_date"),
            current_index=index,
            total=total,
            picked=picked,
            matched_rows=item.get("matched_rows_so_far", 0),
            matched_days=item.get("matched_days_so_far", 0),
            elapsed_s=round(time.time() - started_at, 1),
            message=message,
            error=None,
        )

    try:
        result = run_pattern_backfill(
            params,
            days=days,
            end_date=end_date,
            progress=on_progress,
        )
        save_pattern_progress(
            job_key,
            job_type="backfill",
            status="done",
            trade_date=result.get("end_date"),
            current_index=result.get("days"),
            total=result.get("days"),
            picked=0,
            matched_rows=result.get("matched_rows", 0),
            matched_days=result.get("matched_days", 0),
            elapsed_s=result.get("elapsed_s", round(time.time() - started_at, 1)),
            message="回扫完成",
            result_json=json.dumps({
                "start_date": result.get("start_date"),
                "end_date": result.get("end_date"),
                "days": result.get("days"),
                "matched_days": result.get("matched_days"),
                "matched_rows": result.get("matched_rows"),
                "elapsed_s": result.get("elapsed_s"),
            }, ensure_ascii=False, sort_keys=True),
            error=None,
        )
    except Exception as exc:
        app.logger.exception("Pattern backfill job failed")
        save_pattern_progress(
            job_key,
            job_type="backfill",
            status="error",
            elapsed_s=round(time.time() - started_at, 1),
            message="回扫失败",
            error=str(exc),
        )
    finally:
        SCAN_LOCK.release()


def build_empty_pattern_meta(params, universe=0):
    return {
        "pool": params["pool"],
        "trade_date": params.get("trade_date"),
        "index_code": params["index_code"],
        "universe": universe,
        "scanned": 0,
        "matched": 0,
        "elapsed_s": 0,
        "params": {"pattern_type": params.get("pattern_type", "four_pin")},
    }


def pattern_type_from_params_json(params_json):
    try:
        return (json.loads(params_json or "{}").get("pattern_type") or "four_pin")
    except (TypeError, json.JSONDecodeError):
        return "four_pin"


def delete_existing_pattern_scope(conn, params):
    rows = conn.execute("""
        SELECT id, params_json
        FROM pattern_scan_runs
        WHERE trade_date = ?
          AND pool = ?
          AND COALESCE(index_code, '') = COALESCE(?, '')
    """, (
        params["trade_date"],
        params["pool"],
        params["index_code"],
    )).fetchall()
    run_ids = [
        row["id"] for row in rows
        if pattern_type_from_params_json(row["params_json"]) == params.get("pattern_type", "four_pin")
    ]
    if not run_ids:
        return 0
    placeholders = ",".join("?" for _ in run_ids)
    conn.execute(f"DELETE FROM pattern_picks WHERE run_id IN ({placeholders})", run_ids)
    conn.execute(f"DELETE FROM pattern_scan_runs WHERE id IN ({placeholders})", run_ids)
    return len(run_ids)


def chunked(items, size=500):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def delete_pattern_history(conn, pattern_type=None):
    ensure_pattern_tables(conn)
    rows = conn.execute("""
        SELECT id, params_json
        FROM pattern_scan_runs
        ORDER BY id
    """).fetchall()
    if pattern_type in ("four_pin", "bottom_reversal"):
        run_ids = [
            row["id"] for row in rows
            if pattern_type_from_params_json(row["params_json"]) == pattern_type
        ]
    else:
        run_ids = [row["id"] for row in rows]

    pick_count = 0
    for batch in chunked(run_ids):
        placeholders = ",".join("?" for _ in batch)
        row = conn.execute(
            f"SELECT COUNT(*) AS count FROM pattern_picks WHERE run_id IN ({placeholders})",
            batch,
        ).fetchone()
        pick_count += row["count"] if row else 0
        conn.execute(f"DELETE FROM pattern_picks WHERE run_id IN ({placeholders})", batch)
        conn.execute(f"DELETE FROM pattern_scan_runs WHERE id IN ({placeholders})", batch)

    conn.execute("DELETE FROM pattern_scan_progress WHERE job_key IN ('pattern_scan', 'pattern_backfill')")
    conn.commit()
    return {"deleted_runs": len(run_ids), "deleted_picks": pick_count}


def save_pattern_scan_result(conn, params, payload, status_code):
    ensure_pattern_tables(conn)
    meta = payload.get("meta") or build_empty_pattern_meta(params)
    rows = payload.get("rows") or []
    status = "ok" if status_code == 200 else "error"
    delete_existing_pattern_scope(conn, params)
    cur = conn.execute("""
        INSERT INTO pattern_scan_runs (
            trade_date, pool, index_code, lookback_days, chart_bars,
            min_amount_wan, min_turnover, max_body_pct, max_body_range_pct,
            max_amp_pct, doji_body_pct, max_ma40_distance,
            universe, scanned, row_count, elapsed_s, status, error, params_json
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        params["trade_date"], params["pool"], params["index_code"],
        params["lookback_days"], params["chart_bars"],
        params["min_amount_wan"], params["min_turnover"],
        params["max_body_pct"], params["max_body_range_pct"],
        params["max_amp_pct"], params["doji_body_pct"],
        params["max_ma40_distance"], meta.get("universe"),
        meta.get("scanned"), len(rows), meta.get("elapsed_s"),
        status, payload.get("error"),
        json.dumps(public_pattern_params(params), ensure_ascii=False, sort_keys=True),
    ))
    run_id = cur.lastrowid
    for row in rows:
        row_copy = dict(row)
        bars = row_copy.pop("bars", [])
        row_copy.pop("chart", None)
        conn.execute("""
            INSERT INTO pattern_picks (
                run_id, trade_date, code, name, close_price, pct_change,
                amount_yi, turnover, score, reasons, row_json, bars_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            run_id, params["trade_date"], row.get("code"), row.get("name"),
            row.get("close"), row.get("pct"), row.get("amount_yi"),
            row.get("turnover"), row.get("score"), row.get("reasons"),
            json.dumps(row_copy, ensure_ascii=False, sort_keys=True),
            json.dumps(bars, ensure_ascii=False, sort_keys=True),
        ))
    conn.commit()
    return run_id, len(rows)


def pattern_run_params(run):
    try:
        return json.loads(run["params_json"] or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}


def pattern_run_matches_type(run, pattern_type):
    if not pattern_type:
        return True
    params = pattern_run_params(run)
    return (params.get("pattern_type") or "four_pin") == pattern_type


def saved_pattern_row_passes_filters(row, run_params, filters=None):
    pattern_type = (run_params or {}).get("pattern_type", "four_pin")
    filters = filters or {}
    min_cap = filters.get("min_market_cap_yi")
    if min_cap:
        market_cap = row.get("market_cap_yi")
        if market_cap is None or market_cap < min_cap:
            return False

    if pattern_type == "four_pin":
        max_close_gap = filters.get(
            "max_close_pair_distance",
            (run_params or {}).get("max_close_pair_distance", 1.0),
        )
        first_close_gap = row.get("first_third_close_gap")
        second_close_gap = row.get("second_fourth_close_gap")
        if first_close_gap is None or second_close_gap is None:
            return False
        if first_close_gap > max_close_gap or second_close_gap > max_close_gap:
            return False
    elif pattern_type == "bottom_reversal":
        min_close_position = filters.get(
            "min_bottom_close_position",
            (run_params or {}).get("min_bottom_close_position", 55.0),
        )
        require_confirm = filters.get(
            "require_bottom_confirm",
            (run_params or {}).get("require_bottom_confirm", 1),
        )
        min_volume_ratio = filters.get(
            "min_bottom_volume_ratio",
            (run_params or {}).get("min_bottom_volume_ratio", 0.0),
        )
        min_rebound_pct = filters.get(
            "min_bottom_rebound_pct",
            (run_params or {}).get("min_bottom_rebound_pct", 0.0),
        )
        min_pct_change = filters.get(
            "min_bottom_pct_change",
            (run_params or {}).get("min_bottom_pct_change", -20.0),
        )
        min_strong_gain = filters.get(
            "min_bottom_strong_gain_pct",
            (run_params or {}).get("min_bottom_strong_gain_pct", 0.0),
        )
        require_close_above_prev = filters.get(
            "require_bottom_close_above_prev",
            (run_params or {}).get("require_bottom_close_above_prev", 0),
        )
        if row.get("close_position_pct") is None or row.get("close_position_pct") < min_close_position:
            return False
        if require_close_above_prev and not row.get("close_above_prev"):
            return False
        if row.get("pct") is None or row.get("pct") < min_pct_change:
            return False
        if row.get("rebound_pct") is None or row.get("rebound_pct") < min_rebound_pct:
            return False
        if min_volume_ratio and (row.get("volume_ratio") is None or row.get("volume_ratio") < min_volume_ratio):
            return False
        pattern_name = row.get("pattern_name")
        above_ma20 = (
            row.get("close") is not None
            and row.get("ma20") is not None
            and row.get("close") >= row.get("ma20")
        )
        if pattern_name == "早晨之星":
            if row.get("pct") is None or row.get("pct") < min_strong_gain:
                return False
            if row.get("close_position_pct") < 60:
                return False
        elif pattern_name == "看涨吞没":
            if row.get("pct") is None or row.get("pct") < min_strong_gain:
                return False
            if row.get("close_position_pct") < 60:
                return False
        elif pattern_name == "曙光初现":
            if row.get("pct") is None or row.get("pct") < min_strong_gain:
                return False
            if row.get("close_position_pct") < 65:
                return False
        elif pattern_name in ("锤头线", "倒锤头线"):
            if not (row.get("close_position_pct") >= 70 and above_ma20):
                return False
        if require_confirm:
            close_price = row.get("close")
            ma20 = row.get("ma20")
            above_ma20 = close_price is not None and ma20 is not None and close_price >= ma20
            bullish_pattern = row.get("pattern_name") in ("看涨吞没", "曙光初现", "早晨之星")
            high_close = row.get("close_position_pct") is not None and row.get("close_position_pct") >= 70
            if not (above_ma20 or bullish_pattern or high_close):
                return False
    return True


def load_pattern_rows_for_run(conn, run, highlight=4, filters=None):
    run_params = pattern_run_params(run)
    rows = []
    for pick in conn.execute("""
        SELECT row_json, bars_json
        FROM pattern_picks
        WHERE run_id = ?
        ORDER BY score DESC, amount_yi DESC, code
    """, (run["id"],)).fetchall():
        row = json.loads(pick["row_json"] or "{}")
        if not saved_pattern_row_passes_filters(row, run_params, filters):
            continue
        bars = json.loads(pick["bars_json"] or "[]")
        row["bars"] = bars
        if not row.get("chart") and bars:
            row["chart"] = build_candlestick_chart(bars, highlight=highlight)
        rows.append(row)
    return rows


def load_latest_pattern_result(conn, trade_date=None, pattern_type=None, filters=None):
    ensure_pattern_tables(conn)
    if trade_date:
        runs = conn.execute("""
            SELECT *
            FROM pattern_scan_runs
            WHERE trade_date = ?
            ORDER BY id DESC
            LIMIT 100
        """, (trade_date,)).fetchall()
    else:
        runs = conn.execute("""
            SELECT *
            FROM pattern_scan_runs
            ORDER BY trade_date DESC, id DESC
            LIMIT 300
        """).fetchall()
    selected = None
    selected_rows = []
    empty_candidate = None
    empty_candidate_rows = []
    for item in runs:
        if not pattern_run_matches_type(item, pattern_type):
            continue
        rows = load_pattern_rows_for_run(conn, item, filters=filters)
        if rows:
            selected = item
            selected_rows = rows
            break
        if empty_candidate is None:
            empty_candidate = item
            empty_candidate_rows = rows
    if not selected:
        if empty_candidate is None:
            return None
        selected = empty_candidate
        selected_rows = empty_candidate_rows

    run = selected
    rows = selected_rows
    params = pattern_run_params(run)
    return {
        "meta": {
            "run_id": run["id"],
            "trade_date": run["trade_date"],
            "pool": run["pool"],
            "index_code": run["index_code"],
            "universe": run["universe"],
            "scanned": run["scanned"],
            "matched": run["row_count"],
            "elapsed_s": run["elapsed_s"],
            "created_at": run["created_at"],
            "params": {
                "pattern_type": params.get("pattern_type", "four_pin"),
                "max_body_pct": run["max_body_pct"],
                "doji_body_pct": run["doji_body_pct"],
                "max_amp_pct": run["max_amp_pct"],
                "max_ma40_distance": run["max_ma40_distance"],
                "min_amount_wan": run["min_amount_wan"],
                "min_turnover": run["min_turnover"],
                "min_market_cap_yi": params.get("min_market_cap_yi"),
                "max_pair_distance": params.get("max_pair_distance"),
                "max_close_pair_distance": params.get("max_close_pair_distance"),
                "min_level_gap": params.get("min_level_gap"),
                "min_shadow_pct": params.get("min_shadow_pct"),
                "max_shadowless_count": params.get("max_shadowless_count"),
                "bottom_lookback_days": params.get("bottom_lookback_days"),
                "max_bottom_position": params.get("max_bottom_position"),
                "min_prior_drop_pct": params.get("min_prior_drop_pct"),
                "bottom_max_body_pct": params.get("bottom_max_body_pct"),
                "min_bottom_volume_ratio": params.get("min_bottom_volume_ratio"),
                "min_bottom_rebound_pct": params.get("min_bottom_rebound_pct"),
                "min_bottom_pct_change": params.get("min_bottom_pct_change"),
                "min_bottom_strong_gain_pct": params.get("min_bottom_strong_gain_pct"),
                "require_bottom_confirm": params.get("require_bottom_confirm"),
                "min_bottom_close_position": params.get("min_bottom_close_position"),
                "require_bottom_close_above_prev": params.get("require_bottom_close_above_prev"),
            },
            "source_params": params,
        },
        "rows": rows,
    }


def load_pattern_history(conn, days=None, hits_only=True, pattern_type=None,
                         filters=None, page=1, page_size=10):
    ensure_pattern_tables(conn)
    row = conn.execute("""
        SELECT MAX(trade_date) AS end_date
        FROM pattern_scan_runs
        WHERE length(trade_date) = 10
    """).fetchone()
    end_date = row["end_date"] if row and row["end_date"] else None
    if not end_date:
        return {
            "start_date": None,
            "end_date": None,
            "days": days,
            "page": page,
            "page_size": page_size,
            "has_next": False,
            "runs": [],
        }
    start_date = None
    conditions = ["length(r.trade_date) = 10", "r.trade_date <= ?"]
    values = [end_date]
    if days:
        start_dt = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=days)
        start_date = start_dt.strftime("%Y-%m-%d")
        conditions.append("r.trade_date >= ?")
        values.append(start_date)
    if hits_only:
        conditions.append("r.row_count > 0")
    where_sql = " AND ".join(conditions)
    offset = (page - 1) * page_size

    if hits_only:
        trade_date_rows = conn.execute(f"""
            SELECT DISTINCT r.trade_date
            FROM pattern_scan_runs r
            WHERE {where_sql}
            ORDER BY r.trade_date DESC
        """, values).fetchall()
        run_conditions = ["trade_date = ?", "row_count > 0"]
        run_where_sql = " AND ".join(run_conditions)
        matching_runs = []
        for date_row in trade_date_rows:
            trade_date = date_row["trade_date"]
            runs = conn.execute(f"""
                SELECT *
                FROM pattern_scan_runs
                WHERE {run_where_sql}
                ORDER BY id DESC
                LIMIT 100
            """, (trade_date,)).fetchall()
            for run in runs:
                if not pattern_run_matches_type(run, pattern_type):
                    continue
                picks = load_pattern_rows_for_run(conn, run, highlight=4, filters=filters)
                if not picks:
                    continue
                params = pattern_run_params(run)
                matching_runs.append({
                    "run_id": run["id"],
                    "trade_date": run["trade_date"],
                    "pool": run["pool"],
                    "pattern_type": params.get("pattern_type", "four_pin"),
                    "index_code": run["index_code"],
                    "universe": run["universe"],
                    "scanned": run["scanned"],
                    "matched": run["row_count"],
                    "elapsed_s": run["elapsed_s"],
                    "created_at": run["created_at"],
                    "rows": picks,
                })
                break

        total_rows = sum(len(run["rows"]) for run in matching_runs)
        page_runs = matching_runs[offset:offset + page_size]
        page_trade_dates = [run["trade_date"] for run in page_runs]
        page_row_count = sum(len(run["rows"]) for run in page_runs)
        return {
            "start_date": start_date,
            "end_date": end_date,
            "days": days,
            "hits_only": hits_only,
            "page": page,
            "page_size": page_size,
            "pagination_mode": "trade_dates",
            "has_prev": page > 1,
            "has_next": offset + page_size < len(matching_runs),
            "page_trade_dates": page_trade_dates,
            "page_row_count": page_row_count,
            "total_rows": total_rows,
            "total_trade_dates": len(matching_runs),
            "runs": page_runs,
        }

    trade_date_rows = conn.execute(f"""
        SELECT DISTINCT r.trade_date
        FROM pattern_scan_runs r
        WHERE {where_sql}
        ORDER BY r.trade_date DESC
        LIMIT ? OFFSET ?
    """, values + [page_size + 1, offset]).fetchall()
    page_trade_dates = [r["trade_date"] for r in trade_date_rows[:page_size]]
    has_next = len(trade_date_rows) > page_size

    result_runs = []
    run_conditions = ["trade_date = ?"]
    if hits_only:
        run_conditions.append("row_count > 0")
    run_where_sql = " AND ".join(run_conditions)
    for trade_date in page_trade_dates:
        runs = conn.execute(f"""
            SELECT *
            FROM pattern_scan_runs
            WHERE {run_where_sql}
            ORDER BY id DESC
            LIMIT 100
        """, (trade_date,)).fetchall()
        for run in runs:
            if not pattern_run_matches_type(run, pattern_type):
                continue
            picks = load_pattern_rows_for_run(conn, run, highlight=4, filters=filters)
            if hits_only and not picks:
                continue
            params = pattern_run_params(run)
            result_runs.append({
                "run_id": run["id"],
                "trade_date": run["trade_date"],
                "pool": run["pool"],
                "pattern_type": params.get("pattern_type", "four_pin"),
                "index_code": run["index_code"],
                "universe": run["universe"],
                "scanned": run["scanned"],
                "matched": run["row_count"],
                "elapsed_s": run["elapsed_s"],
                "created_at": run["created_at"],
                "rows": picks,
            })
            break

    return {
        "start_date": start_date,
        "end_date": end_date,
        "days": days,
        "hits_only": hits_only,
        "page": page,
        "page_size": page_size,
        "has_prev": page > 1,
        "has_next": has_next,
        "page_trade_dates": page_trade_dates,
        "runs": result_runs,
    }


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
    <a class="nav-link" href="/pattern">收盘形态</a>
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
      data.weight_complete ? `${data.name} 成分股权重 Top 10` : `${data.name} 成分股前 10`;
    const updated = data.updated_at ? ` · 成分股更新 ${data.updated_at}` : '';
    const coverage = data.total_count ? Math.round((data.weight_coverage || 0) * 100) : 0;
    const modeText = data.weight_complete
      ? '按权重排序'
      : (data.weight_count ? `权重不完整 ${coverage}%，显示成分股前 10` : '暂无权重，显示成分股前 10');
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
  <div style="display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end">
    <a class="nav-link" href="/pattern">收盘形态</a>
    <a class="nav-link" href="/">行业宽度</a>
  </div>
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


PATTERN_HTML = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>收盘 K 线形态扫描</title>
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
    align-items:flex-end;
    justify-content:space-between;
    gap:16px;
    padding-bottom:18px;
    margin-bottom:18px;
    border-bottom:1px solid var(--border);
  }
  h1 { color:#fff; font-size:20px; font-weight:500; letter-spacing:.04em; }
  .sub { margin-top:5px; color:var(--text-dim); font-size:11px; }
  .nav { display:flex; gap:8px; flex-wrap:wrap; justify-content:flex-end; }
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
    grid-template-columns: repeat(10, minmax(86px, 1fr));
    gap:10px;
    align-items:end;
    padding:14px;
    margin-bottom:14px;
    border:1px solid var(--border);
    border-radius:8px;
    background:var(--surface);
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
  button.secondary {
    border:1px solid var(--border);
    background:var(--surface2);
    color:var(--text);
  }
  button:disabled { opacity:.55; cursor:default; }
  .status {
    display:flex;
    flex-wrap:wrap;
    gap:10px;
    margin-bottom:16px;
  }
  .pill {
    display:flex;
    align-items:center;
    gap:8px;
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
  .grid {
    display:grid;
    grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
    gap:14px;
  }
  .card {
    border:1px solid var(--border);
    border-radius:8px;
    background:var(--surface);
    overflow:hidden;
  }
  .card-head {
    display:flex;
    align-items:flex-start;
    justify-content:space-between;
    gap:12px;
    padding:12px 13px 10px;
    border-bottom:1px solid var(--border);
    background:rgba(255,255,255,.018);
  }
  .stock-title { color:#fff; font-size:14px; font-weight:500; }
  .stock-code { display:block; margin-top:2px; color:var(--text-dim); font:10px 'DM Mono', monospace; }
  .stock-link { color:inherit; text-decoration:none; }
  .stock-link:hover .stock-title { color:#8eb1ff; }
  .stock-link:hover .stock-code { color:#8eb1ff; }
  .score {
    min-width:48px;
    padding:4px 8px;
    border-radius:999px;
    background:rgba(61,127,255,.14);
    color:#8eb1ff;
    text-align:center;
    font:500 12px 'DM Mono', monospace;
  }
  .chart { padding:10px 10px 4px; }
  .chart-link { display:block; color:inherit; text-decoration:none; }
  .metrics {
    display:grid;
    grid-template-columns: repeat(4, 1fr);
    gap:1px;
    background:var(--border);
    border-top:1px solid var(--border);
  }
  .metric {
    min-height:50px;
    padding:9px 10px;
    background:var(--surface);
  }
  .metric-label { margin-bottom:5px; color:var(--text-dim); font-size:10px; }
  .metric-value { color:var(--text); font:500 13px 'DM Mono', monospace; }
  .up { color:var(--red); }
  .down { color:var(--green); }
  .reason {
    padding:10px 12px 12px;
    color:var(--text-dim);
    font-size:11px;
    line-height:1.6;
    min-height:42px;
  }
  .section-title {
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:12px;
    margin:22px 0 12px;
    color:#fff;
    font-size:15px;
    font-weight:500;
  }
  .history {
    display:flex;
    flex-direction:column;
    gap:10px;
  }
  .history-toolbar {
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:12px;
    margin-bottom:10px;
    color:var(--text-dim);
    font-size:11px;
  }
  .history-actions {
    display:flex;
    align-items:center;
    gap:8px;
  }
  .history-actions button {
    min-width:72px;
    padding:0 10px;
  }
  .history-head {
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:10px;
    padding:10px 12px;
    border:1px solid var(--border);
    border-radius:8px;
    background:var(--surface2);
    color:var(--text);
    cursor:pointer;
  }
  .history-head:hover { border-color:var(--accent); }
  .history-date { font:500 13px 'DM Mono', monospace; color:#fff; }
  .history-meta { color:var(--text-dim); font-size:11px; }
  .history-count {
    min-width:52px;
    padding:5px 8px;
    border-radius:999px;
    background:rgba(61,127,255,.14);
    color:#8eb1ff;
    text-align:center;
    font:500 12px 'DM Mono', monospace;
  }
  .history-body {
    display:none;
    margin-top:10px;
  }
  .history-item.open .history-body { display:block; }
  .loading {
    min-height:260px;
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
  @media (max-width: 1180px) {
    .toolbar { grid-template-columns: repeat(5, minmax(86px, 1fr)); }
  }
  @media (max-width: 700px) {
    body { padding:20px 14px; }
    .header { align-items:flex-start; flex-direction:column; }
    .nav { justify-content:flex-start; }
    .toolbar { grid-template-columns: repeat(2, minmax(86px, 1fr)); }
    .grid { grid-template-columns: 1fr; }
    .metrics { grid-template-columns: repeat(2, 1fr); }
  }
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>收盘 K 线形态扫描</h1>
    <div class="sub">四根十字针 / 底部反转形态</div>
  </div>
  <div class="nav">
    <a class="nav-link" href="/">行业宽度</a>
    <a class="nav-link" href="/momentum">14:30 选股</a>
  </div>
</div>

<div class="toolbar">
  <label>股票池
    <select id="pool" onchange="syncIndexWithPool()">
      <option value="all">全市场</option>
      <option value="sector">行业池</option>
      <option value="index">指数成分</option>
    </select>
  </label>
  <label>形态
    <select id="patternType" onchange="onPatternTypeChange()">
      <option value="four_pin">四根十字针</option>
      <option value="bottom_reversal" selected>底部反转</option>
    </select>
  </label>
  <label>指数
    <select id="indexCode" onchange="syncPoolWithIndex()"></select>
  </label>
  <label>交易日
    <input id="tradeDate" placeholder="留空取最新">
  </label>
  <label>针实体%
    <input id="maxBodyPct" type="number" value="1.05" step="0.05">
  </label>
  <label>十字实体%
    <input id="dojiBodyPct" type="number" value="1.05" step="0.05">
  </label>
  <label>最大振幅%
    <input id="maxAmpPct" type="number" value="6.0" step="0.1">
  </label>
  <label>实体占振幅%
    <input id="maxBodyRangePct" type="number" value="35" step="1">
  </label>
  <label>MA40距离%
    <input id="maxMa40Distance" type="number" value="0" step="0.5">
  </label>
  <label>同位偏差%
    <input id="maxPairDistance" type="number" value="0.5" step="0.1">
  </label>
  <label>收盘同差%
    <input id="maxClosePairDistance" type="number" value="1.0" step="0.1">
  </label>
  <label>高低差%
    <input id="minLevelGap" type="number" value="0.8" step="0.05">
  </label>
  <label>影线最小%
    <input id="minShadowPct" type="number" value="1" step="1">
  </label>
  <label>缺影线数
    <input id="maxShadowlessCount" type="number" value="0" step="1" min="0" max="4">
  </label>
  <label>低位回看
    <input id="bottomLookbackDays" type="number" value="60" step="5">
  </label>
  <label>低位位置%
    <input id="maxBottomPosition" type="number" value="35" step="5">
  </label>
  <label>前期跌幅%
    <input id="minPriorDropPct" type="number" value="10" step="0.5">
  </label>
  <label>反转实体%
    <input id="bottomMaxBodyPct" type="number" value="3.0" step="0.1">
  </label>
  <label>收盘位置%
    <input id="minBottomClosePosition" type="number" value="55" step="5">
  </label>
  <label>反转量比
    <input id="minBottomVolumeRatio" type="number" value="1.2" step="0.1">
  </label>
  <label>低点反弹%
    <input id="minBottomReboundPct" type="number" value="2.0" step="0.5">
  </label>
  <label>日涨幅≥%
    <input id="minBottomPctChange" type="number" value="2.0" step="0.5">
  </label>
  <label>强形涨幅≥%
    <input id="minBottomStrongGainPct" type="number" value="3.0" step="0.5">
  </label>
  <label>高于前收
    <select id="requireBottomCloseAbovePrev">
      <option value="1">要求</option>
      <option value="0">不要求</option>
    </select>
  </label>
  <label>成交额万元
    <input id="minAmount" type="number" value="0" step="1000">
  </label>
  <label>总市值亿
    <input id="minMarketCapYi" type="number" value="100" step="50">
  </label>
  <label>换手%
    <input id="minTurnover" type="number" value="0" step="0.1">
  </label>
  <button id="scanBtn" onclick="scan()">扫描并保存</button>
  <button class="secondary" onclick="loadLatest()">最近结果</button>
  <button class="secondary" id="clearBtn" onclick="clearPatternHistory(false)">清空当前形态</button>
  <button class="secondary" id="clearAllBtn" onclick="clearPatternHistory(false, true)">清空全部历史</button>
  <button class="secondary" id="clearBackfillBtn" onclick="clearPatternHistory(true)">清空并回扫</button>
  <button class="secondary" id="backfillBtn" onclick="backfillPattern()">回扫</button>
</div>

<div class="status">
  <div class="pill">交易日 <b id="statDate">—</b></div>
  <div class="pill">股票池 <b id="statPool">—</b></div>
  <div class="pill">扫描 <b id="statScanned">—</b></div>
  <div class="pill">命中 <b id="statMatched">—</b></div>
  <div class="pill">耗时 <b id="statElapsed">—</b></div>
  <div class="pill">进度 <b id="statProgress">—</b></div>
  <div class="pill">保存 <b id="statSaved">—</b></div>
</div>

<div id="result"><div class="loading"><span class="spinner"></span>加载最近结果…</div></div>
<div class="section-title">
  <span>历史命中</span>
  <button class="secondary" onclick="loadHistory(1)">刷新历史</button>
</div>
<div id="history"><div class="loading"><span class="spinner"></span>加载历史…</div></div>

<script>
const esc = value => String(value ?? '').replace(/[&<>"']/g, ch => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
}[ch]));
const fmt = (value, digits=2) => value === null || value === undefined ? '—' : Number(value).toFixed(digits);
const cls = value => Number(value || 0) > 0 ? 'up' : Number(value || 0) < 0 ? 'down' : '';
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
let historyPage = 1;
const historyPageSize = 10;

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
  if (pool !== 'index') document.getElementById('indexCode').value = '';
}

function params() {
  const p = new URLSearchParams();
  ['patternType','pool','indexCode','tradeDate','maxBodyPct','dojiBodyPct','maxAmpPct',
   'maxBodyRangePct','maxMa40Distance','maxPairDistance','maxClosePairDistance','minLevelGap',
   'minShadowPct','maxShadowlessCount','bottomLookbackDays','maxBottomPosition',
   'minPriorDropPct','bottomMaxBodyPct','minBottomClosePosition',
   'minBottomVolumeRatio','minBottomReboundPct','minBottomPctChange',
   'minBottomStrongGainPct','requireBottomCloseAbovePrev',
   'minAmount','minMarketCapYi','minTurnover'].forEach(id => {
    const value = document.getElementById(id).value;
    if (value !== '') p.set(id, value);
  });
  p.set('limit', document.getElementById('patternType').value === 'bottom_reversal' ? '10' : '80');
  p.set('chartBars', '70');
  p.set('save', '1');
  return p.toString();
}

function setMeta(meta, savedText='—') {
  document.getElementById('statDate').textContent = meta?.trade_date || '—';
  document.getElementById('statPool').textContent = meta?.pool || '—';
  document.getElementById('statScanned').textContent = meta?.scanned ?? '—';
  document.getElementById('statMatched').textContent = meta?.matched ?? '—';
  document.getElementById('statElapsed').textContent = meta?.elapsed_s === undefined ? '—' : `${meta.elapsed_s}s`;
  document.getElementById('statSaved').textContent = savedText;
}

function setProgressText(text) {
  document.getElementById('statProgress').textContent = text || '—';
}

function progressText(job) {
  if (!job || job.status === 'idle') return '—';
  if (job.status === 'running') {
    const total = Number(job.total || 0);
    const current = Number(job.current_index || 0);
    const pct = total > 0 ? ` ${Math.floor(current * 100 / total)}%` : '';
    const hits = job.matched_rows === null || job.matched_rows === undefined ? '' : ` 命中${job.matched_rows}`;
    const prefix = job.message ? `${job.message} · ` : '';
    return `${prefix}${current}/${total || '?'}${pct}${hits}`;
  }
  if (job.status === 'done') {
    return `完成 ${job.matched_days || 0}天/${job.matched_rows || 0}条`;
  }
  if (job.status === 'error') return '失败';
  return job.status || '—';
}

function patternBackfillDays(patternType=document.getElementById('patternType')?.value) {
  return patternType === 'four_pin' ? 365 : 30;
}

function patternBackfillLabel(patternType=document.getElementById('patternType')?.value) {
  return patternType === 'four_pin' ? '1年' : '1个月';
}

function updateBackfillButtons() {
  const label = patternBackfillLabel();
  const backfillBtn = document.getElementById('backfillBtn');
  const clearBackfillBtn = document.getElementById('clearBackfillBtn');
  if (backfillBtn) backfillBtn.textContent = `回扫${label}`;
  if (clearBackfillBtn) clearBackfillBtn.textContent = `清空并回扫${label}`;
}

async function loadPatternProgress(renderBox=false) {
  const res = await fetch('/api/pattern/progress?job=pattern_backfill');
  const job = await res.json();
  if (!res.ok) throw new Error(job.error || '进度加载失败');
  const text = progressText(job);
  setProgressText(text);
  if (renderBox && job.status === 'running') {
    const detail = job.trade_date ? `当前 ${esc(job.trade_date)} · ` : '';
    const elapsed = job.elapsed_s === null || job.elapsed_s === undefined ? '' : ` · ${fmt(job.elapsed_s, 1)}s`;
    const label = patternBackfillLabel(job.params?.pattern_type);
    document.getElementById('history').innerHTML =
      `<div class="loading"><span class="spinner"></span>正在回扫最近${label}：${detail}${esc(text)}${elapsed}</div>`;
  }
  return job;
}

function metric(label, value, className='') {
  return `<div class="metric"><div class="metric-label">${label}</div><div class="metric-value ${className}">${value}</div></div>`;
}

function xueqiuSymbol(code) {
  const text = String(code || '').trim();
  if (/^(SH|SZ|BJ)\d{6}$/i.test(text)) return text.toUpperCase();
  if (/^(5|6|9)/.test(text)) return `SH${text}`;
  if (/^(0|2|3)/.test(text)) return `SZ${text}`;
  if (/^(4|8|92)/.test(text)) return `BJ${text}`;
  return text;
}

function xueqiuUrl(code) {
  const symbol = xueqiuSymbol(code);
  return symbol ? `https://xueqiu.com/S/${encodeURIComponent(symbol)}` : '#';
}

function rowCard(row) {
  const isBottom = row.pattern_type === 'bottom_reversal';
  const stockUrl = xueqiuUrl(row.code);
  const patternMetrics = isBottom ? `
      ${metric('形态', esc(row.pattern_name || '底部反转'))}
      ${metric('低位位置', row.bottom_position_pct === null || row.bottom_position_pct === undefined ? '—' : `${fmt(row.bottom_position_pct, 1)}%`)}
      ${metric('前期跌幅', row.prior_drop_pct === null || row.prior_drop_pct === undefined ? '—' : `${fmt(row.prior_drop_pct, 2)}%`)}
      ${metric('低点反弹', row.rebound_pct === null || row.rebound_pct === undefined ? '—' : `${fmt(row.rebound_pct, 2)}%`)}
      ${metric('收盘位置', row.close_position_pct === null || row.close_position_pct === undefined ? '—' : `${fmt(row.close_position_pct, 1)}%`)}
      ${metric('形态天数', `${row.pattern_days ?? '—'}天`)}
      ${metric('量比', row.volume_ratio === null || row.volume_ratio === undefined ? '—' : fmt(row.volume_ratio, 2))}
      ${metric('最大实体', `${fmt(row.doji_body_pct, 2)}%`)}
      ${metric('形态振幅', `${fmt(row.range5_pct, 2)}%`)}
      ${metric('MA40距', row.ma40_distance === null || row.ma40_distance === undefined ? '—' : `${fmt(row.ma40_distance, 2)}%`)}
    ` : `
      ${metric('最大实体', `${fmt(row.doji_body_pct, 2)}%`)}
      ${metric('4针振幅', `${fmt(row.range5_pct, 2)}%`)}
      ${metric('1/3偏差', `${fmt(row.first_third_gap, 2)}%`)}
      ${metric('2/4偏差', `${fmt(row.second_fourth_gap, 2)}%`)}
      ${metric('1/3收差', `${fmt(row.first_third_close_gap, 2)}%`)}
      ${metric('2/4收差', `${fmt(row.second_fourth_close_gap, 2)}%`)}
      ${metric('高低差', `${fmt(row.level_gap, 2)}%`)}
      ${metric('缺影线', `${row.shadowless_count ?? 0}根`)}
      ${metric('MA40距', row.ma40_distance === null || row.ma40_distance === undefined ? '—' : `${fmt(row.ma40_distance, 2)}%`)}
    `;
  return `<article class="card">
    <div class="card-head">
      <div>
        <a class="stock-link" href="${stockUrl}" target="_blank" rel="noopener noreferrer">
          <div class="stock-title">${esc(row.name || '')}</div>
          <span class="stock-code">${esc(row.code)}${row.pattern_name ? ` · ${esc(row.pattern_name)}` : ''}</span>
        </a>
      </div>
      <div class="score">${fmt(row.score, 1)}</div>
    </div>
    <div class="chart"><a class="chart-link" href="${stockUrl}" target="_blank" rel="noopener noreferrer">${row.chart || ''}</a></div>
    <div class="metrics">
      ${metric('收盘', fmt(row.close, 2))}
      ${metric('涨跌幅', `${fmt(row.pct, 2)}%`, cls(row.pct))}
      ${metric('成交额', `${fmt(row.amount_yi, 2)}亿`)}
      ${metric('总市值', row.market_cap_yi === null || row.market_cap_yi === undefined ? '—' : `${fmt(row.market_cap_yi, 0)}亿`)}
      ${metric('换手', row.turnover === null || row.turnover === undefined ? '—' : `${fmt(row.turnover, 2)}%`)}
      ${patternMetrics}
    </div>
    <div class="reason">${esc(row.reasons || '')}</div>
  </article>`;
}

function render(data, savedText='—') {
  setMeta(data.meta || {}, savedText);
  const rows = data.rows || [];
  if (!rows.length) {
    document.getElementById('result').innerHTML = '<div class="loading">暂无符合条件的股票</div>';
    return;
  }
  document.getElementById('result').innerHTML = `<div class="grid">${rows.map(rowCard).join('')}</div>`;
}

function renderHistory(data) {
  const runs = data.runs || [];
  const page = Number(data.page || historyPage || 1);
  historyPage = page;
  const dates = data.page_trade_dates || [];
  const fromDate = dates.length ? dates[dates.length - 1] : '—';
  const toDate = dates.length ? dates[0] : '—';
  const rowCount = Number(data.page_row_count || 0);
  const rangeText = data.pagination_mode === 'none'
    ? (dates.length
      ? `${rowCount} 条记录 · ${esc(fromDate)} 至 ${esc(toDate)}`
      : '暂无记录')
    : data.pagination_mode === 'rows'
    ? (dates.length
      ? `第 ${page} 页 · ${rowCount} 条记录 · ${esc(fromDate)} 至 ${esc(toDate)}`
      : `第 ${page} 页 · 暂无记录`)
    : (dates.length
      ? `第 ${page} 页 · ${esc(fromDate)} 至 ${esc(toDate)} · ${dates.length} 个交易日`
      : `第 ${page} 页 · 暂无交易日`);
  const pagerActions = data.pagination_mode === 'none' ? '' : `
      <button class="secondary" onclick="changeHistoryPage(-1)" ${data.has_prev ? '' : 'disabled'}>上一页</button>
      <button class="secondary" onclick="changeHistoryPage(1)" ${data.has_next ? '' : 'disabled'}>下一页</button>
  `;
  const pager = `<div class="history-toolbar">
    <div>${rangeText}</div>
    <div class="history-actions">${pagerActions}</div>
  </div>`;
  if (!runs.length) {
    document.getElementById('history').innerHTML = `${pager}<div class="loading">当前页暂无历史命中记录</div>`;
    return;
  }
  document.getElementById('history').innerHTML = `${pager}<div class="history">${runs.map(run => `
    <section class="history-item">
      <div class="history-head" onclick="toggleHistoryRun(this)">
        <div>
          <div class="history-date">${esc(run.trade_date)}</div>
          <div class="history-meta">run ${run.run_id} · 命中 ${run.matched} · 扫描 ${run.scanned ?? '—'} · ${esc(run.created_at || '')}</div>
        </div>
        <div class="history-count">${(run.rows || []).length} 条</div>
      </div>
      <div class="history-body">
        <div class="grid">${(run.rows || []).map(rowCard).join('')}</div>
      </div>
    </section>
  `).join('')}</div>`;
}

function changeHistoryPage(delta) {
  const nextPage = Math.max(1, historyPage + delta);
  if (nextPage === historyPage && delta < 0) return;
  loadHistory(nextPage);
}

function toggleHistoryRun(head) {
  head.closest('.history-item')?.classList.toggle('open');
}

function onPatternTypeChange() {
  historyPage = 1;
  updateBackfillButtons();
  loadLatest(false);
  loadHistory(1);
}

async function loadLatest(refreshHistory=true) {
  document.getElementById('result').innerHTML = '<div class="loading"><span class="spinner"></span>加载最近结果…</div>';
  try {
    const res = await fetch('/api/pattern/latest?' + params());
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || '加载失败');
    render(data, data.meta?.created_at || '已保存');
    if (refreshHistory) loadHistory(1);
  } catch (err) {
    setMeta({}, '—');
    document.getElementById('result').innerHTML = `<div class="loading">${esc(err.message)}</div>`;
  }
}

async function loadHistory(page=historyPage) {
  historyPage = Math.max(1, Number(page || 1));
  document.getElementById('history').innerHTML = '<div class="loading"><span class="spinner"></span>加载历史…</div>';
  try {
    const p = new URLSearchParams(params());
    p.set('hitsOnly', '1');
    p.set('page', String(historyPage));
    p.set('pageSize', String(historyPageSize));
    const res = await fetch('/api/pattern/history?' + p.toString());
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || '历史加载失败');
    renderHistory(data);
  } catch (err) {
    document.getElementById('history').innerHTML = `<div class="loading">历史加载失败：${esc(err.message)}</div>`;
  }
}

async function scan() {
  const btn = document.getElementById('scanBtn');
  btn.disabled = true;
  document.getElementById('result').innerHTML = '<div class="loading"><span class="spinner"></span>扫描中…</div>';
  try {
    const res = await fetch('/api/pattern/scan?' + params());
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || '扫描失败');
    render(data, data.saved ? `run ${data.run_id}` : '未保存');
    historyPage = 1;
    loadHistory(1);
  } catch (err) {
    document.getElementById('result').innerHTML = `<div class="loading">扫描失败：${esc(err.message)}</div>`;
  } finally {
    btn.disabled = false;
  }
}

async function backfillPattern() {
  const btn = document.getElementById('backfillBtn');
  const days = patternBackfillDays();
  const label = patternBackfillLabel();
  btn.disabled = true;
  btn.textContent = '回扫中…';
  document.getElementById('history').innerHTML = `<div class="loading"><span class="spinner"></span>正在回扫最近${label}…</div>`;
  try {
    const p = new URLSearchParams(params());
    p.set('days', String(days));
    p.delete('save');
    const res = await fetch('/api/pattern/backfill?' + p.toString());
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || '回扫失败');
    for (;;) {
      const job = await loadPatternProgress(true);
      if (job.status === 'done') {
        document.getElementById('statSaved').textContent =
          `${label} ${job.matched_days || 0} 天 / ${job.matched_rows || 0} 条`;
        break;
      }
      if (job.status === 'error') {
        throw new Error(job.error || '回扫失败');
      }
      await sleep(2000);
    }
    historyPage = 1;
    await loadHistory(1);
    await loadLatest();
  } catch (err) {
    document.getElementById('history').innerHTML = `<div class="loading">回扫失败：${esc(err.message)}</div>`;
  } finally {
    btn.disabled = false;
    updateBackfillButtons();
  }
}

async function clearPatternHistory(thenBackfill=false, clearAll=false) {
  const patternSelect = document.getElementById('patternType');
  const patternLabel = clearAll
    ? '全部形态'
    : (patternSelect.options[patternSelect.selectedIndex]?.text || '当前形态');
  const backfillLabel = patternBackfillLabel();
  const message = thenBackfill
    ? `确认清空所有${patternLabel}历史记录，并重新回扫最近${backfillLabel}？`
    : `确认清空所有${patternLabel}历史记录？`;
  if (!window.confirm(message)) return;

  const clearBtn = document.getElementById('clearBtn');
  const clearAllBtn = document.getElementById('clearAllBtn');
  const clearBackfillBtn = document.getElementById('clearBackfillBtn');
  clearBtn.disabled = true;
  clearAllBtn.disabled = true;
  clearBackfillBtn.disabled = true;
  clearBackfillBtn.textContent = thenBackfill ? '清空中…' : clearBackfillBtn.textContent;
  document.getElementById('history').innerHTML = `<div class="loading"><span class="spinner"></span>正在清空${esc(patternLabel)}历史…</div>`;
  try {
    const p = new URLSearchParams(params());
    p.delete('save');
    if (clearAll) p.set('patternType', 'all');
    p.set('confirm', '1');
    const res = await fetch('/api/pattern/clear?' + p.toString(), { method: 'POST' });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || '清空失败');
    document.getElementById('statSaved').textContent =
      `已清空 ${data.deleted_runs || 0} 次 / ${data.deleted_picks || 0} 条`;
    historyPage = 1;
    await loadHistory(1);
    await loadLatest(false);
    if (thenBackfill) await backfillPattern();
  } catch (err) {
    document.getElementById('history').innerHTML = `<div class="loading">清空失败：${esc(err.message)}</div>`;
  } finally {
    clearBtn.disabled = false;
    clearAllBtn.disabled = false;
    clearBackfillBtn.disabled = false;
    updateBackfillButtons();
  }
}

loadIndices();
loadPatternProgress();
updateBackfillButtons();
loadLatest(false);
loadHistory(1);
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


@app.route("/pattern")
def pattern_page():
    return render_template_string(PATTERN_HTML)


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
                SUM(CASE WHEN weight IS NOT NULL THEN weight ELSE 0 END) AS weight_sum,
                MAX(weight_date) AS weight_date,
                MAX(updated_at) AS updated_at
            FROM index_constituents
            WHERE index_code = ?
        """, (code,)).fetchone()

        weight_count = summary["weight_count"] or 0
        total_count = summary["total_count"] or 0
        weight_sum = summary["weight_sum"] or 0
        weight_coverage = weight_count / total_count if total_count else 0
        weight_complete = weight_coverage >= 0.98 and weight_sum >= 95.0
        if weight_complete:
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
                    NULL AS weight,
                    NULL AS weight_date
                FROM index_constituents ic
                LEFT JOIN stocks s ON s.code = ic.stock_code
                WHERE ic.index_code = ?
                ORDER BY ic.stock_code ASC
                LIMIT ?
            """, (code, limit)).fetchall()

        return jsonify({
            "code": idx["code"],
            "name": idx["name"],
            "total_count": total_count,
            "weight_count": weight_count,
            "weight_sum": weight_sum,
            "weight_coverage": weight_coverage,
            "weight_complete": weight_complete,
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


@app.route("/api/pattern/latest")
def api_pattern_latest():
    params = build_pattern_params(request.args)
    trade_date = params.get("trade_date")
    pattern_type = params["pattern_type"]
    filters = params if saved_pattern_filters_enabled(request.args) else None
    conn = get_db()
    try:
        payload = load_latest_pattern_result(
            conn, trade_date=trade_date, pattern_type=pattern_type, filters=filters
        )
    finally:
        conn.close()
    if not payload:
        return jsonify({
            "error": "暂无保存的形态扫描结果，请先点击扫描并保存",
            "meta": build_empty_pattern_meta(build_pattern_params(request.args)),
            "rows": [],
        }), 404
    return jsonify(payload)


@app.route("/api/pattern/scan")
def api_pattern_scan():
    started_at = time.time()
    if not SCAN_LOCK.acquire(blocking=False):
        return jsonify({
            "error": "已有扫描任务进行中，请等待上一次扫描结束",
            "meta": build_empty_pattern_meta(build_pattern_params(request.args)),
            "rows": [],
        }), 429
    try:
        params = build_pattern_params(request.args)
        save_pattern_progress(
            "pattern_scan",
            job_type="scan",
            status="running",
            started_at=local_now_text(),
            trade_date=params.get("trade_date"),
            current_index=0,
            total=1,
            picked=0,
            matched_rows=0,
            matched_days=0,
            elapsed_s=0,
            message="正在扫描",
            params_json=json.dumps(params, ensure_ascii=False, sort_keys=True),
            result_json=None,
            error=None,
        )
        payload, status = perform_pattern_scan(params, started_at=started_at)
        save_requested = request.args.get("save", "0") in ("1", "true", "yes")
        if save_requested:
            conn = get_db()
            try:
                run_id, saved = save_pattern_scan_result(conn, params, payload, status)
            finally:
                conn.close()
            payload["run_id"] = run_id
            payload["saved"] = saved
        matched = len(payload.get("rows") or [])
        save_pattern_progress(
            "pattern_scan",
            job_type="scan",
            status="done" if status == 200 else "error",
            trade_date=(payload.get("meta") or {}).get("trade_date"),
            current_index=1,
            total=1,
            picked=matched,
            matched_rows=matched,
            matched_days=1 if matched else 0,
            elapsed_s=(payload.get("meta") or {}).get("elapsed_s"),
            message="扫描完成" if status == 200 else "扫描失败",
            result_json=json.dumps({
                "trade_date": (payload.get("meta") or {}).get("trade_date"),
                "matched_rows": matched,
                "elapsed_s": (payload.get("meta") or {}).get("elapsed_s"),
            }, ensure_ascii=False, sort_keys=True),
            error=payload.get("error"),
        )
        return jsonify(payload), status
    except Exception as exc:
        save_pattern_progress(
            "pattern_scan",
            job_type="scan",
            status="error",
            elapsed_s=round(time.time() - started_at, 1),
            message="扫描失败",
            error=str(exc),
        )
        raise
    finally:
        SCAN_LOCK.release()


@app.route("/api/pattern/history")
def api_pattern_history():
    days_arg = request.args.get("days")
    days = None
    if days_arg not in (None, ""):
        try:
            days = clamp(int(days_arg), 1, 3650)
        except (TypeError, ValueError):
            days = None
    page = to_int_arg("page", 1, 1, 10000)
    page_size = to_int_arg("pageSize", 10, 1, 10)
    hits_only = request.args.get("hitsOnly", "1") not in ("0", "false", "no")
    params = build_pattern_params(request.args)
    pattern_type = params["pattern_type"]
    filters = params if saved_pattern_filters_enabled(request.args) else None
    conn = get_db()
    try:
        payload = load_pattern_history(
            conn,
            days=days,
            hits_only=hits_only,
            pattern_type=pattern_type,
            filters=filters,
            page=page,
            page_size=page_size,
        )
    finally:
        conn.close()
    return jsonify(payload)


@app.route("/api/pattern/clear", methods=["POST"])
def api_pattern_clear():
    if request.args.get("confirm") != "1":
        return jsonify({"error": "缺少确认参数"}), 400
    if not SCAN_LOCK.acquire(blocking=False):
        return jsonify({"error": "已有扫描任务进行中，请等待上一次扫描结束"}), 429
    try:
        raw_pattern_type = get_source_value(request.args, "patternType", "pattern_type")
        pattern_type = None if raw_pattern_type in ("all", "*") else build_pattern_params(request.args)["pattern_type"]
        conn = get_db()
        try:
            result = delete_pattern_history(conn, pattern_type=pattern_type)
        finally:
            conn.close()
        result["pattern_type"] = pattern_type or "all"
        return jsonify(result)
    finally:
        SCAN_LOCK.release()


@app.route("/api/pattern/backfill")
def api_pattern_backfill():
    if not SCAN_LOCK.acquire(blocking=False):
        return jsonify({"error": "已有扫描任务进行中，请等待上一次扫描结束"}), 429
    params = build_pattern_params(request.args)
    if request.args.get("days") in (None, ""):
        days = default_pattern_backfill_days(params)
    else:
        days = to_int_arg("days", default_pattern_backfill_days(params), 1, 3650)
    save_pattern_progress(
        "pattern_backfill",
        job_type="backfill",
        status="running",
        started_at=local_now_text(),
        trade_date=params.get("trade_date"),
        current_index=0,
        total=0,
        picked=0,
        matched_rows=0,
        matched_days=0,
        elapsed_s=0,
        message="回扫任务已启动",
        params_json=json.dumps(params, ensure_ascii=False, sort_keys=True),
        result_json=None,
        error=None,
    )
    thread = threading.Thread(
        target=run_pattern_backfill_job,
        args=(params, days, params.get("trade_date")),
        daemon=True,
    )
    try:
        thread.start()
    except Exception:
        SCAN_LOCK.release()
        raise
    return jsonify({
        "status": "running",
        "job_key": "pattern_backfill",
        "message": "回扫任务已启动",
        "days": days,
    })


@app.route("/api/pattern/progress")
def api_pattern_progress():
    job_key = request.args.get("job", "pattern_backfill")
    return jsonify(load_pattern_progress(job_key))


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


def build_cli_pattern_params(args):
    return build_pattern_params({
        "pattern_type": args.pattern_type,
        "pool": args.pool,
        "index_code": args.index_code,
        "trade_date": args.trade_date,
        "min_amount": args.pattern_min_amount,
        "limit": args.limit,
        "lookback_days": args.pattern_lookback_days,
        "chart_bars": args.pattern_chart_bars,
        "max_body_pct": args.pattern_max_body_pct,
        "max_body_range_pct": args.pattern_max_body_range_pct,
        "max_amp_pct": args.pattern_max_amp_pct,
        "doji_body_pct": args.pattern_doji_body_pct,
        "max_ma40_distance": args.pattern_max_ma40_distance,
        "max_pair_distance": args.pattern_max_pair_distance,
        "max_close_pair_distance": args.pattern_max_close_pair_distance,
        "min_level_gap": args.pattern_min_level_gap,
        "min_shadow_pct": args.pattern_min_shadow_pct,
        "max_shadowless_count": args.pattern_max_shadowless_count,
        "bottom_lookback_days": args.pattern_bottom_lookback_days,
        "max_bottom_position": args.pattern_max_bottom_position,
        "min_prior_drop_pct": args.pattern_min_prior_drop_pct,
        "bottom_max_body_pct": args.pattern_bottom_max_body_pct,
        "min_bottom_volume_ratio": args.pattern_min_bottom_volume_ratio,
        "min_bottom_rebound_pct": args.pattern_min_bottom_rebound_pct,
        "min_bottom_pct_change": args.pattern_min_bottom_pct_change,
        "min_bottom_strong_gain_pct": args.pattern_min_bottom_strong_gain_pct,
        "require_bottom_confirm": args.pattern_require_bottom_confirm,
        "min_bottom_close_position": args.pattern_min_bottom_close_position,
        "require_bottom_close_above_prev": args.pattern_require_bottom_close_above_prev,
        "min_turnover": args.pattern_min_turnover,
        "min_market_cap_yi": args.pattern_min_market_cap_yi,
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


def print_pattern_summary(payload, status_code, run_id=None, saved=None):
    meta = payload.get("meta") or {}
    print(
        f"形态扫描: status={status_code} run_id={run_id or '-'} "
        f"trade_date={meta.get('trade_date')} pool={meta.get('pool')} "
        f"scanned={meta.get('scanned', 0)} picked={len(payload.get('rows') or [])} "
        f"saved={saved if saved is not None else '-'} elapsed={meta.get('elapsed_s', 0)}s"
    )
    if payload.get("error"):
        print(f"错误: {payload['error']}")
    for row in (payload.get("rows") or [])[:20]:
        print(
            f"  {row['code']} {row.get('name') or ''} "
            f"{row.get('pattern_name') or row.get('pattern_type') or ''} "
            f"close={row.get('close')} pct={row.get('pct')}% "
            f"body={row.get('doji_body_pct')}% "
            f"13gap={row.get('first_third_gap')}% "
            f"24gap={row.get('second_fourth_gap')}% "
            f"bottom={row.get('bottom_position_pct') if row.get('bottom_position_pct') is not None else '-'}% "
            f"drop={row.get('prior_drop_pct') if row.get('prior_drop_pct') is not None else '-'}% "
            f"13close={row.get('first_third_close_gap')}% "
            f"24close={row.get('second_fourth_close_gap')}% "
            f"level={row.get('level_gap')}% "
            f"mcap={row.get('market_cap_yi') or '-'}亿 "
            f"shadowless={row.get('shadowless_count', 0)} "
            f"score={row.get('score')}"
        )
    if len(payload.get("rows") or []) > 20:
        print(f"  ... 还有 {len(payload['rows']) - 20} 条")


def print_pattern_backfill_progress(item, index, total):
    if item.get("phase"):
        print(f"[{index}/{total}] {item.get('message', item.get('phase'))}")
        return
    print(
        f"[{index}/{total}] {item['trade_date']} "
        f"picked={item['picked']} saved={item['saved']} "
        f"elapsed={item.get('meta', {}).get('elapsed_s', 0)}s"
    )


def print_pattern_backfill_summary(result):
    print(
        f"形态回扫完成: {result.get('start_date')} -> {result.get('end_date')} "
        f"交易日={result.get('days', 0)} "
        f"命中交易日={result.get('matched_days', 0)} "
        f"命中记录={result.get('matched_rows', 0)} "
        f"耗时={result.get('elapsed_s', 0)}s"
    )


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
    actions.add_argument("--pattern-scan-save", action="store_true",
                         help="扫描并保存收盘K线形态")
    actions.add_argument("--pattern-backfill", action="store_true",
                         help="回扫并保存最近一段时间的四针形态结果")

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
    parser.add_argument("--pattern-lookback-days", type=int, default=120)
    parser.add_argument("--pattern-chart-bars", type=int, default=70)
    parser.add_argument("--pattern-type", default="four_pin",
                        choices=["four_pin", "bottom_reversal"],
                        help="收盘形态类型：four_pin=四根十字针，bottom_reversal=底部反转")
    parser.add_argument("--pattern-max-body-pct", type=float, default=1.05,
                        help="四根十字针最大实体幅度，单位%")
    parser.add_argument("--pattern-max-body-range-pct", type=float, default=35.0,
                        help="十字针实体占振幅上限，单位%")
    parser.add_argument("--pattern-max-amp-pct", type=float, default=6.0,
                        help="四根K线单日最大振幅，单位%")
    parser.add_argument("--pattern-doji-body-pct", type=float, default=1.05,
                        help="四根十字针最大实体幅度，单位%")
    parser.add_argument("--pattern-max-ma40-distance", type=float, default=0.0,
                        help="收盘价距离MA40的最大幅度，0表示不限制")
    parser.add_argument("--pattern-max-pair-distance", type=float, default=0.5,
                        help="第1/3、第2/4同位允许偏差，单位%")
    parser.add_argument("--pattern-max-close-pair-distance", type=float, default=1.0,
                        help="第1/3、第2/4收盘价允许偏差，单位%")
    parser.add_argument("--pattern-min-level-gap", type=float, default=0.8,
                        help="第1/3高位中枢相对第2/4低位中枢的最小差，单位%")
    parser.add_argument("--pattern-min-shadow-pct", type=float, default=1.0,
                        help="上下影线占单根振幅的最小比例，单位%")
    parser.add_argument("--pattern-max-shadowless-count", type=int, default=0,
                        help="四根K线中允许缺上影或下影的最大根数")
    parser.add_argument("--pattern-bottom-lookback-days", type=int, default=60,
                        help="底部反转低位判定回看天数")
    parser.add_argument("--pattern-max-bottom-position", type=float, default=35.0,
                        help="底部反转收盘价在回看区间中的最高位置，单位%")
    parser.add_argument("--pattern-min-prior-drop-pct", type=float, default=10.0,
                        help="底部反转前期最小回撤幅度，单位%")
    parser.add_argument("--pattern-bottom-max-body-pct", type=float, default=3.0,
                        help="底部反转单根/星线最大实体幅度，单位%")
    parser.add_argument("--pattern-min-bottom-volume-ratio", type=float, default=1.2,
                        help="底部反转最低形态量比")
    parser.add_argument("--pattern-min-bottom-rebound-pct", type=float, default=2.0,
                        help="底部反转低点反弹下限，单位%")
    parser.add_argument("--pattern-min-bottom-pct-change", type=float, default=2.0,
                        help="底部反转当日涨幅下限，单位%")
    parser.add_argument("--pattern-min-bottom-strong-gain-pct", type=float, default=3.0,
                        help="组合反转形态最低当日涨幅，单位%")
    parser.add_argument("--pattern-require-bottom-confirm", type=int, default=1,
                        help="底部反转是否要求MA20/组合形态/高位收盘确认，1=要求，0=不要求")
    parser.add_argument("--pattern-min-bottom-close-position", type=float, default=55.0,
                        help="底部反转当日收盘价在日内振幅中的最低位置，单位%")
    parser.add_argument("--pattern-require-bottom-close-above-prev", type=int, default=1,
                        help="底部反转是否要求收盘价高于前一日，1=要求，0=不要求")
    parser.add_argument("--pattern-min-turnover", type=float, default=0.0,
                        help="最低换手率，单位%")
    parser.add_argument("--pattern-min-market-cap-yi", type=float, default=100.0,
                        help="最低总市值，单位亿元；0表示不限制")
    parser.add_argument("--pattern-min-amount", type=float, default=None,
                        help="收盘形态扫描最低成交额，单位万元；不填则不限制")
    parser.add_argument("--pattern-backfill-days", type=int, default=None,
                        help="收盘形态历史回扫天数；不填时四根针365天，底部反转30天")
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
    elif args.pattern_scan_save:
        pattern_params = build_cli_pattern_params(args)
        payload, status_code = perform_pattern_scan(
            pattern_params,
            started_at=time.time(),
        )
        conn = get_db()
        try:
            run_id, saved = save_pattern_scan_result(
                conn, pattern_params, payload, status_code
            )
        finally:
            conn.close()
        print_pattern_summary(
            payload,
            status_code,
            run_id=run_id,
            saved=saved,
        )
    elif args.pattern_backfill:
        pattern_params = build_cli_pattern_params(args)
        result = run_pattern_backfill(
            pattern_params,
            days=args.pattern_backfill_days or default_pattern_backfill_days(pattern_params),
            end_date=args.trade_date,
            progress=print_pattern_backfill_progress,
        )
        print_pattern_backfill_summary(result)


# ─────────────────────────────────────────────────────────────────
# 启动
# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
