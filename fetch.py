"""
股票数据入库系统 — 阶段一：股票列表 + 历史价格
================================================
使用方式：
  python stock_db_pipeline.py --test      # 用10只股票验证流程
  python stock_db_pipeline.py --init      # 全量初始化（支持断点续传）
  python stock_db_pipeline.py --sync      # 每日收盘后增量同步
  python stock_db_pipeline.py --status    # 查看数据库状态

定时任务（crontab）：
  30 16 * * 1-5  cd /your/path && python stock_db_pipeline.py --sync >> cron.log 2>&1
"""

import sqlite3
import requests
import time
import argparse
import logging
import sys
import random
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────────────────────────────────────────────
# 配置
# ─────────────────────────────────────────────────────────────────
DB_PATH          = "stock_data.db"
LOG_FILE         = "pipeline.log"
REQUEST_INTERVAL = 1.0    # 正常请求间隔（秒）- 增加到1秒避免限流
RETRY_INTERVAL   = 10.0   # 限流后等待时间（秒）- 增加到10秒
MAX_RETRIES      = 5      # 单次请求最大重试次数 - 增加到5次
FAIL_ALERT_RATIO = 0.05   # 失败率超过5%时告警
HISTORY_START    = "20100101"

STOCK_LIST_URL = "https://push2delay.eastmoney.com/api/qt/clist/get"
KLINE_URL      = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
SINA_PRICE_URL = "https://hq.sinajs.cn/list="  # 新浪实时行情接口
SINA_REFERER   = "https://finance.sina.com.cn/"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": "https://www.eastmoney.com/",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }


def get_sina_headers():
    headers = get_headers()
    headers["Referer"] = SINA_REFERER
    headers["Accept"] = "application/javascript, text/plain, */*"
    return headers

# ─────────────────────────────────────────────────────────────────
# 日志
# ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# HTTP Session（带自动重试）
# ─────────────────────────────────────────────────────────────────
def make_session():
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=2,                         # 重试等待：2s, 4s, 8s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("http://",  adapter)
    session.mount("https://", adapter)
    return session

SESSION = make_session()


def safe_get(url, params, timeout=15):
    """带限流检测的 GET，遇到429或空响应自动等待重试"""
    for attempt in range(MAX_RETRIES):
        try:
            # 每次请求使用随机User-Agent
            headers = get_headers()
            resp = SESSION.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code == 429:
                log.warning(f"  限流(429)，等待 {RETRY_INTERVAL}s  attempt={attempt+1}")
                time.sleep(RETRY_INTERVAL)
                continue
            if not resp.text or len(resp.text) < 10:
                log.warning(f"  空响应，等待重试  attempt={attempt+1}")
                time.sleep(RETRY_INTERVAL)
                continue
            return resp
        except requests.exceptions.Timeout:
            log.warning(f"  超时，重试  attempt={attempt+1}")
            time.sleep(RETRY_INTERVAL)
        except requests.exceptions.ConnectionError as e:
            log.warning(f"  连接错误: {e}，重试  attempt={attempt+1}")
            time.sleep(RETRY_INTERVAL)
    raise RuntimeError(f"请求失败，已重试 {MAX_RETRIES} 次: {url}")


def safe_get_sina(symbols, timeout=10):
    """带重试的新浪行情请求"""
    url = f"{SINA_PRICE_URL}{symbols}"
    for attempt in range(MAX_RETRIES):
        try:
            resp = SESSION.get(url, headers=get_sina_headers(), timeout=timeout)
            if resp.status_code == 429:
                log.warning(f"  新浪限流(429)，等待 {RETRY_INTERVAL}s  attempt={attempt+1}")
                time.sleep(RETRY_INTERVAL)
                continue
            resp.raise_for_status()
            if not resp.text or 'var hq_str_' not in resp.text:
                log.warning(f"  新浪空响应，等待重试  attempt={attempt+1}")
                time.sleep(RETRY_INTERVAL)
                continue
            return resp
        except requests.exceptions.Timeout:
            log.warning(f"  新浪超时，重试  attempt={attempt+1}")
            time.sleep(RETRY_INTERVAL)
        except requests.exceptions.ConnectionError as e:
            log.warning(f"  新浪连接错误: {e}，重试  attempt={attempt+1}")
            time.sleep(RETRY_INTERVAL)
        except requests.exceptions.RequestException as e:
            log.warning(f"  新浪请求异常: {e}，重试  attempt={attempt+1}")
            time.sleep(RETRY_INTERVAL)
    raise RuntimeError(f"新浪请求失败，已重试 {MAX_RETRIES} 次: {url}")


def parse_sina_quote_line(line):
    """解析单行新浪行情，返回 (code, fields)"""
    if "=" not in line or '"' not in line:
        return None
    prefix, payload = line.split("=", 1)
    symbol = prefix.rsplit("_", 1)[-1]
    if len(symbol) <= 2:
        return None
    code = symbol[2:]
    fields = payload.strip().strip('";').split(",")
    return code, fields


def to_sina_symbol(code):
    """将证券代码映射为新浪行情前缀代码"""
    if code.startswith("92"):
        return "bj" + code
    return ("sh" if code.startswith(("5", "6", "9")) else "sz") + code


# ─────────────────────────────────────────────────────────────────
# 交易日判断
# ─────────────────────────────────────────────────────────────────
def is_trading_day():
    """判断今天是否为交易日（通过新浪接口检测）"""
    today = datetime.today()
    # 周末直接返回False
    if today.weekday() >= 5:  # 5=周六, 6=周日
        return False

    try:
        # 用上证指数判断是否有交易
        resp = safe_get_sina("sh000001", timeout=5)
        parsed = parse_sina_quote_line(resp.text.strip())
        if not parsed:
            return True
        _, data = parsed
        # 如果当前价格为0或日期不是今天，说明不是交易日
        if len(data) > 30 and data[3] and float(data[3]) > 0:
            trade_date = data[30]  # 格式: 2026-03-16
            return trade_date == today.strftime("%Y-%m-%d")
    except Exception as e:
        log.warning(f"交易日判断失败: {e}，默认认为是交易日")
        return True  # 出错时默认认为是交易日，避免漏数据

    return False


def get_price_sina_batch(codes):
    """批量获取多只股票当日实时价格（新浪接口）"""
    symbols = ",".join(to_sina_symbol(c) for c in codes)

    try:
        resp = safe_get_sina(symbols, timeout=10)
        result = {}
        for line in resp.text.strip().split("\n"):
            parsed = parse_sina_quote_line(line)
            if not parsed:
                continue
            code, data = parsed
            if len(data) > 30 and data[3]:
                try:
                    prev_close = float(data[2]) if data[2] else None
                    close = float(data[3])
                    result[code] = {
                        "trade_date": data[30],      # 交易日期
                        "open":       float(data[1]),
                        "close":      close,          # 当前价
                        "high":       float(data[4]),
                        "low":        float(data[5]),
                        # 新浪实时接口返回的是股数，这里统一换算成“手”以匹配日K数据口径
                        "volume":     float(data[8]) / 100.0,
                        "amount":     float(data[9]),
                        "pct_change": ((close - prev_close) / prev_close * 100.0)
                                      if prev_close else None,
                        "turnover":   None,
                    }
                except (ValueError, IndexError):
                    continue
        return result
    except Exception as e:
        log.warning(f"新浪接口批量获取失败: {e}")
        return {}


# ─────────────────────────────────────────────────────────────────
# 建库
# ─────────────────────────────────────────────────────────────────
def init_db(conn):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            code           TEXT PRIMARY KEY,   -- 纯数字代码，如 000630
            secucode       TEXT UNIQUE,        -- 带市场后缀，如 000630.SZ
            name           TEXT,
            market         TEXT,               -- '0'=深市  '1'=沪市
            price_latest   REAL,               -- 最新收盘价
            history_start  DATE,               -- 价格数据起始日
            history_end    DATE,               -- 价格数据最新日
            is_delisted    INTEGER DEFAULT 0,  -- 1=已退市（预留）
            created_at     DATETIME DEFAULT (datetime('now','localtime')),
            updated_at     DATETIME DEFAULT (datetime('now','localtime'))
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_prices (
            code        TEXT  NOT NULL,
            trade_date  DATE  NOT NULL,
            open        REAL,
            close       REAL,
            high        REAL,
            low         REAL,
            volume      REAL,   -- 成交量（手）
            amount      REAL,   -- 成交额（元）
            pct_change  REAL,   -- 涨跌幅%
            turnover    REAL,   -- 换手率%
            PRIMARY KEY (code, trade_date)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            sync_type   TEXT,        -- 'init' | 'daily' | 'test'
            sync_date   DATE,
            total       INTEGER,
            success     INTEGER,
            failed      INTEGER,
            new_stocks  INTEGER,     -- 本次发现的新股数量
            duration_s  REAL,
            note        TEXT,
            created_at  DATETIME DEFAULT (datetime('now','localtime'))
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_dp_code_date ON daily_prices(code, trade_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dp_date      ON daily_prices(trade_date)")

    conn.commit()
    log.info("数据库表结构就绪")


# ─────────────────────────────────────────────────────────────────
# 数据抓取
# ─────────────────────────────────────────────────────────────────
def fetch_stock_list():
    """拉取全市场股票列表（沪深主板 + 创业板 + 科创板）"""
    market_configs = [
        ("m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2", "0", ".SZ"),
        ("m:1+t:2+f:!2,m:1+t:23+f:!2,m:1+t:3+f:!2",  "1", ".SH"),
    ]
    all_stocks = []
    for fs, market, suffix in market_configs:
        page = 1
        while True:
            params = {
                "fid": "f62", "po": 1, "pz": 100, "pn": page,
                "np": 1, "fltt": 2, "invt": 2,
                "ut": "8dec03ba335b81bf4ebdf7b29ec27d15",
                "fs": fs,
                "fields": "f12,f14,f2",   # 代码, 名称, 最新价
            }
            resp  = safe_get(STOCK_LIST_URL, params)
            data  = resp.json().get("data", {})
            diff  = data.get("diff", [])
            total = data.get("total", 0)

            for item in diff:
                code = item.get("f12", "")
                all_stocks.append({
                    "code":         code,
                    "secucode":     f"{code}{suffix}",
                    "name":         item.get("f14", ""),
                    "market":       market,
                    "price_latest": item.get("f2"),
                })

            fetched = (page - 1) * 100 + len(diff)
            log.info(f"  股票列表 market={market}  {fetched}/{total}")
            if fetched >= total or not diff:
                break
            page += 1
            time.sleep(REQUEST_INTERVAL + random.uniform(0, 0.5))  # 添加随机延迟

    log.info(f"股票列表完成，共 {len(all_stocks)} 只")
    return all_stocks


def fetch_kline(code, market, start_date, end_date=None):
    """日K线（前复权），返回列表"""
    if end_date is None:
        end_date = datetime.today().strftime("%Y%m%d")
    params = {
        "secid":   f"{market}.{code}",
        "ut":      "8dec03ba335b81bf4ebdf7b29ec27d15",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        # 字段顺序：日期,开盘,收盘,最高,最低,成交量,成交额,振幅,涨跌幅,涨跌额,换手率
        "klt": 101, "fqt": 1,
        "beg": start_date, "end": end_date, "lmt": 10000,
    }
    resp   = safe_get(KLINE_URL, params)
    klines = resp.json().get("data", {}).get("klines", [])

    rows = []
    for k in klines:
        p = k.split(",")
        if len(p) < 11:
            continue
        def v(x): return float(x) if x not in ("-", "") else None
        rows.append({
            "trade_date": p[0],
            "open":       v(p[1]),
            "close":      v(p[2]),
            "high":       v(p[3]),
            "low":        v(p[4]),
            "volume":     v(p[5]),
            "amount":     v(p[6]),
            "pct_change": v(p[8]),
            "turnover":   v(p[10]),
        })
    return rows


# ─────────────────────────────────────────────────────────────────
# 写入数据库
# ─────────────────────────────────────────────────────────────────
def upsert_stocks(conn, stocks):
    conn.executemany("""
        INSERT INTO stocks (code, secucode, name, market, price_latest, updated_at)
        VALUES (:code, :secucode, :name, :market, :price_latest, datetime('now','localtime'))
        ON CONFLICT(code) DO UPDATE SET
            name         = excluded.name,
            price_latest = excluded.price_latest,
            updated_at   = datetime('now','localtime')
    """, stocks)
    conn.commit()


def insert_daily_prices(conn, code, rows):
    if not rows:
        return 0
    conn.executemany("""
        INSERT INTO daily_prices
            (code, trade_date, open, close, high, low, volume, amount, pct_change, turnover)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(code, trade_date) DO UPDATE SET
            open       = excluded.open,
            close      = excluded.close,
            high       = excluded.high,
            low        = excluded.low,
            volume     = excluded.volume,
            amount     = excluded.amount,
            pct_change = excluded.pct_change,
            turnover   = excluded.turnover
    """, [(code, r.get("trade_date"), r.get("open"), r.get("close"), r.get("high"),
           r.get("low"), r.get("volume"), r.get("amount"), r.get("pct_change"), r.get("turnover"))
          for r in rows])
    conn.commit()
    return len(rows)


def update_stock_price_range(conn, code, rows):
    """更新 stocks 表的价格范围和最新价"""
    if not rows:
        return
    conn.execute("""
        UPDATE stocks SET
            history_start = CASE
                WHEN history_start IS NULL OR history_start > ? THEN ?
                ELSE history_start END,
            history_end   = ?,
            price_latest  = ?,
            updated_at    = datetime('now','localtime')
        WHERE code = ?
    """, (rows[0]["trade_date"], rows[0]["trade_date"],
          rows[-1]["trade_date"], rows[-1]["close"], code))
    conn.commit()


# ─────────────────────────────────────────────────────────────────
# 单只股票同步
# ─────────────────────────────────────────────────────────────────
def sync_one(conn, code, market, mode="init", use_sina_today=False):
    cur = conn.cursor()

    if mode == "init":
        start_date = HISTORY_START
        rows = fetch_kline(code, market, start_date)
        insert_daily_prices(conn, code, rows)
        update_stock_price_range(conn, code, rows)
    else:
        # daily：从上次最新日期的次日开始
        cur.execute("SELECT history_end FROM stocks WHERE code=?", (code,))
        row  = cur.fetchone()
        last = row[0] if row and row[0] else None
        today_dash = datetime.today().strftime("%Y-%m-%d")
        today = datetime.today().strftime("%Y%m%d")

        # 如果今天是交易日且使用新浪接口
        if use_sina_today and last and last >= today_dash:
            # 已经有今天的数据，用新浪接口更新今天的价格
            sina_data = get_price_sina_batch([code])
            if code in sina_data:
                rows = [sina_data[code]]
                insert_daily_prices(conn, code, rows)
                update_stock_price_range(conn, code, rows)
            return

        if last:
            if last >= today_dash:
                # 当天盘中已写入过日K时，继续刷新今天这根K线，直到收盘定稿。
                start_date = today
            else:
                next_day = (datetime.strptime(last, "%Y-%m-%d")
                            + timedelta(days=1)).strftime("%Y%m%d")
                start_date = next_day
        else:
            start_date = HISTORY_START

        rows = fetch_kline(code, market, start_date)
        insert_daily_prices(conn, code, rows)
        update_stock_price_range(conn, code, rows)

    time.sleep(REQUEST_INTERVAL + random.uniform(0, 0.5))  # 添加随机延迟


# ─────────────────────────────────────────────────────────────────
# 批量执行框架
# ─────────────────────────────────────────────────────────────────
def run_batch(conn, stock_rows, mode, sync_type, new_stocks=0, use_sina_today=False):
    t0     = time.time()
    total  = len(stock_rows)
    ok, fail = 0, 0
    fail_codes = []

    # 如果是daily模式且使用新浪接口，批量获取今日价格
    if mode == "daily" and use_sina_today:
        log.info("使用新浪接口批量获取今日价格...")
        batch_size = 100
        market_by_code = dict(stock_rows)
        all_codes = [code for code, _ in stock_rows]
        today_dash = datetime.today().strftime("%Y-%m-%d")
        today = datetime.today().strftime("%Y%m%d")

        for batch_start in range(0, len(all_codes), batch_size):
            batch_codes = all_codes[batch_start:batch_start + batch_size]
            sina_prices = get_price_sina_batch(batch_codes)

            for code in batch_codes:
                row = sina_prices.get(code)
                if row and row.get("trade_date") == today_dash:
                    try:
                        rows = [row]
                        insert_daily_prices(conn, code, rows)
                        update_stock_price_range(conn, code, rows)
                        ok += 1
                    except Exception as e:
                        fail += 1
                        fail_codes.append(code)
                        log.warning(f"  ❌ {code} 失败: {e}")
                else:
                    try:
                        fallback_rows = fetch_kline(code, market_by_code[code], today)
                        if fallback_rows and fallback_rows[-1]["trade_date"] == today_dash:
                            rows = [fallback_rows[-1]]
                            insert_daily_prices(conn, code, rows)
                            update_stock_price_range(conn, code, rows)
                            ok += 1
                        else:
                            fail += 1
                            fail_codes.append(code)
                    except Exception as e:
                        fail += 1
                        fail_codes.append(code)
                        log.warning(f"  ❌ {code} fallback失败: {e}")

            if (batch_start + batch_size) % 500 == 0 or (batch_start + batch_size) >= total:
                elapsed = (time.time() - t0) / 60
                done = ok + fail
                eta = (elapsed / done * (total - done)) if done > 0 else 0
                log.info(f"  进度 {done}/{total}  成功:{ok}  失败:{fail}  "
                        f"已用:{elapsed:.1f}min  预计剩余:{eta:.1f}min")

            time.sleep(0.1)  # 批量请求间隔
    else:
        # 原有逻辑：逐个同步
        for i, (code, market) in enumerate(stock_rows):
            try:
                sync_one(conn, code, market, mode=mode, use_sina_today=False)
                ok += 1
            except Exception as e:
                fail += 1
                fail_codes.append(code)
                log.warning(f"  ❌ {code} 失败: {e}")

            if (i + 1) % 100 == 0 or (i + 1) == total:
                elapsed = (time.time() - t0) / 60
                done    = ok + fail
                eta     = (elapsed / done * (total - done)) if done > 0 else 0
                log.info(f"  进度 {i+1}/{total}  "
                         f"成功:{ok}  失败:{fail}  "
                         f"已用:{elapsed:.1f}min  预计剩余:{eta:.1f}min")

    duration   = time.time() - t0
    fail_ratio = fail / total if total > 0 else 0
    if fail_ratio > FAIL_ALERT_RATIO:
        log.warning(f"⚠️  失败率 {fail_ratio*100:.1f}% 超过阈值！"
                    f"失败代码（前20）: {fail_codes[:20]}")

    conn.execute("""
        INSERT INTO sync_log
            (sync_type, sync_date, total, success, failed, new_stocks, duration_s, note)
        VALUES (?,?,?,?,?,?,?,?)
    """, (sync_type, datetime.today().strftime("%Y-%m-%d"),
          total, ok, fail, new_stocks, round(duration, 1),
          str(fail_codes[:20]) if fail_codes else None))
    conn.commit()

    log.info(f"完成  耗时:{duration/60:.1f}min  成功:{ok}  失败:{fail}")
    return ok, fail


# ─────────────────────────────────────────────────────────────────
# 运行模式
# ─────────────────────────────────────────────────────────────────
def run_test(conn):
    log.info("=" * 55)
    log.info("测试模式（10只股票）")
    test_list = [
        {"code": "000630", "secucode": "000630.SZ", "name": "铜陵有色", "market": "0", "price_latest": None},
        {"code": "601600", "secucode": "601600.SH", "name": "中国铝业", "market": "1", "price_latest": None},
        {"code": "000960", "secucode": "000960.SZ", "name": "锡业股份", "market": "0", "price_latest": None},
        {"code": "002092", "secucode": "002092.SZ", "name": "中泰化学", "market": "0", "price_latest": None},
        {"code": "600346", "secucode": "600346.SH", "name": "恒力石化", "market": "1", "price_latest": None},
        {"code": "601233", "secucode": "601233.SH", "name": "桐昆股份", "market": "1", "price_latest": None},
        {"code": "002042", "secucode": "002042.SZ", "name": "云天化",   "market": "0", "price_latest": None},
        {"code": "000825", "secucode": "000825.SZ", "name": "太钢不锈", "market": "0", "price_latest": None},
        {"code": "002714", "secucode": "002714.SZ", "name": "牧原股份", "market": "0", "price_latest": None},
        {"code": "002299", "secucode": "002299.SZ", "name": "圣农发展", "market": "0", "price_latest": None},
    ]
    upsert_stocks(conn, test_list)
    rows = [(s["code"], s["market"]) for s in test_list]
    run_batch(conn, rows, mode="init", sync_type="test")
    run_status(conn)


def run_init(conn, resume=True):
    log.info("=" * 55)
    log.info("全量初始化开始")

    stocks = fetch_stock_list()
    upsert_stocks(conn, stocks)

    cur = conn.cursor()
    if resume:
        cur.execute("SELECT code FROM stocks WHERE history_end IS NOT NULL")
        done = {r[0] for r in cur.fetchall()}
        log.info(f"断点续传：跳过已完成 {len(done)} 只")
    else:
        done = set()

    cur.execute("SELECT code, market FROM stocks ORDER BY code")
    rows = [(c, m) for c, m in cur.fetchall() if c not in done]
    log.info(f"待处理：{len(rows)} 只")

    run_batch(conn, rows, mode="init", sync_type="init")


def run_daily_sync(conn):
    log.info("=" * 55)
    log.info(f"每日同步  {datetime.today().strftime('%Y-%m-%d %H:%M')}")

    # 判断是否为交易日
    if not is_trading_day():
        log.info("今天不是交易日，跳过同步")
        return

    # 刷新股票列表，自动捕获新上市
    new_list = fetch_stock_list()
    cur = conn.cursor()
    cur.execute("SELECT code FROM stocks")
    existing  = {r[0] for r in cur.fetchall()}
    upsert_stocks(conn, new_list)
    new_codes = {s["code"] for s in new_list} - existing
    if new_codes:
        log.info(f"新上市股票 {len(new_codes)} 只: {sorted(new_codes)[:10]}")

    cur.execute("SELECT code, market FROM stocks ORDER BY code")
    rows = cur.fetchall()
    run_batch(conn, rows, mode="daily", sync_type="daily", new_stocks=len(new_codes), use_sina_today=True)


def run_status(conn):
    cur = conn.cursor()
    print("\n" + "=" * 55)
    print("数据库状态")
    print("=" * 55)

    for label, sql in [
        ("股票总数",      "SELECT COUNT(*) FROM stocks"),
        ("已同步价格",    "SELECT COUNT(*) FROM stocks WHERE history_end IS NOT NULL"),
        ("未同步价格",    "SELECT COUNT(*) FROM stocks WHERE history_end IS NULL"),
        ("价格记录总数",  "SELECT COUNT(*) FROM daily_prices"),
    ]:
        cur.execute(sql)
        print(f"  {label:<14}: {cur.fetchone()[0]:>10,}")

    cur.execute("SELECT MIN(trade_date), MAX(trade_date) FROM daily_prices")
    r = cur.fetchone()
    print(f"  {'价格日期范围':<14}: {r[0]} ~ {r[1]}")

    print("\n最近5次同步:")
    cur.execute("""
        SELECT sync_type, sync_date, total, success, failed, new_stocks, duration_s
        FROM sync_log ORDER BY id DESC LIMIT 5
    """)
    for r in cur.fetchall():
        fail_pct = r[4] / r[2] * 100 if r[2] > 0 else 0
        flag = " ⚠️" if fail_pct > FAIL_ALERT_RATIO * 100 else ""
        print(f"  [{r[0]:6}] {r[1]}  "
              f"total:{r[2]:5}  ok:{r[3]:5}  "
              f"fail:{r[4]:4}({fail_pct:.1f}%)  "
              f"新股:{r[5]}  {r[6]}s{flag}")

    print("\n样本数据（5只）:")
    cur.execute("""
        SELECT code, name, history_start, history_end, price_latest
        FROM stocks
        WHERE history_end IS NOT NULL
        ORDER BY updated_at DESC
        LIMIT 5
    """)
    print(f"  {'代码':<8} {'名称':<10} {'起始日':<12} {'最新日':<12} {'最新价':>8}")
    print("  " + "-" * 52)
    for r in cur.fetchall():
        print(f"  {r[0]:<8} {(r[1] or ''):<10} "
              f"{(r[2] or '-'):<12} {(r[3] or '-'):<12} "
              f"{str(r[4] or '-'):>8}")
    print()


# ─────────────────────────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="股票数据入库系统 — 阶段一")
    parser.add_argument("--test",      action="store_true", help="测试模式（10只股票）")
    parser.add_argument("--init",      action="store_true", help="全量初始化（支持断点续传）")
    parser.add_argument("--sync",      action="store_true", help="每日增量同步")
    parser.add_argument("--status",    action="store_true", help="查看数据库状态")
    parser.add_argument("--no-resume", action="store_true", help="init 时不断点续传，重新全量")
    parser.add_argument("--db",        default=DB_PATH,     help="数据库文件路径")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")   # 64MB 缓存
    init_db(conn)

    if args.test:
        run_test(conn)
    elif args.init:
        run_init(conn, resume=not args.no_resume)
    elif args.sync:
        run_daily_sync(conn)
    else:
        run_status(conn)

    conn.close()


if __name__ == "__main__":
    main()
