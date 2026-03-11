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
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────────────────────────────────────────────
# 配置
# ─────────────────────────────────────────────────────────────────
DB_PATH          = "stock_data.db"
LOG_FILE         = "pipeline.log"
REQUEST_INTERVAL = 0.35   # 正常请求间隔（秒）
RETRY_INTERVAL   = 5.0    # 限流后等待时间（秒）
MAX_RETRIES      = 3      # 单次请求最大重试次数
FAIL_ALERT_RATIO = 0.05   # 失败率超过5%时告警
HISTORY_START    = "20100101"

STOCK_LIST_URL = "https://push2delay.eastmoney.com/api/qt/clist/get"
KLINE_URL      = "https://push2his.eastmoney.com/api/qt/stock/kline/get"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.eastmoney.com/",
}

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
        backoff_factor=1,                         # 重试等待：1s, 2s, 4s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://",  adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session

SESSION = make_session()


def safe_get(url, params, timeout=15):
    """带限流检测的 GET，遇到429或空响应自动等待重试"""
    for attempt in range(MAX_RETRIES):
        try:
            resp = SESSION.get(url, params=params, timeout=timeout)
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
            time.sleep(REQUEST_INTERVAL)

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
        INSERT OR IGNORE INTO daily_prices
            (code, trade_date, open, close, high, low, volume, amount, pct_change, turnover)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, [(code, r["trade_date"], r["open"], r["close"], r["high"],
           r["low"], r["volume"], r["amount"], r["pct_change"], r["turnover"])
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
def sync_one(conn, code, market, mode="init"):
    cur = conn.cursor()

    if mode == "init":
        start_date = HISTORY_START
    else:
        # daily：从上次最新日期的次日开始
        cur.execute("SELECT history_end FROM stocks WHERE code=?", (code,))
        row  = cur.fetchone()
        last = row[0] if row and row[0] else None
        if last:
            next_day = (datetime.strptime(last, "%Y-%m-%d")
                        + timedelta(days=1)).strftime("%Y%m%d")
            today = datetime.today().strftime("%Y%m%d")
            if next_day > today:
                return   # 已是最新，跳过
            start_date = next_day
        else:
            start_date = HISTORY_START

    rows = fetch_kline(code, market, start_date)
    insert_daily_prices(conn, code, rows)
    update_stock_price_range(conn, code, rows)
    time.sleep(REQUEST_INTERVAL)


# ─────────────────────────────────────────────────────────────────
# 批量执行框架
# ─────────────────────────────────────────────────────────────────
def run_batch(conn, stock_rows, mode, sync_type, new_stocks=0):
    t0     = time.time()
    total  = len(stock_rows)
    ok, fail = 0, 0
    fail_codes = []

    for i, (code, market) in enumerate(stock_rows):
        try:
            sync_one(conn, code, market, mode=mode)
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
    run_batch(conn, rows, mode="daily", sync_type="daily", new_stocks=len(new_codes))


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
