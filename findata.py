"""
财务数据入库 — findata_pipeline.py
=====================================
抓取并存储：
  pb_history   — 月度PB历史
  roe_history  — 半年报/年报 ROE历史

使用方式：
  python findata_pipeline.py --stocks "000630,002092"   # 指定股票
  python findata_pipeline.py --index 399395             # 某指数全部成分股
  python findata_pipeline.py --all                      # stocks表全量（慢）
  python findata_pipeline.py --status                   # 查看数据状态
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

DB_PATH          = "stock_data.db"
REQUEST_INTERVAL = 0.35
RETRY_INTERVAL   = 5.0
MAX_RETRIES      = 3
FINDATA_URL      = "https://datacenter.eastmoney.com/securities/api/data/v1/get"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.eastmoney.com/",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────────────
def make_session():
    s = requests.Session()
    retry = Retry(total=MAX_RETRIES, backoff_factor=1,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(HEADERS)
    return s

SESSION = make_session()

def safe_get(url, params, timeout=15):
    for attempt in range(MAX_RETRIES):
        try:
            resp = SESSION.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                log.warning(f"限流，等待{RETRY_INTERVAL}s")
                time.sleep(RETRY_INTERVAL)
                continue
            if not resp.text or len(resp.text) < 10:
                time.sleep(RETRY_INTERVAL)
                continue
            return resp
        except Exception as e:
            log.warning(f"请求失败 attempt={attempt+1}: {e}")
            time.sleep(RETRY_INTERVAL)
    raise RuntimeError(f"请求失败{MAX_RETRIES}次")


# ─────────────────────────────────────────────────────────────────
# 建表
# ─────────────────────────────────────────────────────────────────
def init_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pb_history (
            code        TEXT  NOT NULL,
            trade_date  DATE  NOT NULL,
            pb          REAL,
            PRIMARY KEY (code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS roe_history (
            code          TEXT  NOT NULL,
            report_date   DATE  NOT NULL,
            period_name   TEXT,        -- '中报' | '年报'
            roe           REAL,
            netprofit     REAL,
            notice_date   DATE,        -- 实际披露日
            PRIMARY KEY (code, report_date)
        )
    """)
    # 在stocks表加PB/ROE的起止日期字段（如果没有的话）
    for col in ["pb_start", "pb_end", "roe_start", "roe_end"]:
        try:
            conn.execute(f"ALTER TABLE stocks ADD COLUMN {col} DATE")
        except Exception:
            pass  # 已存在
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pb_code_date  ON pb_history(code, trade_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_roe_code_date ON roe_history(code, report_date)")
    conn.commit()
    log.info("财务数据表就绪")


# ─────────────────────────────────────────────────────────────────
# 抓取
# ─────────────────────────────────────────────────────────────────
def fetch_pb(secucode, page_size=300):
    """月度PB历史"""
    params = {
        "reportName": "RPT_CUSTOM_DMSK_TREND",
        "columns":    "SECUCODE,TRADE_DATE,INDICATOR_VALUE",
        "filter":     f'(SECUCODE="{secucode}")(INDICATORTYPE=2)(DATETYPE=4)',
        "pageNumber": 1, "pageSize": page_size,
        "sortTypes": "1", "sortColumns": "TRADE_DATE",
        "source": "HSF10", "client": "PC",
    }
    resp = safe_get(FINDATA_URL, params)
    rows = resp.json().get("result", {}).get("data", []) or []
    return [
        {"trade_date": r["TRADE_DATE"][:10], "pb": r["INDICATOR_VALUE"]}
        for r in rows if r.get("INDICATOR_VALUE") is not None
    ]


def fetch_roe(secucode):
    """全量半年报+年报ROE，自动翻页"""
    all_rows, page = [], 1
    while True:
        params = {
            "reportName": "RPT_F10_FINANCE_DUPONT",
            "columns":    "SECUCODE,REPORT_DATE,REPORT_DATE_NAME,ROE,NETPROFIT,NOTICE_DATE",
            "filter":     f'(SECUCODE="{secucode}")',
            "pageNumber": page, "pageSize": 50,
            "sortTypes": "-1", "sortColumns": "REPORT_DATE",
            "source": "HSF10", "client": "PC",
        }
        resp   = safe_get(FINDATA_URL, params)
        result = resp.json().get("result") or {}
        rows   = result.get("data", []) or []
        total  = result.get("count", 0)
        all_rows.extend(rows)
        if len(all_rows) >= total or not rows:
            break
        page += 1
        time.sleep(REQUEST_INTERVAL)
    return [
        {
            "report_date": r["REPORT_DATE"][:10],
            "period_name": r.get("REPORT_DATE_NAME", ""),
            "roe":         r.get("ROE"),
            "netprofit":   r.get("NETPROFIT"),
            "notice_date": (r.get("NOTICE_DATE") or "")[:10] or None,
        }
        for r in all_rows
        if r.get("REPORT_DATE_NAME") in ("中报", "年报")
    ]


# ─────────────────────────────────────────────────────────────────
# 写入
# ─────────────────────────────────────────────────────────────────
def insert_pb(conn, code, rows):
    if not rows: return
    conn.executemany(
        "INSERT OR IGNORE INTO pb_history (code, trade_date, pb) VALUES (?,?,?)",
        [(code, r["trade_date"], r["pb"]) for r in rows]
    )
    conn.commit()

def insert_roe(conn, code, rows):
    if not rows: return
    conn.executemany("""
        INSERT OR REPLACE INTO roe_history
            (code, report_date, period_name, roe, netprofit, notice_date)
        VALUES (?,?,?,?,?,?)
    """, [(code, r["report_date"], r["period_name"],
           r["roe"], r["netprofit"], r["notice_date"]) for r in rows])
    conn.commit()

def update_ranges(conn, code, pb_rows, roe_rows):
    sets, params = [], []
    if pb_rows:
        dates = [r["trade_date"] for r in pb_rows]
        sets += ["pb_start=?", "pb_end=?"]
        params += [min(dates), max(dates)]
    if roe_rows:
        dates = [r["report_date"] for r in roe_rows]
        sets += ["roe_start=?", "roe_end=?"]
        params += [min(dates), max(dates)]
    if not sets: return
    sets += ["updated_at=datetime('now','localtime')"]
    params.append(code)
    conn.execute(f"UPDATE stocks SET {', '.join(sets)} WHERE code=?", params)
    conn.commit()


# ─────────────────────────────────────────────────────────────────
# 同步单只股票
# ─────────────────────────────────────────────────────────────────
def sync_one(conn, code, secucode, force=False):
    cur = conn.cursor()
    today = datetime.today()

    # PB：检查是否需要更新
    cur.execute("SELECT pb_end FROM stocks WHERE code=?", (code,))
    row    = cur.fetchone()
    pb_end = row[0] if row and row[0] else None
    if force or pb_end is None:
        need_pb = True
    else:
        days_old = (today - datetime.strptime(pb_end, "%Y-%m-%d")).days
        need_pb  = days_old >= 28   # 月度数据，超28天更新

    pb_rows = []
    if need_pb:
        pb_rows = fetch_pb(secucode)
        insert_pb(conn, code, pb_rows)
        time.sleep(REQUEST_INTERVAL)

    # ROE：检查是否需要更新
    cur.execute("SELECT roe_end FROM stocks WHERE code=?", (code,))
    row     = cur.fetchone()
    roe_end = row[0] if row and row[0] else None
    if force or roe_end is None:
        need_roe = True
    else:
        days_old    = (today - datetime.strptime(roe_end, "%Y-%m-%d")).days
        in_season   = today.month in {1, 4, 8, 10}
        need_roe    = days_old >= (30 if in_season else 90)

    roe_rows = []
    if need_roe:
        roe_rows = fetch_roe(secucode)
        insert_roe(conn, code, roe_rows)
        time.sleep(REQUEST_INTERVAL)

    update_ranges(conn, code, pb_rows, roe_rows)
    return bool(pb_rows or roe_rows)


# ─────────────────────────────────────────────────────────────────
# 批量
# ─────────────────────────────────────────────────────────────────
def run_batch(conn, stock_list, force=False):
    """stock_list: [(code, secucode), ...]"""
    total = len(stock_list)
    ok, fail, skip = 0, 0, 0
    t0 = time.time()

    for i, (code, secucode) in enumerate(stock_list):
        try:
            updated = sync_one(conn, code, secucode, force=force)
            if updated:
                ok += 1
            else:
                skip += 1
        except Exception as e:
            fail += 1
            log.warning(f"  ❌ {code}: {e}")

        if (i + 1) % 50 == 0 or (i + 1) == total:
            elapsed = (time.time() - t0) / 60
            log.info(f"  进度 {i+1}/{total}  更新:{ok}  跳过:{skip}  失败:{fail}  "
                     f"耗时:{elapsed:.1f}min")

    log.info(f"完成  更新:{ok}  跳过:{skip}  失败:{fail}")


# ─────────────────────────────────────────────────────────────────
# 命令
# ─────────────────────────────────────────────────────────────────
def cmd_by_codes(conn, codes_str, force=False):
    codes = [c.strip() for c in codes_str.split(",") if c.strip()]
    cur   = conn.cursor()
    cur.execute(f"""
        SELECT code, secucode FROM stocks
        WHERE code IN ({','.join('?'*len(codes))})
    """, codes)
    rows = cur.fetchall()
    if not rows:
        log.error("stocks表中找不到这些代码，请先运行 stock_db_pipeline.py --init")
        return
    log.info(f"同步 {len(rows)} 只股票的PB/ROE")
    run_batch(conn, [(r[0], r[1]) for r in rows], force=force)


def cmd_by_index(conn, index_code, force=False):
    cur = conn.cursor()
    cur.execute("""
        SELECT s.code, s.secucode
        FROM index_constituents ic
        JOIN stocks s ON s.code = ic.stock_code
        WHERE ic.index_code = ?
    """, (index_code,))
    rows = cur.fetchall()
    if not rows:
        log.error(f"指数 [{index_code}] 没有成分股数据，请先运行 index_pipeline.py")
        return
    log.info(f"同步 [{index_code}] {len(rows)} 只成分股的PB/ROE")
    run_batch(conn, [(r[0], r[1]) for r in rows], force=force)


def cmd_all(conn, force=False):
    cur = conn.cursor()
    cur.execute("SELECT code, secucode FROM stocks ORDER BY code")
    rows = cur.fetchall()
    log.info(f"全量同步 {len(rows)} 只股票的PB/ROE（预计较长时间）")
    run_batch(conn, [(r[0], r[1]) for r in rows], force=force)


def cmd_status(conn):
    cur = conn.cursor()
    print("\n" + "=" * 55)
    print("财务数据状态")
    print("=" * 55)
    for label, sql in [
        ("有PB数据的股票",  "SELECT COUNT(*) FROM stocks WHERE pb_end IS NOT NULL"),
        ("PB记录总数",      "SELECT COUNT(*) FROM pb_history"),
        ("有ROE数据的股票", "SELECT COUNT(*) FROM stocks WHERE roe_end IS NOT NULL"),
        ("ROE记录总数",     "SELECT COUNT(*) FROM roe_history"),
    ]:
        cur.execute(sql)
        print(f"  {label:<18}: {cur.fetchone()[0]:>8,}")

    cur.execute("SELECT MIN(trade_date), MAX(trade_date) FROM pb_history")
    r = cur.fetchone()
    print(f"  {'PB日期范围':<18}: {r[0]} ~ {r[1]}")

    cur.execute("SELECT MIN(report_date), MAX(report_date) FROM roe_history")
    r = cur.fetchone()
    print(f"  {'ROE日期范围':<18}: {r[0]} ~ {r[1]}")

    print("\n最新数据样本（5只）:")
    cur.execute("""
        SELECT s.code, s.name,
            (SELECT pb FROM pb_history WHERE code=s.code ORDER BY trade_date DESC LIMIT 1) pb,
            (SELECT roe FROM roe_history WHERE code=s.code ORDER BY report_date DESC LIMIT 1) roe,
            s.pb_end, s.roe_end
        FROM stocks s
        WHERE s.pb_end IS NOT NULL
        ORDER BY s.pb_end DESC LIMIT 5
    """)
    print(f"  {'代码':<8} {'名称':<10} {'最新PB':>8} {'最新ROE':>9} {'PB至':<12} {'ROE至':<12}")
    print("  " + "-" * 58)
    for r in cur.fetchall():
        print(f"  {r[0]:<8} {(r[1] or ''):<10} "
              f"{str(round(r[2],2) if r[2] else '-'):>8} "
              f"{str(round(r[3],2) if r[3] else '-'):>9} "
              f"{(r[4] or '-'):<12} {(r[5] or '-'):<12}")
    print()


# ─────────────────────────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="PB/ROE财务数据入库")
    parser.add_argument("--stocks",  metavar="CODES", help="指定股票代码，逗号分隔，如 000630,002092")
    parser.add_argument("--index",   metavar="CODE",  help="同步某指数所有成分股")
    parser.add_argument("--all",     action="store_true", help="全量同步（所有股票）")
    parser.add_argument("--force",   action="store_true", help="强制重新抓取（忽略已有数据）")
    parser.add_argument("--status",  action="store_true", help="查看数据状态")
    parser.add_argument("--db",      default=DB_PATH,     help="数据库路径")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    init_db(conn)

    if args.stocks:
        cmd_by_codes(conn, args.stocks, force=args.force)
    elif args.index:
        cmd_by_index(conn, args.index, force=args.force)
    elif args.all:
        cmd_all(conn, force=args.force)
    elif args.status:
        cmd_status(conn)
    else:
        parser.print_help()

    conn.close()


if __name__ == "__main__":
    main()
