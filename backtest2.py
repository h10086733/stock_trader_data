"""
低PB+ROE亏损策略回测 v2 — backtest2.py
=======================================
基于《点石成金2》《点石成金3》策略升级：

核心逻辑：
  ROE(半年/年报) < 0
  PB < 行业绝对阈值（有色<1.5, 化工<1.0, 养殖<2.0, 航运<1.0, 钢铁石化<1.0...）
  ★★ 牛市过滤：买入时PB > 行业阈值*1.5 则跳过（股价被牛市推高）
  ★★ 跌幅确认：股价距近3年最高点跌幅 > 50%（可选）

新增行业：养殖 / 航运 / 钢铁石化 / 造纸 / 煤炭建材（共8大类）

买入价：ROE披露日后第一个有价格的交易日收盘价
卖出（取先到者）：
  ① ROE连续2季度转正
  ② 涨幅达到100%
  ③ PB回到历史中位数
  ④ 强制截止（未卖出则以最新价计算）

使用方式：
  python backtest2.py --import-stocks          # 导入扩展股票池
  python backtest2.py --fetch-data             # 抓取PB/ROE数据（需网络）
  python backtest2.py --run                    # 全量回测
  python backtest2.py --run --industry 有色金属
  python backtest2.py --run --start 2012-01-01
  python backtest2.py --run --doc-only         # 只回测文档明确提及的股票
  python backtest2.py --report                 # 汇总报告
  python backtest2.py --report --industry 化工
  python backtest2.py --trades                 # 交易明细
  python backtest2.py --status                 # 数据就绪状态
"""

import sqlite3
import argparse
import logging
import sys
import csv
import io
import os
import time
import json
import random
import requests
from datetime import datetime, timedelta, date
from collections import defaultdict

DB_PATH = "stock_data.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# 行业参数（来自文档）
# ─────────────────────────────────────────────────────────────────
INDUSTRY_PARAMS = {
    "有色金属": {
        "pb_threshold":   1.5,   # 文档：有色必须到1.5以下
        "roe_threshold":  0.0,   # ROE < 0
        "drawdown_min":   0.50,  # 建议跌幅 > 50%
        "take_profit":    1.0,   # 100%止盈
        "pb_exit_mult":   4.0,   # PB回到阈值*4倍再卖（有色历史PB高点常达6-8）
        "notes":          "有色必须到1.5以下，被套时间更短",
    },
    "基础化工": {
        "pb_threshold":   1.0,   # 文档：化工到1.0更安全
        "roe_threshold":  0.0,
        "drawdown_min":   0.50,
        "take_profit":    1.0,
        "pb_exit_mult":   3.5,   # 化工PB高点常达3-4
        "notes":          "化工分歧大，但1以下最安全",
    },
    "养殖": {
        "pb_threshold":   2.0,   # 鸡在2左右，猪看头均市值
        "roe_threshold":  0.0,
        "drawdown_min":   0.40,
        "take_profit":    1.5,   # 养殖弹性更大
        "pb_exit_mult":   2.5,
        "notes":          "鸡<2，猪看头均市值（本脚本用pb<3做猪）",
    },
    "航运": {
        "pb_threshold":   1.0,   # 文档：航运肯定1左右
        "roe_threshold":  0.0,
        "drawdown_min":   0.45,
        "take_profit":    1.0,
        "pb_exit_mult":   3.0,
        "notes":          "航运慎选，周期性较不规律",
    },
    "钢铁石化": {
        "pb_threshold":   1.0,
        "roe_threshold":  0.0,
        "drawdown_min":   0.50,
        "take_profit":    1.0,
        "pb_exit_mult":   3.5,
        "notes":          "太钢不锈0.96，桐昆0.9均为底部案例",
    },
    "造纸": {
        "pb_threshold":   1.0,   # 文档：造纸1左右
        "roe_threshold":  0.0,
        "drawdown_min":   0.45,
        "take_profit":    1.0,
        "pb_exit_mult":   3.0,
        "notes":          "造纸1左右",
    },
    "煤炭建材": {
        "pb_threshold":   0.8,   # 文档：兖矿pb=0.75，冀东pb=0.85
        "roe_threshold":  0.0,
        "drawdown_min":   0.50,
        "take_profit":    0.75,  # 成熟期弹性减少
        "pb_exit_mult":   2.5,
        "notes":          "成熟期行业，弹性减少（文档3建议谨慎）",
    },
    "其他": {
        "pb_threshold":   1.5,
        "roe_threshold":  0.0,
        "drawdown_min":   0.50,
        "take_profit":    1.0,
        "pb_exit_mult":   3.5,
        "notes":          "其他行业，参数保守",
    },
}

# 文档明确提及的核心案例（优先回测分析）
DOC_MENTIONED = {
    "000807",  # 云铝股份
    "002267",  # 山东海化
    "600096",  # 云天化
    "002092",  # 中泰化学
    "002068",  # 黑猫股份
    "601233",  # 桐昆股份
    "601071",  # 中盐化工
    "600497",  # 驰宏锌锗
    "000878",  # 云南铜业
    "000630",  # 铜陵有色
    "600456",  # 宝钛股份
    "600111",  # 北方稀土
    "002466",  # 天齐锂业
    "000547",  # 山东黄金
    "000975",  # 银泰黄金（估）
    "603993",  # 洛阳钼业
    "000893",  # 益生股份
    "600975",  # 新五丰
    "002714",  # 牧原股份
    "002100",  # 天邦食品
    "002234",  # 民和股份
    "601919",  # 中远海控
    "600026",  # 中远海能
    "601975",  # 招商南油
    "000825",  # 太钢不锈
    "600346",  # 恒力石化
    "002493",  # 荣盛石化
    "002078",  # 太阳纸业
    "600966",  # 博汇纸业
    "600188",  # 兖矿能源
    "601898",  # 中煤能源
    "601088",  # 中国神华
    "000401",  # 冀东水泥
    "600585",  # 海螺水泥
}

# 内嵌股票池 CSV（避免依赖外部文件）
STOCK_POOL_CSV = """stock_code,stock_name,sector,parent_sector,pb_threshold,doc_mentioned
000807,云铝股份,铝,有色金属,1.5,1
601600,中国铝业,铝,有色金属,1.5,0
000612,焦作万方,铝,有色金属,1.5,0
000878,云南铜业,铜,有色金属,1.5,1
000630,铜陵有色,铜,有色金属,1.5,1
600362,江西铜业,铜,有色金属,1.5,0
601899,紫金矿业,铜,有色金属,1.5,0
600497,驰宏锌锗,铅锌锡,有色金属,1.5,1
000960,锡业股份,铅锌锡,有色金属,1.5,1
600456,宝钛股份,钛,有色金属,1.5,1
002466,天齐锂业,锂,有色金属,1.5,1
000975,银泰黄金,黄金,有色金属,1.5,1
600547,山东黄金,黄金,有色金属,1.5,1
600111,北方稀土,稀土,有色金属,1.5,1
000831,中钨高新,钨,有色金属,1.5,1
603993,洛阳钼业,钼,有色金属,1.5,1
601168,西部矿业,铜铅锌,有色金属,1.5,0
600600,青岛金王,铝,有色金属,1.5,0
002053,云铜锌业,铜,有色金属,1.5,0
600961,株冶集团,铅锌锡,有色金属,1.5,0
603798,康普顿,铝,有色金属,1.5,0
600096,云天化,化工,基础化工,1.0,1
002267,山东海化,化工,基础化工,1.0,1
002092,中泰化学,化工,基础化工,1.0,1
002068,黑猫股份,化工,基础化工,1.0,1
601233,桐昆股份,化工,基础化工,1.0,1
601071,中盐化工,化工,基础化工,1.0,1
600426,华鲁恒升,化工,基础化工,1.0,0
000830,鲁西化工,化工,基础化工,1.0,0
600409,三友化工,化工,基础化工,1.0,0
600388,龙元建设,化工,基础化工,1.0,0
002648,卫星石化,化工,基础化工,1.0,0
600618,氯碱化工,化工,基础化工,1.0,0
600230,沧州大化,化工,基础化工,1.0,0
002407,多氟多,化工,基础化工,1.0,0
000731,四川美丰,化工,基础化工,1.0,0
002001,新和成,化工,基础化工,1.0,0
600352,浙江龙盛,化工,基础化工,1.0,0
000893,益生股份,养鸡,养殖,2.0,1
002234,民和股份,养鸡,养殖,2.0,1
603609,禾丰股份,养鸡,养殖,2.0,0
002714,牧原股份,养猪,养殖,3.0,1
600975,新五丰,养猪,养殖,3.0,1
002157,正邦科技,养猪,养殖,3.0,0
000876,新希望,养猪,养殖,3.0,0
002100,天邦食品,养猪,养殖,3.0,1
601919,中远海控,航运,航运,1.0,1
600026,中远海能,航运,航运,1.0,1
601975,招商南油,航运,航运,1.0,1
601872,招商轮船,航运,航运,1.0,0
600018,上港集团,航运,航运,1.0,0
600115,东方航空,航运,航运,1.0,0
600029,南方航空,航运,航运,1.0,0
000825,太钢不锈,钢铁,钢铁石化,1.0,1
000932,华菱钢铁,钢铁,钢铁石化,1.0,0
600019,宝钢股份,钢铁,钢铁石化,1.0,0
601005,重庆钢铁,钢铁,钢铁石化,1.0,0
600346,恒力石化,石化,钢铁石化,1.0,1
002493,荣盛石化,石化,钢铁石化,1.0,1
600688,上海石化,石化,钢铁石化,1.0,0
600028,中国石化,石化,钢铁石化,1.0,0
002078,太阳纸业,造纸,造纸,1.0,1
600966,博汇纸业,造纸,造纸,1.0,1
600308,华泰股份,造纸,造纸,1.0,0
600188,兖矿能源,煤炭,煤炭建材,0.8,1
601898,中煤能源,煤炭,煤炭建材,0.8,1
601088,中国神华,煤炭,煤炭建材,0.8,1
000401,冀东水泥,建材,煤炭建材,0.8,1
600585,海螺水泥,建材,煤炭建材,0.8,1
601992,金隅集团,建材,煤炭建材,0.8,0
"""


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ─────────────────────────────────────────────────────────────────
# 建表
# ─────────────────────────────────────────────────────────────────
def init_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS v2_stocks (
            stock_code      TEXT PRIMARY KEY,
            stock_name      TEXT,
            sector          TEXT,
            parent_sector   TEXT,
            pb_threshold    REAL,
            doc_mentioned   INTEGER DEFAULT 0,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS v2_signals (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code      TEXT,
            stock_name      TEXT,
            industry        TEXT,         -- parent_sector
            sector          TEXT,         -- 子行业
            report_date     DATE,
            period_name     TEXT,
            notice_date     DATE,
            roe_value       REAL,
            pb_at_buy       REAL,
            pb_threshold    REAL,
            buy_date        DATE,
            buy_price       REAL,
            drawdown_from_peak REAL,      -- 距近3年高点跌幅（0~1）
            bull_filter_flag INTEGER DEFAULT 0,  -- 1=被牛市过滤
            doc_mentioned   INTEGER DEFAULT 0,
            signal_strength INTEGER,      -- 1-4，满足条件数
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(stock_code, report_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS v2_trades (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id       INTEGER UNIQUE,
            stock_code      TEXT,
            stock_name      TEXT,
            industry        TEXT,
            buy_date        DATE,
            buy_price       REAL,
            sell_date       DATE,
            sell_price      REAL,
            sell_reason     TEXT,
            hold_days       INTEGER,
            return_pct      REAL,
            max_drawdown    REAL,
            max_gain        REAL,
            signal_strength INTEGER,
            doc_mentioned   INTEGER,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    log.info("v2回测表初始化完成")


# ─────────────────────────────────────────────────────────────────
# 导入股票池
# ─────────────────────────────────────────────────────────────────
def import_stocks(conn):
    reader = csv.DictReader(io.StringIO(STOCK_POOL_CSV.strip()))
    rows = list(reader)
    inserted = updated = 0
    for row in rows:
        code = row["stock_code"].strip()
        existing = conn.execute(
            "SELECT 1 FROM v2_stocks WHERE stock_code=?", (code,)
        ).fetchone()
        if existing:
            conn.execute("""
                UPDATE v2_stocks SET stock_name=?,sector=?,parent_sector=?,
                pb_threshold=?,doc_mentioned=? WHERE stock_code=?
            """, (row["stock_name"], row["sector"], row["parent_sector"],
                  float(row["pb_threshold"]), int(row["doc_mentioned"]), code))
            updated += 1
        else:
            conn.execute("""
                INSERT INTO v2_stocks(stock_code,stock_name,sector,parent_sector,pb_threshold,doc_mentioned)
                VALUES(?,?,?,?,?,?)
            """, (code, row["stock_name"], row["sector"], row["parent_sector"],
                  float(row["pb_threshold"]), int(row["doc_mentioned"])))
            inserted += 1
    conn.commit()
    log.info(f"股票池导入完成：新增{inserted}只，更新{updated}只，共{len(rows)}只")

    # 按行业汇总
    cur = conn.execute("""
        SELECT parent_sector, COUNT(*) as cnt, SUM(doc_mentioned) as doc_cnt
        FROM v2_stocks GROUP BY parent_sector ORDER BY parent_sector
    """)
    log.info("行业分布：")
    for r in cur:
        log.info(f"  {r['parent_sector']:12s} {r['cnt']:3d}只（文档提及{r['doc_cnt']}只）")


# ─────────────────────────────────────────────────────────────────
# 从东方财富抓取PB/ROE（复用findata_pipeline逻辑）
# ─────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.eastmoney.com",
}

def get_secid(code):
    return f"1.{code}" if code.startswith(("6", "5")) else f"0.{code}"

FINDATA_URL = "https://datacenter.eastmoney.com/securities/api/data/v1/get"

def code_to_secucode(code):
    """000807 -> 000807.SZ，600519 -> 600519.SH"""
    if code.startswith(("6", "5")):
        return f"{code}.SH"
    return f"{code}.SZ"

def safe_get(url, params, timeout=12, retries=2):
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            if i == retries - 1:
                raise
            time.sleep(1)

def fetch_pb_history(code):
    """抓取月度PB历史（与findata_pipeline保持一致）"""
    secucode = code_to_secucode(code)
    params = {
        "reportName": "RPT_CUSTOM_DMSK_TREND",
        "columns":    "SECUCODE,TRADE_DATE,INDICATOR_VALUE",
        "filter":     f'(SECUCODE="{secucode}")(INDICATORTYPE=2)(DATETYPE=4)',
        "pageNumber": 1,
        "pageSize":   300,
        "sortTypes":  "1",
        "sortColumns":"TRADE_DATE",
        "source":     "HSF10",
        "client":     "PC",
    }
    try:
        r = safe_get(FINDATA_URL, params)
        rows = r.json().get("result", {}).get("data", []) or []
        return [
            (row["TRADE_DATE"][:10], float(row["INDICATOR_VALUE"]))
            for row in rows
            if row.get("TRADE_DATE") and row.get("INDICATOR_VALUE") is not None
        ]
    except Exception as e:
        log.warning(f"fetch_pb {code}: {e}")
        return []

def fetch_roe_history(code):
    """抓取半年/年报ROE历史（与findata_pipeline保持一致）"""
    secucode = code_to_secucode(code)
    all_rows, page = [], 1
    while True:
        params = {
            "reportName": "RPT_F10_FINANCE_DUPONT",
            "columns":    "SECUCODE,REPORT_DATE,REPORT_DATE_NAME,ROE,NETPROFIT,NOTICE_DATE",
            "filter":     f'(SECUCODE="{secucode}")',
            "pageNumber": page,
            "pageSize":   50,
            "sortTypes":  "-1",
            "sortColumns":"REPORT_DATE",
            "source":     "HSF10",
            "client":     "PC",
        }
        try:
            resp   = safe_get(FINDATA_URL, params)
            result = resp.json().get("result") or {}
            rows   = result.get("data", []) or []
            total  = result.get("count", 0)
            all_rows.extend(rows)
            if len(all_rows) >= total or not rows:
                break
            page += 1
            time.sleep(0.3)
        except Exception as e:
            log.warning(f"fetch_roe {code} page{page}: {e}")
            break
    return [
        {
            "report_date": r["REPORT_DATE"][:10],
            "period_name": r.get("REPORT_DATE_NAME", ""),
            "roe":         r.get("ROE"),
            "notice_date": (r.get("NOTICE_DATE") or "")[:10] or None,
        }
        for r in all_rows
        if r.get("REPORT_DATE_NAME") in ("中报", "年报") and r.get("ROE") is not None
    ]

def fetch_price_history(code):
    """抓取日K线（前复权）"""
    secid = get_secid(code)
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": 101,   # 日K
        "fqt": 1,     # 前复权
        "beg": "20080101",
        "end": "20500101",
        "lmt": 5000,
    }
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        data = r.json()
        klines = (data.get("data") or {}).get("klines") or []
        result = []
        for k in klines:
            parts = k.split(",")
            if len(parts) >= 3:
                result.append((parts[0], float(parts[2])))  # (date, close)
        return result
    except Exception as e:
        return []

def save_pb(conn, code, rows):
    for dt, val in rows:
        conn.execute(
            "INSERT OR REPLACE INTO pb_history(code,trade_date,pb) VALUES(?,?,?)",
            (code, dt, val)
        )
    conn.commit()

def save_roe(conn, code, rows):
    for row in rows:
        conn.execute("""
            INSERT OR REPLACE INTO roe_history
            (code,report_date,period_name,roe,notice_date)
            VALUES(?,?,?,?,?)
        """, (code, row["report_date"], row["period_name"], row["roe"], row["notice_date"]))
    conn.commit()

def save_prices(conn, code, rows):
    for dt, close in rows:
        conn.execute("""
            INSERT OR REPLACE INTO daily_prices(code,trade_date,close)
            VALUES(?,?,?)
        """, (code, dt, close))
    conn.commit()

def fetch_data_for_stocks(conn, codes=None, force=False):
    """抓取所有v2_stocks的PB/ROE/价格数据"""
    if codes is None:
        cur = conn.execute("SELECT stock_code, stock_name FROM v2_stocks ORDER BY stock_code")
        stocks = [(r["stock_code"], r["stock_name"]) for r in cur]
    else:
        stocks = [(c, c) for c in codes]

    total = len(stocks)
    log.info(f"准备抓取 {total} 只股票的数据...")

    for i, (code, name) in enumerate(stocks, 1):
        log.info(f"[{i}/{total}] {code} {name}")

        # 检查是否需要更新
        if not force:
            pb_cnt = conn.execute(
                "SELECT COUNT(*) as c FROM pb_history WHERE code=?", (code,)
            ).fetchone()["c"]
            roe_cnt = conn.execute(
                "SELECT COUNT(*) as c FROM roe_history WHERE code=?", (code,)
            ).fetchone()["c"]
            price_cnt = conn.execute(
                "SELECT COUNT(*) as c FROM daily_prices WHERE code=?", (code,)
            ).fetchone()["c"]
            if pb_cnt > 20 and roe_cnt > 4 and price_cnt > 200:
                log.info(f"  已有数据（pb:{pb_cnt} roe:{roe_cnt} price:{price_cnt}），跳过")
                continue

        # 抓PB
        pb_rows = fetch_pb_history(code)
        if pb_rows:
            save_pb(conn, code, pb_rows)
            log.info(f"  PB: {len(pb_rows)}条")
        time.sleep(0.3 + random.random() * 0.2)

        # 抓ROE
        roe_rows = fetch_roe_history(code)
        if roe_rows:
            save_roe(conn, code, roe_rows)
            log.info(f"  ROE: {len(roe_rows)}条")
        time.sleep(0.3 + random.random() * 0.2)

        # 抓价格
        price_rows = fetch_price_history(code)
        if price_rows:
            save_prices(conn, code, price_rows)
            log.info(f"  Price: {len(price_rows)}条")
        time.sleep(0.5 + random.random() * 0.3)

    log.info("数据抓取完成")


# ─────────────────────────────────────────────────────────────────
# 回测核心
# ─────────────────────────────────────────────────────────────────

def get_pb_on_date(conn, code, dt):
    """获取某日期最近的PB值（向前找30天）"""
    r = conn.execute("""
        SELECT pb FROM pb_history
        WHERE code=? AND trade_date <= ? AND trade_date >= date(?,'-30 days')
        ORDER BY trade_date DESC LIMIT 1
    """, (code, dt, dt)).fetchone()
    return r["pb"] if r else None

def get_pb_percentile(conn, code, dt, years=5):
    """计算某日期之前N年PB的分位数分布"""
    start = str(int(dt[:4]) - years) + dt[4:]
    rows = conn.execute("""
        SELECT pb FROM pb_history
        WHERE code=? AND trade_date < ? AND trade_date >= ?
        ORDER BY pb
    """, (code, dt, start)).fetchall()
    pbs = [r["pb"] for r in rows if r["pb"] is not None and r["pb"] > 0]
    if len(pbs) < 12:
        return None
    n = len(pbs)
    return {
        "p10": pbs[int(n * 0.10)],
        "p25": pbs[int(n * 0.25)],
        "p50": pbs[int(n * 0.50)],
        "p75": pbs[int(n * 0.75)],
        "min": pbs[0],
        "max": pbs[-1],
        "count": n,
    }

def get_price_on_or_after(conn, code, dt):
    """获取某日期当天或之后第一个交易日的收盘价"""
    r = conn.execute("""
        SELECT trade_date, close FROM daily_prices
        WHERE code=? AND trade_date >= ? AND close > 0
        ORDER BY trade_date ASC LIMIT 1
    """, (code, dt)).fetchone()
    if r:
        return r["trade_date"], r["close"]
    return None, None

def get_peak_price(conn, code, end_dt, years=3):
    """获取过去N年最高价"""
    start = str(int(end_dt[:4]) - years) + end_dt[4:]
    r = conn.execute("""
        SELECT MAX(close) as peak FROM daily_prices
        WHERE code=? AND trade_date < ? AND trade_date >= ?
    """, (code, end_dt, start)).fetchone()
    return r["peak"] if r and r["peak"] else None

def check_bull_market_filter(conn, code, buy_date, pb_at_buy, pb_threshold):
    """
    牛市过滤：检查买入前12个月内PB是否曾高于阈值*2.0
    文档案例：2015年股价被牛市推高，即使ROE亏损也不能买
    """
    start = str(int(buy_date[:4]) - 1) + buy_date[4:]
    r = conn.execute("""
        SELECT MAX(pb) as max_pb FROM pb_history
        WHERE code=? AND trade_date < ? AND trade_date >= ?
    """, (code, buy_date, start)).fetchone()
    if r and r["max_pb"] and r["max_pb"] > pb_threshold * 2.5:
        return True  # 被牛市推高，过滤
    return False

def get_exit_conditions(conn, code, buy_date, buy_price, pb_threshold,
                        take_profit_pct=1.0, pb_exit_mult=2.5,
                        report_date=None):
    """
    计算卖出条件，返回卖出日期、价格、原因
    """
    # 获取买入后所有价格
    prices = conn.execute("""
        SELECT trade_date, close FROM daily_prices
        WHERE code=? AND trade_date > ?
        ORDER BY trade_date ASC
    """, (code, buy_date)).fetchall()

    if not prices:
        return None, None, "no_data", 0, 0

    max_gain    = 0.0
    max_dd      = 0.0
    peak_hold   = buy_price

    # 获取买入后ROE记录
    roes = conn.execute("""
        SELECT report_date, period_name, roe, notice_date
        FROM roe_history
        WHERE code=? AND notice_date > ?
        ORDER BY notice_date ASC
    """, (code, buy_date)).fetchall()

    # 连续2季度ROE > 5% 才卖（不只是转正，要明确回升）
    # 文档复盘：ROE刚转正就卖往往只赚2-3%，错过主升浪
    ROE_RECOVER_THRESHOLD = 5.0
    roe_recover_date = None
    pos_streak = 0
    for roe_row in roes:
        if roe_row["roe"] and roe_row["roe"] >= ROE_RECOVER_THRESHOLD:
            pos_streak += 1
            if pos_streak >= 2:
                roe_recover_date = roe_row["notice_date"]
                break
        else:
            pos_streak = 0

    pb_exit_threshold = pb_threshold * pb_exit_mult

    for price_row in prices:
        dt    = price_row["trade_date"]
        close = price_row["close"]
        if close <= 0:
            continue

        ret = (close - buy_price) / buy_price

        # 更新统计
        if close > peak_hold:
            peak_hold = close
        dd_from_peak = (peak_hold - close) / peak_hold
        max_dd   = max(max_dd, dd_from_peak)
        max_gain = max(max_gain, ret)

        # 卖出条件1：涨幅达止盈
        if ret >= take_profit_pct:
            return dt, close, "take_profit", max_dd * 100, max_gain * 100

        # 卖出条件2：ROE连续2季度转正（披露日当天或之后第一个交易日）
        if roe_recover_date and dt >= roe_recover_date:
            return dt, close, "roe_recover", max_dd * 100, max_gain * 100

        # 卖出条件3：PB回到历史中位数区域
        pb_now = get_pb_on_date(conn, code, dt)
        if pb_now and pb_now >= pb_exit_threshold:
            return dt, close, "pb_exit", max_dd * 100, max_gain * 100

    # 未触发卖出，以最新价计算
    last = prices[-1]
    close = last["close"]
    ret = (close - buy_price) / buy_price
    return last["trade_date"], close, "end_of_data", max_dd * 100, max_gain * 100


# ─────────────────────────────────────────────────────────────────
# 主回测逻辑
# ─────────────────────────────────────────────────────────────────
def run_backtest(conn, industry_filter=None, start_date="2010-01-01",
                 doc_only=False, min_strength=2):

    init_tables(conn)

    # 清空旧结果
    filter_clause = ""
    params = []
    if industry_filter:
        filter_clause = " AND industry=?"
        params.append(industry_filter)
    conn.execute(f"DELETE FROM v2_signals WHERE 1=1{filter_clause}", params)
    conn.execute(f"DELETE FROM v2_trades WHERE 1=1" +
                 (f" AND signal_id IN (SELECT id FROM v2_signals WHERE industry=?)"
                  if industry_filter else ""),
                 ([industry_filter] if industry_filter else []))
    conn.commit()

    # 取股票池
    sql = "SELECT * FROM v2_stocks"
    sql_params = []
    conditions = []
    if industry_filter:
        conditions.append("parent_sector=?")
        sql_params.append(industry_filter)
    if doc_only:
        conditions.append("doc_mentioned=1")
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    stocks = conn.execute(sql, sql_params).fetchall()

    log.info(f"回测股票数：{len(stocks)}只，起始日期：{start_date}"
             + (f"，行业：{industry_filter}" if industry_filter else "")
             + ("，仅文档提及" if doc_only else ""))

    total_signals = 0
    total_trades  = 0

    for stock in stocks:
        code    = stock["stock_code"]
        name    = stock["stock_name"]
        industry = stock["parent_sector"]
        pb_thr  = stock["pb_threshold"]
        params_ind = INDUSTRY_PARAMS.get(industry, INDUSTRY_PARAMS["其他"])
        doc_ment = stock["doc_mentioned"]

        # 获取ROE历史
        roes = conn.execute("""
            SELECT report_date, period_name, roe, notice_date
            FROM roe_history
            WHERE code=? AND notice_date >= ?
            ORDER BY notice_date ASC
        """, (code, start_date)).fetchall()

        if not roes:
            continue

        for roe_row in roes:
            rd     = roe_row["report_date"]
            pname  = roe_row["period_name"]
            nd     = roe_row["notice_date"]
            roe    = roe_row["roe"]

            # 条件1：ROE < 0
            if roe is None or roe >= 0:
                continue

            # 获取买入日&价格
            buy_date, buy_price = get_price_on_or_after(conn, code, nd)
            if not buy_date or not buy_price:
                continue

            # 获取买入日PB
            pb = get_pb_on_date(conn, code, buy_date)
            if pb is None:
                continue

            # 条件2：PB < 行业阈值
            cond_pb = 1 if pb < pb_thr else 0

            # 牛市过滤（买入前12月PB曾高于阈值*2.5）
            bull_filter = 0
            if cond_pb:
                if check_bull_market_filter(conn, code, buy_date, pb, pb_thr):
                    bull_filter = 1

            # 条件3：股价跌幅 > drawdown_min
            peak = get_peak_price(conn, code, buy_date, years=3)
            drawdown = 0.0
            cond_drawdown = 0
            if peak and peak > 0 and buy_price < peak:
                drawdown = (peak - buy_price) / peak
                if drawdown >= params_ind["drawdown_min"]:
                    cond_drawdown = 1

            # 信号强度
            strength = 1  # ROE < 0 是基础
            if cond_pb:
                strength += 1
            if cond_drawdown:
                strength += 1
            if cond_pb and not bull_filter:
                strength += 0  # 牛市过滤不额外加分，但会标记

            # 跳过强度不足
            if strength < min_strength:
                continue

            # 记录信号（跳过牛市过滤的信号，仍记录但标记）
            pb_stats = get_pb_percentile(conn, code, buy_date)

            try:
                conn.execute("""
                    INSERT OR IGNORE INTO v2_signals
                    (stock_code,stock_name,industry,sector,report_date,period_name,
                     notice_date,roe_value,pb_at_buy,pb_threshold,buy_date,buy_price,
                     drawdown_from_peak,bull_filter_flag,doc_mentioned,signal_strength)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (code, name, industry, stock["sector"], rd, pname, nd, roe,
                      pb, pb_thr, buy_date, buy_price,
                      round(drawdown * 100, 2), bull_filter, doc_ment, strength))
                total_signals += 1
            except Exception as e:
                continue

        conn.commit()

    log.info(f"信号生成完成：{total_signals}个")

    # ── 模拟交易 ──
    signals = conn.execute("""
        SELECT * FROM v2_signals
        WHERE bull_filter_flag=0
        ORDER BY buy_date ASC
    """ + (f" -- industry={industry_filter}" if industry_filter else "")).fetchall()

    # 只处理指定行业
    if industry_filter:
        signals = [s for s in signals if s["industry"] == industry_filter]

    log.info(f"有效信号（排除牛市过滤）：{len(signals)}个，开始模拟交易...")

    for sig in signals:
        industry = sig["industry"]
        params_ind = INDUSTRY_PARAMS.get(industry, INDUSTRY_PARAMS["其他"])

        sell_date, sell_price, sell_reason, max_dd, max_gain = get_exit_conditions(
            conn,
            sig["stock_code"],
            sig["buy_date"],
            sig["buy_price"],
            sig["pb_threshold"],
            take_profit_pct=params_ind["take_profit"],
            pb_exit_mult=params_ind["pb_exit_mult"],
            report_date=sig["report_date"],
        )

        if not sell_date or not sell_price:
            continue

        ret = (sell_price - sig["buy_price"]) / sig["buy_price"] * 100
        hold_days = (datetime.strptime(sell_date, "%Y-%m-%d")
                     - datetime.strptime(sig["buy_date"], "%Y-%m-%d")).days

        try:
            conn.execute("""
                INSERT OR REPLACE INTO v2_trades
                (signal_id,stock_code,stock_name,industry,buy_date,buy_price,
                 sell_date,sell_price,sell_reason,hold_days,return_pct,
                 max_drawdown,max_gain,signal_strength,doc_mentioned)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (sig["id"], sig["stock_code"], sig["stock_name"], industry,
                  sig["buy_date"], sig["buy_price"], sell_date, sell_price,
                  sell_reason, hold_days, round(ret, 2),
                  round(max_dd, 2), round(max_gain, 2),
                  sig["signal_strength"], sig["doc_mentioned"]))
            total_trades += 1
        except Exception as e:
            log.warning(f"交易记录失败 {sig['stock_code']}: {e}")

    conn.commit()
    log.info(f"模拟交易完成：{total_trades}笔")


# ─────────────────────────────────────────────────────────────────
# 报告输出
# ─────────────────────────────────────────────────────────────────
def print_report(conn, industry_filter=None, doc_only=False):
    cond = ""
    params = []
    if industry_filter:
        cond += " AND industry=?"
        params.append(industry_filter)
    if doc_only:
        cond += " AND doc_mentioned=1"

    # ── 整体汇总 ──
    total = conn.execute(f"SELECT COUNT(*) as c FROM v2_trades WHERE 1=1{cond}", params).fetchone()["c"]
    if total == 0:
        print("暂无回测结果，请先运行 --run")
        return

    wins = conn.execute(
        f"SELECT COUNT(*) as c FROM v2_trades WHERE return_pct>0{cond}", params
    ).fetchone()["c"]
    big_wins = conn.execute(
        f"SELECT COUNT(*) as c FROM v2_trades WHERE return_pct>=100{cond}", params
    ).fetchone()["c"]

    stats = conn.execute(f"""
        SELECT AVG(return_pct) as avg_ret,
               AVG(hold_days)  as avg_days,
               AVG(max_drawdown) as avg_dd,
               AVG(max_gain)   as avg_gain,
               MIN(return_pct) as min_ret,
               MAX(return_pct) as max_ret
        FROM v2_trades WHERE 1=1{cond}
    """, params).fetchone()

    print("\n" + "═" * 60)
    print("  低PB+ROE亏损策略回测报告 v2")
    if industry_filter:
        print(f"  行业：{industry_filter}")
    if doc_only:
        print("  仅文档明确提及的股票")
    print("═" * 60)
    print(f"  总交易数:   {total:4d} 笔")
    print(f"  盈利笔数:   {wins:4d} 笔   胜率: {wins/total*100:.1f}%")
    print(f"  翻倍笔数:   {big_wins:4d} 笔   占比: {big_wins/total*100:.1f}%")
    print(f"  平均收益率: {stats['avg_ret']:+.1f}%")
    print(f"  最大收益率: {stats['max_ret']:+.1f}%")
    print(f"  最差收益率: {stats['min_ret']:+.1f}%")
    print(f"  平均持仓:   {stats['avg_days']:.0f} 天 ({stats['avg_days']/365:.1f}年)")
    print(f"  平均最大回撤:{stats['avg_dd']:.1f}%")
    print(f"  平均最大浮盈:{stats['avg_gain']:.1f}%")

    # ── 按行业分组 ──
    print("\n  ── 按行业 ──")
    print(f"  {'行业':10s}  {'笔数':>4s}  {'胜率':>6s}  {'均收益':>8s}  {'翻倍%':>6s}  {'均持天':>6s}")
    print("  " + "-" * 52)
    rows = conn.execute(f"""
        SELECT industry,
               COUNT(*) as n,
               SUM(CASE WHEN return_pct>0 THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN return_pct>=100 THEN 1 ELSE 0 END) as big_wins,
               AVG(return_pct) as avg_ret,
               AVG(hold_days) as avg_days
        FROM v2_trades WHERE 1=1{cond}
        GROUP BY industry ORDER BY avg_ret DESC
    """, params).fetchall()
    for r in rows:
        wr = r["wins"] / r["n"] * 100
        bw = r["big_wins"] / r["n"] * 100
        print(f"  {r['industry']:10s}  {r['n']:4d}  {wr:5.1f}%  {r['avg_ret']:+7.1f}%  "
              f"{bw:5.1f}%  {r['avg_days']:5.0f}天")

    # ── 按卖出原因 ──
    print("\n  ── 卖出原因 ──")
    rows = conn.execute(f"""
        SELECT sell_reason, COUNT(*) as n, AVG(return_pct) as avg_ret
        FROM v2_trades WHERE 1=1{cond}
        GROUP BY sell_reason ORDER BY n DESC
    """, params).fetchall()
    REASON_CN = {
        "take_profit": "达到止盈(+100%)",
        "roe_recover": "ROE连续2季>5%",
        "pb_exit":     "PB回到高位",
        "end_of_data": "数据截止（未卖出）",
    }
    for r in rows:
        reason_cn = REASON_CN.get(r["sell_reason"], r["sell_reason"])
        print(f"  {reason_cn:20s}: {r['n']:3d}笔  均收益{r['avg_ret']:+.1f}%")

    # ── 信号强度 vs 收益率 ──
    print("\n  ── 信号强度 vs 收益率 ──")
    rows = conn.execute(f"""
        SELECT signal_strength, COUNT(*) as n,
               SUM(CASE WHEN return_pct>0 THEN 1 ELSE 0 END)*1.0/COUNT(*)*100 as wr,
               AVG(return_pct) as avg_ret,
               SUM(CASE WHEN return_pct>=100 THEN 1 ELSE 0 END)*1.0/COUNT(*)*100 as big_win_rate
        FROM v2_trades WHERE 1=1{cond}
        GROUP BY signal_strength ORDER BY signal_strength
    """, params).fetchall()
    for r in rows:
        stars = "★" * (r["signal_strength"] or 1)
        print(f"  {stars:4s}(强度{r['signal_strength']}) {r['n']:3d}笔  "
              f"胜率{r['wr']:.1f}%  均收益{r['avg_ret']:+.1f}%  翻倍率{r['big_win_rate']:.1f}%")

    # ── 牛市过滤效果 ──
    bull_filtered = conn.execute(
        f"SELECT COUNT(*) as c FROM v2_signals WHERE bull_filter_flag=1{cond.replace('industry','industry')}", params
    ).fetchone()["c"]
    print(f"\n  ── 牛市过滤 ──")
    print(f"  被过滤信号数（买入前PB曾虚高）: {bull_filtered}个")
    print("  这些信号若买入，往往正处于牛市高位，风险极大")

    # ── 核心行业对比（排除养殖/造纸/煤炭建材）──
    core_cond = cond + " AND industry NOT IN ('养殖','造纸','煤炭建材')"
    core_n = conn.execute(
        f"SELECT COUNT(*) as c FROM v2_trades WHERE 1=1{core_cond}", params
    ).fetchone()["c"]
    if core_n > 0:
        core_w = conn.execute(
            f"SELECT COUNT(*) as c FROM v2_trades WHERE return_pct>0{core_cond}", params
        ).fetchone()["c"]
        core_s = conn.execute(f"""
            SELECT AVG(return_pct) as avg_ret,
                   SUM(CASE WHEN return_pct>=100 THEN 1 ELSE 0 END) as big_wins
            FROM v2_trades WHERE 1=1{core_cond}
        """, params).fetchone()
        print(f"\n  ── 核心行业（有色+化工+航运+钢铁石化）──")
        print(f"  交易数: {core_n}笔  胜率: {core_w/core_n*100:.1f}%  "
              f"均收益: {core_s['avg_ret']:+.1f}%  "
              f"翻倍率: {core_s['big_wins']/core_n*100:.1f}%")

    # ── 精选：核心行业 + 强度≥3 ──
    elite_cond = core_cond + " AND signal_strength>=3"
    elite_n = conn.execute(
        f"SELECT COUNT(*) as c FROM v2_trades WHERE 1=1{elite_cond}", params
    ).fetchone()["c"]
    if elite_n > 0:
        elite_w = conn.execute(
            f"SELECT COUNT(*) as c FROM v2_trades WHERE return_pct>0{elite_cond}", params
        ).fetchone()["c"]
        elite_s = conn.execute(f"""
            SELECT AVG(return_pct) as avg_ret,
                   SUM(CASE WHEN return_pct>=100 THEN 1 ELSE 0 END) as big_wins
            FROM v2_trades WHERE 1=1{elite_cond}
        """, params).fetchone()
        print(f"\n  ── 精选：核心行业 + 信号强度≥3（股价跌>50%确认）──")
        print(f"  交易数: {elite_n}笔  胜率: {elite_w/elite_n*100:.1f}%  "
              f"均收益: {elite_s['avg_ret']:+.1f}%  "
              f"翻倍率: {elite_s['big_wins']/elite_n*100:.1f}%")

    print("\n" + "═" * 60)


def print_trades(conn, industry_filter=None, min_strength=None,
                 doc_only=False, top=200, sort_by="return_pct"):
    """打印详细交易日志（含买卖点位、信号背景）"""
    cond   = ""
    params = []
    if industry_filter:
        cond += " AND t.industry=?"
        params.append(industry_filter)
    if min_strength:
        cond += " AND t.signal_strength>=?"
        params.append(min_strength)
    if doc_only:
        cond += " AND t.doc_mentioned=1"

    order = {"return_pct": "t.return_pct DESC",
             "buy_date":   "t.buy_date ASC",
             "hold_days":  "t.hold_days DESC"}.get(sort_by, "t.return_pct DESC")

    rows = conn.execute(f"""
        SELECT t.*, s.roe_value, s.pb_at_buy, s.pb_threshold,
               s.drawdown_from_peak, s.report_date, s.period_name, s.sector
        FROM v2_trades t
        LEFT JOIN v2_signals s ON t.signal_id = s.id
        WHERE 1=1{cond}
        ORDER BY {order}
        LIMIT {top}
    """, params).fetchall()

    if not rows:
        print("暂无交易记录")
        return

    REASON_CN = {
        "take_profit": "止盈+100%",
        "roe_recover": "ROE连续2季>5%",
        "pb_exit":     "PB回到高位",
        "end_of_data": "持有中(截止)",
    }

    # 按行业分组输出
    by_industry = {}
    for r in rows:
        ind = r["industry"]
        by_industry.setdefault(ind, []).append(r)

    total_ret = sum(r["return_pct"] for r in rows)
    win_n     = sum(1 for r in rows if r["return_pct"] > 0)
    double_n  = sum(1 for r in rows if r["return_pct"] >= 100)

    print("\n" + "═" * 100)
    print(f"  交易日志详情  共{len(rows)}笔  胜率{win_n/len(rows)*100:.1f}%  "
          f"均收益{total_ret/len(rows):+.1f}%  翻倍率{double_n/len(rows)*100:.1f}%")
    print("═" * 100)

    for ind, trades in by_industry.items():
        ind_ret  = sum(t["return_pct"] for t in trades)
        ind_win  = sum(1 for t in trades if t["return_pct"] > 0)
        ind_dbl  = sum(1 for t in trades if t["return_pct"] >= 100)
        print(f"\n┌─ {ind}  {len(trades)}笔  "
              f"胜率{ind_win/len(trades)*100:.0f}%  "
              f"均收益{ind_ret/len(trades):+.1f}%  "
              f"翻倍{ind_dbl}笔")
        print(f"│  {'代码':<8} {'名称':<8} {'子行业':<10} "
              f"{'报告期':<10} {'ROE%':>6} {'买入PB':>6} {'PB阈值':>6} {'跌幅%':>6}  "
              f"{'买入日':<12} {'买价':>7}  "
              f"{'卖出日':<12} {'卖价':>7}  "
              f"{'收益%':>7} {'持仓天':>5}  "
              f"{'最大回撤':>6} {'最大浮盈':>6}  卖出原因  强度")
        print("│  " + "─" * 96)
        for t in trades:
            reason   = REASON_CN.get(t["sell_reason"], t["sell_reason"])
            stars    = "★" * (t["signal_strength"] or 1)
            doc_mark = "◆" if t["doc_mentioned"] else " "
            roe_str  = f"{t['roe_value']:+.1f}" if t["roe_value"] is not None else "  N/A"
            pb_str   = f"{t['pb_at_buy']:.2f}"  if t["pb_at_buy"]  is not None else " N/A"
            pbt_str  = f"{t['pb_threshold']:.1f}" if t["pb_threshold"] is not None else "N/A"
            dd_str   = f"{t['drawdown_from_peak']:.1f}" if t["drawdown_from_peak"] is not None else " N/A"
            period   = (t["report_date"] or "")[:7]
            sector   = (t["sector"] or "")[:8]

            # 收益着色标记
            ret = t["return_pct"]
            if ret >= 100:
                ret_mark = f"+{ret:.1f}%🚀"
            elif ret >= 50:
                ret_mark = f"+{ret:.1f}%↑"
            elif ret > 0:
                ret_mark = f"+{ret:.1f}%"
            else:
                ret_mark = f"{ret:.1f}%↓"

            print(f"│{doc_mark} {t['stock_code']:<8} {t['stock_name']:<8} {sector:<10} "
                  f"{period:<10} {roe_str:>6} {pb_str:>6} {pbt_str:>6} {dd_str:>6}%  "
                  f"{t['buy_date']:<12} {t['buy_price']:>7.2f}  "
                  f"{t['sell_date']:<12} {t['sell_price']:>7.2f}  "
                  f"{ret_mark:>10} {t['hold_days']:>5}天  "
                  f"{t['max_drawdown']:>5.1f}% {t['max_gain']:>6.1f}%  "
                  f"{reason:<12} {stars}")

        print("└" + "─" * 98)

    print(f"\n  图例：◆=文档明确提及  🚀=翻倍  ↑=涨幅>50%  ↓=亏损")
    print(f"  买入PB=实际买入时PB  跌幅%=距近3年高点已跌幅度  最大回撤/浮盈=持仓期间")
    print("═" * 100)


def export_trades_csv(conn, filepath="trade_log.csv", industry_filter=None, min_strength=None):
    """导出交易日志到CSV"""
    cond   = ""
    params = []
    if industry_filter:
        cond += " AND t.industry=?"
        params.append(industry_filter)
    if min_strength:
        cond += " AND t.signal_strength>=?"
        params.append(min_strength)

    rows = conn.execute(f"""
        SELECT t.stock_code, t.stock_name, t.industry, s.sector,
               s.report_date, s.period_name, s.roe_value, s.pb_at_buy, s.pb_threshold,
               s.drawdown_from_peak,
               t.buy_date, t.buy_price,
               t.sell_date, t.sell_price, t.sell_reason,
               t.return_pct, t.hold_days,
               t.max_drawdown, t.max_gain,
               t.signal_strength, t.doc_mentioned
        FROM v2_trades t
        LEFT JOIN v2_signals s ON t.signal_id = s.id
        WHERE 1=1{cond}
        ORDER BY t.industry, t.buy_date
    """, params).fetchall()

    import csv as csv_mod
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv_mod.writer(f)
        writer.writerow([
            "股票代码","股票名称","大类行业","子行业",
            "报告期","期间类型","ROE%","买入PB","PB阈值",
            "距高点跌幅%",
            "买入日期","买入价格",
            "卖出日期","卖出价格","卖出原因",
            "收益率%","持仓天数",
            "最大回撤%","最大浮盈%",
            "信号强度","文档提及"
        ])
        for r in rows:
            reason_cn = {
                "take_profit": "止盈+100%",
                "roe_recover": "ROE连续2季>5%",
                "pb_exit":     "PB回到高位",
                "end_of_data": "持有中",
            }.get(r["sell_reason"], r["sell_reason"])
            writer.writerow([
                r["stock_code"], r["stock_name"], r["industry"], r["sector"] or "",
                r["report_date"], r["period_name"], r["roe_value"], r["pb_at_buy"], r["pb_threshold"],
                r["drawdown_from_peak"],
                r["buy_date"], r["buy_price"],
                r["sell_date"], r["sell_price"], reason_cn,
                round(r["return_pct"], 2), r["hold_days"],
                round(r["max_drawdown"], 2), round(r["max_gain"], 2),
                r["signal_strength"], "是" if r["doc_mentioned"] else ""
            ])
    print(f"已导出 {len(rows)} 条记录到 {filepath}")


    """打印交易明细"""
    cond  = ""
    params = []
    if industry_filter:
        cond += " AND industry=?"
        params.append(industry_filter)
    if min_strength:
        cond += " AND signal_strength>=?"
        params.append(min_strength)
    if doc_only:
        cond += " AND doc_mentioned=1"

    rows = conn.execute(f"""
        SELECT * FROM v2_trades WHERE 1=1{cond}
        ORDER BY return_pct DESC LIMIT {top}
    """, params).fetchall()

    print(f"\n{'代码':8s} {'名称':8s} {'行业':10s} {'买入日':12s} {'买价':>7s} "
          f"{'卖出日':12s} {'卖价':>7s} {'收益%':>7s} {'持天':>5s} {'卖出原因':16s} {'强度'}")
    print("-" * 110)
    for r in rows:
        reason_cn = {
            "take_profit": "止盈+100%",
            "roe_recover": "ROE转正",
            "pb_exit":     "PB中位数",
            "end_of_data": "数据截止",
        }.get(r["sell_reason"], r["sell_reason"])
        stars = "★" * (r["signal_strength"] or 1)
        mark  = "★文档" if r["doc_mentioned"] else ""
        print(f"{r['stock_code']:8s} {r['stock_name']:8s} {r['industry']:10s} "
              f"{r['buy_date']:12s} {r['buy_price']:7.2f} "
              f"{r['sell_date']:12s} {r['sell_price']:7.2f} "
              f"{r['return_pct']:+7.1f}% {r['hold_days']:5d} "
              f"{reason_cn:16s} {stars}{mark}")


def print_status(conn):
    """数据就绪状态检查"""
    try:
        total_stocks = conn.execute("SELECT COUNT(*) as c FROM v2_stocks").fetchone()["c"]
    except:
        print("v2_stocks表不存在，请先运行 --import-stocks")
        return

    print(f"\n{'='*55}")
    print("  数据就绪状态（backtest2）")
    print(f"{'='*55}")
    print(f"  股票池: {total_stocks}只")

    # 分行业数据覆盖
    rows = conn.execute("""
        SELECT s.parent_sector,
               COUNT(*) as total,
               SUM(CASE WHEN pb_cnt.c > 20 THEN 1 ELSE 0 END) as pb_ok,
               SUM(CASE WHEN roe_cnt.c > 4  THEN 1 ELSE 0 END) as roe_ok,
               SUM(CASE WHEN p_cnt.c > 200  THEN 1 ELSE 0 END) as price_ok
        FROM v2_stocks s
        LEFT JOIN (SELECT code, COUNT(*) as c FROM pb_history GROUP BY code) pb_cnt
            ON s.stock_code = pb_cnt.code
        LEFT JOIN (SELECT code, COUNT(*) as c FROM roe_history GROUP BY code) roe_cnt
            ON s.stock_code = roe_cnt.code
        LEFT JOIN (SELECT code, COUNT(*) as c FROM daily_prices GROUP BY code) p_cnt
            ON s.stock_code = p_cnt.code
        GROUP BY s.parent_sector ORDER BY s.parent_sector
    """).fetchall()

    print(f"\n  {'行业':12s}  {'总计':>4s}  {'PB✓':>5s}  {'ROE✓':>5s}  {'价格✓':>5s}")
    print("  " + "-" * 42)
    for r in rows:
        print(f"  {r['parent_sector']:12s}  {r['total']:4d}  "
              f"{r['pb_ok']:4d}  {r['roe_ok']:4d}  {r['price_ok']:4d}")

    # 信号&交易数
    try:
        sig_n = conn.execute("SELECT COUNT(*) as c FROM v2_signals").fetchone()["c"]
        trd_n = conn.execute("SELECT COUNT(*) as c FROM v2_trades").fetchone()["c"]
        flt_n = conn.execute(
            "SELECT COUNT(*) as c FROM v2_signals WHERE bull_filter_flag=1"
        ).fetchone()["c"]
        print(f"\n  信号数: {sig_n}（牛市过滤: {flt_n}）  交易数: {trd_n}")
    except:
        pass

    print(f"{'='*55}")


# ─────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────
def fetch_missing_pb(conn, force=False):
    """只抓PB数据缺失（<20条）的股票"""
    rows = conn.execute("""
        SELECT s.stock_code, s.stock_name, s.parent_sector,
               COALESCE(pb.c, 0) as pb_cnt
        FROM v2_stocks s
        LEFT JOIN (SELECT code, COUNT(*) as c FROM pb_history GROUP BY code) pb
            ON s.stock_code = pb.code
        WHERE COALESCE(pb.c, 0) < 20
        ORDER BY s.parent_sector, s.stock_code
    """).fetchall()

    if not rows:
        log.info("所有股票PB数据已就绪，无需抓取")
        return

    log.info(f"需要补充PB数据的股票：{len(rows)}只")
    for r in rows:
        log.info(f"  {r['stock_code']} {r['stock_name']} ({r['parent_sector']}) 当前{r['pb_cnt']}条")

    log.info("\n开始抓取...")
    for i, r in enumerate(rows, 1):
        code = r["stock_code"]
        log.info(f"[{i}/{len(rows)}] {code} {r['stock_name']}")

        pb_rows = fetch_pb_history(code)
        if pb_rows:
            save_pb(conn, code, pb_rows)
            log.info(f"  PB: {len(pb_rows)}条 ✓")
        else:
            log.warning(f"  PB: 无数据")

        time.sleep(0.5 + random.random() * 0.3)

    log.info("PB补充抓取完成")

def main():
    p = argparse.ArgumentParser(description="低PB+ROE回测 v2")
    p.add_argument("--import-stocks", action="store_true",  help="导入扩展股票池")
    p.add_argument("--fetch-data",    action="store_true",  help="抓取PB/ROE/价格数据")
    p.add_argument("--fetch-pb",      action="store_true",  help="只补充缺失PB数据的股票（快速）")
    p.add_argument("--run",           action="store_true",  help="运行回测")
    p.add_argument("--report",        action="store_true",  help="查看报告")
    p.add_argument("--trades",        action="store_true",  help="查看交易明细")
    p.add_argument("--export",        action="store_true",  help="导出CSV交易日志")
    p.add_argument("--sort-by",       type=str, default="return_pct",
                   choices=["return_pct","buy_date","hold_days"], help="排序方式")
    p.add_argument("--status",        action="store_true",  help="数据就绪状态")
    p.add_argument("--industry",      type=str,             help="过滤行业（如 有色金属）")
    p.add_argument("--start",         type=str, default="2010-01-01", help="回测起始日期")
    p.add_argument("--doc-only",      action="store_true",  help="只处理文档明确提及的股票")
    p.add_argument("--min-strength",  type=int, default=2,  help="最小信号强度（默认2）")
    p.add_argument("--top",           type=int, default=50, help="明细显示条数")
    p.add_argument("--force",         action="store_true",  help="强制重新抓取数据")
    args = p.parse_args()

    conn = get_db()
    init_tables(conn)

    if args.import_stocks:
        import_stocks(conn)

    if args.fetch_data:
        fetch_data_for_stocks(conn, force=args.force)

    if args.fetch_pb:
        fetch_missing_pb(conn, force=args.force)

    if args.run:
        run_backtest(
            conn,
            industry_filter=args.industry,
            start_date=args.start,
            doc_only=args.doc_only,
            min_strength=args.min_strength,
        )

    if args.report:
        print_report(conn, industry_filter=args.industry, doc_only=args.doc_only)

    if args.trades:
        print_trades(conn, industry_filter=args.industry,
                     min_strength=args.min_strength,
                     doc_only=args.doc_only, top=args.top,
                     sort_by=args.sort_by)

    if args.export:
        export_trades_csv(conn,
                          filepath="trade_log.csv",
                          industry_filter=args.industry,
                          min_strength=args.min_strength)

    if args.status:
        print_status(conn)

    conn.close()


if __name__ == "__main__":
    main()


# ─────────────────────────────────────────────────────────────────
# 补充：只抓缺失PB数据的股票
# ─────────────────────────────────────────────────────────────────
