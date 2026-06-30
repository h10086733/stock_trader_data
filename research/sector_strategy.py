"""
自定义行业股票池管理 + 低PB策略扫描 — sector_strategy.py
===========================================================
功能：
  1. 从CSV导入行业股票池（子行业 + 大类）
  2. 维护 sectors / sector_stocks 两张表
  3. 基于股票池进行低PB/ROE策略扫描

数据库新增表：
  sectors       — 行业定义（大类 + 子行业）
  sector_stocks — 行业-股票关联

使用方式：
  python sector_strategy.py --import-csv           # 导入4个CSV文件
  python sector_strategy.py --list                  # 查看所有行业
  python sector_strategy.py --scan                  # 扫描所有行业
  python sector_strategy.py --scan-sector 铜        # 扫描单个子行业
  python sector_strategy.py --scan-sector 有色金属  # 扫描大类
  python sector_strategy.py --date 2020-06-30 --scan  # 历史回测
  python sector_strategy.py --signals               # 查看近期信号
"""

import sqlite3
import csv
import os
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

DB_PATH = "stock_data.db"

# 4个CSV文件路径（放在同目录下即可）
CSV_FILES = [
    ("有色金属", "nonferrous.csv"),
    ("化工",     "chemicals_1.csv"),
    ("化工",     "chemicals_2.csv"),
    ("化工",     "chemicals_3.csv"),
]

# 策略参数：大类 → 阈值
SECTOR_STRATEGY_PARAMS = {
    "有色金属": {
        "pb_threshold":    2.0,
        "roe_threshold":   -5.0,
        "loss_ratio_min":  0.30,
        "consec_loss_warn":6,
    },
    "化工": {
        "pb_threshold":    2.0,
        "roe_threshold":   -2.0,
        "loss_ratio_min":  0.30,
        "consec_loss_warn":6,
    },
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# 建表
# ─────────────────────────────────────────────────────────────────
def init_db(conn):
    # 行业定义
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sectors (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,        -- 子行业名，如 铜/锂/化肥
            parent_name TEXT,                 -- 大类名，如 有色金属/化工
            stock_count INTEGER DEFAULT 0,
            created_at  DATETIME DEFAULT (datetime('now','localtime')),
            UNIQUE(name, parent_name)
        )
    """)

    # 行业-股票关联
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sector_stocks (
            sector_name TEXT NOT NULL,        -- 子行业名
            parent_name TEXT NOT NULL,        -- 大类名
            stock_code  TEXT NOT NULL,        -- 纯数字代码
            stock_name  TEXT,
            PRIMARY KEY (sector_name, stock_code)
        )
    """)

    # 信号表（和 strategy_scanner 共用，但 strategy_id 改用 sector_name）
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sector_signals (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_name  TEXT NOT NULL,       -- 大类：有色金属/化工
            sector_name  TEXT NOT NULL,       -- 子行业：铜/锂/化肥
            stock_code   TEXT NOT NULL,
            stock_name   TEXT,
            signal_date  DATE NOT NULL,
            pb_value     REAL,
            roe_value    REAL,
            loss_ratio   REAL,                -- 子行业内亏损占比
            consec_loss  INTEGER,
            signal_grade TEXT,               -- 强/中/弱
            note         TEXT,
            scanned_at   DATETIME DEFAULT (datetime('now','localtime')),
            UNIQUE(sector_name, stock_code, signal_date)
        )
    """)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_sec_sig_date   ON sector_signals(signal_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sec_sig_stock  ON sector_signals(stock_code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sec_sig_sector ON sector_signals(sector_name)")

    # 确保 pb_history / roe_history 已建表
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pb_history (
            code        TEXT NOT NULL,
            trade_date  DATE NOT NULL,
            pb          REAL,
            PRIMARY KEY (code, trade_date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS roe_history (
            code          TEXT NOT NULL,
            report_date   DATE NOT NULL,
            period_name   TEXT,
            roe           REAL,
            netprofit     REAL,
            notice_date   DATE,
            PRIMARY KEY (code, report_date)
        )
    """)
    for col in ["pb_start","pb_end","roe_start","roe_end"]:
        try:
            conn.execute(f"ALTER TABLE stocks ADD COLUMN {col} DATE")
        except Exception:
            pass

    conn.commit()
    log.info("数据库表就绪")


# ─────────────────────────────────────────────────────────────────
# 导入 CSV
# ─────────────────────────────────────────────────────────────────
def import_csvs(conn, csv_dir="."):
    """
    自动扫描 csv_dir 下所有CSV，格式：行业,股票代码,股票名称
    股票代码格式：000630.SZ 或 000630
    """
    csv_dir = Path(csv_dir)
    files   = list(csv_dir.glob("*.csv"))
    if not files:
        log.error(f"在 {csv_dir} 下没有找到CSV文件")
        return

    all_rows = []
    for fpath in sorted(files):
        log.info(f"读取: {fpath.name}")
        with open(fpath, encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            headers = next(reader, None)
            for row in reader:
                if len(row) < 3:
                    continue
                sub_sector = row[0].strip()
                raw_code   = row[1].strip()
                stock_name = row[2].strip()
                if not sub_sector or not raw_code:
                    continue
                # 解析代码和市场
                if "." in raw_code:
                    code, suffix = raw_code.split(".", 1)
                    market = "1" if suffix.upper() == "SH" else "0"
                    secucode = raw_code.upper()
                else:
                    code     = raw_code.zfill(6)
                    market   = "1" if code.startswith("6") else "0"
                    suffix   = "SH" if market == "1" else "SZ"
                    secucode = f"{code}.{suffix}"

                all_rows.append({
                    "sub_sector": sub_sector,
                    "code":       code.zfill(6),
                    "secucode":   secucode,
                    "name":       stock_name,
                    "market":     market,
                })

    if not all_rows:
        log.error("CSV解析结果为空")
        return

    # 推断大类（有色金属 vs 化工）
    nonferrous_keywords = {"锂","钴","钨","稀土","黄金","铅锌","铜","铝","镍","锌","铅",
                           "钼","锡","锗","铟","银","铂","钯","磁性材料"}
    def infer_parent(sub):
        return "有色金属" if sub in nonferrous_keywords else "化工"

    # 写入 sectors 表
    sectors_seen = {}
    for r in all_rows:
        sub    = r["sub_sector"]
        parent = infer_parent(sub)
        r["parent"] = parent
        if (sub, parent) not in sectors_seen:
            sectors_seen[(sub, parent)] = 0
        sectors_seen[(sub, parent)] += 1

    conn.executemany("""
        INSERT INTO sectors (name, parent_name, stock_count)
        VALUES (?, ?, ?)
        ON CONFLICT(name, parent_name) DO UPDATE SET
            stock_count = excluded.stock_count
    """, [(sub, parent, cnt) for (sub, parent), cnt in sectors_seen.items()])

    # 写入 sector_stocks 表（清空后重导）
    subs = list(sectors_seen.keys())
    placeholders = ",".join("?" * len(subs))
    conn.execute(f"""
        DELETE FROM sector_stocks
        WHERE (sector_name, parent_name) IN (
            SELECT name, parent_name FROM sectors
            WHERE (name, parent_name) IN ({','.join(['(?,?)'] * len(subs))})
        )
    """, [v for pair in subs for v in pair])

    conn.executemany("""
        INSERT OR REPLACE INTO sector_stocks (sector_name, parent_name, stock_code, stock_name)
        VALUES (?, ?, ?, ?)
    """, [(r["sub_sector"], r["parent"], r["code"], r["name"]) for r in all_rows])

    # 同步到 stocks 表（确保后续能抓价格/PB/ROE）
    conn.executemany("""
        INSERT OR IGNORE INTO stocks (code, secucode, name, market)
        VALUES (?, ?, ?, ?)
    """, [(r["code"], r["secucode"], r["name"], r["market"]) for r in all_rows])

    conn.commit()

    # 统计
    log.info(f"\n导入完成：")
    cur = conn.cursor()
    for parent in ["有色金属", "化工"]:
        cur.execute("""
            SELECT COUNT(DISTINCT stock_code), COUNT(DISTINCT sector_name)
            FROM sector_stocks WHERE parent_name=?
        """, (parent,))
        r = cur.fetchone()
        log.info(f"  {parent}：{r[1]}个子行业  {r[0]}只股票")

    log.info(f"\n子行业明细：")
    cur.execute("""
        SELECT parent_name, name, stock_count
        FROM sectors ORDER BY parent_name, name
    """)
    cur_parent = None
    for row in cur.fetchall():
        if row[0] != cur_parent:
            cur_parent = row[0]
            print(f"\n  【{cur_parent}】")
        print(f"    {row[1]:<16} {row[2]} 只")

    print(f"\n  ⚠️  下一步：抓取这些股票的PB和ROE数据")
    print(f"  运行：python findata_pipeline.py --stocks $(python sector_strategy.py --get-codes)")
    print()


# ─────────────────────────────────────────────────────────────────
# 核心：PB / ROE 查询（复用 findata 的逻辑）
# ─────────────────────────────────────────────────────────────────
def get_sector_loss_ratio(conn, sector_name, parent_name, as_of_date):
    """子行业内 ROE<0 的股票占比"""
    cur = conn.cursor()

    # 优先用 notice_date
    cur.execute("""
        SELECT ss.stock_code, r.roe
        FROM sector_stocks ss
        JOIN roe_history r ON r.code = ss.stock_code
        WHERE ss.sector_name = ? AND ss.parent_name = ?
          AND r.notice_date IS NOT NULL
          AND r.notice_date <= ?
          AND r.report_date = (
              SELECT MAX(r2.report_date) FROM roe_history r2
              WHERE r2.code = ss.stock_code
                AND r2.notice_date IS NOT NULL
                AND r2.notice_date <= ?
          )
    """, (sector_name, parent_name, as_of_date, as_of_date))
    rows = cur.fetchall()

    # 回退：report_date + 4个月
    if len(rows) < 2:
        cur.execute("""
            SELECT ss.stock_code, r.roe
            FROM sector_stocks ss
            JOIN roe_history r ON r.code = ss.stock_code
            WHERE ss.sector_name = ? AND ss.parent_name = ?
              AND date(r.report_date, '+4 months') <= ?
              AND r.report_date = (
                  SELECT MAX(r2.report_date) FROM roe_history r2
                  WHERE r2.code = ss.stock_code
                    AND date(r2.report_date, '+4 months') <= ?
              )
        """, (sector_name, parent_name, as_of_date, as_of_date))
        rows = cur.fetchall()

    if not rows:
        return None, 0, 0

    total      = len(rows)
    loss_count = sum(1 for _, roe in rows if roe is not None and roe < 0)
    return loss_count / total, total, loss_count


def get_stock_roe(conn, code, as_of_date):
    cur = conn.cursor()
    cur.execute("""
        SELECT roe, report_date, period_name
        FROM roe_history
        WHERE code = ?
          AND notice_date IS NOT NULL AND notice_date <= ?
        ORDER BY report_date DESC LIMIT 10
    """, (code, as_of_date))
    rows = cur.fetchall()
    if not rows:
        cur.execute("""
            SELECT roe, report_date, period_name
            FROM roe_history
            WHERE code = ?
              AND date(report_date, '+4 months') <= ?
            ORDER BY report_date DESC LIMIT 10
        """, (code, as_of_date))
        rows = cur.fetchall()
    if not rows:
        return None, None, 0
    consec = sum(1 for r in rows if r[0] is not None and r[0] < 0
                 for _ in [None] if rows.index(r) == sum(
                     1 for rr in rows[:rows.index(r)] if rr[0] is not None and rr[0] < 0))
    # 简单版连续亏损计数
    consec = 0
    for r in rows:
        if r[0] is not None and r[0] < 0:
            consec += 1
        else:
            break
    return rows[0][0], rows[0][1], consec


def get_stock_pb(conn, code, as_of_date):
    cur = conn.cursor()
    cur.execute("""
        SELECT pb, trade_date FROM pb_history
        WHERE code=? AND trade_date<=?
        ORDER BY trade_date DESC LIMIT 1
    """, (code, as_of_date))
    row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)


# ─────────────────────────────────────────────────────────────────
# 扫描单个子行业
# ─────────────────────────────────────────────────────────────────
def scan_sector(conn, sector_name, parent_name, as_of_date, params):
    pb_thr         = params["pb_threshold"]
    roe_thr        = params["roe_threshold"]
    loss_ratio_min = params["loss_ratio_min"]
    consec_warn    = params["consec_loss_warn"]

    # 行业集体亏损
    loss_ratio, total_n, loss_n = get_sector_loss_ratio(
        conn, sector_name, parent_name, as_of_date)

    if loss_ratio is None:
        return [], "no_data"

    if loss_ratio < loss_ratio_min:
        return [], "loss_insufficient"

    # 扫描个股
    cur = conn.cursor()
    cur.execute("""
        SELECT stock_code, stock_name FROM sector_stocks
        WHERE sector_name=? AND parent_name=?
    """, (sector_name, parent_name))
    stocks = cur.fetchall()

    signals = []
    for code, name in stocks:
        roe, report_date, consec = get_stock_roe(conn, code, as_of_date)
        if roe is None or roe >= roe_thr:
            continue

        pb, pb_date = get_stock_pb(conn, code, as_of_date)
        if pb is None or pb >= pb_thr:
            continue

        if consec > consec_warn:
            grade = "弱"
            note  = f"连续亏损{consec}期，警惕大周期下行"
        elif pb < pb_thr * 0.7:
            grade = "强"
            note  = f"PB极低({pb:.2f})"
        else:
            grade = "中"
            note  = ""

        signals.append({
            "parent_name":  parent_name,
            "sector_name":  sector_name,
            "stock_code":   code,
            "stock_name":   name,
            "signal_date":  as_of_date,
            "pb_value":     pb,
            "roe_value":    roe,
            "loss_ratio":   loss_ratio,
            "consec_loss":  consec,
            "signal_grade": grade,
            "note":         f"ROE:{roe:.2f}% PB:{pb:.2f} 行业亏损:{loss_ratio*100:.1f}% "
                            f"报告期:{report_date} {note}".strip(),
        })

    return signals, f"{loss_n}/{total_n}={loss_ratio*100:.1f}%"


# ─────────────────────────────────────────────────────────────────
# 命令
# ─────────────────────────────────────────────────────────────────
def cmd_import(conn, csv_dir="."):
    import_csvs(conn, csv_dir)


def cmd_list(conn):
    cur = conn.cursor()
    print(f"\n{'='*60}")
    print("行业股票池")
    print(f"{'='*60}")
    for parent in ["有色金属", "化工"]:
        params = SECTOR_STRATEGY_PARAMS.get(parent, {})
        print(f"\n【{parent}】  PB<{params.get('pb_threshold','?')}  "
              f"ROE<{params.get('roe_threshold','?')}%")
        cur.execute("""
            SELECT name, stock_count FROM sectors
            WHERE parent_name=? ORDER BY name
        """, (parent,))
        for row in cur.fetchall():
            # 看有多少只有PB/ROE数据
            cur.execute("""
                SELECT COUNT(*) FROM sector_stocks ss
                JOIN stocks s ON s.code = ss.stock_code
                WHERE ss.sector_name=? AND s.pb_end IS NOT NULL
            """, (row[0],))
            pb_n = cur.fetchone()[0]
            cur.execute("""
                SELECT COUNT(*) FROM sector_stocks ss
                JOIN stocks s ON s.code = ss.stock_code
                WHERE ss.sector_name=? AND s.roe_end IS NOT NULL
            """, (row[0],))
            roe_n = cur.fetchone()[0]
            flag = " ✅" if pb_n >= row[1] * 0.8 else " ⚠️需抓数据"
            print(f"  {row[0]:<16} {row[1]:>3}只  "
                  f"PB:{pb_n}/{row[1]}  ROE:{roe_n}/{row[1]}{flag}")
    print()


def cmd_scan(conn, as_of_date=None, filter_sector=None):
    if as_of_date is None:
        as_of_date = datetime.today().strftime("%Y-%m-%d")

    print(f"\n{'='*65}")
    print(f"低PB周期股扫描  {as_of_date}")
    print(f"{'='*65}")

    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT parent_name, name FROM sectors
        ORDER BY parent_name, name
    """)
    all_sectors = cur.fetchall()

    if filter_sector:
        all_sectors = [s for s in all_sectors
                       if filter_sector in (s[0], s[1])]

    all_signals = []
    cur_parent  = None

    for parent_name, sector_name in all_sectors:
        params = SECTOR_STRATEGY_PARAMS.get(parent_name)
        if not params:
            continue

        if parent_name != cur_parent:
            cur_parent = parent_name
            print(f"\n  ▶ {parent_name}  "
                  f"(PB<{params['pb_threshold']}  "
                  f"ROE<{params['roe_threshold']}%  "
                  f"行业亏损≥{params['loss_ratio_min']*100:.0f}%)")

        signals, status = scan_sector(
            conn, sector_name, parent_name, as_of_date, params)

        if status == "no_data":
            print(f"    [{sector_name}]  — 无ROE数据")
            continue
        elif status == "loss_insufficient":
            # 静默跳过（大多数子行业在正常期都无信号）
            continue
        else:
            # status = "x/y=z%" 行业亏损情况
            grade_sym = {"强": "★★★", "中": "★★ ", "弱": "★  "}
            if signals:
                print(f"\n    ✅ [{sector_name}]  行业亏损:{status}  → {len(signals)}个信号")
                for s in sorted(signals, key=lambda x: x["pb_value"]):
                    print(f"       {grade_sym.get(s['signal_grade'],''):>4} "
                          f"[{s['stock_code']}] {(s['stock_name'] or ''):<10} "
                          f"PB:{s['pb_value']:.2f}  ROE:{s['roe_value']:.2f}%  "
                          f"{s.get('note','')}")

        # 保存信号
        if signals:
            conn.executemany("""
                INSERT OR REPLACE INTO sector_signals
                    (parent_name, sector_name, stock_code, stock_name,
                     signal_date, pb_value, roe_value, loss_ratio,
                     consec_loss, signal_grade, note)
                VALUES (:parent_name,:sector_name,:stock_code,:stock_name,
                        :signal_date,:pb_value,:roe_value,:loss_ratio,
                        :consec_loss,:signal_grade,:note)
            """, signals)
            conn.commit()
        all_signals.extend(signals)

    if not all_signals:
        print("\n  当前日期无信号（行业亏损占比均未达阈值，或数据不足）")
    else:
        strong = sum(1 for s in all_signals if s["signal_grade"] == "强")
        mid    = sum(1 for s in all_signals if s["signal_grade"] == "中")
        weak   = sum(1 for s in all_signals if s["signal_grade"] == "弱")
        print(f"\n{'='*65}")
        print(f"汇总：{len(all_signals)} 个信号  "
              f"★★★强:{strong}  ★★中:{mid}  ★弱:{weak}")
        print(f"{'='*65}")
    print()


def cmd_signals(conn, days=180):
    cur = conn.cursor()
    cur.execute("""
        SELECT signal_date, parent_name, sector_name,
               stock_code, stock_name, signal_grade,
               pb_value, roe_value, loss_ratio, note
        FROM sector_signals
        WHERE signal_date >= date('now', ?)
        ORDER BY signal_date DESC, signal_grade, pb_value
    """, (f"-{days} days",))
    rows = cur.fetchall()

    print(f"\n{'='*70}")
    print(f"最近{days}天信号（共{len(rows)}条）")
    print(f"{'='*70}")
    grade_sym = {"强": "★★★", "中": "★★ ", "弱": "★  "}
    cur_date = None
    for r in rows:
        if r[0] != cur_date:
            cur_date = r[0]
            print(f"\n  📅 {cur_date}")
        print(f"     {grade_sym.get(r[5],''):>4} [{r[3]}]{(r[4] or ''):<10} "
              f"{r[2]:<12} PB:{r[6]:.2f}  ROE:{r[7]:.2f}%  "
              f"行业亏损:{r[8]*100:.1f}%")
    if not rows:
        print("  暂无记录")
    print()


def cmd_get_codes(conn):
    """输出所有股票代码（供 findata_pipeline 使用）"""
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT stock_code FROM sector_stocks")
    codes = [r[0] for r in cur.fetchall()]
    print(",".join(codes))


# ─────────────────────────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="行业股票池管理 + 低PB策略扫描")
    parser.add_argument("--import-csv",   action="store_true",  help="从当前目录CSV导入行业股票池")
    parser.add_argument("--csv-dir",      default="./data",          help="CSV文件所在目录")
    parser.add_argument("--list",         action="store_true",  help="查看所有行业及数据状态")
    parser.add_argument("--scan",         action="store_true",  help="扫描所有行业信号")
    parser.add_argument("--scan-sector",  metavar="NAME",       help="扫描指定行业（子行业或大类）")
    parser.add_argument("--date",         metavar="YYYY-MM-DD", help="指定扫描日期（历史回测）")
    parser.add_argument("--signals",      action="store_true",  help="查看近期信号")
    parser.add_argument("--days",         type=int, default=180,help="--signals 查看天数")
    parser.add_argument("--get-codes",    action="store_true",  help="输出所有股票代码（供findata使用）")
    parser.add_argument("--db",           default=DB_PATH,      help="数据库路径")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    init_db(conn)

    if args.import_csv:
        cmd_import(conn, args.csv_dir)
    elif args.list:
        cmd_list(conn)
    elif args.scan or args.scan_sector:
        cmd_scan(conn, as_of_date=args.date,
                 filter_sector=args.scan_sector)
    elif args.signals:
        cmd_signals(conn, days=args.days)
    elif args.get_codes:
        cmd_get_codes(conn)
    else:
        parser.print_help()

    conn.close()


if __name__ == "__main__":
    main()
