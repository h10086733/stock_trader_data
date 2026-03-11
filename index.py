"""
指数成分股管理 — index_pipeline.py
=====================================
功能：
  维护指数列表，从中证指数官网下载成分股XLS，导入数据库

预置指数：
  上证50    000016
  沪深300   000300
  中证500   000905
  中证1000  000852
  中证军工  399967
  申万证券  399707
  中证医疗  399989

使用方式：
  python index_pipeline.py --list                          # 查看已维护的指数
  python index_pipeline.py --add 000016 "上证50"          # 新增指数
  python index_pipeline.py --update 000300                 # 更新指定指数成分股
  python index_pipeline.py --update-all                    # 更新所有指数成分股
  python index_pipeline.py --init-all                      # 一次性导入所有预置指数
  python index_pipeline.py --constituents 000300           # 查看指数成分股列表
"""

import sqlite3
import requests
import argparse
import logging
import sys
import io
import time
from datetime import datetime

# ── 依赖 xlrd（读取老格式.xls）和 openpyxl（读取.xlsx）
try:
    import xlrd
    HAS_XLRD = True
except ImportError:
    HAS_XLRD = False

try:
    import openpyxl
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False

# ─────────────────────────────────────────────────────────────────
# 配置
# ─────────────────────────────────────────────────────────────────
DB_PATH = "stock_data.db"

# 中证指数官网成分股下载URL模板
# {code} 替换为指数代码，如 000300
CSINDEX_URL_TEMPLATE = (
    "https://oss-ch.csindex.com.cn/static/html/csindex/public/"
    "uploads/file/autofile/cons/{code}cons.xls"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.csindex.com.cn/",
}

# 预置指数列表（code, name, url_template_key）
PRESET_INDICES = [
    ("000016", "上证50",   CSINDEX_URL_TEMPLATE),
    ("000300", "沪深300",  CSINDEX_URL_TEMPLATE),
    ("000905", "中证500",  CSINDEX_URL_TEMPLATE),
    ("000852", "中证1000", CSINDEX_URL_TEMPLATE),
    ("399967", "中证军工", CSINDEX_URL_TEMPLATE),
    ("399707", "申万证券", CSINDEX_URL_TEMPLATE),
    ("399989", "中证医疗", CSINDEX_URL_TEMPLATE),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# 建表（在已有 stock_data.db 上扩展）
# ─────────────────────────────────────────────────────────────────
def init_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS indices (
            code         TEXT PRIMARY KEY,   -- 指数代码，如 000300
            name         TEXT NOT NULL,      -- 指数名称，如 沪深300
            url_template TEXT,               -- 成分股下载URL模板
            constituent_count INTEGER,       -- 当前成分股数量
            last_updated DATE,               -- 上次更新成分股的日期
            created_at   DATETIME DEFAULT (datetime('now','localtime'))
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS index_constituents (
            index_code   TEXT NOT NULL,      -- 指数代码
            stock_code   TEXT NOT NULL,      -- 股票代码（纯数字）
            stock_name   TEXT,               -- 股票名称
            exchange     TEXT,               -- 交易所：SH / SZ
            weight       REAL,               -- 权重%（如有）
            in_date      DATE,               -- 纳入日期（如有）
            updated_at   DATETIME DEFAULT (datetime('now','localtime')),
            PRIMARY KEY (index_code, stock_code)
        )
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ic_stock
        ON index_constituents(stock_code)
    """)

    conn.commit()
    log.info("指数相关表就绪")


# ─────────────────────────────────────────────────────────────────
# 下载并解析成分股 XLS
# ─────────────────────────────────────────────────────────────────
def download_xls(url):
    """下载XLS文件，返回原始bytes"""
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.content


def parse_xls(content, index_code):
    """
    解析中证指数成分股XLS
    典型格式（从第2行开始）：
      日期 | 指数代码 | 指数名称 | 成分券代码 | 成分券名称 | 交易所
    返回: [{"stock_code": "600519", "stock_name": "贵州茅台", "exchange": "SH"}, ...]
    """
    constituents = []

    # 优先用 xlrd 读 .xls 格式
    if HAS_XLRD:
        try:
            wb  = xlrd.open_workbook(file_contents=content)
            ws  = wb.sheet_by_index(0)
            log.info(f"  XLS格式，共 {ws.nrows} 行，{ws.ncols} 列")

            # 打印前3行，帮助确认列位置
            for r in range(min(3, ws.nrows)):
                row_vals = [str(ws.cell_value(r, c)) for c in range(ws.ncols)]
                log.info(f"  第{r}行: {row_vals}")

            # 自动识别列位置（找包含"代码"、"名称"、"交易所"的表头行）
            header_row = 0
            code_col = name_col = exch_col = None
            for r in range(min(5, ws.nrows)):
                row = [str(ws.cell_value(r, c)).strip() for c in range(ws.ncols)]
                for c, val in enumerate(row):
                    if "成分券代码" in val or ("代码" in val and "指数" not in val):
                        code_col = c
                    if "成分券名称" in val or ("名称" in val and "指数" not in val):
                        name_col = c
                    if "交易所" in val:
                        exch_col = c
                if code_col is not None:
                    header_row = r
                    break

            # 如果找不到表头，用默认列（通常是第3,4,5列）
            if code_col is None:
                code_col, name_col, exch_col = 3, 4, 5
                header_row = 1
                log.warning(f"  未识别到表头，使用默认列位置 code={code_col} name={name_col}")

            log.info(f"  表头行={header_row} 代码列={code_col} 名称列={name_col} 交易所列={exch_col}")

            for r in range(header_row + 1, ws.nrows):
                code_val = str(ws.cell_value(r, code_col)).strip()
                # 清理代码：去掉.0等浮点后缀，补齐6位
                code_val = code_val.split(".")[0].zfill(6)
                if not code_val or not code_val.isdigit():
                    continue
                name_val = str(ws.cell_value(r, name_col)).strip() if name_col else ""
                exch_val = str(ws.cell_value(r, exch_col)).strip() if exch_col else ""
                # 标准化交易所
                if "上交所" in exch_val or "沪" in exch_val or "SSH" in exch_val.upper():
                    exch = "SH"
                elif "深交所" in exch_val or "深" in exch_val or "SZE" in exch_val.upper():
                    exch = "SZ"
                else:
                    # 根据代码前缀推断
                    exch = "SH" if code_val.startswith("6") else "SZ"

                constituents.append({
                    "stock_code": code_val,
                    "stock_name": name_val,
                    "exchange":   exch,
                })
            return constituents
        except Exception as e:
            log.warning(f"  xlrd解析失败: {e}，尝试openpyxl")

    # 备用：openpyxl 读 .xlsx
    if HAS_OPENPYXL:
        try:
            wb = openpyxl.load_workbook(io.BytesIO(content))
            ws = wb.active
            rows = list(ws.iter_rows(values_only=True))
            log.info(f"  XLSX格式，共 {len(rows)} 行")

            # 找表头行
            header_row = 0
            code_col = name_col = exch_col = None
            for r_idx, row in enumerate(rows[:5]):
                row = [str(c).strip() if c else "" for c in row]
                for c_idx, val in enumerate(row):
                    if "成分券代码" in val or ("代码" in val and "指数" not in val):
                        code_col = c_idx
                    if "成分券名称" in val or ("名称" in val and "指数" not in val):
                        name_col = c_idx
                    if "交易所" in val:
                        exch_col = c_idx
                if code_col is not None:
                    header_row = r_idx
                    break

            if code_col is None:
                code_col, name_col, exch_col = 3, 4, 5
                header_row = 1

            for row in rows[header_row + 1:]:
                if not row or row[code_col] is None:
                    continue
                code_val = str(row[code_col]).strip().split(".")[0].zfill(6)
                if not code_val.isdigit():
                    continue
                name_val = str(row[name_col]).strip() if name_col and row[name_col] else ""
                exch_val = str(row[exch_col]).strip() if exch_col and row[exch_col] else ""
                exch = "SH" if (code_val.startswith("6") or "上" in exch_val) else "SZ"
                constituents.append({
                    "stock_code": code_val,
                    "stock_name": name_val,
                    "exchange":   exch,
                })
            return constituents
        except Exception as e:
            log.error(f"  openpyxl解析也失败: {e}")

    raise RuntimeError("xlrd 和 openpyxl 都不可用，请先安装：pip install xlrd openpyxl")


# ─────────────────────────────────────────────────────────────────
# 数据库操作
# ─────────────────────────────────────────────────────────────────
def upsert_index(conn, code, name, url_template=None):
    """新增或更新指数基本信息"""
    conn.execute("""
        INSERT INTO indices (code, name, url_template)
        VALUES (?, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
            name         = excluded.name,
            url_template = COALESCE(excluded.url_template, url_template)
    """, (code, name, url_template or CSINDEX_URL_TEMPLATE))
    conn.commit()
    log.info(f"指数 [{code}] {name} 已写入")


def save_constituents(conn, index_code, constituents):
    """
    保存成分股（全量替换：先删旧数据，再插入新数据）
    """
    # 删除该指数旧成分股
    conn.execute("DELETE FROM index_constituents WHERE index_code=?", (index_code,))

    # 批量插入新成分股
    conn.executemany("""
        INSERT INTO index_constituents
            (index_code, stock_code, stock_name, exchange, updated_at)
        VALUES (?, ?, ?, ?, datetime('now','localtime'))
    """, [(index_code, c["stock_code"], c["stock_name"], c["exchange"])
          for c in constituents])

    # 更新指数统计
    conn.execute("""
        UPDATE indices SET
            constituent_count = ?,
            last_updated      = date('now','localtime')
        WHERE code = ?
    """, (len(constituents), index_code))

    conn.commit()
    log.info(f"  [{index_code}] 成分股已更新，共 {len(constituents)} 只")


def sync_constituents_to_stocks(conn, index_code):
    """
    将成分股中不在 stocks 表的股票自动补充进去
    （后续 stock_db_pipeline.py --sync 会自动抓取它们的价格）
    """
    conn.execute("""
        INSERT OR IGNORE INTO stocks (code, secucode, name, market)
        SELECT
            ic.stock_code,
            ic.stock_code || CASE ic.exchange WHEN 'SH' THEN '.SH' ELSE '.SZ' END,
            ic.stock_name,
            CASE ic.exchange WHEN 'SH' THEN '1' ELSE '0' END
        FROM index_constituents ic
        WHERE ic.index_code = ?
          AND NOT EXISTS (
              SELECT 1 FROM stocks s WHERE s.code = ic.stock_code
          )
    """, (index_code,))
    new_count = conn.execute(
        "SELECT changes()"
    ).fetchone()[0]
    conn.commit()
    if new_count > 0:
        log.info(f"  [{index_code}] 向 stocks 表补充了 {new_count} 只新股票")


# ─────────────────────────────────────────────────────────────────
# 核心流程：下载 + 解析 + 入库
# ─────────────────────────────────────────────────────────────────
def import_index(conn, index_code, index_name=None):
    """
    下载指定指数的成分股XLS并导入数据库
    """
    # 获取URL模板
    cur = conn.cursor()
    cur.execute("SELECT name, url_template FROM indices WHERE code=?", (index_code,))
    row = cur.fetchone()
    if row:
        name         = index_name or row[0]
        url_template = row[1] or CSINDEX_URL_TEMPLATE
    else:
        name         = index_name or index_code
        url_template = CSINDEX_URL_TEMPLATE
        upsert_index(conn, index_code, name, url_template)

    url = url_template.format(code=index_code)
    log.info(f"下载成分股  [{index_code}] {name}")
    log.info(f"  URL: {url}")

    try:
        content      = download_xls(url)
        constituents = parse_xls(content, index_code)
        if not constituents:
            log.error(f"  [{index_code}] 解析结果为空，请检查XLS格式")
            return False

        log.info(f"  解析完成，共 {len(constituents)} 只成分股")
        log.info(f"  前5只: {[c['stock_code'] + ' ' + c['stock_name'] for c in constituents[:5]]}")

        save_constituents(conn, index_code, constituents)
        sync_constituents_to_stocks(conn, index_code)
        return True

    except requests.HTTPError as e:
        log.error(f"  [{index_code}] 下载失败: {e}")
        return False
    except Exception as e:
        log.error(f"  [{index_code}] 处理失败: {e}")
        return False


# ─────────────────────────────────────────────────────────────────
# CLI 命令
# ─────────────────────────────────────────────────────────────────
def cmd_list(conn):
    """列出所有维护的指数"""
    cur = conn.cursor()
    cur.execute("""
        SELECT code, name, constituent_count, last_updated
        FROM indices ORDER BY code
    """)
    rows = cur.fetchall()
    if not rows:
        print("暂无维护的指数，使用 --init-all 导入预置指数")
        return
    print(f"\n{'='*55}")
    print("已维护指数列表")
    print(f"{'='*55}")
    print(f"  {'代码':<10} {'名称':<12} {'成分股数':>8} {'最后更新':<12}")
    print("  " + "-" * 48)
    for r in rows:
        print(f"  {r[0]:<10} {r[1]:<12} {str(r[2] or '-'):>8} {str(r[3] or '未更新'):<12}")
    print()


def cmd_add(conn, code, name):
    """新增指数并立即导入成分股"""
    log.info(f"新增指数: [{code}] {name}")
    upsert_index(conn, code, name)
    import_index(conn, code, name)
    cmd_constituents(conn, code, limit=10)


def cmd_update(conn, code):
    """更新指定指数的成分股"""
    cur = conn.cursor()
    cur.execute("SELECT name FROM indices WHERE code=?", (code,))
    row = cur.fetchone()
    if not row:
        log.error(f"指数 [{code}] 不存在，请先用 --add 添加")
        return
    import_index(conn, code)


def cmd_update_all(conn):
    """更新所有指数的成分股"""
    cur = conn.cursor()
    cur.execute("SELECT code, name FROM indices ORDER BY code")
    indices = cur.fetchall()
    if not indices:
        log.warning("没有维护的指数，使用 --init-all 先导入预置指数")
        return
    log.info(f"更新所有指数，共 {len(indices)} 个")
    ok, fail = 0, 0
    for code, name in indices:
        success = import_index(conn, code, name)
        if success:
            ok += 1
        else:
            fail += 1
        time.sleep(1)   # 礼貌性间隔
    log.info(f"全部更新完成  成功:{ok}  失败:{fail}")


def cmd_init_all(conn):
    """一次性导入所有预置指数"""
    log.info(f"导入预置指数，共 {len(PRESET_INDICES)} 个")
    # 先写入指数基本信息
    for code, name, url_tpl in PRESET_INDICES:
        upsert_index(conn, code, name, url_tpl)
    # 逐个下载成分股
    ok, fail = 0, 0
    for code, name, _ in PRESET_INDICES:
        log.info(f"\n── [{code}] {name} ──")
        success = import_index(conn, code, name)
        if success:
            ok += 1
        else:
            fail += 1
        time.sleep(1)
    log.info(f"\n预置指数导入完成  成功:{ok}  失败:{fail}")
    cmd_list(conn)


def cmd_constituents(conn, code, limit=20):
    """查看指数成分股"""
    cur = conn.cursor()
    cur.execute("SELECT name, constituent_count, last_updated FROM indices WHERE code=?", (code,))
    idx = cur.fetchone()
    if not idx:
        print(f"指数 [{code}] 不存在")
        return

    print(f"\n{'='*55}")
    print(f"[{code}] {idx[0]}  共{idx[1]}只  更新于:{idx[2]}")
    print(f"{'='*55}")

    cur.execute("""
        SELECT ic.stock_code, ic.stock_name, ic.exchange,
               s.price_latest, s.history_end
        FROM index_constituents ic
        LEFT JOIN stocks s ON s.code = ic.stock_code
        WHERE ic.index_code = ?
        ORDER BY ic.stock_code
        LIMIT ?
    """, (code, limit))

    print(f"  {'代码':<8} {'名称':<12} {'交易所':<6} {'最新价':>8} {'价格至':<12}")
    print("  " + "-" * 50)
    for r in cur.fetchall():
        print(f"  {r[0]:<8} {(r[1] or ''):<12} {(r[2] or ''):<6} "
              f"{str(r[3] or '-'):>8} {str(r[4] or '未同步'):<12}")
    if idx[1] and idx[1] > limit:
        print(f"  ... 共 {idx[1]} 只，显示前 {limit} 只")
    print()


# ─────────────────────────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="指数成分股管理")
    parser.add_argument("--list",          action="store_true",    help="查看所有维护的指数")
    parser.add_argument("--add",           nargs=2,
                        metavar=("CODE", "NAME"),                  help="新增指数并导入成分股，如: --add 000016 上证50")
    parser.add_argument("--update",        metavar="CODE",         help="更新指定指数成分股")
    parser.add_argument("--update-all",    action="store_true",    help="更新所有指数成分股")
    parser.add_argument("--init-all",      action="store_true",    help="导入所有预置指数")
    parser.add_argument("--constituents",  metavar="CODE",         help="查看指数成分股列表")
    parser.add_argument("--db",            default=DB_PATH,        help="数据库路径")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    init_db(conn)

    if args.list:
        cmd_list(conn)
    elif args.add:
        cmd_add(conn, args.add[0], args.add[1])
    elif args.update:
        cmd_update(conn, args.update)
    elif args.update_all:
        cmd_update_all(conn)
    elif args.init_all:
        cmd_init_all(conn)
    elif args.constituents:
        cmd_constituents(conn, args.constituents)
    else:
        parser.print_help()

    conn.close()


if __name__ == "__main__":
    main()
