"""
指数成分股管理 — index.py
=====================================
功能：
  维护指数列表，从中证指数 / 国证指数官网下载成分股XLS，导入数据库

预置指数：
  上证50    000016
  沪深300   000300
  中证500   000905
  中证1000  000852
  中证军工  399967
  申万证券  399707
  中证医疗  399989

使用方式：
  python index.py --list                          # 查看已维护的指数
  python index.py --add 000016 "上证50"          # 新增指数
  python index.py --channel cnindex --add 980092 "国证行业指数"  # 通过国证渠道新增指数
  python index.py --update 000300                 # 更新指定指数成分股
  python index.py --update-all                    # 更新所有指数成分股
  python index.py --init-all                      # 一次性导入所有预置指数
  python index.py --constituents 000300           # 查看指数成分股列表
"""

import sqlite3
import requests
import argparse
import logging
import sys
import io
import time

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
DEFAULT_CHANNEL = "csindex"

# 中证指数官网成分股下载URL模板
# {code} 替换为指数代码，如 000300
CSINDEX_URL_TEMPLATE = (
    "https://oss-ch.csindex.com.cn/static/html/csindex/public/"
    "uploads/file/autofile/cons/{code}cons.xls"
)

# 国证指数官网样本下载URL模板
CNINDEX_URL_TEMPLATE = (
    "https://www.cnindex.com.cn/sample-detail/download?indexcode={code}"
)

COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

DOWNLOAD_CHANNELS = {
    "csindex": {
        "url_template": CSINDEX_URL_TEMPLATE,
        "headers": {
            **COMMON_HEADERS,
            "Referer": "https://www.csindex.com.cn/",
        },
    },
    "cnindex": {
        "url_template": CNINDEX_URL_TEMPLATE,
        "headers": {
            **COMMON_HEADERS,
            "Referer": "https://www.cnindex.com.cn/",
        },
    },
}

# 预置指数列表（code, name, channel）
PRESET_INDICES = [
    ("000016", "上证50",   "csindex"),
    ("000300", "沪深300",  "csindex"),
    ("000905", "中证500",  "csindex"),
    ("000852", "中证1000", "csindex"),
    ("399967", "中证军工", "csindex"),
    ("399707", "申万证券", "csindex"),
    ("399989", "中证医疗", "csindex"),
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
            channel      TEXT,               -- 下载渠道：csindex / cnindex
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

    migrate_indices_schema(conn)
    conn.commit()
    log.info("指数相关表就绪")


def migrate_indices_schema(conn):
    """兼容老库：补充 channel 字段，并为历史记录推断来源"""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(indices)")}
    if "channel" not in cols:
        conn.execute("ALTER TABLE indices ADD COLUMN channel TEXT")

    conn.execute(f"""
        UPDATE indices
        SET channel = CASE
            WHEN channel IS NOT NULL AND TRIM(channel) <> '' THEN channel
            WHEN url_template LIKE '%cnindex.com.cn%' THEN 'cnindex'
            ELSE '{DEFAULT_CHANNEL}'
        END
        WHERE channel IS NULL OR TRIM(channel) = ''
    """)


# ─────────────────────────────────────────────────────────────────
# 下载并解析成分股 XLS
# ─────────────────────────────────────────────────────────────────
def get_channel_config(channel):
    config = DOWNLOAD_CHANNELS.get(channel)
    if not config:
        raise ValueError(
            f"不支持的渠道 [{channel}]，可选: {', '.join(sorted(DOWNLOAD_CHANNELS))}"
        )
    return config


def infer_channel_from_url_template(url_template):
    if url_template and "cnindex.com.cn" in url_template:
        return "cnindex"
    return DEFAULT_CHANNEL


def normalize_stock_code(value):
    if value is None:
        return None

    if isinstance(value, float) and value.is_integer():
        text = str(int(value))
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.upper().endswith((".SH", ".SZ", ".BJ")):
            text = text.rsplit(".", 1)[0]
        if text.endswith(".0"):
            text = text[:-2]

    if not text.isdigit():
        digits = "".join(ch for ch in text if ch.isdigit())
        if len(digits) != 6:
            return None
        text = digits

    code = text.zfill(6)
    return code if len(code) == 6 else None


def infer_exchange(stock_code, exchange_text=""):
    text = str(exchange_text or "").strip().upper()
    raw = str(exchange_text or "").strip()
    if "上交所" in raw or "沪" in raw or text in ("SH", "SSH", "XSHG"):
        return "SH"
    if "深交所" in raw or "深" in raw or text in ("SZ", "SZE", "XSHE"):
        return "SZ"
    return "SH" if stock_code.startswith(("5", "6", "9")) else "SZ"


def parse_weight(value):
    if value is None:
        return None
    text = str(value).strip().rstrip("%")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def find_header_columns(row):
    code_col = name_col = exch_col = weight_col = None

    for idx, cell in enumerate(row):
        val = str(cell or "").strip().replace(" ", "").replace("\n", "")
        if not val:
            continue

        if code_col is None and (
            "成分券代码" in val
            or "样本代码" in val
            or ("代码" in val and "指数" not in val and "日期" not in val)
        ):
            code_col = idx
            continue

        if name_col is None and (
            "成分券名称" in val
            or "样本简称" in val
            or ("名称" in val and "指数" not in val)
        ):
            name_col = idx
            continue

        if exch_col is None and "交易所" in val:
            exch_col = idx
            continue

        if weight_col is None and "权重" in val:
            weight_col = idx

    return code_col, name_col, exch_col, weight_col


def parse_constituent_row(row, code_col, name_col, exch_col, weight_col):
    if code_col is None or code_col >= len(row):
        return None

    stock_code = normalize_stock_code(row[code_col])
    if not stock_code:
        return None

    stock_name = ""
    if name_col is not None and name_col < len(row) and row[name_col] is not None:
        stock_name = str(row[name_col]).strip()

    exch_text = ""
    if exch_col is not None and exch_col < len(row) and row[exch_col] is not None:
        exch_text = str(row[exch_col]).strip()

    item = {
        "stock_code": stock_code,
        "stock_name": stock_name,
        "exchange": infer_exchange(stock_code, exch_text),
    }

    if weight_col is not None and weight_col < len(row):
        weight = parse_weight(row[weight_col])
        if weight is not None:
            item["weight"] = weight

    return item


def download_xls(url, headers):
    """下载XLS文件，返回原始bytes"""
    resp = requests.get(url, headers=headers, timeout=30)
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

            # 自动识别列位置（兼容中证/国证两类格式）
            header_row = 0
            code_col = name_col = exch_col = weight_col = None
            for r in range(min(5, ws.nrows)):
                row = [str(ws.cell_value(r, c)).strip() for c in range(ws.ncols)]
                code_col, name_col, exch_col, weight_col = find_header_columns(row)
                if code_col is not None and name_col is not None:
                    header_row = r
                    break

            # 如果找不到表头，用中证默认列（第3/4/5列）
            if code_col is None:
                code_col, name_col, exch_col, weight_col = 3, 4, 5, None
                header_row = 1
                log.warning(f"  未识别到表头，使用默认列位置 code={code_col} name={name_col}")

            log.info(
                f"  表头行={header_row} 代码列={code_col} 名称列={name_col} "
                f"交易所列={exch_col} 权重列={weight_col}"
            )

            for r in range(header_row + 1, ws.nrows):
                row = [ws.cell_value(r, c) for c in range(ws.ncols)]
                item = parse_constituent_row(row, code_col, name_col, exch_col, weight_col)
                if item:
                    constituents.append(item)
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
            code_col = name_col = exch_col = weight_col = None
            for r_idx, row in enumerate(rows[:5]):
                row = [str(c).strip() if c else "" for c in row]
                code_col, name_col, exch_col, weight_col = find_header_columns(row)
                if code_col is not None and name_col is not None:
                    header_row = r_idx
                    break

            if code_col is None:
                code_col, name_col, exch_col, weight_col = 3, 4, 5, None
                header_row = 1

            for row in rows[header_row + 1:]:
                if not row:
                    continue
                item = parse_constituent_row(row, code_col, name_col, exch_col, weight_col)
                if item:
                    constituents.append(item)
            return constituents
        except Exception as e:
            log.error(f"  openpyxl解析也失败: {e}")

    raise RuntimeError("xlrd 和 openpyxl 都不可用，请先安装：pip install xlrd openpyxl")


# ─────────────────────────────────────────────────────────────────
# 数据库操作
# ─────────────────────────────────────────────────────────────────
def table_exists(conn, table_name):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    ).fetchone()
    return row is not None


def upsert_index(conn, code, name, channel=None, url_template=None):
    """新增或更新指数基本信息"""
    resolved_channel = channel or infer_channel_from_url_template(url_template)
    resolved_url_template = url_template or get_channel_config(resolved_channel)["url_template"]
    conn.execute("""
        INSERT INTO indices (code, name, channel, url_template)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
            name         = excluded.name,
            channel      = COALESCE(excluded.channel, channel),
            url_template = COALESCE(excluded.url_template, url_template)
    """, (code, name, resolved_channel, resolved_url_template))
    conn.commit()
    log.info(f"指数 [{code}] {name} 已写入  渠道:{resolved_channel}")


def save_constituents(conn, index_code, constituents):
    """
    保存成分股（全量替换：先删旧数据，再插入新数据）
    """
    # 删除该指数旧成分股
    conn.execute("DELETE FROM index_constituents WHERE index_code=?", (index_code,))

    # 批量插入新成分股
    conn.executemany("""
        INSERT INTO index_constituents
            (index_code, stock_code, stock_name, exchange, weight, in_date, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now','localtime'))
    """, [(index_code,
           c["stock_code"],
           c["stock_name"],
           c["exchange"],
           c.get("weight"),
           c.get("in_date"))
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
    if not table_exists(conn, "stocks"):
        log.warning("stocks 表不存在，跳过成分股到 stocks 的自动同步")
        return

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
def import_index(conn, index_code, index_name=None, channel=None):
    """
    下载指定指数的成分股XLS并导入数据库
    """
    # 获取URL模板
    cur = conn.cursor()
    cur.execute("SELECT name, channel, url_template FROM indices WHERE code=?", (index_code,))
    row = cur.fetchone()
    if row:
        name = index_name or row[0]
        resolved_channel = channel or row[1] or infer_channel_from_url_template(row[2])
        url_template = row[2] or get_channel_config(resolved_channel)["url_template"]
    else:
        name = index_name or index_code
        resolved_channel = channel or DEFAULT_CHANNEL
        url_template = get_channel_config(resolved_channel)["url_template"]

    upsert_index(conn, index_code, name, resolved_channel, url_template)

    url = url_template.format(code=index_code)
    headers = get_channel_config(resolved_channel)["headers"]
    log.info(f"下载成分股  [{index_code}] {name}")
    log.info(f"  渠道: {resolved_channel}")
    log.info(f"  URL: {url}")

    try:
        content = download_xls(url, headers)
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
        SELECT code, name, channel, constituent_count, last_updated
        FROM indices ORDER BY code
    """)
    rows = cur.fetchall()
    if not rows:
        print("暂无维护的指数，使用 --init-all 导入预置指数")
        return
    print(f"\n{'='*55}")
    print("已维护指数列表")
    print(f"{'='*55}")
    print(f"  {'代码':<10} {'名称':<12} {'渠道':<10} {'成分股数':>8} {'最后更新':<12}")
    print("  " + "-" * 58)
    for r in rows:
        print(
            f"  {r[0]:<10} {r[1]:<12} {(r[2] or DEFAULT_CHANNEL):<10} "
            f"{str(r[3] or '-'):>8} {str(r[4] or '未更新'):<12}"
        )
    print()


def cmd_add(conn, code, name, channel):
    """新增指数并立即导入成分股"""
    log.info(f"新增指数: [{code}] {name}  渠道:{channel}")
    upsert_index(conn, code, name, channel)
    import_index(conn, code, name, channel)
    cmd_constituents(conn, code, limit=10)


def cmd_update(conn, code, channel=None):
    """更新指定指数的成分股"""
    cur = conn.cursor()
    cur.execute("SELECT name FROM indices WHERE code=?", (code,))
    row = cur.fetchone()
    if not row:
        log.error(f"指数 [{code}] 不存在，请先用 --add 添加")
        return
    import_index(conn, code, channel=channel)


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
    for code, name, channel in PRESET_INDICES:
        upsert_index(conn, code, name, channel)
    # 逐个下载成分股
    ok, fail = 0, 0
    for code, name, channel in PRESET_INDICES:
        log.info(f"\n── [{code}] {name} ──")
        success = import_index(conn, code, name, channel)
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
    cur.execute("""
        SELECT name, channel, constituent_count, last_updated
        FROM indices WHERE code=?
    """, (code,))
    idx = cur.fetchone()
    if not idx:
        print(f"指数 [{code}] 不存在")
        return

    print(f"\n{'='*55}")
    print(f"[{code}] {idx[0]}  渠道:{idx[1] or DEFAULT_CHANNEL}  共{idx[2]}只  更新于:{idx[3]}")
    print(f"{'='*55}")

    if table_exists(conn, "stocks"):
        cur.execute("""
            SELECT ic.stock_code, ic.stock_name, ic.exchange, ic.weight,
                   s.price_latest, s.history_end
            FROM index_constituents ic
            LEFT JOIN stocks s ON s.code = ic.stock_code
            WHERE ic.index_code = ?
            ORDER BY ic.stock_code
            LIMIT ?
        """, (code, limit))
    else:
        cur.execute("""
            SELECT ic.stock_code, ic.stock_name, ic.exchange, ic.weight,
                   NULL AS price_latest, NULL AS history_end
            FROM index_constituents ic
            WHERE ic.index_code = ?
            ORDER BY ic.stock_code
            LIMIT ?
        """, (code, limit))

    print(f"  {'代码':<8} {'名称':<12} {'交易所':<6} {'权重%':>7} {'最新价':>8} {'价格至':<12}")
    print("  " + "-" * 60)
    for r in cur.fetchall():
        weight = "-" if r[3] is None else f"{r[3]:.2f}"
        print(f"  {r[0]:<8} {(r[1] or ''):<12} {(r[2] or ''):<6} "
              f"{weight:>7} {str(r[4] or '-'):>8} {str(r[5] or '未同步'):<12}")
    if idx[2] and idx[2] > limit:
        print(f"  ... 共 {idx[2]} 只，显示前 {limit} 只")
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
    parser.add_argument("--channel",       choices=sorted(DOWNLOAD_CHANNELS),
                        help="成分股下载渠道；新增时默认 csindex，国证指数请用 cnindex")
    parser.add_argument("--db",            default=DB_PATH,        help="数据库路径")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    init_db(conn)

    if args.list:
        cmd_list(conn)
    elif args.add:
        cmd_add(conn, args.add[0], args.add[1], args.channel or DEFAULT_CHANNEL)
    elif args.update:
        cmd_update(conn, args.update, args.channel)
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
