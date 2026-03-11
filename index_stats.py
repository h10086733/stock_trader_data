"""
行业宽度指标计算 — index_stats_pipeline.py
==========================================
指标逻辑：
  每只成分股当日得分：
    收盘价 >= 近20日最高价 → +1（创20日新高）
    收盘价 <= 近20日最低价 → -1（创20日新低）
    否则 → 0
  行业净值 = Σ得分 / 成分股有效数量    范围：-1 ~ +1

使用方式：
  python index_stats_pipeline.py --calc-today        # 计算今日所有行业指标（每日收盘后）
  python index_stats_pipeline.py --backfill 000300   # 回填指定指数历史净值
  python index_stats_pipeline.py --backfill-all      # 回填所有指数历史净值
  python index_stats_pipeline.py --show 000300       # 查看指数最近净值
  python index_stats_pipeline.py --show-all          # 查看所有指数最新净值

定时任务（在 stock_db_pipeline --sync 完成后执行）：
  35 16 * * 1-5  cd /path && python index_stats_pipeline.py --calc-today >> cron.log 2>&1
"""

import sqlite3
import argparse
import logging
import sys
from datetime import datetime, timedelta

DB_PATH = "stock_data.db"
WINDOW  = 20    # 新高/新低的回望窗口（交易日）

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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS index_daily_stats (
            index_code   TEXT  NOT NULL,
            trade_date   DATE  NOT NULL,
            score_sum    REAL,    -- Σ得分（正负抵消后的总和）
            high_count   INTEGER, -- 创20日新高的股票数
            low_count    INTEGER, -- 创20日新低的股票数
            valid_count  INTEGER, -- 参与计算的有效股票数（有足够历史数据）
            total_count  INTEGER, -- 成分股总数
            net_value    REAL,    -- 净值 = score_sum / valid_count
            PRIMARY KEY (index_code, trade_date)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_ids_date
        ON index_daily_stats(trade_date)
    """)
    conn.commit()
    log.info("index_daily_stats 表就绪")


# ─────────────────────────────────────────────────────────────────
# 核心计算：用纯 SQL 完成，速度快
# ─────────────────────────────────────────────────────────────────
def calc_index_one_day(conn, index_code, trade_date):
    """
    计算指定指数在指定交易日的宽度指标。
    trade_date: 'YYYY-MM-DD'
    返回 dict 或 None（数据不足时）
    """
    cur = conn.cursor()

    # 获取该指数所有成分股
    cur.execute("""
        SELECT stock_code FROM index_constituents
        WHERE index_code = ?
    """, (index_code,))
    stocks = [r[0] for r in cur.fetchall()]
    if not stocks:
        return None

    total_count = len(stocks)
    score_sum   = 0
    high_count  = 0
    low_count   = 0
    valid_count = 0

    # 用 SQL 批量计算所有成分股的20日高低点
    # 先拿到每只股票在 trade_date 当日及之前20个交易日的收盘价
    placeholders = ",".join("?" * total_count)
    cur.execute(f"""
        SELECT
            p.code,
            p.close                              AS today_close,
            MAX(h.close)                         AS high_20,
            MIN(h.close)                         AS low_20,
            COUNT(h.trade_date)                  AS days_count
        FROM daily_prices p
        JOIN daily_prices h
            ON h.code = p.code
           AND h.trade_date <= p.trade_date
           AND h.trade_date >= (
               -- 取 trade_date 之前第20个交易日（含当日共20日）
               SELECT d2.trade_date
               FROM daily_prices d2
               WHERE d2.code = p.code
                 AND d2.trade_date <= p.trade_date
               ORDER BY d2.trade_date DESC
               LIMIT 1 OFFSET {WINDOW - 1}
           )
        WHERE p.code IN ({placeholders})
          AND p.trade_date = ?
          AND p.close IS NOT NULL
        GROUP BY p.code
    """, stocks + [trade_date])

    rows = cur.fetchall()

    for code, today_close, high_20, low_20, days_count in rows:
        if today_close is None or high_20 is None or low_20 is None:
            continue
        if days_count < WINDOW:
            # 历史数据不足20日，跳过（新股等）
            continue

        valid_count += 1
        if today_close >= high_20:
            score_sum  += 1
            high_count += 1
        elif today_close <= low_20:
            score_sum  -= 1
            low_count  += 1

    if valid_count == 0:
        return None

    net_value = round(score_sum / valid_count, 6)

    return {
        "index_code":  index_code,
        "trade_date":  trade_date,
        "score_sum":   score_sum,
        "high_count":  high_count,
        "low_count":   low_count,
        "valid_count": valid_count,
        "total_count": total_count,
        "net_value":   net_value,
    }


def save_stat(conn, stat):
    conn.execute("""
        INSERT OR REPLACE INTO index_daily_stats
            (index_code, trade_date, score_sum, high_count, low_count,
             valid_count, total_count, net_value)
        VALUES (:index_code, :trade_date, :score_sum, :high_count, :low_count,
                :valid_count, :total_count, :net_value)
    """, stat)
    conn.commit()


# ─────────────────────────────────────────────────────────────────
# 获取所有交易日列表（从 daily_prices 推导）
# ─────────────────────────────────────────────────────────────────
def get_trade_dates(conn, start_date=None, end_date=None):
    """
    从 daily_prices 中取所有交易日（用沪深300成分股推算，覆盖完整）
    """
    cur = conn.cursor()
    # 用数据量最多的股票之一来代表交易日历
    cur.execute("""
        SELECT trade_date FROM daily_prices
        WHERE code = (
            SELECT code FROM daily_prices
            GROUP BY code ORDER BY COUNT(*) DESC LIMIT 1
        )
        AND trade_date >= COALESCE(?, '2010-01-01')
        AND trade_date <= COALESCE(?, date('now'))
        ORDER BY trade_date
    """, (start_date, end_date))
    return [r[0] for r in cur.fetchall()]


# ─────────────────────────────────────────────────────────────────
# 命令：计算今日
# ─────────────────────────────────────────────────────────────────
def cmd_calc_today(conn):
    today = datetime.today().strftime("%Y-%m-%d")
    log.info(f"计算今日（{today}）行业宽度指标")

    cur = conn.cursor()
    cur.execute("SELECT code, name FROM indices ORDER BY code")
    indices = cur.fetchall()
    if not indices:
        log.warning("没有维护的指数，请先运行 index_pipeline.py --init-all")
        return

    results = []
    for index_code, index_name in indices:
        stat = calc_index_one_day(conn, index_code, today)
        if stat:
            save_stat(conn, stat)
            results.append((index_code, index_name, stat))
            log.info(f"  [{index_code}] {index_name:<10}  "
                     f"净值:{stat['net_value']:+.4f}  "
                     f"新高:{stat['high_count']}  新低:{stat['low_count']}  "
                     f"有效:{stat['valid_count']}/{stat['total_count']}")
        else:
            log.warning(f"  [{index_code}] {index_name} 今日无数据（可能未收盘或数据未同步）")

    log.info(f"今日计算完成，共 {len(results)} 个指数")


# ─────────────────────────────────────────────────────────────────
# 命令：回填历史（新导入指数 or 重算）
# ─────────────────────────────────────────────────────────────────
def cmd_backfill(conn, index_code, force=False):
    cur = conn.cursor()
    cur.execute("SELECT name FROM indices WHERE code=?", (index_code,))
    row = cur.fetchone()
    if not row:
        log.error(f"指数 [{index_code}] 不存在")
        return
    index_name = row[0]

    # 确定回填起点
    if force:
        start_date = None  # 全量重算
        log.info(f"强制全量回填 [{index_code}] {index_name}")
    else:
        # 从已有数据的最后一天+1开始
        cur.execute("""
            SELECT MAX(trade_date) FROM index_daily_stats
            WHERE index_code = ?
        """, (index_code,))
        last = cur.fetchone()[0]
        if last:
            start_date = (datetime.strptime(last, "%Y-%m-%d")
                          + timedelta(days=1)).strftime("%Y-%m-%d")
            log.info(f"增量回填 [{index_code}] {index_name}，从 {start_date} 开始")
        else:
            start_date = None
            log.info(f"首次回填 [{index_code}] {index_name}，全量历史")

    # 需要至少 WINDOW 天的价格数据才能开始计算
    # 找到成分股中最早有 WINDOW 天数据的日期
    cur.execute("""
        SELECT MIN(start_date) FROM (
            SELECT (
                SELECT trade_date FROM daily_prices
                WHERE code = ic.stock_code
                ORDER BY trade_date ASC
                LIMIT 1 OFFSET ?
            ) AS start_date
            FROM index_constituents ic
            WHERE ic.index_code = ?
              AND ic.stock_code IN (SELECT code FROM stocks WHERE history_end IS NOT NULL)
        ) t
    """, (WINDOW - 1, index_code))
    r = cur.fetchone()
    earliest_calc_date = r[0] if r and r[0] else None

    if start_date is None and earliest_calc_date:
        start_date = earliest_calc_date
    elif start_date is None:
        start_date = "2010-01-01"

    # 获取交易日列表
    trade_dates = get_trade_dates(conn, start_date=start_date)
    if not trade_dates:
        log.warning(f"  [{index_code}] 没有可计算的交易日")
        return

    log.info(f"  待计算 {len(trade_dates)} 个交易日：{trade_dates[0]} ~ {trade_dates[-1]}")

    ok, skip = 0, 0
    for i, td in enumerate(trade_dates):
        stat = calc_index_one_day(conn, index_code, td)
        if stat:
            save_stat(conn, stat)
            ok += 1
        else:
            skip += 1

        if (i + 1) % 250 == 0:  # 每250个交易日（约1年）打一次进度
            log.info(f"  进度 {i+1}/{len(trade_dates)}  已算:{ok}  跳过:{skip}")

    log.info(f"  [{index_code}] 回填完成  计算:{ok}  跳过:{skip}")


def cmd_backfill_all(conn, force=False):
    cur = conn.cursor()
    cur.execute("SELECT code, name FROM indices ORDER BY code")
    indices = cur.fetchall()
    if not indices:
        log.warning("没有维护的指数")
        return
    log.info(f"回填所有指数，共 {len(indices)} 个")
    for code, name in indices:
        log.info(f"\n── [{code}] {name} ──")
        cmd_backfill(conn, code, force=force)
    log.info("\n全部回填完成")


def cmd_backfill_recent(conn, days=20, force=False):
    """
    补算所有指数最近 N 个交易日的净值
    force=False：已有数据的日期跳过（默认，适合日常补漏）
    force=True ：强制重算覆盖（适合修复bug后重算）
    """
    trade_dates = get_trade_dates(conn)
    if not trade_dates:
        log.error("daily_prices 中没有交易日数据")
        return

    recent_dates = trade_dates[-days:]
    mode_str = "强制覆盖" if force else "跳过已有"
    log.info(f"补算最近 {days} 个交易日：{recent_dates[0]} ~ {recent_dates[-1]}  模式:{mode_str}")

    cur = conn.cursor()
    cur.execute("SELECT code, name FROM indices ORDER BY code")
    indices = cur.fetchall()
    if not indices:
        log.warning("没有维护的指数，请先运行 index_pipeline.py --init-all")
        return

    total_calc = total_skip_exist = total_skip_nodata = 0
    for index_code, index_name in indices:
        calc = skip_exist = skip_nodata = 0

        # 查出该指数已有数据的日期集合
        if not force:
            cur.execute("""
                SELECT trade_date FROM index_daily_stats
                WHERE index_code = ? AND trade_date >= ? AND trade_date <= ?
            """, (index_code, recent_dates[0], recent_dates[-1]))
            existing = {r[0] for r in cur.fetchall()}
        else:
            existing = set()

        for td in recent_dates:
            if td in existing:
                skip_exist += 1
                continue
            stat = calc_index_one_day(conn, index_code, td)
            if stat:
                save_stat(conn, stat)
                calc += 1
            else:
                skip_nodata += 1

        log.info(f"  [{index_code}] {index_name:<10}  "
                 f"计算:{calc}  跳过(已有):{skip_exist}  跳过(无数据):{skip_nodata}")
        total_calc       += calc
        total_skip_exist += skip_exist
        total_skip_nodata+= skip_nodata

    log.info(f"\n完成！计算:{total_calc}  跳过(已有):{total_skip_exist}  跳过(无数据):{total_skip_nodata}")
    cmd_show_all(conn)


# ─────────────────────────────────────────────────────────────────
# 命令：查看净值
# ─────────────────────────────────────────────────────────────────
def cmd_show(conn, index_code, days=30):
    cur = conn.cursor()
    cur.execute("SELECT name FROM indices WHERE code=?", (index_code,))
    row = cur.fetchone()
    if not row:
        log.error(f"指数 [{index_code}] 不存在")
        return

    cur.execute("""
        SELECT trade_date, net_value, score_sum, high_count, low_count,
               valid_count, total_count
        FROM index_daily_stats
        WHERE index_code = ?
        ORDER BY trade_date DESC
        LIMIT ?
    """, (index_code, days))
    rows = cur.fetchall()
    if not rows:
        print(f"[{index_code}] {row[0]} 暂无数据")
        return

    print(f"\n{'='*65}")
    print(f"[{index_code}] {row[0]}  最近 {len(rows)} 个交易日")
    print(f"{'='*65}")
    print(f"  {'日期':<12} {'净值':>8} {'新高':>6} {'新低':>6} {'得分':>6} {'有效/总':>8}")
    print("  " + "-" * 52)
    for r in rows:
        bar = _bar(r[1])  # 简单可视化
        print(f"  {r[0]:<12} {r[1]:>+8.4f} {r[2]:>6} {r[3]:>6} {r[4]:>6} "
              f"{r[5]:>4}/{r[6]:<4} {bar}")
    print()


def cmd_show_all(conn):
    """显示所有指数最新一天的净值"""
    cur = conn.cursor()
    cur.execute("""
        SELECT i.code, i.name, s.trade_date, s.net_value,
               s.high_count, s.low_count, s.valid_count, s.total_count
        FROM indices i
        LEFT JOIN index_daily_stats s
            ON s.index_code = i.code
           AND s.trade_date = (
               SELECT MAX(trade_date) FROM index_daily_stats
               WHERE index_code = i.code
           )
        ORDER BY i.code
    """)
    rows = cur.fetchall()
    if not rows:
        print("暂无数据")
        return

    print(f"\n{'='*70}")
    print(f"所有指数最新净值  ({datetime.today().strftime('%Y-%m-%d')})")
    print(f"{'='*70}")
    print(f"  {'代码':<10} {'名称':<12} {'日期':<12} {'净值':>8} {'新高':>5} "
          f"{'新低':>5} {'有效/总':>8}  走势")
    print("  " + "-" * 65)
    for r in rows:
        code, name, date, nv, hc, lc, vc, tc = r
        if nv is None:
            print(f"  {code:<10} {name:<12} {'无数据'}")
            continue
        bar = _bar(nv)
        print(f"  {code:<10} {name:<12} {(date or ''):<12} {nv:>+8.4f} "
              f"{(hc or 0):>5} {(lc or 0):>5} {(vc or 0):>4}/{(tc or 0):<4} {bar}")
    print()


def _bar(net_value):
    """用ASCII简单展示净值正负"""
    if net_value is None:
        return ""
    n = int(abs(net_value) * 10)
    if net_value >= 0:
        return "+" * min(n, 10)
    else:
        return "-" * min(n, 10)


# ─────────────────────────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="行业宽度指标计算")
    parser.add_argument("--calc-today",   action="store_true",   help="计算今日所有行业指标")
    parser.add_argument("--backfill",     metavar="CODE",        help="回填指定指数历史净值")
    parser.add_argument("--backfill-all", action="store_true",   help="回填所有指数历史净值")
    parser.add_argument("--recent-days",  type=int, metavar="N",   help="补算所有指数最近N个交易日净值，如: --recent-days 20")
    parser.add_argument("--force",        action="store_true",   help="配合--backfill使用，强制全量重算")
    parser.add_argument("--show",         metavar="CODE",        help="查看指数最近净值")
    parser.add_argument("--show-all",     action="store_true",   help="查看所有指数最新净值")
    parser.add_argument("--days",         type=int, default=30,  help="--show 显示天数，默认30")
    parser.add_argument("--db",           default=DB_PATH,       help="数据库路径")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-128000")   # 128MB缓存（回填时查询量大）
    init_db(conn)

    if args.calc_today:
        cmd_calc_today(conn)
    elif args.backfill:
        cmd_backfill(conn, args.backfill, force=args.force)
    elif args.backfill_all:
        cmd_backfill_all(conn, force=args.force)
    elif args.recent_days:
        cmd_backfill_recent(conn, days=args.recent_days, force=args.force)
    elif args.show:
        cmd_show(conn, args.show, days=args.days)
    elif args.show_all:
        cmd_show_all(conn)
    else:
        parser.print_help()

    conn.close()


if __name__ == "__main__":
    main()
