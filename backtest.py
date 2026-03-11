"""
低PB周期股策略历史回测 — backtest.py
=======================================
买入逻辑（三重确认）：
  条件1：子行业集体亏损 ≥ 30%（ROE<0的股票占比）
  条件2：个股ROE < 行业阈值（有色 < -5%，化工 < -2%）
  条件3：PB < 行业历史PB的 25分位数 × 1.2
  条件4：股价较近3年最高点跌幅 > 55%（可选，增强确认）

信号强度（满足条件数）：
  满足3个 → 观察（★）
  满足4个 → 建仓（★★★）

买入价：ROE披露日（notice_date）后第一个有价格的交易日收盘价
卖出（取先到者）：
  ① ROE连续2个季度转正
  ② 涨幅达到100%
  ③ PB回到历史中位数（50分位）

使用方式：
  python backtest.py --run                    # 全量回测
  python backtest.py --run --sector 铜         # 只回测铜行业
  python backtest.py --run --start 2015-01-01  # 指定起始日期
  python backtest.py --report                  # 查看回测报告
  python backtest.py --trades                  # 查看所有交易明细
  python backtest.py --trades --sector 铜       # 某行业交易明细
"""

import sqlite3
import argparse
import logging
import sys
from datetime import datetime, timedelta
from collections import defaultdict

DB_PATH = "stock_data.db"

# 策略参数
SECTOR_PARAMS = {
    "有色金属": {
        "roe_threshold":    -5.0,
        "loss_ratio_min":   0.30,
        "pb_percentile":    25,      # 用历史PB 25分位作为阈值基准
        "pb_pct_multiplier":1.2,     # 阈值 = 25分位 × 1.2
        "drawdown_min":     0.55,    # 股价跌幅需 > 55%
        "take_profit":      1.0,     # 涨幅100%止盈
        "pb_exit_pct":      50,      # PB回到50分位卖出
    },
    "化工": {
        "roe_threshold":    -2.0,
        "loss_ratio_min":   0.30,
        "pb_percentile":    25,
        "pb_pct_multiplier":1.2,
        "drawdown_min":     0.55,
        "take_profit":      1.0,
        "pb_exit_pct":      50,
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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bt_signals (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_name     TEXT,
            sector_name     TEXT,
            stock_code      TEXT,
            stock_name      TEXT,
            signal_date     DATE,        -- ROE披露日
            report_date     DATE,        -- 报告期
            period_name     TEXT,        -- 中报/年报
            roe_value       REAL,
            pb_value        REAL,        -- 买入日PB
            pb_threshold    REAL,        -- 本次买入PB阈值（动态）
            loss_ratio      REAL,        -- 行业亏损占比
            drawdown        REAL,        -- 距近3年高点跌幅
            cond_roe        INTEGER,     -- 条件2是否满足
            cond_pb         INTEGER,     -- 条件3是否满足
            cond_drawdown   INTEGER,     -- 条件4是否满足
            signal_strength INTEGER,     -- 满足条件数（3或4）
            buy_date        DATE,        -- 实际买入日（披露日后第一个交易日）
            buy_price       REAL,
            UNIQUE(sector_name, stock_code, signal_date)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bt_trades (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id       INTEGER,
            parent_name     TEXT,
            sector_name     TEXT,
            stock_code      TEXT,
            stock_name      TEXT,
            buy_date        DATE,
            buy_price       REAL,
            sell_date       DATE,
            sell_price      REAL,
            sell_reason     TEXT,        -- 'take_profit'|'roe_recover'|'pb_median'|'end_of_data'
            hold_days       INTEGER,
            return_pct      REAL,        -- 收益率%
            max_drawdown    REAL,        -- 持仓期间最大回撤%
            max_gain        REAL,        -- 持仓期间最大浮盈%
            signal_strength INTEGER,
            UNIQUE(signal_id)
        )
    """)

    conn.commit()
    log.info("回测表就绪")


# ─────────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────────
def get_price_on_or_after(conn, code, date_str):
    """取 date_str 当天或之后第一个有收盘价的交易日"""
    cur = conn.cursor()
    cur.execute("""
        SELECT trade_date, close FROM daily_prices
        WHERE code=? AND trade_date>=? AND close IS NOT NULL
        ORDER BY trade_date ASC LIMIT 1
    """, (code, date_str))
    row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)


def get_price_on_or_before(conn, code, date_str):
    cur = conn.cursor()
    cur.execute("""
        SELECT trade_date, close FROM daily_prices
        WHERE code=? AND trade_date<=? AND close IS NOT NULL
        ORDER BY trade_date DESC LIMIT 1
    """, (code, date_str))
    row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)


def get_pb_percentile(conn, code, as_of_date, percentile):
    """计算股票在 as_of_date 之前所有历史PB的N分位数"""
    cur = conn.cursor()
    cur.execute("""
        SELECT pb FROM pb_history
        WHERE code=? AND trade_date<? AND pb IS NOT NULL
        ORDER BY pb ASC
    """, (code, as_of_date))
    pbs = [r[0] for r in cur.fetchall()]
    if len(pbs) < 12:   # 至少1年数据
        return None
    idx = int(len(pbs) * percentile / 100)
    return pbs[max(0, idx-1)]


def get_pb_at_date(conn, code, as_of_date):
    cur = conn.cursor()
    cur.execute("""
        SELECT pb FROM pb_history
        WHERE code=? AND trade_date<=?
        ORDER BY trade_date DESC LIMIT 1
    """, (code, as_of_date))
    row = cur.fetchone()
    return row[0] if row else None


def get_high_3y(conn, code, as_of_date):
    """近3年最高收盘价"""
    cur = conn.cursor()
    start = (datetime.strptime(as_of_date, "%Y-%m-%d") - timedelta(days=1095)).strftime("%Y-%m-%d")
    cur.execute("""
        SELECT MAX(close) FROM daily_prices
        WHERE code=? AND trade_date>=? AND trade_date<=? AND close IS NOT NULL
    """, (code, start, as_of_date))
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def get_sector_loss_ratio(conn, sector_name, parent_name, as_of_date):
    """子行业 ROE<0 占比"""
    cur = conn.cursor()
    cur.execute("""
        SELECT ss.stock_code, r.roe
        FROM sector_stocks ss
        JOIN roe_history r ON r.code = ss.stock_code
        WHERE ss.sector_name=? AND ss.parent_name=?
          AND date(r.report_date,'+4 months') <= ?
          AND r.report_date = (
              SELECT MAX(r2.report_date) FROM roe_history r2
              WHERE r2.code=ss.stock_code
                AND date(r2.report_date,'+4 months') <= ?
          )
    """, (sector_name, parent_name, as_of_date, as_of_date))
    rows = cur.fetchall()
    if len(rows) < 2:
        return None, 0, 0
    total      = len(rows)
    loss_count = sum(1 for _, roe in rows if roe is not None and roe < 0)
    return loss_count / total, total, loss_count


def get_roe_after(conn, code, after_date, count=2):
    """获取 after_date 之后发布的最多 count 条 ROE（用于判断卖出条件）"""
    cur = conn.cursor()
    cur.execute("""
        SELECT roe, report_date, notice_date
        FROM roe_history
        WHERE code=? AND report_date>?
        ORDER BY report_date ASC LIMIT ?
    """, (code, after_date, count))
    return cur.fetchall()


def get_prices_range(conn, code, start_date, end_date):
    """取一段时间内所有收盘价"""
    cur = conn.cursor()
    cur.execute("""
        SELECT trade_date, close FROM daily_prices
        WHERE code=? AND trade_date>=? AND trade_date<=? AND close IS NOT NULL
        ORDER BY trade_date ASC
    """, (code, start_date, end_date))
    return cur.fetchall()


# ─────────────────────────────────────────────────────────────────
# 模拟持仓：确定卖出日期和价格
# ─────────────────────────────────────────────────────────────────
def simulate_hold(conn, code, buy_date, buy_price, report_date, params):
    """
    从 buy_date 开始持有，按卖出规则找到最早触发的卖出点
    返回: (sell_date, sell_price, sell_reason, hold_days, return_pct, max_dd, max_gain)
    """
    # 获取买入后所有价格数据（最长3年）
    end_limit = (datetime.strptime(buy_date, "%Y-%m-%d") + timedelta(days=1095)).strftime("%Y-%m-%d")
    prices = get_prices_range(conn, code, buy_date, end_limit)
    if not prices:
        return None, None, None, None, None, None, None

    pb_exit_pct  = params["pb_exit_pct"]
    take_profit  = params["take_profit"]

    # 计算 PB 卖出阈值（50分位）
    pb_median = get_pb_percentile(conn, code, buy_date, pb_exit_pct)

    # 获取买入后的 ROE 数据
    future_roes = get_roe_after(conn, code, report_date, count=4)

    max_price  = buy_price
    min_price  = buy_price
    sell_date  = prices[-1][0]
    sell_price = prices[-1][1]
    sell_reason= "end_of_data"

    for i, (td, close) in enumerate(prices):
        if close is None:
            continue
        max_price = max(max_price, close)
        min_price = min(min_price, close)

        # 卖出条件1：涨幅达到 take_profit（默认100%）
        gain_pct = (close - buy_price) / buy_price
        if gain_pct >= take_profit:
            sell_date   = td
            sell_price  = close
            sell_reason = "take_profit_100pct"
            break

        # 卖出条件2：PB 回到历史中位数
        if pb_median:
            cur_pb = get_pb_at_date(conn, code, td)
            if cur_pb and cur_pb >= pb_median:
                sell_date   = td
                sell_price  = close
                sell_reason = "pb_median"
                break

        # 卖出条件3：ROE 连续2个季度转正（用披露日判断）
        consec_positive = 0
        for roe_val, roe_rd, roe_nd in future_roes:
            notice = roe_nd or (
                datetime.strptime(roe_rd, "%Y-%m-%d") + timedelta(days=120)
            ).strftime("%Y-%m-%d")
            if notice <= td and roe_val is not None and roe_val > 0:
                consec_positive += 1
            else:
                consec_positive = 0
        if consec_positive >= 2:
            sell_date   = td
            sell_price  = close
            sell_reason = "roe_recover_2q"
            break

    hold_days  = (datetime.strptime(sell_date, "%Y-%m-%d") -
                  datetime.strptime(buy_date,  "%Y-%m-%d")).days
    return_pct = (sell_price - buy_price) / buy_price * 100
    max_gain   = (max_price - buy_price) / buy_price * 100
    max_dd     = (min_price - buy_price) / buy_price * 100  # 负数

    return sell_date, sell_price, sell_reason, hold_days, return_pct, max_dd, max_gain


# ─────────────────────────────────────────────────────────────────
# 主回测流程
# ─────────────────────────────────────────────────────────────────
def run_backtest(conn, filter_sector=None, start_date="2010-01-01"):
    cur = conn.cursor()

    # 清除旧数据
    if filter_sector:
        conn.execute("DELETE FROM bt_signals WHERE sector_name=? OR parent_name=?",
                     (filter_sector, filter_sector))
        conn.execute("DELETE FROM bt_trades WHERE sector_name=? OR parent_name=?",
                     (filter_sector, filter_sector))
    else:
        conn.execute("DELETE FROM bt_signals")
        conn.execute("DELETE FROM bt_trades")
    conn.commit()

    # 获取所有子行业
    cur.execute("""
        SELECT DISTINCT parent_name, sector_name FROM sector_stocks
        ORDER BY parent_name, sector_name
    """)
    sectors = cur.fetchall()
    if filter_sector:
        sectors = [s for s in sectors if filter_sector in (s[0], s[1])]

    log.info(f"回测范围: {len(sectors)} 个子行业  起始: {start_date}")

    total_signals = 0
    total_trades  = 0

    for parent_name, sector_name in sectors:
        params = SECTOR_PARAMS.get(parent_name)
        if not params:
            continue

        # 获取子行业所有股票的所有 ROE 报告（中报+年报）
        cur.execute("""
            SELECT DISTINCT r.code, s.name, r.report_date, r.period_name,
                            r.roe, r.notice_date
            FROM sector_stocks ss
            JOIN roe_history r ON r.code = ss.stock_code
            LEFT JOIN stocks s ON s.code = r.code
            WHERE ss.sector_name=? AND ss.parent_name=?
              AND r.report_date >= ?
            ORDER BY r.code, r.report_date
        """, (sector_name, parent_name, start_date))
        roe_rows = cur.fetchall()

        if not roe_rows:
            continue

        sig_count = 0
        for code, name, report_date, period_name, roe, notice_date in roe_rows:
            # 确定信号日期（实际可用数据的日期）
            if notice_date:
                signal_date = notice_date
            else:
                # 估算：中报约8月底，年报约4月底
                rd = datetime.strptime(report_date, "%Y-%m-%d")
                signal_date = (rd + timedelta(days=120)).strftime("%Y-%m-%d")

            if signal_date < start_date:
                continue

            # ── 条件1：行业集体亏损 ──
            loss_ratio, total_n, loss_n = get_sector_loss_ratio(
                conn, sector_name, parent_name, signal_date)
            if loss_ratio is None or loss_ratio < params["loss_ratio_min"]:
                continue

            # ── 条件2：个股ROE < 阈值 ──
            cond_roe = 1 if (roe is not None and roe < params["roe_threshold"]) else 0

            # ── 条件3：PB < 历史25分位 × 1.2 ──
            pb_val  = get_pb_at_date(conn, code, signal_date)
            pb_25   = get_pb_percentile(conn, code, signal_date, params["pb_percentile"])
            if pb_val is None:
                continue
            pb_threshold = (pb_25 * params["pb_pct_multiplier"]) if pb_25 else None
            cond_pb = 1 if (pb_threshold and pb_val < pb_threshold) else 0

            # ── 条件4：股价跌幅 > 55% ──
            _, price_now = get_price_on_or_after(conn, code, signal_date)
            high_3y = get_high_3y(conn, code, signal_date)
            if price_now and high_3y and high_3y > 0:
                drawdown    = (price_now - high_3y) / high_3y   # 负数
                cond_dd     = 1 if drawdown <= -params["drawdown_min"] else 0
            else:
                drawdown    = None
                cond_dd     = 0

            # 满足条件数（条件1已满足，计2/3/4）
            strength = 1 + cond_roe + cond_pb + cond_dd  # 最少1（仅行业亏损），最多4

            # 必须至少满足：行业亏损 + ROE（条件1+2）才记录信号
            if cond_roe == 0:
                continue

            # 买入价：信号日后第一个交易日
            buy_date, buy_price = get_price_on_or_after(conn, code, signal_date)
            if not buy_date or not buy_price:
                continue

            # 写入信号表
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO bt_signals
                        (parent_name, sector_name, stock_code, stock_name,
                         signal_date, report_date, period_name, roe_value,
                         pb_value, pb_threshold, loss_ratio, drawdown,
                         cond_roe, cond_pb, cond_drawdown, signal_strength,
                         buy_date, buy_price)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (parent_name, sector_name, code, name,
                      signal_date, report_date, period_name, roe,
                      pb_val, pb_threshold, loss_ratio,
                      drawdown, cond_roe, cond_pb, cond_dd, strength,
                      buy_date, buy_price))
                sig_count += 1
            except Exception:
                pass

        conn.commit()

        # ── 模拟持仓，计算收益 ──
        cur.execute("""
            SELECT id, stock_code, stock_name, buy_date, buy_price,
                   report_date, signal_strength
            FROM bt_signals
            WHERE sector_name=? AND parent_name=?
        """, (sector_name, parent_name))
        sig_rows = cur.fetchall()

        for sig_id, code, name, buy_date, buy_price, report_date, strength in sig_rows:
            if not buy_price:
                continue
            sell_date, sell_price, sell_reason, hold_days, ret_pct, max_dd, max_gain = \
                simulate_hold(conn, code, buy_date, buy_price, report_date, params)
            if sell_date is None:
                continue
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO bt_trades
                        (signal_id, parent_name, sector_name, stock_code, stock_name,
                         buy_date, buy_price, sell_date, sell_price, sell_reason,
                         hold_days, return_pct, max_drawdown, max_gain, signal_strength)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (sig_id, parent_name, sector_name, code, name,
                      buy_date, buy_price, sell_date, sell_price, sell_reason,
                      hold_days, ret_pct, max_dd, max_gain, strength))
                total_trades += 1
            except Exception:
                pass

        conn.commit()
        total_signals += sig_count
        log.info(f"  [{sector_name}]  信号:{sig_count}  交易:{len(sig_rows)}")

    log.info(f"\n回测完成  总信号:{total_signals}  总交易:{total_trades}")


# ─────────────────────────────────────────────────────────────────
# 回测报告
# ─────────────────────────────────────────────────────────────────
def print_report(conn, filter_sector=None):
    cur = conn.cursor()

    where = ""
    params_q = []
    if filter_sector:
        where    = "WHERE (sector_name=? OR parent_name=?)"
        params_q = [filter_sector, filter_sector]

    cur.execute(f"""
        SELECT COUNT(*), AVG(return_pct), MAX(return_pct), MIN(return_pct),
               AVG(hold_days), AVG(max_drawdown), AVG(max_gain),
               SUM(CASE WHEN return_pct > 0 THEN 1 ELSE 0 END),
               SUM(CASE WHEN return_pct >= 100 THEN 1 ELSE 0 END)
        FROM bt_trades {where}
    """, params_q)
    r = cur.fetchone()

    if not r or not r[0]:
        print("暂无回测数据，请先运行 --run")
        return

    total, avg_ret, max_ret, min_ret, avg_days, avg_dd, avg_gain, win_n, double_n = r
    win_rate    = win_n / total * 100 if total else 0
    double_rate = double_n / total * 100 if total else 0

    print(f"\n{'='*65}")
    print(f"低PB周期股策略回测报告" + (f"  [{filter_sector}]" if filter_sector else ""))
    print(f"{'='*65}")
    print(f"  总交易笔数:   {total:>6}")
    print(f"  胜率(>0):     {win_rate:>6.1f}%  ({win_n}/{total})")
    print(f"  翻倍率(≥100%):{double_rate:>6.1f}%  ({double_n}/{total})")
    print(f"  平均收益:     {avg_ret:>+6.1f}%")
    print(f"  最大收益:     {max_ret:>+6.1f}%")
    print(f"  最大亏损:     {min_ret:>+6.1f}%")
    print(f"  平均持仓:     {avg_days:>6.0f} 天 ({avg_days/30:.1f}个月)")
    print(f"  平均最大回撤: {avg_dd:>+6.1f}%")
    print(f"  平均最大浮盈: {avg_gain:>+6.1f}%")

    # 按信号强度分组
    print(f"\n  按信号强度分组:")
    print(f"  {'强度':<6} {'笔数':>5} {'胜率':>7} {'平均收益':>9} {'翻倍率':>8} {'平均持仓':>9}")
    print("  " + "-" * 48)
    for strength in [4, 3, 2, 1]:
        cur.execute("""
            SELECT COUNT(*),
                   AVG(return_pct),
                   SUM(CASE WHEN return_pct>0 THEN 1 ELSE 0 END),
                   SUM(CASE WHEN return_pct>=100 THEN 1 ELSE 0 END),
                   AVG(hold_days)
            FROM bt_trades
            WHERE signal_strength=?
        """ + (f" AND ({where[6:]})" if where else ""), [strength] + params_q)
        sr = cur.fetchone()
        if sr and sr[0]:
            n, avg_r, w, d, h = sr
            stars = "★" * strength
            print(f"  {stars:<6} {n:>5} {w/n*100:>6.1f}% {avg_r:>+8.1f}% "
                  f"{d/n*100:>7.1f}% {h:>7.0f}天")

    # 按子行业分组
    print(f"\n  按子行业分组:")
    print(f"  {'行业':<12} {'笔数':>5} {'胜率':>7} {'平均收益':>9} {'翻倍率':>8}")
    print("  " + "-" * 46)
    cur.execute(f"""
        SELECT sector_name, COUNT(*),
               AVG(return_pct),
               SUM(CASE WHEN return_pct>0 THEN 1 ELSE 0 END),
               SUM(CASE WHEN return_pct>=100 THEN 1 ELSE 0 END)
        FROM bt_trades {where}
        GROUP BY sector_name
        ORDER BY AVG(return_pct) DESC
    """, params_q)
    for row in cur.fetchall():
        s, n, avg_r, w, d = row
        print(f"  {s:<12} {n:>5} {w/n*100:>6.1f}% {avg_r:>+8.1f}% {d/n*100:>7.1f}%")

    # 按卖出原因分组
    print(f"\n  按卖出原因分组:")
    cur.execute(f"""
        SELECT sell_reason, COUNT(*), AVG(return_pct), AVG(hold_days)
        FROM bt_trades {where}
        GROUP BY sell_reason ORDER BY COUNT(*) DESC
    """, params_q)
    reason_label = {
        "take_profit_100pct": "涨幅100%止盈",
        "pb_median":          "PB回中位数",
        "roe_recover_2q":     "ROE连续2季转正",
        "end_of_data":        "数据截止",
    }
    for row in cur.fetchall():
        reason, n, avg_r, avg_h = row
        label = reason_label.get(reason, reason)
        print(f"  {label:<16} {n:>4}笔  平均收益:{avg_r:>+7.1f}%  平均持仓:{avg_h:>5.0f}天")

    print()


# ─────────────────────────────────────────────────────────────────
# 交易明细
# ─────────────────────────────────────────────────────────────────
def print_trades(conn, filter_sector=None, min_strength=1, limit=50):
    cur = conn.cursor()

    where_parts = [f"signal_strength >= {min_strength}"]
    params_q    = []
    if filter_sector:
        where_parts.append("(sector_name=? OR parent_name=?)")
        params_q += [filter_sector, filter_sector]
    where = "WHERE " + " AND ".join(where_parts)

    cur.execute(f"""
        SELECT sector_name, stock_code, stock_name,
               buy_date, buy_price, sell_date, sell_price,
               return_pct, hold_days, sell_reason, signal_strength,
               max_drawdown, max_gain
        FROM bt_trades {where}
        ORDER BY return_pct DESC
        LIMIT ?
    """, params_q + [limit])
    rows = cur.fetchall()

    label = f"  [{filter_sector}]" if filter_sector else ""
    print(f"\n{'='*80}")
    print(f"交易明细{label}  信号强度≥{min_strength}  共{len(rows)}笔")
    print(f"{'='*80}")
    stars_map = {4:"★★★★",3:"★★★",2:"★★",1:"★"}
    reason_short = {
        "take_profit_100pct": "止盈100%",
        "pb_median":          "PB中位",
        "roe_recover_2q":     "ROE转正",
        "end_of_data":        "数据截止",
    }
    print(f"  {'行业':<10}{'代码':<8}{'名称':<10}{'买入日':<12}{'卖出日':<12}"
          f"{'收益':>8}{'持仓':>7}{'回撤':>8}{'原因':<10}{'强度'}")
    print("  " + "-" * 88)
    for r in rows:
        sector, code, name, bd, bp, sd, sp, ret, days, reason, strength, dd, mg = r
        ret_flag = "↑" if ret > 0 else "↓"
        print(f"  {(sector or ''):<10}{code:<8}{(name or ''):<10}{bd:<12}{(sd or '-'):<12}"
              f"{ret:>+7.1f}%{days:>6}天{dd:>+7.1f}%  "
              f"{reason_short.get(reason,'?'):<10}{stars_map.get(strength,'')}")
    print()


# ─────────────────────────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="低PB周期股策略历史回测")
    parser.add_argument("--run",      action="store_true",   help="运行回测")
    parser.add_argument("--report",   action="store_true",   help="查看回测报告")
    parser.add_argument("--trades",   action="store_true",   help="查看交易明细")
    parser.add_argument("--sector",   metavar="NAME",        help="只回测/查看指定行业")
    parser.add_argument("--start",    default="2010-01-01",  help="回测起始日期")
    parser.add_argument("--strength", type=int, default=2,   help="--trades 最低信号强度")
    parser.add_argument("--limit",    type=int, default=50,  help="--trades 显示条数")
    parser.add_argument("--db",       default=DB_PATH,       help="数据库路径")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-128000")
    init_db(conn)

    if args.run:
        run_backtest(conn, filter_sector=args.sector, start_date=args.start)
        print_report(conn, filter_sector=args.sector)
    elif args.report:
        print_report(conn, filter_sector=args.sector)
    elif args.trades:
        print_trades(conn, filter_sector=args.sector,
                     min_strength=args.strength, limit=args.limit)
    else:
        parser.print_help()

    conn.close()


if __name__ == "__main__":
    main()
