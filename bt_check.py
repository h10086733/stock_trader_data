"""
回测数据准备检查 — bt_check.py
================================
检查回测所需数据是否就绪，并给出下一步操作指引
运行：python bt_check.py
"""

import sqlite3
import sys

DB_PATH = "stock_data.db"

def check():
    try:
        conn = sqlite3.connect(DB_PATH)
        cur  = conn.cursor()
    except Exception as e:
        print(f"❌ 打不开数据库: {e}")
        print("   请先运行 stock_db_pipeline.py --init")
        sys.exit(1)

    issues   = []
    ok_items = []

    print("=" * 60)
    print("回测数据准备状态检查")
    print("=" * 60)

    # ── 1. 行业股票池 ──
    cur.execute("SELECT COUNT(DISTINCT sector_name), COUNT(*) FROM sector_stocks")
    r = cur.fetchone()
    if r and r[1] > 0:
        ok_items.append(f"✅ 行业股票池：{r[0]}个子行业，{r[1]}只股票")
    else:
        issues.append(("行业股票池未导入",
                       "python sector_strategy.py --import-csv"))

    # ── 2. 价格数据 ──
    cur.execute("""
        SELECT COUNT(DISTINCT s.code), MIN(dp.trade_date), MAX(dp.trade_date)
        FROM sector_stocks ss
        JOIN stocks s ON s.code = ss.stock_code
        JOIN daily_prices dp ON dp.code = s.code
    """)
    r = cur.fetchone()
    if r and r[0] and r[0] > 100:
        ok_items.append(f"✅ 价格数据：{r[0]}只股票有价格  {r[1]} ~ {r[2]}")
    elif r and r[0]:
        issues.append((f"价格数据不足（只有{r[0]}只）",
                       "python stock_db_pipeline.py --init"))
    else:
        issues.append(("价格数据缺失",
                       "python stock_db_pipeline.py --init"))

    # ── 3. PB数据 ──
    cur.execute("""
        SELECT COUNT(DISTINCT p.code), COUNT(*), MIN(p.trade_date), MAX(p.trade_date)
        FROM sector_stocks ss
        JOIN pb_history p ON p.code = ss.stock_code
    """)
    r = cur.fetchone()
    cur.execute("SELECT COUNT(DISTINCT stock_code) FROM sector_stocks")
    total_stocks = cur.fetchone()[0] or 1

    if r and r[0] and r[0] >= total_stocks * 0.8:
        ok_items.append(f"✅ PB数据：{r[0]}/{total_stocks}只  {r[1]}条  {r[2]} ~ {r[3]}")
    elif r and r[0] and r[0] > 0:
        pct = r[0] / total_stocks * 100
        issues.append((f"PB数据不完整（{r[0]}/{total_stocks}只，{pct:.0f}%）",
                       "python findata_pipeline.py --stocks $(python sector_strategy.py --get-codes)"))
    else:
        issues.append(("PB数据缺失",
                       "python findata_pipeline.py --stocks $(python sector_strategy.py --get-codes)"))

    # ── 4. ROE数据 ──
    cur.execute("""
        SELECT COUNT(DISTINCT r.code), COUNT(*), MIN(r.report_date), MAX(r.report_date)
        FROM sector_stocks ss
        JOIN roe_history r ON r.code = ss.stock_code
    """)
    r = cur.fetchone()
    if r and r[0] and r[0] >= total_stocks * 0.8:
        ok_items.append(f"✅ ROE数据：{r[0]}/{total_stocks}只  {r[1]}条  {r[2]} ~ {r[3]}")
    elif r and r[0] and r[0] > 0:
        pct = r[0] / total_stocks * 100
        issues.append((f"ROE数据不完整（{r[0]}/{total_stocks}只，{pct:.0f}%）",
                       "python findata_pipeline.py --stocks $(python sector_strategy.py --get-codes)"))
    else:
        issues.append(("ROE数据缺失",
                       "python findata_pipeline.py --stocks $(python sector_strategy.py --get-codes)"))

    # ── 5. 回测表 ──
    try:
        cur.execute("SELECT COUNT(*) FROM bt_signals")
        sig_n = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM bt_trades")
        trade_n = cur.fetchone()[0]
        if trade_n > 0:
            ok_items.append(f"✅ 已有回测结果：{sig_n}个信号  {trade_n}笔交易")
        else:
            ok_items.append("⏳ 回测表已建，尚未运行")
    except Exception:
        ok_items.append("⏳ 回测表未建（运行--run时自动创建）")

    # ── 输出 ──
    print()
    for item in ok_items:
        print(f"  {item}")

    print()
    if issues:
        print("❌ 缺失数据，请按顺序执行：")
        print()
        for i, (desc, cmd) in enumerate(issues, 1):
            print(f"  步骤{i}：{desc}")
            print(f"         {cmd}")
            print()
        print("完成后重新运行 python bt_check.py 确认")
    else:
        print("🎉 数据就绪！可以开始回测：")
        print()
        print("  # 全量回测（所有行业 2010年至今）")
        print("  python backtest.py --run")
        print()
        print("  # 只回测有色金属")
        print("  python backtest.py --run --sector 有色金属")
        print()
        print("  # 只回测单个子行业（如铜）")
        print("  python backtest.py --run --sector 铜")
        print()
        print("  # 查看报告")
        print("  python backtest.py --report")
        print()
        print("  # 查看交易明细（只看强信号）")
        print("  python backtest.py --trades --strength 3")

    print("=" * 60)
    conn.close()
    return len(issues) == 0


if __name__ == "__main__":
    ready = check()
    sys.exit(0 if ready else 1)
