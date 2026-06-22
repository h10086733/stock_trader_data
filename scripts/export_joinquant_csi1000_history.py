"""
在 JoinQuant / 聚宽 Notebook 中运行，导出中证1000历史成分股快照。

用法：
  1. 打开聚宽研究环境 Notebook。
  2. 粘贴并运行本脚本。
  3. 下载生成的 csi1000_history_cons_joinquant.csv。
  4. 放到本项目 data/ 目录后执行：
       python index_history.py --import-csv data/csi1000_history_cons_joinquant.csv --index-code 000852
"""

from jqdata import get_index_stocks
import pandas as pd


INDEX_SYMBOL = "000852.XSHG"
OUT_PATH = "csi1000_history_cons_joinquant.csv"
START_DATE = "2016-01-01"
END_DATE = None  # None 表示运行当天


def main():
    end = pd.Timestamp(END_DATE).normalize() if END_DATE else pd.Timestamp.today().normalize()
    month_ends = pd.date_range(START_DATE, end, freq="ME")
    dates = sorted({START_DATE, *month_ends.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")})

    rows = []
    for date in dates:
        stocks = get_index_stocks(INDEX_SYMBOL, date=date)
        print(date, len(stocks))
        for code in stocks:
            stock_code, exchange = code.split(".", 1)
            rows.append({
                "index_code": "000852",
                "snapshot_date": date,
                "stock_code": stock_code,
                "exchange": exchange,
                "jq_code": code,
                "source": "joinquant",
            })

    df = pd.DataFrame(rows)
    df.to_csv(OUT_PATH, index=False)
    print("saved", OUT_PATH, df.shape)


main()
