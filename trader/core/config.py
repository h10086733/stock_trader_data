"""集中配置：数据库路径、输出目录、外部 API 端点、HTTP 重试参数。

这是全项目配置的单一来源。旧脚本各自硬编码 ``DB_PATH = "stock_data.db"``、
各自定义东方财富/新浪 URL，这里收口为一处，可由环境变量覆盖。
"""
from __future__ import annotations

import os
from pathlib import Path

# 仓库根目录：本文件位于 <root>/trader/core/config.py
BASE_DIR = Path(__file__).resolve().parents[2]

# ── 数据库 ────────────────────────────────────────────────────────────
# 兼容历史使用过的两个环境变量名，最后回退到仓库根目录下的 stock_data.db。
DB_PATH = Path(
    os.environ.get("STOCK_TRADER_DB_PATH")
    or os.environ.get("STOCK_DB_PATH")
    or (BASE_DIR / "stock_data.db")
)

# ── 输出与日志 ────────────────────────────────────────────────────────
OUTPUT_DIR = Path(os.environ.get("STOCK_TRADER_OUTPUT_DIR") or (BASE_DIR / "outputs"))
CACHE_DIR = OUTPUT_DIR / "cache"
LOG_FILE = Path(os.environ.get("STOCK_TRADER_LOG_FILE") or (BASE_DIR / "pipeline.log"))

# ── HTTP 重试参数 ─────────────────────────────────────────────────────
REQUEST_INTERVAL = 1.0    # 正常请求间隔（秒）
RETRY_INTERVAL = 10.0     # 限流后等待时间（秒）
MAX_RETRIES = 5           # 单次请求最大重试次数
REQUEST_TIMEOUT = 15      # 默认超时（秒）

# ── 东方财富 / 新浪 接口端点 ──────────────────────────────────────────
STOCK_LIST_URL = "https://push2delay.eastmoney.com/api/qt/clist/get"
KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
TRENDS_URL = "https://push2delay.eastmoney.com/api/qt/stock/trends2/get"
FINDATA_URL = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
SINA_PRICE_URL = "https://hq.sinajs.cn/list="
SINA_REFERER = "https://finance.sina.com.cn/"
EASTMONEY_QUOTE_URLS = (
    "https://push2delay.eastmoney.com/api/qt/ulist.np/get",
    "https://push2.eastmoney.com/api/qt/ulist.np/get",
)
EASTMONEY_TRENDS_URLS = (
    "https://push2delay.eastmoney.com/api/qt/stock/trends2/get",
    "https://push2.eastmoney.com/api/qt/stock/trends2/get",
)

# ── HTTP 公共 Headers ─────────────────────────────────────────────────
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Referer": "https://finance.sina.com.cn/",
}

# ── 数据起点 ──────────────────────────────────────────────────────────
HISTORY_START = "20100101"


def ensure_dirs() -> None:
    """确保输出/缓存目录存在。"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
