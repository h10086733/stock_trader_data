"""股票代码归一化与市场推断。

历史上这些函数散落在 fetch.py / app.py / hc_strategy_scanner.py /
signal_tool.py / index.py / supplement_selected_klines.py 各自实现，
存在细微差异。这里收口为单一实现。
"""
from __future__ import annotations


def zfill6(code) -> str:
    """统一为 6 位数字字符串。"""
    return str(code or "").strip().zfill(6)


def normalize_stock_code(value):
    """把各种来源的代码（可能带 .SH/.SZ/.BJ 后缀、float、.0 尾巴）归一为 6 位代码。

    无法解析时返回 None。来源：index.py:200。
    """
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


def is_bj_code(code, secucode: str = "") -> bool:
    """是否北交所代码。来源：hc_strategy_scanner.py:180。"""
    code = zfill6(code)
    return code.startswith(("8", "9")) or str(secucode).upper().endswith(".BJ")


def is_20cm_or_bj_code(code) -> bool:
    """20cm 涨跌幅（创业板/科创板）或北交所。来源：app.py:5618。"""
    code = zfill6(code)
    return code.startswith(("300", "301", "688", "689", "8", "9"))


def infer_market(code, market=None) -> str:
    """推断东方财富 market 标记：'1'=沪市 '0'=深市。来源：app.py:2466。

    若已知 market（'0'/'1'）直接返回。
    """
    if market in ("0", "1"):
        return market
    code = zfill6(code)
    return "1" if code.startswith(("5", "6", "9")) else "0"


def infer_secucode(code, market: str | None = None) -> str:
    """推断带交易所后缀的代码，如 600000.SH。来源：hc_strategy_scanner.py:185。"""
    code = zfill6(code)
    if market == "1" or code.startswith(("5", "6", "9")):
        return f"{code}.SH"
    return f"{code}.SZ"


def to_sina_symbol(code) -> str:
    """转新浪行情符号，如 sh600000 / sz000001 / bj920819。来源：app.py:2460。"""
    code = zfill6(code)
    if code.startswith("92"):
        return "bj" + code
    return ("sh" if code.startswith(("5", "6", "9")) else "sz") + code


def to_baostock_code(code, market: str | None = None) -> str:
    """转 baostock 代码，如 sh.600000 / sz.000001。来源：app.py:2472。

    传入 market 时按 market 判断（supplement_selected_klines.py 口径），
    否则按代码前缀判断。
    """
    code = zfill6(code)
    if market in ("0", "1"):
        return ("sh." if market == "1" else "sz.") + code
    return ("sh." if code.startswith(("5", "6", "9")) else "sz.") + code


def infer_exchange(stock_code, exchange_text: str = "") -> str:
    """推断交易所简称 SH/SZ/BJ。来源：index.py:227。"""
    text = str(exchange_text or "").strip().upper()
    raw = str(exchange_text or "").strip()
    if "上交所" in raw or "沪" in raw or text in ("SH", "SSH", "XSHG"):
        return "SH"
    if "深交所" in raw or "深" in raw or text in ("SZ", "SZE", "XSHE"):
        return "SZ"
    if "北交所" in raw or "京" in raw or text in ("BJ", "XBSE"):
        return "BJ"
    code = zfill6(stock_code)
    if code.startswith(("5", "6", "9")):
        return "SH"
    if code.startswith("8"):
        return "BJ"
    return "SZ"


def limit_rate_for_code(code, name: str = "") -> float:
    """涨跌停比例（小数）。ST=0.05，北交所=0.30，20cm=0.20，其余=0.10。

    来源：signal_tool.py:334。
    """
    code = zfill6(code)
    if "ST" in (name or "").upper():
        return 0.05
    if code.startswith(("8", "9")):
        return 0.30
    if code.startswith(("300", "301", "688", "689")):
        return 0.20
    return 0.10


def limit_pct_for_code(code, name: str = "") -> float:
    """涨停百分比阈值（百分数）。ST=4.8，20cm=19.5，其余=9.8。

    来源：app.py:1006 hc_limit_pct。注意这是用于"触板/封板"判断的略保守阈值，
    与 limit_rate_for_code 的理论值不同。
    """
    code = zfill6(code)
    name = str(name or "").upper()
    if "ST" in name:
        return 4.8
    if code.startswith(("300", "301", "688", "689")):
        return 19.5
    return 9.8
