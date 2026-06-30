"""通用小工具：数值清洗、类型转换、交易日归一化、分块。

来源：app.py 中散落的纯函数（clamp/to_float/coerce_int/coerce_float/
clean_number/json_safe/normalize_trade_date/local_now_text/chunked）。HC 策略
层与后续 web 层共用，集中于此避免重复。注意 ``request`` 相关的 to_int_arg/
to_float_arg 属于 web 层，不放这里。
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta
from datetime import time as dt_time


def clamp(value, low, high):
    return max(low, min(high, value))


def to_float(value, default=None):
    try:
        if value in (None, "", "-"):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def coerce_int(value, default, min_value=None, max_value=None):
    try:
        value = int(value)
    except (TypeError, ValueError):
        value = default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def coerce_float(value, default, min_value=None, max_value=None):
    try:
        value = float(value)
    except (TypeError, ValueError):
        value = default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def clean_number(value, default=0.0):
    try:
        num = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(num):
        return default
    return num


def json_safe(value):
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    try:
        if math.isnan(value):
            return None
    except TypeError:
        pass
    return value


def normalize_trade_date(value, default=None):
    if value in (None, ""):
        return default
    text = str(value).strip()
    if not text:
        return default
    for fmt in ("%Y-%m-%d", "%Y-%m-%e", "%Y/%m/%d", "%Y/%m/%e"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    parts = text.replace("/", "-").split("-")
    if len(parts) == 3:
        try:
            year, month, day = (int(part) for part in parts)
            return datetime(year, month, day).strftime("%Y-%m-%d")
        except (TypeError, ValueError):
            pass
    return text


def local_now_text():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def chunked(items, size=500):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def get_source_value(source, *names, default=None):
    """从 dict/Mapping 中依次尝试多个 key，返回第一个非 None 值。"""
    for name in names:
        val = source.get(name)
        if val is not None:
            return val
    return default


def parse_cutoff_time(value: str) -> dt_time:
    """解析 'HH:MM' 字符串为 time 对象，解析失败时返回 14:30。"""
    try:
        hour, minute = [int(x) for x in value.split(":", 1)]
        return dt_time(hour, minute)
    except Exception:
        return dt_time(14, 30)


def default_scan_trade_date() -> str:
    """返回当前扫描交易日（凌晨 6 点前仍归前日）。"""
    now = datetime.now()
    if now.time() < dt_time(6, 0):
        now = now - timedelta(days=1)
    return now.strftime("%Y-%m-%d")


def trade_elapsed_ratio(cutoff: dt_time) -> float:
    """返回截止时刻占当日交易时间的比例（0.05–1.0）。"""
    morning_start = dt_time(9, 30)
    morning_end = dt_time(11, 30)
    afternoon_start = dt_time(13, 0)
    afternoon_end = dt_time(15, 0)

    def minutes_between(start, end):
        return (datetime.combine(datetime.today(), end)
                - datetime.combine(datetime.today(), start)).seconds / 60

    elapsed = 0.0
    if cutoff > morning_start:
        elapsed += minutes_between(morning_start, min(cutoff, morning_end))
    if cutoff > afternoon_start:
        elapsed += minutes_between(afternoon_start, min(cutoff, afternoon_end))
    return clamp(elapsed / 240.0, 0.05, 1.0)

