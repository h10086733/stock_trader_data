"""Web 工具函数：Flask 环境下的请求参数解析辅助。"""
from __future__ import annotations

from flask import request
from trader.core.utils import coerce_int, coerce_float, clamp


def to_int_arg(name: str, default: int, min_value=None, max_value=None) -> int:
    return coerce_int(request.args.get(name), default, min_value, max_value)


def to_float_arg(name: str, default: float, min_value=None, max_value=None) -> float:
    return coerce_float(request.args.get(name), default, min_value, max_value)
