"""统一日志配置。

历史上 fetch.py 用 basicConfig 输出到 pipeline.log + stdout。这里收口为一个
幂等的 setup 函数，避免多模块重复配置 handler。
"""
from __future__ import annotations

import logging
import sys

from . import config

_CONFIGURED = False


def setup(level=logging.INFO, to_file: bool = True, to_stdout: bool = True) -> logging.Logger:
    """配置根 logger（幂等）。返回项目 logger。"""
    global _CONFIGURED
    if not _CONFIGURED:
        handlers = []
        if to_file:
            handlers.append(logging.FileHandler(config.LOG_FILE, encoding="utf-8"))
        if to_stdout:
            handlers.append(logging.StreamHandler(sys.stdout))
        logging.basicConfig(
            level=level,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=handlers,
        )
        # urllib3 逐次重试日志噪音很大，压低级别。
        logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
        logging.getLogger("urllib3.util.retry").setLevel(logging.ERROR)
        _CONFIGURED = True
    return logging.getLogger("trader")


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
