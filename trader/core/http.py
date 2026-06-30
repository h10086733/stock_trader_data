"""统一 HTTP 客户端：带重试的 Session、东方财富/新浪请求封装。

历史上 fetch.py 与 findata.py 各自实现了一份 ``make_session`` + ``safe_get``，
csi1000_timing.py / backtest2.py / app.py 又各自写东财请求。这里收口为一处。
"""
from __future__ import annotations

import logging
import random
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import config

log = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
]


def eastmoney_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": "https://www.eastmoney.com/",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }


def sina_headers() -> dict:
    headers = eastmoney_headers()
    headers["Referer"] = config.SINA_REFERER
    headers["Accept"] = "application/javascript, text/plain, */*"
    return headers


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=config.MAX_RETRIES,
        backoff_factor=2,  # 重试等待：2s, 4s, 8s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# 模块级共享 Session（线程安全：requests.Session 的 GET 可并发使用）
SESSION = make_session()


def safe_get(url, params=None, timeout=None):
    """带限流检测的 GET，遇 429 或空响应自动等待重试。来源：fetch.py:111。"""
    timeout = timeout or config.REQUEST_TIMEOUT
    for attempt in range(config.MAX_RETRIES):
        try:
            resp = SESSION.get(url, params=params, headers=eastmoney_headers(), timeout=timeout)
            if resp.status_code == 429:
                log.warning("  限流(429)，等待 %ss  attempt=%s", config.RETRY_INTERVAL, attempt + 1)
                time.sleep(config.RETRY_INTERVAL)
                continue
            if not resp.text or len(resp.text) < 10:
                log.warning("  空响应，等待重试  attempt=%s", attempt + 1)
                time.sleep(config.RETRY_INTERVAL)
                continue
            return resp
        except requests.exceptions.Timeout:
            log.warning("  超时，重试  attempt=%s", attempt + 1)
            time.sleep(config.RETRY_INTERVAL)
        except requests.exceptions.ConnectionError as exc:
            log.warning("  连接错误: %s，重试  attempt=%s", exc, attempt + 1)
            time.sleep(config.RETRY_INTERVAL)
    raise RuntimeError(f"请求失败，已重试 {config.MAX_RETRIES} 次: {url}")


def safe_get_sina(symbols, timeout=10):
    """带重试的新浪行情请求。来源：fetch.py:135。"""
    url = f"{config.SINA_PRICE_URL}{symbols}"
    for attempt in range(config.MAX_RETRIES):
        try:
            resp = SESSION.get(url, headers=sina_headers(), timeout=timeout)
            if resp.status_code == 429:
                log.warning("  新浪限流(429)，等待 %ss  attempt=%s", config.RETRY_INTERVAL, attempt + 1)
                time.sleep(config.RETRY_INTERVAL)
                continue
            resp.raise_for_status()
            if not resp.text or "var hq_str_" not in resp.text:
                log.warning("  新浪空响应，等待重试  attempt=%s", attempt + 1)
                time.sleep(config.RETRY_INTERVAL)
                continue
            return resp
        except requests.exceptions.Timeout:
            log.warning("  新浪超时，重试  attempt=%s", attempt + 1)
            time.sleep(config.RETRY_INTERVAL)
        except requests.exceptions.ConnectionError as exc:
            log.warning("  新浪连接错误: %s，重试  attempt=%s", exc, attempt + 1)
            time.sleep(config.RETRY_INTERVAL)
        except requests.exceptions.RequestException as exc:
            log.warning("  新浪请求异常: %s，重试  attempt=%s", exc, attempt + 1)
            time.sleep(config.RETRY_INTERVAL)
    raise RuntimeError(f"新浪请求失败，已重试 {config.MAX_RETRIES} 次")


def get_json(url, params=None, timeout=None):
    """GET 并解析 JSON。"""
    resp = safe_get(url, params=params, timeout=timeout)
    return resp.json()
