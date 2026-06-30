"""Momentum 动量蓝图：/api/momentum/*"""
from __future__ import annotations

import threading
import time

from flask import Blueprint, jsonify, request

from trader.core.db import connect_existing
from trader.web.utils import to_int_arg
from trader.strategies.momentum import (
    build_momentum_params,
    perform_momentum_scan,
    load_momentum_profit_summary,
)

bp = Blueprint("momentum", __name__)
_SCAN_LOCK = threading.Lock()


@bp.route("/api/momentum/profit")
def api_momentum_profit():
    days = to_int_arg("days", 30, 1, 250)
    conn = connect_existing()
    try:
        return jsonify(load_momentum_profit_summary(conn, days=days))
    finally:
        conn.close()


@bp.route("/api/momentum/scan")
def api_momentum_scan():
    started_at = time.time()
    if not _SCAN_LOCK.acquire(blocking=False):
        return jsonify({
            "error": "已有扫描任务进行中，请等待上一次扫描结束",
            "meta": {
                "quoted": 0, "prefiltered": 0, "verified": 0,
                "minute_success": 0, "minute_failed": 0,
                "cache_hits": 0, "elapsed_s": 0,
            },
            "rows": [],
        }), 429
    try:
        payload, status = perform_momentum_scan(
            build_momentum_params(request.args),
            started_at=started_at,
        )
        return jsonify(payload), status
    finally:
        _SCAN_LOCK.release()
