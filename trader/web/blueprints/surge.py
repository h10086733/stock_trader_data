"""Surge（次日涨停）蓝图：/api/surge/*

start_surge_scan / get_surge_progress / load_surge_history / load_surge_batch
仍在 app.py 中（尚未迁到 trader/strategies/surge/），通过直接 import 使用。
"""
from flask import Blueprint, jsonify, request
from trader.core.db import connect_existing
from trader.web.utils import to_int_arg

bp = Blueprint("surge", __name__)

# 从 app.py 中的运行时状态（在 create_app 时注入）
_get_surge_progress = None
_start_surge_scan = None
_load_surge_history = None
_load_surge_batch = None
_build_surge_params = None


def register_handlers(*, get_progress, start_scan, load_history, load_batch, build_params):
    """由 create_app 注入 app.py 中的函数引用（过渡期用）。"""
    global _get_surge_progress, _start_surge_scan, _load_surge_history, _load_surge_batch, _build_surge_params
    _get_surge_progress = get_progress
    _start_surge_scan = start_scan
    _load_surge_history = load_history
    _load_surge_batch = load_batch
    _build_surge_params = build_params


@bp.route("/api/surge/scan", methods=["POST"])
def api_surge_scan():
    params = _build_surge_params(request.args)
    payload, status = _start_surge_scan(params)
    return jsonify(payload), status


@bp.route("/api/surge/progress")
def api_surge_progress():
    return jsonify(_get_surge_progress())


@bp.route("/api/surge/history")
def api_surge_history():
    limit = to_int_arg("limit", 20, 1, 100)
    conn = connect_existing()
    try:
        batches = _load_surge_history(conn, limit=limit)
    finally:
        conn.close()
    return jsonify({"batches": batches})


@bp.route("/api/surge/latest")
def api_surge_latest():
    limit = to_int_arg("limit", 10, 1, 1000)
    conn = connect_existing()
    try:
        payload = _load_surge_batch(conn, limit=limit)
    finally:
        conn.close()
    if not payload:
        return jsonify({"error": "暂无涨停/大涨扫描批次"}), 404
    return jsonify(payload)


@bp.route("/api/surge/batch/<int:batch_id>")
def api_surge_batch(batch_id):
    limit = to_int_arg("limit", 10, 1, 1000)
    conn = connect_existing()
    try:
        payload = _load_surge_batch(conn, batch_id=batch_id, limit=limit)
    finally:
        conn.close()
    if not payload:
        return jsonify({"error": "批次不存在"}), 404
    return jsonify(payload)
