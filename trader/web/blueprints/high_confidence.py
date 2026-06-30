"""高置信选股蓝图：/api/high-confidence/*"""
from flask import Blueprint, jsonify, request
from trader.core.utils import normalize_trade_date
from trader.strategies.high_confidence.core import (
    build_hc_params,
    get_hc_progress,
    high_confidence_payload,
    start_high_confidence_sync,
)

bp = Blueprint("high_confidence", __name__)


@bp.route("/api/high-confidence/scan")
def api_high_confidence_scan():
    params = build_hc_params(request.args)
    return jsonify(high_confidence_payload(params))


@bp.route("/api/high-confidence/progress")
def api_high_confidence_progress():
    trade_date = normalize_trade_date(
        request.args.get("date") or request.args.get("tradeDate"), None,
    )
    progress = get_hc_progress(trade_date)
    return jsonify(progress or {"phase": "idle", "message": "暂无同步任务", "percent": 0})


@bp.route("/api/high-confidence/sync", methods=["POST"])
def api_high_confidence_sync():
    params = build_hc_params(request.args)
    payload, status = start_high_confidence_sync(params)
    return jsonify(payload), status
