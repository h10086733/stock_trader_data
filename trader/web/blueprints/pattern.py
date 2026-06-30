"""Pattern 形态蓝图：/api/pattern/*"""
from __future__ import annotations

import json
import threading
import time

from flask import Blueprint, jsonify, request

from trader.core.db import connect_existing
from trader.core.utils import clamp, get_source_value, local_now_text, normalize_trade_date
from trader.web.utils import to_int_arg
from trader.strategies.pattern import (
    build_pattern_params,
    public_pattern_params,
    saved_pattern_filters_enabled,
    build_empty_pattern_meta,
    default_pattern_backfill_days,
    normalize_pattern_backfill_params,
    perform_pattern_scan,
    run_pattern_backfill_job,
    save_pattern_progress,
    load_pattern_progress,
    save_pattern_scan_result,
    delete_pattern_history,
    load_latest_pattern_result,
    load_pattern_history,
)

bp = Blueprint("pattern", __name__)
_SCAN_LOCK = threading.Lock()


@bp.route("/api/pattern/latest")
def api_pattern_latest():
    params = build_pattern_params(request.args)
    trade_date = params.get("trade_date")
    pattern_type = params["pattern_type"]
    filters = params if saved_pattern_filters_enabled(request.args) else None
    conn = connect_existing()
    try:
        payload = load_latest_pattern_result(conn, trade_date=trade_date,
                                              pattern_type=pattern_type, filters=filters)
    finally:
        conn.close()
    if not payload:
        return jsonify({
            "error": "暂无保存的形态扫描结果，请先点击扫描并保存",
            "meta": build_empty_pattern_meta(build_pattern_params(request.args)),
            "rows": [],
        }), 404
    return jsonify(payload)


@bp.route("/api/pattern/scan")
def api_pattern_scan():
    started_at = time.time()
    if not _SCAN_LOCK.acquire(blocking=False):
        return jsonify({
            "error": "已有扫描任务进行中，请等待上一次扫描结束",
            "meta": build_empty_pattern_meta(build_pattern_params(request.args)),
            "rows": [],
        }), 429
    try:
        params = build_pattern_params(request.args)
        save_pattern_progress(
            "pattern_scan", job_type="scan", status="running",
            started_at=local_now_text(), trade_date=params.get("trade_date"),
            current_index=0, total=1, picked=0, matched_rows=0, matched_days=0,
            elapsed_s=0, message="正在扫描",
            params_json=json.dumps(params, ensure_ascii=False, sort_keys=True),
            result_json=None, error=None,
        )
        payload, status = perform_pattern_scan(params, started_at=started_at)
        save_requested = request.args.get("save", "0") in ("1", "true", "yes")
        if save_requested:
            conn = connect_existing()
            try:
                run_id, saved = save_pattern_scan_result(conn, params, payload, status)
            finally:
                conn.close()
            payload["run_id"] = run_id
            payload["saved"] = saved
        matched = len(payload.get("rows") or [])
        save_pattern_progress(
            "pattern_scan", job_type="scan",
            status="done" if status == 200 else "error",
            trade_date=(payload.get("meta") or {}).get("trade_date"),
            current_index=1, total=1, picked=matched, matched_rows=matched,
            matched_days=1 if matched else 0,
            elapsed_s=(payload.get("meta") or {}).get("elapsed_s"),
            message="扫描完成" if status == 200 else "扫描失败",
            result_json=json.dumps({
                "trade_date": (payload.get("meta") or {}).get("trade_date"),
                "matched_rows": matched,
                "elapsed_s": (payload.get("meta") or {}).get("elapsed_s"),
            }, ensure_ascii=False, sort_keys=True),
            error=payload.get("error"),
        )
        return jsonify(payload), status
    except Exception as exc:
        save_pattern_progress(
            "pattern_scan", job_type="scan", status="error",
            elapsed_s=round(time.time() - started_at, 1),
            message="扫描失败", error=str(exc),
        )
        raise
    finally:
        _SCAN_LOCK.release()


@bp.route("/api/pattern/history")
def api_pattern_history():
    days_arg = request.args.get("days")
    days = None
    if days_arg not in (None, ""):
        try:
            days = clamp(int(days_arg), 1, 3650)
        except (TypeError, ValueError):
            days = None
    page = to_int_arg("page", 1, 1, 10000)
    page_size = to_int_arg("pageSize", 10, 1, 10)
    hits_only = request.args.get("hitsOnly", "1") not in ("0", "false", "no")
    params = build_pattern_params(request.args)
    pattern_type = params["pattern_type"]
    filters = params if saved_pattern_filters_enabled(request.args) else None
    conn = connect_existing()
    try:
        payload = load_pattern_history(
            conn, days=days, hits_only=hits_only, pattern_type=pattern_type,
            filters=filters, page=page, page_size=page_size,
        )
    finally:
        conn.close()
    return jsonify(payload)


@bp.route("/api/pattern/clear", methods=["POST"])
def api_pattern_clear():
    if request.args.get("confirm") != "1":
        return jsonify({"error": "缺少确认参数"}), 400
    if not _SCAN_LOCK.acquire(blocking=False):
        return jsonify({"error": "已有扫描任务进行中，请等待上一次扫描结束"}), 429
    try:
        raw_pt = get_source_value(request.args, "patternType", "pattern_type")
        pattern_type = None if raw_pt in ("all", "*") else build_pattern_params(request.args)["pattern_type"]
        conn = connect_existing()
        try:
            result = delete_pattern_history(conn, pattern_type=pattern_type)
        finally:
            conn.close()
        result["pattern_type"] = pattern_type or "all"
        return jsonify(result)
    finally:
        _SCAN_LOCK.release()


@bp.route("/api/pattern/backfill")
def api_pattern_backfill():
    if not _SCAN_LOCK.acquire(blocking=False):
        return jsonify({"error": "已有扫描任务进行中，请等待上一次扫描结束"}), 429
    params = normalize_pattern_backfill_params(build_pattern_params(request.args))
    if request.args.get("days") in (None, ""):
        days = default_pattern_backfill_days(params)
    else:
        days = to_int_arg("days", default_pattern_backfill_days(params), 1, 3650)
    save_pattern_progress(
        "pattern_backfill", job_type="backfill", status="running",
        started_at=local_now_text(), trade_date=params.get("trade_date"),
        current_index=0, total=0, picked=0, matched_rows=0, matched_days=0,
        elapsed_s=0, message="回扫任务已启动",
        params_json=json.dumps(params, ensure_ascii=False, sort_keys=True),
        result_json=None, error=None,
    )
    thread = threading.Thread(
        target=run_pattern_backfill_job,
        args=(params, days, params.get("trade_date")),
        daemon=True,
    )
    try:
        thread.start()
    except Exception:
        _SCAN_LOCK.release()
        raise
    return jsonify({"status": "running", "job_key": "pattern_backfill",
                    "message": "回扫任务已启动", "days": days})


@bp.route("/api/pattern/progress")
def api_pattern_progress():
    job_key = request.args.get("job", "pattern_backfill")
    return jsonify(load_pattern_progress(job_key))
