"""CSI1000 择时蓝图：/api/csi1000-timing/*"""
from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime, timedelta

from flask import Blueprint, jsonify, request

from trader.core.db import connect_existing
from trader.core.utils import (
    clamp, json_safe, normalize_trade_date, local_now_text, coerce_int,
)
from trader.web.utils import to_int_arg
import trader.strategies.csi1000.timing as csi1000_timing

bp = Blueprint("csi1000", __name__)

_BASE_DIR = str(__import__("trader.core.config", fromlist=["BASE_DIR"]).BASE_DIR)
_BACKTEST_START = "2016-06-20"
_EXCEL_PATH = os.path.join(_BASE_DIR, "data", "历史新高新低300和1000.xlsx")
_RUN_KEY = "default"


def _csi1000_direction_text(direction):
    return {"LONG": "做多", "SHORT": "做空", "FLAT": "空仓"}.get(direction, direction or "")


def _csi1000_display_exit_reason(row, latest_signal):
    if row["exit_date"] is None:
        return "持有中"
    return str(row["exit_reason"] or "")


def _row_to_plain(row):
    if row is None:
        return None
    return {k: row[k] for k in row.keys()}


def _load_timing_payload(days: int = 180, run_key: str = _RUN_KEY) -> dict:
    days = clamp(int(days), 30, 3650)
    conn = connect_existing()
    try:
        csi1000_timing.init_db(conn)
        latest_row = conn.execute("""
            SELECT trade_date, signal, trade_state, action, reason, csi_close,
                   csi_score, hs300_score, csi_score_ma3, hs300_score_ma3,
                   vol_ratio_5_20, price_from_low10, drawdown_from_high10,
                   pct_2d, payload_json, updated_at
            FROM csi1000_timing_signals
            ORDER BY trade_date DESC LIMIT 1
        """).fetchone()
        latest_signal = _row_to_plain(latest_row) or {}
        if latest_signal.get("payload_json"):
            try:
                latest_signal["payload"] = json.loads(latest_signal.pop("payload_json"))
            except (TypeError, ValueError):
                latest_signal.pop("payload_json", None)
        anchor_text = latest_signal.get("trade_date") or datetime.now().strftime("%Y-%m-%d")
        anchor = datetime.strptime(anchor_text, "%Y-%m-%d")
        cutoff = (anchor - timedelta(days=days)).strftime("%Y-%m-%d")

        trade_rows = conn.execute("""
            SELECT id, run_key, direction, entry_date, entry_price, exit_date, exit_price,
                   exit_reason, hold_days, return_pct, signal_date, entry_reason
            FROM csi1000_timing_trades
            WHERE run_key = ?
              AND COALESCE(exit_date, entry_date) >= ?
            ORDER BY entry_date DESC, id DESC
        """, (run_key, cutoff)).fetchall()
        trades = []
        for row in trade_rows:
            item = _row_to_plain(row)
            item["direction_text"] = _csi1000_direction_text(item["direction"])
            item["exit_reason_text"] = _csi1000_display_exit_reason(row, latest_signal)
            item["is_open_mark"] = item["exit_reason_text"] == "持有中"
            trades.append(item)

        all_rows = conn.execute("""
            SELECT direction, return_pct FROM csi1000_timing_trades
            WHERE run_key = ? AND COALESCE(exit_date, entry_date) >= ?
        """, (run_key, cutoff)).fetchall()
        returns = [float(r["return_pct"]) for r in all_rows if r["return_pct"] is not None]
        long_returns = [float(r["return_pct"]) for r in all_rows
                        if r["direction"] == "LONG" and r["return_pct"] is not None]
        short_returns = [float(r["return_pct"]) for r in all_rows
                         if r["direction"] == "SHORT" and r["return_pct"] is not None]
        summary = {
            "days": days, "start_date": cutoff, "end_date": anchor_text,
            "trade_count": len(returns),
            "win_rate_pct": (sum(1 for x in returns if x > 0) / len(returns) * 100) if returns else 0,
            "return_sum_pct": sum(returns),
            "long_count": len(long_returns), "long_return_sum_pct": sum(long_returns),
            "short_count": len(short_returns), "short_return_sum_pct": sum(short_returns),
        }
        return {
            "run_key": run_key, "latest_signal": latest_signal,
            "summary": summary, "trades": trades, "updated_at": local_now_text(),
        }
    finally:
        conn.close()


def _run_timing_job(end_date=None, sync_index=False, backfill_width=False,
                    lookback_days=None) -> dict:
    cfg = csi1000_timing.STRATEGY_PRESETS["low_dd"]
    cfg = csi1000_timing.replace(cfg, fee_bps=2.0)
    target_date = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
    end_dash = target_date.strftime("%Y-%m-%d")
    start_dash = (
        (target_date - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        if lookback_days else _BACKTEST_START
    )
    from trader.core.config import DB_PATH
    conn = csi1000_timing.connect(DB_PATH)
    try:
        csi1000_timing.init_db(conn)
        if backfill_width:
            width_start = (target_date - timedelta(days=90)).strftime("%Y-%m-%d")
            csi1000_timing.cmd_backfill_width(conn, start=width_start, end=end_dash, force=True)
        df = csi1000_timing.load_frame_by_source(conn, "db", _EXCEL_PATH,
                                                  start=start_dash, end=end_dash)
        if df.empty:
            raise RuntimeError("没有可用的中证1000择时数据")
        if len(df) < 30:
            raise RuntimeError(f"可回测数据不足：合并后只有 {len(df)} 个交易日")
        signals = csi1000_timing.generate_and_save_signals(conn, df, cfg)
        result = csi1000_timing.backtest(conn, df, cfg, _RUN_KEY)
        latest = signals.tail(1).to_dict("records")[0] if not signals.empty else {}
        return {
            "ok": True, "run_key": _RUN_KEY,
            "start": start_dash, "end": end_dash,
            "synced_index": bool(sync_index),
            "backfilled_width": bool(backfill_width),
            "result": result,
            "latest_signal": {k: json_safe(v) for k, v in latest.items()},
            "updated_at": local_now_text(),
        }
    finally:
        conn.close()


@bp.route("/api/csi1000-timing")
def api_csi1000_timing():
    days = to_int_arg("days", 180, 30, 3650)
    return jsonify(_load_timing_payload(days=days))


@bp.route("/api/csi1000-timing/refresh", methods=["POST"])
def api_csi1000_timing_refresh():
    sync_index = str(request.args.get("syncIndex") or "").lower() in ("1", "true", "yes", "on")
    backfill_width = str(request.args.get("backfillWidth") or "").lower() in ("1", "true", "yes", "on")
    end_date = normalize_trade_date(request.args.get("date") or request.args.get("tradeDate"), None)
    payload = _run_timing_job(end_date=end_date, sync_index=sync_index, backfill_width=backfill_width)
    return jsonify(payload)


@bp.route("/api/csi1000-timing/run-today", methods=["POST"])
def api_csi1000_timing_run_today():
    script_path = os.path.join(_BASE_DIR, "scripts", "run_csi1000_1450_job.sh")
    started_at = time.time()
    proc = subprocess.run(
        [script_path], cwd=_BASE_DIR, text=True,
        capture_output=True, timeout=900, check=False,
    )
    payload = {
        "ok": proc.returncode == 0, "returncode": proc.returncode,
        "elapsed_s": round(time.time() - started_at, 1),
        "stdout_tail": proc.stdout[-6000:], "stderr_tail": proc.stderr[-6000:],
        "updated_at": local_now_text(),
    }
    if proc.returncode != 0:
        return jsonify(payload), 500
    payload["data"] = _load_timing_payload(days=180)
    return jsonify(payload), 200
