"""Pattern 策略持久化：建表、进度追踪、保存/查询扫描结果。

来源：app.py ~342-494, ~491-564, ~4859-4990, ~4986-5500。
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta

from trader.core.db import connect_existing
from trader.core.utils import local_now_text, chunked
from trader.strategies.pattern.params import (
    public_pattern_params,
    pattern_type_from_params_json,
    build_empty_pattern_meta,
)
from trader.strategies.pattern.detection import (
    BOTTOM_SINGLE_PIN_PATTERNS,
    BOTTOM_STRONG_PATTERNS,
    bottom_pattern_group,
    bottom_pattern_allowed_names,
)
from trader.strategies.pattern.candle import build_candlestick_chart

_PROGRESS_COLUMNS = [
    "job_key", "job_type", "status", "started_at", "updated_at", "trade_date",
    "current_index", "total", "picked", "matched_rows", "matched_days",
    "elapsed_s", "message", "params_json", "result_json", "error",
]


# ── DDL ───────────────────────────────────────────────────────────────────

def ensure_pattern_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pattern_scan_runs (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date           DATE NOT NULL,
            pool                 TEXT NOT NULL,
            index_code           TEXT,
            lookback_days        INTEGER,
            chart_bars           INTEGER,
            min_amount_wan       REAL,
            min_turnover         REAL,
            max_body_pct         REAL,
            max_body_range_pct   REAL,
            max_amp_pct          REAL,
            doji_body_pct        REAL,
            max_ma40_distance    REAL,
            universe             INTEGER,
            scanned              INTEGER,
            row_count            INTEGER,
            elapsed_s            REAL,
            status               TEXT,
            error                TEXT,
            params_json          TEXT,
            created_at           DATETIME DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pattern_picks (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id         INTEGER NOT NULL,
            trade_date     DATE NOT NULL,
            code           TEXT NOT NULL,
            name           TEXT,
            close_price    REAL,
            pct_change     REAL,
            amount_yi      REAL,
            turnover       REAL,
            score          REAL,
            reasons        TEXT,
            row_json       TEXT,
            bars_json      TEXT,
            created_at     DATETIME DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pattern_scan_progress (
            job_key       TEXT PRIMARY KEY,
            job_type      TEXT,
            status        TEXT,
            started_at    DATETIME,
            updated_at    DATETIME,
            trade_date    DATE,
            current_index INTEGER,
            total         INTEGER,
            picked        INTEGER,
            matched_rows  INTEGER,
            matched_days  INTEGER,
            elapsed_s     REAL,
            message       TEXT,
            params_json   TEXT,
            result_json   TEXT,
            error         TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pattern_runs_date ON pattern_scan_runs(trade_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pattern_picks_run ON pattern_picks(run_id)")
    conn.commit()


# ── 进度追踪 ──────────────────────────────────────────────────────────────

def save_pattern_progress(job_key: str = "pattern_backfill", **updates):
    now = local_now_text()
    conn = connect_existing()
    try:
        ensure_pattern_tables(conn)
        row = conn.execute(
            "SELECT * FROM pattern_scan_progress WHERE job_key = ?", (job_key,)
        ).fetchone()
        data = {col: None for col in _PROGRESS_COLUMNS}
        data["job_key"] = job_key
        if row:
            data.update(dict(row))
        else:
            data["started_at"] = now
        for key, value in updates.items():
            if key in data:
                data[key] = value
        data["updated_at"] = now
        if updates.get("status") in ("running", "queued") and not updates.get("started_at") and not row:
            data["started_at"] = now
        placeholders = ",".join("?" for _ in _PROGRESS_COLUMNS)
        columns = ",".join(_PROGRESS_COLUMNS)
        conn.execute(
            f"REPLACE INTO pattern_scan_progress ({columns}) VALUES ({placeholders})",
            [data[col] for col in _PROGRESS_COLUMNS],
        )
        conn.commit()
    finally:
        conn.close()


def load_pattern_progress(job_key: str = "pattern_backfill") -> dict:
    conn = connect_existing()
    try:
        ensure_pattern_tables(conn)
        row = conn.execute(
            "SELECT * FROM pattern_scan_progress WHERE job_key = ?", (job_key,)
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return {"job_key": job_key, "status": "idle"}
    data = dict(row)
    for key in ("params_json", "result_json"):
        raw = data.pop(key, None)
        plain_key = key[:-5]
        if raw:
            try:
                data[plain_key] = json.loads(raw)
            except (TypeError, ValueError):
                data[plain_key] = raw
        else:
            data[plain_key] = None
    return data


# ── 保存/删除扫描结果 ────────────────────────────────────────────────────

def pattern_run_params(run) -> dict:
    try:
        return json.loads(run["params_json"] or "{}")
    except (TypeError, json.JSONDecodeError):
        return {}


def pattern_run_matches_type(run, pattern_type) -> bool:
    if not pattern_type:
        return True
    params = pattern_run_params(run)
    return (params.get("pattern_type") or "four_pin") == pattern_type


def delete_existing_pattern_scope(conn, params) -> int:
    rows = conn.execute("""
        SELECT id, params_json FROM pattern_scan_runs
        WHERE trade_date = ? AND pool = ? AND COALESCE(index_code,'') = COALESCE(?,'')
    """, (params["trade_date"], params["pool"], params["index_code"])).fetchall()
    run_ids = [
        row["id"] for row in rows
        if pattern_type_from_params_json(row["params_json"]) == params.get("pattern_type", "four_pin")
    ]
    if not run_ids:
        return 0
    placeholders = ",".join("?" for _ in run_ids)
    conn.execute(f"DELETE FROM pattern_picks WHERE run_id IN ({placeholders})", run_ids)
    conn.execute(f"DELETE FROM pattern_scan_runs WHERE id IN ({placeholders})", run_ids)
    return len(run_ids)


def delete_pattern_history(conn, pattern_type=None) -> dict:
    ensure_pattern_tables(conn)
    rows = conn.execute("SELECT id, params_json FROM pattern_scan_runs ORDER BY id").fetchall()
    if pattern_type in ("four_pin", "bottom_reversal"):
        run_ids = [
            row["id"] for row in rows
            if pattern_type_from_params_json(row["params_json"]) == pattern_type
        ]
    else:
        run_ids = [row["id"] for row in rows]
    pick_count = 0
    for batch in chunked(run_ids):
        placeholders = ",".join("?" for _ in batch)
        row = conn.execute(
            f"SELECT COUNT(*) AS count FROM pattern_picks WHERE run_id IN ({placeholders})",
            batch,
        ).fetchone()
        pick_count += row["count"] if row else 0
        conn.execute(f"DELETE FROM pattern_picks WHERE run_id IN ({placeholders})", batch)
        conn.execute(f"DELETE FROM pattern_scan_runs WHERE id IN ({placeholders})", batch)
    conn.execute("DELETE FROM pattern_scan_progress WHERE job_key IN ('pattern_scan','pattern_backfill')")
    conn.commit()
    return {"deleted_runs": len(run_ids), "deleted_picks": pick_count}


def save_pattern_scan_result(conn, params: dict, payload: dict, status_code: int):
    ensure_pattern_tables(conn)
    meta = payload.get("meta") or build_empty_pattern_meta(params)
    rows = payload.get("rows") or []
    status = "ok" if status_code == 200 else "error"
    delete_existing_pattern_scope(conn, params)
    cur = conn.execute("""
        INSERT INTO pattern_scan_runs (
            trade_date, pool, index_code, lookback_days, chart_bars,
            min_amount_wan, min_turnover, max_body_pct, max_body_range_pct,
            max_amp_pct, doji_body_pct, max_ma40_distance,
            universe, scanned, row_count, elapsed_s, status, error, params_json
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        params["trade_date"], params["pool"], params["index_code"],
        params["lookback_days"], params["chart_bars"],
        params["min_amount_wan"], params["min_turnover"],
        params["max_body_pct"], params["max_body_range_pct"],
        params["max_amp_pct"], params["doji_body_pct"],
        params["max_ma40_distance"], meta.get("universe"),
        meta.get("scanned"), len(rows), meta.get("elapsed_s"),
        status, payload.get("error"),
        json.dumps(public_pattern_params(params), ensure_ascii=False, sort_keys=True),
    ))
    run_id = cur.lastrowid
    for row in rows:
        row_copy = dict(row)
        bars = row_copy.pop("bars", [])
        row_copy.pop("chart", None)
        conn.execute("""
            INSERT INTO pattern_picks (
                run_id, trade_date, code, name, close_price, pct_change,
                amount_yi, turnover, score, reasons, row_json, bars_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            run_id, params["trade_date"], row.get("code"), row.get("name"),
            row.get("close"), row.get("pct"), row.get("amount_yi"),
            row.get("turnover"), row.get("score"), row.get("reasons"),
            json.dumps(row_copy, ensure_ascii=False, sort_keys=True),
            json.dumps(bars, ensure_ascii=False, sort_keys=True),
        ))
    conn.commit()
    return run_id, len(rows)


# ── 查询 ─────────────────────────────────────────────────────────────────

def saved_pattern_row_passes_filters(row, run_params, filters=None) -> bool:
    pattern_type = (run_params or {}).get("pattern_type", "four_pin")
    filters = filters or {}
    min_cap = filters.get("min_market_cap_yi")
    if min_cap:
        market_cap = row.get("market_cap_yi")
        if market_cap is None or market_cap < min_cap:
            return False

    if pattern_type == "four_pin":
        max_close_gap = filters.get("max_close_pair_distance",
                                     (run_params or {}).get("max_close_pair_distance", 1.0))
        first_close_gap = row.get("first_third_close_gap")
        second_close_gap = row.get("second_fourth_close_gap")
        if first_close_gap is None or second_close_gap is None:
            return False
        if first_close_gap > max_close_gap or second_close_gap > max_close_gap:
            return False
    elif pattern_type == "bottom_reversal":
        pattern_name = row.get("pattern_name")
        is_single_pin = pattern_name in BOTTOM_SINGLE_PIN_PATTERNS
        if pattern_name not in bottom_pattern_allowed_names(filters or run_params):
            return False

        def _fget(key, default=None):
            return filters.get(key, (run_params or {}).get(key, default))

        min_close_position = _fget("min_bottom_close_position", 55.0)
        require_confirm = _fget("require_bottom_confirm", 1)
        min_vr = _fget("min_bottom_volume_ratio", 0.0)
        max_vr = _fget("max_bottom_volume_ratio", 0.0)
        min_rebound = _fget("min_bottom_rebound_pct", 0.0)
        min_pct = _fget("min_bottom_pct_change", -20.0)
        min_strong = _fget("min_bottom_strong_gain_pct", 0.0)
        req_above_prev = _fget("require_bottom_close_above_prev", 0)
        req_above_ma5 = _fget("require_bottom_above_ma5", 0)
        min_ma5_slope = _fget("min_bottom_ma5_slope_pct")
        req_not_new_low = _fget("require_bottom_not_close_new_low", 0)

        if not is_single_pin:
            if row.get("close_position_pct") is None or row.get("close_position_pct") < min_close_position:
                return False
            if req_above_prev and not row.get("close_above_prev"):
                return False
            if req_above_ma5 and not row.get("above_ma5"):
                return False
            if min_ma5_slope is not None and row.get("ma5_slope_pct") is not None:
                if row["ma5_slope_pct"] < min_ma5_slope:
                    return False
            if req_not_new_low and row.get("close_new_low") is True:
                return False
            if row.get("pct") is None or row.get("pct") < min_pct:
                return False
            if row.get("rebound_pct") is None or row.get("rebound_pct") < min_rebound:
                return False
        if min_vr and (row.get("volume_ratio") is None or row.get("volume_ratio") < min_vr):
            return False
        if not is_single_pin and max_vr and row.get("volume_ratio") is not None:
            if row["volume_ratio"] > max_vr:
                return False
        if is_single_pin:
            if row.get("pct") is None or row.get("pct") < 0:
                return False
            if row.get("volume_ratio") is not None and row["volume_ratio"] > 5.0:
                return False
            if row.get("pin_low_break_5") is True:
                return False
            if row.get("pin_volume_overheat") is True:
                return False

        if pattern_name in ("早晨之星", "看涨吞没"):
            if row.get("pct") is None or row["pct"] < min_strong:
                return False
            if row.get("close_position_pct", 0) < 60:
                return False
        elif pattern_name == "曙光初现":
            if row.get("pct") is None or row["pct"] < min_strong:
                return False
            if row.get("close_position_pct", 0) < 65:
                return False

        if require_confirm:
            if is_single_pin:
                return True
            close_price = row.get("close")
            ma20 = row.get("ma20")
            above_ma20 = close_price is not None and ma20 is not None and close_price >= ma20
            bullish_pattern = row.get("pattern_name") in BOTTOM_STRONG_PATTERNS
            high_close = row.get("close_position_pct") is not None and row["close_position_pct"] >= 70
            if not (above_ma20 or bullish_pattern or high_close):
                return False
    return True


def load_pattern_rows_for_run(conn, run, highlight: int = 4, filters=None) -> list:
    run_params = pattern_run_params(run)
    rows = []
    for pick in conn.execute("""
        SELECT row_json, bars_json FROM pattern_picks
        WHERE run_id = ? ORDER BY score DESC, amount_yi DESC, code
    """, (run["id"],)).fetchall():
        row = json.loads(pick["row_json"] or "{}")
        if not saved_pattern_row_passes_filters(row, run_params, filters):
            continue
        bars = json.loads(pick["bars_json"] or "[]")
        row["bars"] = bars
        if not row.get("chart") and bars:
            row["chart"] = build_candlestick_chart(bars, highlight=highlight)
        rows.append(row)
    return rows


def load_latest_pattern_result(conn, trade_date=None, pattern_type=None, filters=None):
    ensure_pattern_tables(conn)
    if trade_date:
        runs = conn.execute("""
            SELECT * FROM pattern_scan_runs WHERE trade_date = ?
            ORDER BY id DESC LIMIT 100
        """, (trade_date,)).fetchall()
    else:
        runs = conn.execute("""
            SELECT * FROM pattern_scan_runs ORDER BY trade_date DESC, id DESC LIMIT 300
        """).fetchall()
    selected = None
    selected_rows = []
    empty_candidate = None
    empty_candidate_rows = []
    for item in runs:
        if not pattern_run_matches_type(item, pattern_type):
            continue
        rows = load_pattern_rows_for_run(conn, item, filters=filters)
        if rows:
            selected = item
            selected_rows = rows
            break
        if empty_candidate is None:
            empty_candidate = item
            empty_candidate_rows = rows
    if not selected:
        if empty_candidate is None:
            return None
        selected = empty_candidate
        selected_rows = empty_candidate_rows

    run = selected
    rows = selected_rows
    params = pattern_run_params(run)
    return {
        "meta": {
            "run_id": run["id"],
            "trade_date": run["trade_date"],
            "pool": run["pool"],
            "index_code": run["index_code"],
            "universe": run["universe"],
            "scanned": run["scanned"],
            "matched": run["row_count"],
            "elapsed_s": run["elapsed_s"],
            "created_at": run["created_at"],
            "params": {
                "pattern_type": params.get("pattern_type", "four_pin"),
                "max_body_pct": run["max_body_pct"],
                "doji_body_pct": run["doji_body_pct"],
                "max_amp_pct": run["max_amp_pct"],
                "max_ma40_distance": run["max_ma40_distance"],
                "min_amount_wan": run["min_amount_wan"],
                "min_turnover": run["min_turnover"],
                "min_market_cap_yi": params.get("min_market_cap_yi"),
                "max_pair_distance": params.get("max_pair_distance"),
                "max_close_pair_distance": params.get("max_close_pair_distance"),
                "min_level_gap": params.get("min_level_gap"),
                "bottom_lookback_days": params.get("bottom_lookback_days"),
                "max_bottom_position": params.get("max_bottom_position"),
                "min_prior_drop_pct": params.get("min_prior_drop_pct"),
                "bottom_only_bullish_engulfing": params.get("bottom_only_bullish_engulfing", 1),
                "bottom_pattern_group": bottom_pattern_group(params),
                "require_bottom_confirm": params.get("require_bottom_confirm"),
                "source_params": params,
            },
        },
        "rows": rows,
    }


def load_pattern_history(conn, days=None, hits_only=True, pattern_type=None,
                          filters=None, page=1, page_size=10) -> dict:
    ensure_pattern_tables(conn)
    row = conn.execute("""
        SELECT MAX(trade_date) AS end_date FROM pattern_scan_runs
        WHERE length(trade_date) = 10
    """).fetchone()
    end_date = row["end_date"] if row and row["end_date"] else None
    if not end_date:
        return {
            "start_date": None, "end_date": None, "days": days,
            "page": page, "page_size": page_size, "has_next": False, "runs": [],
        }
    start_date = None
    conditions = ["length(r.trade_date) = 10", "r.trade_date <= ?"]
    values = [end_date]
    if days:
        start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=days)).strftime("%Y-%m-%d")
        conditions.append("r.trade_date >= ?")
        values.append(start_date)
    if hits_only:
        conditions.append("r.row_count > 0")
    where_sql = " AND ".join(conditions)
    offset = (page - 1) * page_size

    if hits_only:
        date_rows = conn.execute(f"""
            SELECT DISTINCT r.trade_date FROM pattern_scan_runs r
            WHERE {where_sql} ORDER BY r.trade_date DESC
        """, values).fetchall()
        matching_runs = []
        for date_row in date_rows:
            td = date_row["trade_date"]
            runs = conn.execute(
                "SELECT * FROM pattern_scan_runs WHERE trade_date=? AND row_count>0 ORDER BY id DESC LIMIT 100",
                (td,),
            ).fetchall()
            for run in runs:
                if not pattern_run_matches_type(run, pattern_type):
                    continue
                picks = load_pattern_rows_for_run(conn, run, highlight=4, filters=filters)
                if not picks:
                    continue
                params = pattern_run_params(run)
                matching_runs.append({
                    "run_id": run["id"], "trade_date": run["trade_date"], "pool": run["pool"],
                    "pattern_type": params.get("pattern_type", "four_pin"),
                    "index_code": run["index_code"], "universe": run["universe"],
                    "scanned": run["scanned"], "matched": run["row_count"],
                    "elapsed_s": run["elapsed_s"], "created_at": run["created_at"],
                    "rows": picks,
                })
                break

        total_rows = sum(len(r["rows"]) for r in matching_runs)
        page_runs = matching_runs[offset:offset + page_size]
        return {
            "start_date": start_date, "end_date": end_date, "days": days,
            "hits_only": hits_only, "page": page, "page_size": page_size,
            "pagination_mode": "trade_dates",
            "has_prev": page > 1,
            "has_next": offset + page_size < len(matching_runs),
            "page_trade_dates": [r["trade_date"] for r in page_runs],
            "page_row_count": sum(len(r["rows"]) for r in page_runs),
            "total_rows": total_rows,
            "total_trade_dates": len(matching_runs),
            "runs": page_runs,
        }

    date_rows = conn.execute(f"""
        SELECT DISTINCT r.trade_date FROM pattern_scan_runs r
        WHERE {where_sql} ORDER BY r.trade_date DESC LIMIT ? OFFSET ?
    """, values + [page_size + 1, offset]).fetchall()
    page_trade_dates = [r["trade_date"] for r in date_rows[:page_size]]
    has_next = len(date_rows) > page_size
    result_runs = []
    run_cond = "trade_date = ?" + (" AND row_count > 0" if hits_only else "")
    for td in page_trade_dates:
        runs = conn.execute(
            f"SELECT * FROM pattern_scan_runs WHERE {run_cond} ORDER BY id DESC LIMIT 100",
            (td,),
        ).fetchall()
        for run in runs:
            if not pattern_run_matches_type(run, pattern_type):
                continue
            picks = load_pattern_rows_for_run(conn, run, highlight=4, filters=filters)
            if hits_only and not picks:
                continue
            params = pattern_run_params(run)
            result_runs.append({
                "run_id": run["id"], "trade_date": run["trade_date"], "pool": run["pool"],
                "pattern_type": params.get("pattern_type", "four_pin"),
                "index_code": run["index_code"], "universe": run["universe"],
                "scanned": run["scanned"], "matched": run["row_count"],
                "elapsed_s": run["elapsed_s"], "created_at": run["created_at"],
                "rows": picks,
            })
            break
    return {
        "start_date": start_date, "end_date": end_date, "days": days,
        "hits_only": hits_only, "page": page, "page_size": page_size,
        "has_prev": page > 1, "has_next": has_next,
        "page_trade_dates": page_trade_dates, "runs": result_runs,
    }
