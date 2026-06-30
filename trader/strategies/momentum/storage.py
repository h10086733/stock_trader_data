"""动量策略持久化：建表、保存扫描结果、结算收益、查询汇总。

来源：app.py 中以下函数：
  - ensure_momentum_tables（~262）
  - save_momentum_scan_result（~9844）
  - load_momentum_picks_for_settlement（~9918）
  - settle_momentum_picks（~9942）
  - summarize_backfill_returns（~10102）
  - load_momentum_profit_summary（~10132）
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
from datetime import time as dt_time
from typing import Optional

from trader.core.db import connect_existing
from trader.core.utils import parse_cutoff_time, default_scan_trade_date
from trader.data.realtime import (
    infer_market,
    fetch_minute_kline,
)
from trader.strategies.momentum.params import build_empty_scan_meta


# ── DDL ───────────────────────────────────────────────────────────────────

def ensure_momentum_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS momentum_scan_runs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date      DATE NOT NULL,
            cutoff          TEXT NOT NULL,
            pool            TEXT NOT NULL,
            index_code      TEXT,
            min_gain        REAL,
            max_gain        REAL,
            min_vol_ratio   REAL,
            min_amount_wan  REAL,
            limit_count     INTEGER,
            verify_limit    INTEGER,
            workers         INTEGER,
            universe        INTEGER,
            quoted          INTEGER,
            prefiltered     INTEGER,
            verified        INTEGER,
            minute_success  INTEGER,
            minute_failed   INTEGER,
            cache_hits      INTEGER,
            elapsed_s       REAL,
            row_count       INTEGER,
            status          TEXT,
            error           TEXT,
            params_json     TEXT,
            created_at      DATETIME DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS momentum_picks (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id             INTEGER,
            trade_date         DATE NOT NULL,
            cutoff             TEXT NOT NULL,
            pool               TEXT NOT NULL,
            index_code         TEXT,
            code               TEXT NOT NULL,
            name               TEXT,
            buy_price          REAL,
            buy_pct            REAL,
            score              REAL,
            amount_yi          REAL,
            volume_ratio       REAL,
            volume_full_ratio  REAL,
            close_position     REAL,
            pullback_pct       REAL,
            high_time          TEXT,
            reasons            TEXT,
            row_json           TEXT,
            created_at         DATETIME DEFAULT (datetime('now','localtime')),
            updated_at         DATETIME DEFAULT (datetime('now','localtime')),
            UNIQUE(trade_date, cutoff, pool, index_code, code)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS momentum_pick_returns (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            pick_id        INTEGER NOT NULL UNIQUE,
            buy_date       DATE NOT NULL,
            sell_date      DATE NOT NULL,
            code           TEXT NOT NULL,
            name           TEXT,
            buy_price      REAL,
            sell_price     REAL,
            return_pct     REAL,
            sell_cutoff    TEXT NOT NULL,
            sell_time      TEXT,
            status         TEXT NOT NULL,
            error          TEXT,
            created_at     DATETIME DEFAULT (datetime('now','localtime')),
            updated_at     DATETIME DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mom_picks_date ON momentum_picks(trade_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_mom_returns_date ON momentum_pick_returns(sell_date)")
    conn.commit()


# ── 保存扫描结果 ─────────────────────────────────────────────────────────

def save_momentum_scan_result(conn, params: dict, payload: dict, status_code: int):
    ensure_momentum_tables(conn)
    meta = payload.get("meta") or build_empty_scan_meta(params)
    rows = payload.get("rows") or []
    status = "ok" if status_code == 200 else "error"
    cur = conn.execute("""
        INSERT INTO momentum_scan_runs (
            trade_date, cutoff, pool, index_code,
            min_gain, max_gain, min_vol_ratio, min_amount_wan,
            limit_count, verify_limit, workers,
            universe, quoted, prefiltered, verified,
            minute_success, minute_failed, cache_hits, elapsed_s,
            row_count, status, error, params_json
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        params["trade_date"], params["cutoff"], params["pool"], params["index_code"],
        params["min_gain"], params["max_gain"], params["min_vol_ratio"],
        params["min_amount_wan"], params["limit"], params["verify_limit"],
        params["workers"], meta.get("universe"), meta.get("quoted"),
        meta.get("prefiltered"), meta.get("verified"), meta.get("minute_success"),
        meta.get("minute_failed"), meta.get("cache_hits"), meta.get("elapsed_s"),
        len(rows), status, payload.get("error"),
        json.dumps(params, ensure_ascii=False, sort_keys=True),
    ))
    run_id = cur.lastrowid

    for row in rows:
        conn.execute("""
            INSERT INTO momentum_picks (
                run_id, trade_date, cutoff, pool, index_code,
                code, name, buy_price, buy_pct, score, amount_yi,
                volume_ratio, volume_full_ratio, close_position,
                pullback_pct, high_time, reasons, row_json,
                created_at, updated_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now','localtime'),datetime('now','localtime'))
            ON CONFLICT(trade_date, cutoff, pool, index_code, code) DO UPDATE SET
                run_id            = excluded.run_id,
                name              = excluded.name,
                buy_price         = excluded.buy_price,
                buy_pct           = excluded.buy_pct,
                score             = excluded.score,
                amount_yi         = excluded.amount_yi,
                volume_ratio      = excluded.volume_ratio,
                volume_full_ratio = excluded.volume_full_ratio,
                close_position    = excluded.close_position,
                pullback_pct      = excluded.pullback_pct,
                high_time         = excluded.high_time,
                reasons           = excluded.reasons,
                row_json          = excluded.row_json,
                updated_at        = datetime('now','localtime')
        """, (
            run_id, params["trade_date"], params["cutoff"], params["pool"],
            params["index_code"], row.get("code"), row.get("name"),
            row.get("price"), row.get("pct"), row.get("score"),
            row.get("amount_yi"), row.get("volume_ratio"),
            row.get("volume_full_ratio"), row.get("close_position"),
            row.get("pullback_pct"), row.get("high_time"), row.get("reasons"),
            json.dumps(row, ensure_ascii=False, sort_keys=True),
        ))
    conn.commit()
    return run_id, len(rows)


# ── 结算 ─────────────────────────────────────────────────────────────────

def _latest_pick_trade_date_before(conn, sell_date: str) -> Optional[str]:
    row = conn.execute("""
        SELECT MAX(trade_date) AS trade_date
        FROM momentum_picks WHERE trade_date < ?
    """, (sell_date,)).fetchone()
    return row["trade_date"] if row and row["trade_date"] else None


def load_momentum_picks_for_settlement(conn, buy_date: str, sell_date: str):
    return conn.execute("""
        SELECT p.*, CASE
                   WHEN p.code LIKE '5%' OR p.code LIKE '6%' OR p.code LIKE '9%' THEN '1'
                   ELSE '0'
               END AS market
        FROM momentum_picks p
        LEFT JOIN momentum_pick_returns r ON r.pick_id = p.id
        WHERE p.trade_date = ?
          AND (r.id IS NULL OR r.status != 'sold')
        ORDER BY p.score DESC, p.code
    """, (buy_date,)).fetchall()


def _get_daily_open_price(conn, code: str, trade_date: str) -> Optional[float]:
    row = conn.execute("""
        SELECT open FROM daily_prices WHERE code = ? AND trade_date = ?
    """, (code, trade_date)).fetchone()
    return row["open"] if row and row["open"] else None


def settle_momentum_picks(conn, sell_date: Optional[str] = None,
                           sell_cutoff: str = "10:00",
                           buy_date: Optional[str] = None,
                           allow_daily_fallback: bool = False) -> dict:
    sell_date = sell_date or default_scan_trade_date()
    sell_cutoff = parse_cutoff_time(sell_cutoff).strftime("%H:%M")
    ensure_momentum_tables(conn)
    buy_date = buy_date or _latest_pick_trade_date_before(conn, sell_date)
    if not buy_date:
        return {
            "buy_date": None, "sell_date": sell_date, "sell_cutoff": sell_cutoff,
            "settled": 0, "failed": 0,
            "message": "没有找到待结算的前一交易日选股记录", "rows": [],
        }

    picks = load_momentum_picks_for_settlement(conn, buy_date, sell_date)
    settled = 0
    failed = 0
    result_rows = []
    for pick in picks:
        stock = {
            "code": pick["code"],
            "name": pick["name"] or "",
            "market": infer_market(pick["code"], pick["market"]),
        }
        buy_price = pick["buy_price"]
        status = "sold"
        error = None
        sell_price = None
        sell_time = None
        return_pct = None

        if not buy_price or buy_price <= 0:
            status = "invalid_buy_price"
            error = "买入价为空"
        else:
            bars = fetch_minute_kline(stock, sell_cutoff, trade_date=sell_date)
            sell_bars = [b for b in bars if b.get("close") and b.get("time") <= sell_cutoff]
            if not sell_bars:
                fallback_open = (
                    _get_daily_open_price(conn, pick["code"], sell_date)
                    if allow_daily_fallback else None
                )
                if fallback_open:
                    sell_price = fallback_open
                    sell_time = "09:30*"
                    return_pct = (sell_price - buy_price) / buy_price * 100.0
                    error = "daily_open_fallback"
                else:
                    status = "no_sell_kline"
                    error = "10点前分钟线为空"
            else:
                bar = sell_bars[-1]
                sell_price = bar["close"]
                sell_time = bar["time"]
                return_pct = (sell_price - buy_price) / buy_price * 100.0

        if status == "sold":
            settled += 1
        else:
            failed += 1

        conn.execute("""
            INSERT INTO momentum_pick_returns (
                pick_id, buy_date, sell_date, code, name,
                buy_price, sell_price, return_pct,
                sell_cutoff, sell_time, status, error,
                created_at, updated_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,datetime('now','localtime'),datetime('now','localtime'))
            ON CONFLICT(pick_id) DO UPDATE SET
                sell_date   = excluded.sell_date,
                buy_price   = excluded.buy_price,
                sell_price  = excluded.sell_price,
                return_pct  = excluded.return_pct,
                sell_cutoff = excluded.sell_cutoff,
                sell_time   = excluded.sell_time,
                status      = excluded.status,
                error       = excluded.error,
                updated_at  = datetime('now','localtime')
        """, (
            pick["id"], buy_date, sell_date, pick["code"], pick["name"],
            buy_price, sell_price,
            round(return_pct, 4) if return_pct is not None else None,
            sell_cutoff, sell_time, status, error,
        ))
        result_rows.append({
            "code": pick["code"], "name": pick["name"],
            "buy_price": buy_price, "sell_price": sell_price,
            "return_pct": round(return_pct, 4) if return_pct is not None else None,
            "sell_time": sell_time, "status": status, "error": error,
        })

    conn.commit()
    sold_returns = [r["return_pct"] for r in result_rows if r["return_pct"] is not None]
    avg_return = sum(sold_returns) / len(sold_returns) if sold_returns else None
    return {
        "buy_date": buy_date, "sell_date": sell_date, "sell_cutoff": sell_cutoff,
        "settled": settled, "failed": failed,
        "avg_return_pct": round(avg_return, 4) if avg_return is not None else None,
        "rows": result_rows,
    }


# ── 统计汇总 ─────────────────────────────────────────────────────────────

def summarize_backfill_returns(conn, start_date: str, end_date: str) -> dict:
    row = conn.execute("""
        SELECT COUNT(*) AS n,
               AVG(return_pct) AS avg_return,
               SUM(CASE WHEN return_pct > 0 THEN 1 ELSE 0 END) AS win_count,
               MIN(return_pct) AS min_return,
               MAX(return_pct) AS max_return
        FROM momentum_pick_returns
        WHERE buy_date >= ? AND buy_date <= ? AND status = 'sold'
    """, (start_date, end_date)).fetchone()
    n = row["n"] if row else 0
    return {
        "count": n,
        "avg_return_pct": round(row["avg_return"], 4) if row and row["avg_return"] is not None else None,
        "win_rate_pct": round(row["win_count"] / n * 100.0, 2) if n else None,
        "min_return_pct": round(row["min_return"], 4) if row and row["min_return"] is not None else None,
        "max_return_pct": round(row["max_return"], 4) if row and row["max_return"] is not None else None,
    }


def _exact_return_clause() -> str:
    return """
        AND status = 'sold'
        AND COALESCE(error, '') != 'daily_open_fallback'
        AND sell_time = '10:00'
    """


def load_momentum_profit_summary(conn, days: int = 30, exact_only: bool = True) -> dict:
    ensure_momentum_tables(conn)
    exact_filter = _exact_return_clause() if exact_only else ""
    row = conn.execute(
        "SELECT MAX(buy_date) AS end_date FROM momentum_pick_returns WHERE 1=1 " + exact_filter
    ).fetchone()
    end_date = row["end_date"] if row and row["end_date"] else None
    if not end_date:
        return {
            "start_date": None, "end_date": None, "days": days,
            "summary": {
                "sold_count": 0, "failed_count": 0, "avg_return_pct": None,
                "win_rate_pct": None, "min_return_pct": None, "max_return_pct": None,
            },
            "by_date": [], "recent": [], "exact_only": exact_only,
        }

    start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=days)).strftime("%Y-%m-%d")
    sr = conn.execute("""
        SELECT
            SUM(CASE WHEN status = 'sold' THEN 1 ELSE 0 END) AS sold_count,
            SUM(CASE WHEN status != 'sold' THEN 1 ELSE 0 END) AS failed_count,
            AVG(CASE WHEN status = 'sold' THEN return_pct END) AS avg_return,
            SUM(CASE WHEN status = 'sold' AND return_pct > 0 THEN 1 ELSE 0 END) AS win_count,
            MIN(CASE WHEN status = 'sold' THEN return_pct END) AS min_return,
            MAX(CASE WHEN status = 'sold' THEN return_pct END) AS max_return
        FROM momentum_pick_returns
        WHERE buy_date >= ? AND buy_date <= ?
    """ + exact_filter, (start_date, end_date)).fetchone()
    sold_count = sr["sold_count"] or 0
    summary = {
        "sold_count": sold_count,
        "failed_count": sr["failed_count"] or 0,
        "avg_return_pct": round(sr["avg_return"], 4) if sr["avg_return"] is not None else None,
        "win_rate_pct": round((sr["win_count"] or 0) / sold_count * 100.0, 2) if sold_count else None,
        "min_return_pct": round(sr["min_return"], 4) if sr["min_return"] is not None else None,
        "max_return_pct": round(sr["max_return"], 4) if sr["max_return"] is not None else None,
    }

    by_date = []
    for row in conn.execute("""
        SELECT buy_date,
               COUNT(*) AS total_count,
               SUM(CASE WHEN status = 'sold' THEN 1 ELSE 0 END) AS sold_count,
               SUM(CASE WHEN status != 'sold' THEN 1 ELSE 0 END) AS failed_count,
               AVG(CASE WHEN status = 'sold' THEN return_pct END) AS avg_return,
               SUM(CASE WHEN status = 'sold' AND return_pct > 0 THEN 1 ELSE 0 END) AS win_count
        FROM momentum_pick_returns
        WHERE buy_date >= ? AND buy_date <= ?
    """ + exact_filter + """
        GROUP BY buy_date ORDER BY buy_date DESC
    """, (start_date, end_date)).fetchall():
        day_sold = row["sold_count"] or 0
        by_date.append({
            "buy_date": row["buy_date"],
            "total_count": row["total_count"] or 0,
            "sold_count": day_sold,
            "failed_count": row["failed_count"] or 0,
            "avg_return_pct": round(row["avg_return"], 4) if row["avg_return"] is not None else None,
            "win_rate_pct": round((row["win_count"] or 0) / day_sold * 100.0, 2) if day_sold else None,
        })

    recent = []
    for row in conn.execute("""
        SELECT buy_date, sell_date, code, name,
               buy_price, sell_price, return_pct, sell_time, status, error
        FROM momentum_pick_returns
        WHERE buy_date >= ? AND buy_date <= ?
    """ + exact_filter + """
        ORDER BY buy_date DESC, return_pct DESC LIMIT 12
    """, (start_date, end_date)).fetchall():
        recent.append({
            "buy_date": row["buy_date"], "sell_date": row["sell_date"],
            "code": row["code"], "name": row["name"],
            "buy_price": row["buy_price"], "sell_price": row["sell_price"],
            "return_pct": row["return_pct"], "sell_time": row["sell_time"],
            "status": row["status"], "error": row["error"],
        })

    return {
        "start_date": start_date, "end_date": end_date, "days": days,
        "summary": summary, "by_date": by_date, "recent": recent,
        "exact_only": exact_only,
    }


# re-export for convenience
from trader.strategies.momentum.scan import (  # noqa: E402  (circular-safe, scan doesn't import storage)
    load_daily_metrics_before,
    load_historical_daily_quotes,
)
