"""动量策略定时任务：每日扫描+结算、历史回填。

来源：app.py 中以下函数：
  - run_momentum_daily_job（~10055）
  - get_backfill_trade_dates（~10090）
  - run_momentum_backfill（~10265）
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta

from trader.core.db import connect_existing
from trader.strategies.momentum.scan import (
    perform_momentum_scan,
    perform_historical_momentum_scan,
    perform_daily_fallback_momentum_scan,
    build_daily_fallback_payload_from_history,
    load_daily_history_for_backfill,
)
from trader.strategies.momentum.storage import (
    ensure_momentum_tables,
    save_momentum_scan_result,
    settle_momentum_picks,
    summarize_backfill_returns,
)
from trader.data.realtime import load_stock_universe


def _get_backfill_trade_dates(conn, start_date=None, end_date=None, days=30) -> list[str]:
    if end_date is None:
        row = conn.execute("SELECT MAX(trade_date) FROM daily_prices").fetchone()
        end_date = row[0] if row and row[0] else datetime.today().strftime("%Y-%m-%d")
    if start_date is None:
        start_dt = datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=days)
        start_date = start_dt.strftime("%Y-%m-%d")
    rows = conn.execute("""
        SELECT DISTINCT trade_date FROM daily_prices
        WHERE trade_date >= ? AND trade_date <= ?
        ORDER BY trade_date
    """, (start_date, end_date)).fetchall()
    return [r["trade_date"] for r in rows]


def run_momentum_daily_job(params: dict, sell_date=None, sell_cutoff="10:00",
                            settle_buy_date=None) -> dict:
    """执行当日扫描，并同时对前一交易日的选股结算收益。"""
    conn = connect_existing()
    try:
        settlement = settle_momentum_picks(
            conn,
            sell_date=sell_date or params["trade_date"],
            sell_cutoff=sell_cutoff,
            buy_date=settle_buy_date,
        )
    finally:
        conn.close()

    payload, status_code = perform_momentum_scan(params, started_at=time.time())

    conn = connect_existing()
    try:
        run_id, saved = save_momentum_scan_result(conn, params, payload, status_code)
    finally:
        conn.close()

    return {
        "scan_status": status_code,
        "run_id": run_id,
        "saved": saved,
        "scan": payload,
        "settlement": settlement,
    }


def run_momentum_backfill(params: dict, start_date=None, end_date=None, days=30,
                           sell_cutoff="10:00", progress=None,
                           use_daily_fallback=True,
                           daily_fallback_only=False) -> dict:
    """历史区间回填：逐日扫描并结算下一交易日收益。"""
    conn = connect_existing()
    try:
        ensure_momentum_tables(conn)
        trade_dates = _get_backfill_trade_dates(
            conn, start_date=start_date, end_date=end_date, days=days,
        )
        fast_stocks = None
        fast_histories = None
        if daily_fallback_only and trade_dates:
            fast_stocks = load_stock_universe(conn, pool=params["pool"],
                                               index_code=params["index_code"])
            fast_histories = load_daily_history_for_backfill(
                conn, [s["code"] for s in fast_stocks],
                trade_dates[0], trade_dates[-1],
            )
    finally:
        conn.close()

    results = []
    for i, trade_date in enumerate(trade_dates):
        day_params = dict(params)
        day_params["trade_date"] = trade_date
        next_trade_date = trade_dates[i + 1] if i + 1 < len(trade_dates) else None

        if daily_fallback_only:
            payload, status_code = build_daily_fallback_payload_from_history(
                day_params, fast_stocks or {}, fast_histories or {}, trade_date,
                started_at=time.time(),
            )
            fallback_used = True
        else:
            payload, status_code = perform_historical_momentum_scan(
                day_params, started_at=time.time(),
            )
            fallback_used = False
            if use_daily_fallback and (
                status_code != 200
                or not payload.get("rows")
                or (payload.get("meta") or {}).get("minute_success", 0) == 0
            ):
                payload, status_code = perform_daily_fallback_momentum_scan(
                    day_params, started_at=time.time(),
                )
                fallback_used = True

        conn = connect_existing()
        try:
            run_id, saved = save_momentum_scan_result(conn, day_params, payload, status_code)
            settlement = None
            if next_trade_date:
                settlement = settle_momentum_picks(
                    conn,
                    sell_date=next_trade_date,
                    sell_cutoff=sell_cutoff,
                    buy_date=trade_date,
                    allow_daily_fallback=(use_daily_fallback or fallback_used),
                )
        finally:
            conn.close()

        item = {
            "trade_date": trade_date,
            "sell_date": next_trade_date,
            "status": status_code,
            "run_id": run_id,
            "saved": saved,
            "picked": len(payload.get("rows") or []),
            "error": payload.get("error"),
            "meta": payload.get("meta") or {},
            "fallback_used": fallback_used,
            "settlement": settlement,
        }
        results.append(item)
        if progress:
            progress(item, i + 1, len(trade_dates))

    summary = {}
    if trade_dates:
        conn = connect_existing()
        try:
            summary = summarize_backfill_returns(conn, trade_dates[0], trade_dates[-1])
        finally:
            conn.close()

    return {
        "start_date": trade_dates[0] if trade_dates else start_date,
        "end_date": trade_dates[-1] if trade_dates else end_date,
        "trade_dates": trade_dates,
        "days": len(trade_dates),
        "summary": summary,
        "results": results,
    }
