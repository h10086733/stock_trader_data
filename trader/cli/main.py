"""trader CLI 主入口：argparse 参数定义 + main() 调度器。

来源：app.py parse_cli_args()（~10679）、main()（~10811）及其辅助输出函数（~10529-10680）。
"""
from __future__ import annotations

import argparse
import json
import sys
import time

from trader.core.db import connect_existing
from trader.core.utils import local_now_text
from trader.strategies.momentum import (
    build_momentum_params,
    build_cli_momentum_params,
    perform_momentum_scan,
    save_momentum_scan_result,
    settle_momentum_picks,
    ensure_momentum_tables,
    run_momentum_daily_job,
    run_momentum_backfill,
)
from trader.strategies.pattern import (
    build_cli_pattern_params,
    normalize_pattern_backfill_params,
    default_pattern_backfill_days,
    perform_pattern_scan,
    save_pattern_scan_result,
    run_pattern_backfill,
)
from trader.strategies.csi1000.timing import (
    STRATEGY_PRESETS,
    replace as timing_replace,
    connect as timing_connect,
    init_db as timing_init_db,
    cmd_backfill_width,
    load_frame_by_source,
    generate_and_save_signals,
    backtest as timing_backtest,
)


# ── 输出辅助 ──────────────────────────────────────────────────────────────

def _print_settlement_summary(result):
    print(
        f"收益结算: buy_date={result.get('buy_date') or '-'} "
        f"sell_date={result['sell_date']} cutoff={result['sell_cutoff']} "
        f"sold={result['settled']} failed={result['failed']} "
        f"avg={result.get('avg_return_pct') if result.get('avg_return_pct') is not None else '-'}%"
    )
    for row in result.get("rows", [])[:20]:
        ret = row["return_pct"] if row["return_pct"] is not None else "-"
        print(
            f"  {row['code']} {row.get('name') or ''} "
            f"buy={row.get('buy_price') or '-'} sell={row.get('sell_price') or '-'} "
            f"ret={ret}% status={row['status']}"
        )
    if len(result.get("rows", [])) > 20:
        print(f"  ... 还有 {len(result['rows']) - 20} 条")
    if result.get("message"):
        print(result["message"])


def _print_scan_summary(payload, status_code, run_id=None, saved=None):
    meta = payload.get("meta") or {}
    print(
        f"扫描保存: status={status_code} run_id={run_id or '-'} "
        f"trade_date={meta.get('trade_date')} cutoff={meta.get('cutoff')} "
        f"quoted={meta.get('quoted', 0)} prefiltered={meta.get('prefiltered', 0)} "
        f"verified={meta.get('verified', 0)} picked={len(payload.get('rows') or [])} "
        f"saved={saved if saved is not None else '-'} elapsed={meta.get('elapsed_s', 0)}s"
    )
    if payload.get("error"):
        print(f"错误: {payload['error']}")
    for row in (payload.get("rows") or [])[:20]:
        print(
            f"  {row['code']} {row.get('name') or ''} "
            f"price={row.get('price')} pct={row.get('pct')}% "
            f"score={row.get('score')} reasons={row.get('reasons') or ''}"
        )
    if len(payload.get("rows") or []) > 20:
        print(f"  ... 还有 {len(payload['rows']) - 20} 条")


def _print_pattern_summary(payload, status_code, run_id=None, saved=None):
    meta = payload.get("meta") or {}
    print(
        f"形态扫描: status={status_code} run_id={run_id or '-'} "
        f"trade_date={meta.get('trade_date')} pool={meta.get('pool')} "
        f"scanned={meta.get('scanned', 0)} picked={len(payload.get('rows') or [])} "
        f"saved={saved if saved is not None else '-'} elapsed={meta.get('elapsed_s', 0)}s"
    )
    if payload.get("error"):
        print(f"错误: {payload['error']}")
    for row in (payload.get("rows") or [])[:20]:
        print(
            f"  {row['code']} {row.get('name') or ''} "
            f"{row.get('pattern_name') or row.get('pattern_type') or ''} "
            f"close={row.get('close')} pct={row.get('pct')}% "
            f"score={row.get('score')}"
        )
    if len(payload.get("rows") or []) > 20:
        print(f"  ... 还有 {len(payload['rows']) - 20} 条")


def _print_pattern_backfill_progress(item, index, total):
    if item.get("phase"):
        print(f"[{index}/{total}] {item.get('message', item.get('phase'))}")
        return
    print(
        f"[{index}/{total}] {item['trade_date']} "
        f"picked={item['picked']} saved={item['saved']} "
        f"elapsed={item.get('meta', {}).get('elapsed_s', 0)}s"
    )


def _print_pattern_backfill_summary(result):
    print(
        f"形态回扫完成: {result.get('start_date')} -> {result.get('end_date')} "
        f"交易日={result.get('days', 0)} "
        f"命中交易日={result.get('matched_days', 0)} "
        f"命中记录={result.get('matched_rows', 0)} "
        f"耗时={result.get('elapsed_s', 0)}s"
    )


def _print_recent_returns(limit=30):
    conn = connect_existing()
    try:
        ensure_momentum_tables(conn)
        rows = conn.execute("""
            SELECT r.buy_date, r.sell_date, r.code, r.name,
                   r.buy_price, r.sell_price, r.return_pct,
                   r.sell_time, r.status, r.error
            FROM momentum_pick_returns r
            ORDER BY r.sell_date DESC, r.return_pct DESC
            LIMIT ?
        """, (limit,)).fetchall()
    finally:
        conn.close()
    if not rows:
        print("暂无收益记录")
        return
    for row in rows:
        ret = row["return_pct"] if row["return_pct"] is not None else "-"
        print(
            f"{row['buy_date']} {row['code']} {row.get('name') or ''} "
            f"buy={row.get('buy_price') or '-'} sell={row.get('sell_price') or '-'} "
            f"ret={ret}% @{row.get('sell_time') or '-'} status={row['status']}"
        )


def _print_backfill_progress(item, index, total):
    print(
        f"[{index}/{total}] {item['trade_date']} "
        f"status={item['status']} picked={item['picked']} "
        f"fallback={item.get('fallback_used', False)}"
    )


def _print_backfill_summary(result):
    summary = result.get("summary") or {}
    print(
        f"回填完成: {result.get('start_date')} -> {result.get('end_date')} "
        f"交易日={result.get('days', 0)} count={summary.get('count', 0)} "
        f"avg={summary.get('avg_return_pct') or '-'}% "
        f"win={summary.get('win_rate_pct') or '-'}%"
    )


# ── CSI1000 择时 ──────────────────────────────────────────────────────────

_BASE_DIR = str(__import__("trader.core.config", fromlist=["BASE_DIR"]).BASE_DIR)
_CSI1000_BACKTEST_START = "2016-06-20"
_CSI1000_EXCEL_PATH = _BASE_DIR + "/data/历史新高新低300和1000.xlsx"
_CSI1000_RUN_KEY = "default"


def _run_csi1000_timing_job(end_date=None, sync_index=False, backfill_width=False,
                             lookback_days=None) -> dict:
    from datetime import datetime, timedelta
    from trader.core.utils import json_safe
    from trader.core.config import DB_PATH

    cfg = STRATEGY_PRESETS["low_dd"]
    cfg = timing_replace(cfg, fee_bps=2.0)
    target_date = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
    end_dash = target_date.strftime("%Y-%m-%d")
    start_dash = (
        (target_date - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        if lookback_days else _CSI1000_BACKTEST_START
    )
    conn = timing_connect(DB_PATH)
    try:
        timing_init_db(conn)
        if backfill_width:
            from datetime import timedelta
            width_start = (target_date - timedelta(days=90)).strftime("%Y-%m-%d")
            cmd_backfill_width(conn, start=width_start, end=end_dash, force=True)
        df = load_frame_by_source(conn, "db", _CSI1000_EXCEL_PATH,
                                   start=start_dash, end=end_dash)
        if df.empty:
            raise RuntimeError("没有可用的中证1000择时数据")
        if len(df) < 30:
            raise RuntimeError(f"可回测数据不足：合并后只有 {len(df)} 个交易日")
        signals = generate_and_save_signals(conn, df, cfg)
        result = timing_backtest(conn, df, cfg, _CSI1000_RUN_KEY)
        latest = signals.tail(1).to_dict("records")[0] if not signals.empty else {}
        return {
            "ok": True, "run_key": _CSI1000_RUN_KEY,
            "start": start_dash, "end": end_dash,
            "synced_index": bool(sync_index),
            "backfilled_width": bool(backfill_width),
            "result": result,
            "latest_signal": {k: json_safe(v) for k, v in latest.items()},
            "updated_at": local_now_text(),
        }
    finally:
        conn.close()


# ── argparse ──────────────────────────────────────────────────────────────

def parse_cli_args():
    parser = argparse.ArgumentParser(description="行业宽度与14:30动量选股服务")
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--serve", action="store_true", help="启动 Web 服务")
    actions.add_argument("--momentum-daily", action="store_true",
                         help="结算前一交易日选股收益，并扫描保存今日14:30选股")
    actions.add_argument("--momentum-scan-save", action="store_true",
                         help="只扫描并保存选股")
    actions.add_argument("--momentum-settle", action="store_true",
                         help="只结算前一交易日选股收益")
    actions.add_argument("--momentum-report", action="store_true",
                         help="查看最近收益记录")
    actions.add_argument("--momentum-backfill", action="store_true",
                         help="回填历史14:30选股并按下一交易日10:00前卖出统计收益")
    actions.add_argument("--pattern-scan-save", action="store_true",
                         help="扫描并保存收盘K线形态")
    actions.add_argument("--pattern-backfill", action="store_true",
                         help="回扫并保存最近一段时间的收盘形态结果")
    actions.add_argument("--csi1000-daily", action="store_true",
                         help="重算中证1000择时信号和最近10年回测记录")

    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--pool", default="all", choices=["all", "sector", "index"])
    parser.add_argument("--index-code", default="")
    parser.add_argument("--cutoff", default="14:30")
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--sell-date", default=None)
    parser.add_argument("--sell-cutoff", default="10:00")
    parser.add_argument("--settle-buy-date", default=None)
    parser.add_argument("--min-gain", type=float, default=2.0)
    parser.add_argument("--max-gain", type=float, default=7.5)
    parser.add_argument("--min-vol-ratio", type=float, default=1.5)
    parser.add_argument("--min-amount", type=float, default=8000,
                        help="最低成交额，单位万元")
    parser.add_argument("--limit", type=int, default=80)
    parser.add_argument("--verify-limit", type=int, default=50)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--report-limit", type=int, default=30)
    parser.add_argument("--backfill-days", type=int, default=30)
    parser.add_argument("--backfill-start", default=None)
    parser.add_argument("--backfill-end", default=None)
    parser.add_argument("--no-daily-fallback", action="store_true")
    parser.add_argument("--daily-fallback-only", action="store_true")
    parser.add_argument("--pattern-lookback-days", type=int, default=120)
    parser.add_argument("--pattern-chart-bars", type=int, default=70)
    parser.add_argument("--pattern-type", default="four_pin",
                        choices=["four_pin", "bottom_reversal"])
    parser.add_argument("--pattern-max-body-pct", type=float, default=1.05)
    parser.add_argument("--pattern-max-body-range-pct", type=float, default=35.0)
    parser.add_argument("--pattern-max-amp-pct", type=float, default=6.0)
    parser.add_argument("--pattern-doji-body-pct", type=float, default=1.05)
    parser.add_argument("--pattern-max-ma40-distance", type=float, default=0.0)
    parser.add_argument("--pattern-max-pair-distance", type=float, default=0.5)
    parser.add_argument("--pattern-max-close-pair-distance", type=float, default=1.0)
    parser.add_argument("--pattern-min-level-gap", type=float, default=0.8)
    parser.add_argument("--pattern-min-shadow-pct", type=float, default=1.0)
    parser.add_argument("--pattern-max-shadowless-count", type=int, default=0)
    parser.add_argument("--pattern-bottom-lookback-days", type=int, default=60)
    parser.add_argument("--pattern-max-bottom-position", type=float, default=25.0)
    parser.add_argument("--pattern-min-prior-drop-pct", type=float, default=10.0)
    parser.add_argument("--pattern-bottom-max-body-pct", type=float, default=3.0)
    parser.add_argument("--pattern-bottom-only-bullish-engulfing", type=int, default=1)
    parser.add_argument("--pattern-bottom-group", default="engulfing",
                        choices=["engulfing", "strong", "single", "all"])
    parser.add_argument("--pattern-min-bottom-volume-ratio", type=float, default=2.0)
    parser.add_argument("--pattern-max-bottom-volume-ratio", type=float, default=3.0)
    parser.add_argument("--pattern-min-bottom-rebound-pct", type=float, default=3.0)
    parser.add_argument("--pattern-min-bottom-pct-change", type=float, default=2.5)
    parser.add_argument("--pattern-min-bottom-strong-gain-pct", type=float, default=4.0)
    parser.add_argument("--pattern-require-bottom-confirm", type=int, default=1)
    parser.add_argument("--pattern-min-bottom-close-position", type=float, default=75.0)
    parser.add_argument("--pattern-require-bottom-close-above-prev", type=int, default=1)
    parser.add_argument("--pattern-require-bottom-above-ma5", type=int, default=1)
    parser.add_argument("--pattern-min-bottom-ma5-slope-pct", type=float, default=-1.0)
    parser.add_argument("--pattern-require-bottom-not-close-new-low", type=int, default=1)
    parser.add_argument("--pattern-bottom-new-low-lookback-days", type=int, default=20)
    parser.add_argument("--pattern-win-lookback-days", type=int, default=720)
    parser.add_argument("--pattern-win-hold-days", type=int, default=1)
    parser.add_argument("--pattern-win-target-pct", type=float, default=3.0)
    parser.add_argument("--pattern-min-turnover", type=float, default=0.0)
    parser.add_argument("--pattern-min-market-cap-yi", type=float, default=0.0)
    parser.add_argument("--pattern-min-amount", type=float, default=None)
    parser.add_argument("--pattern-backfill-days", type=int, default=None)
    parser.add_argument("--csi1000-sync-index", action="store_true")
    parser.add_argument("--csi1000-backfill-width", action="store_true")
    parser.add_argument("--csi1000-lookback-days", type=int, default=180)
    return parser.parse_args()


# ── main ──────────────────────────────────────────────────────────────────

def main():
    args = parse_cli_args()

    if len(sys.argv) == 1 or args.serve:
        from trader.web import create_app
        flask_app = create_app()
        flask_app.run(debug=args.debug, host=args.host, port=args.port)
        return

    params = build_cli_momentum_params(args)

    if args.momentum_daily:
        result = run_momentum_daily_job(
            params,
            sell_date=args.sell_date or params["trade_date"],
            sell_cutoff=args.sell_cutoff,
            settle_buy_date=args.settle_buy_date,
        )
        _print_settlement_summary(result["settlement"])
        _print_scan_summary(result["scan"], result["scan_status"],
                             run_id=result["run_id"], saved=result["saved"])

    elif args.csi1000_daily:
        result = _run_csi1000_timing_job(
            end_date=args.trade_date,
            sync_index=args.csi1000_sync_index,
            backfill_width=args.csi1000_backfill_width,
            lookback_days=args.csi1000_lookback_days,
        )
        latest = result.get("latest_signal") or {}
        bt = result.get("result") or {}
        print(json.dumps({
            "updated_at": result.get("updated_at"),
            "start": result.get("start"), "end": result.get("end"),
            "trade_date": latest.get("trade_date"),
            "trade_state": latest.get("trade_state"),
            "action": latest.get("action"),
            "reason": latest.get("reason"),
            "total_return_pct": bt.get("total_return_pct"),
            "max_drawdown_pct": bt.get("max_drawdown_pct"),
            "trade_count": bt.get("trade_count"),
        }, ensure_ascii=False, indent=2))

    elif args.momentum_scan_save:
        payload, status_code = perform_momentum_scan(params, started_at=time.time())
        conn = connect_existing()
        try:
            run_id, saved = save_momentum_scan_result(conn, params, payload, status_code)
        finally:
            conn.close()
        _print_scan_summary(payload, status_code, run_id=run_id, saved=saved)

    elif args.momentum_settle:
        conn = connect_existing()
        try:
            result = settle_momentum_picks(
                conn,
                sell_date=args.sell_date or params["trade_date"],
                sell_cutoff=args.sell_cutoff,
                buy_date=args.settle_buy_date,
            )
        finally:
            conn.close()
        _print_settlement_summary(result)

    elif args.momentum_report:
        _print_recent_returns(args.report_limit)

    elif args.momentum_backfill:
        result = run_momentum_backfill(
            params,
            start_date=args.backfill_start,
            end_date=args.backfill_end,
            days=args.backfill_days,
            sell_cutoff=args.sell_cutoff,
            progress=_print_backfill_progress,
            use_daily_fallback=not args.no_daily_fallback,
            daily_fallback_only=args.daily_fallback_only,
        )
        _print_backfill_summary(result)

    elif args.pattern_scan_save:
        pattern_params = build_cli_pattern_params(args)
        payload, status_code = perform_pattern_scan(pattern_params, started_at=time.time())
        conn = connect_existing()
        try:
            run_id, saved = save_pattern_scan_result(conn, pattern_params, payload, status_code)
        finally:
            conn.close()
        _print_pattern_summary(payload, status_code, run_id=run_id, saved=saved)

    elif args.pattern_backfill:
        pattern_params = normalize_pattern_backfill_params(build_cli_pattern_params(args))
        result = run_pattern_backfill(
            pattern_params,
            days=args.pattern_backfill_days or default_pattern_backfill_days(pattern_params),
            end_date=args.trade_date,
            progress=_print_pattern_backfill_progress,
        )
        _print_pattern_backfill_summary(result)

    else:
        parse_cli_args()  # shows help if no action flag given

