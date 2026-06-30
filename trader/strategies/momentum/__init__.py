"""动量策略：实时扫描、历史回测、日常结算、收益汇总。"""
from trader.strategies.momentum.params import (
    build_momentum_params,
    build_cli_momentum_params,
    build_empty_scan_meta,
)
from trader.strategies.momentum.scan import (
    perform_momentum_scan,
    perform_historical_momentum_scan,
    perform_daily_fallback_momentum_scan,
    evaluate_candidate_with_bars,
    passes_momentum_filters,
    load_daily_metrics_before,
    load_historical_daily_quotes,
    load_daily_history_for_backfill,
    build_daily_fallback_payload_from_history,
)
from trader.strategies.momentum.storage import (
    ensure_momentum_tables,
    save_momentum_scan_result,
    load_momentum_picks_for_settlement,
    settle_momentum_picks,
    summarize_backfill_returns,
    load_momentum_profit_summary,
)
from trader.strategies.momentum.jobs import (
    run_momentum_daily_job,
    run_momentum_backfill,
)

__all__ = [
    "build_momentum_params",
    "build_cli_momentum_params",
    "build_empty_scan_meta",
    "perform_momentum_scan",
    "perform_historical_momentum_scan",
    "perform_daily_fallback_momentum_scan",
    "evaluate_candidate_with_bars",
    "passes_momentum_filters",
    "ensure_momentum_tables",
    "save_momentum_scan_result",
    "load_momentum_picks_for_settlement",
    "settle_momentum_picks",
    "summarize_backfill_returns",
    "load_momentum_profit_summary",
    "load_daily_metrics_before",
    "load_historical_daily_quotes",
    "run_momentum_daily_job",
    "run_momentum_backfill",
]
