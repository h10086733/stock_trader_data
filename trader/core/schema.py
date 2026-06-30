"""集中的数据库表结构定义（DDL）。

重要约束：这些 CREATE TABLE 语句必须与现有 2.3GB 生产库的 schema 完全一致。
所有语句都用 ``IF NOT EXISTS``，对已存在的表是无操作，不会改动现有数据。
列名/类型/主键与原始定义逐字对应（见各函数 docstring 标注的来源）。

策略私有表（v2_*、pb_history、roe_history、sectors* 等）仍由各自模块的
ensure 函数维护，不在此集中。
"""
from __future__ import annotations

import sqlite3

# 进度表列顺序（来源：app.py PATTERN_PROGRESS_COLUMNS）
PATTERN_PROGRESS_COLUMNS = [
    "job_key", "job_type", "status", "started_at", "updated_at", "trade_date",
    "current_index", "total", "picked", "matched_rows", "matched_days",
    "elapsed_s", "message", "params_json", "result_json", "error",
]


def ensure_daily_price_market_cap_columns(conn: sqlite3.Connection) -> None:
    """daily_prices 增补 market_cap_yi / float_market_cap_yi 列。来源：fetch.py:511 / app.py:242。"""
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='daily_prices'"
    ).fetchone()
    if not row:
        return
    columns = {r[1] for r in conn.execute("PRAGMA table_info(daily_prices)").fetchall()}
    if "market_cap_yi" not in columns:
        conn.execute("ALTER TABLE daily_prices ADD COLUMN market_cap_yi REAL")
    if "float_market_cap_yi" not in columns:
        conn.execute("ALTER TABLE daily_prices ADD COLUMN float_market_cap_yi REAL")
    conn.commit()


def ensure_core_tables(conn: sqlite3.Connection) -> None:
    """stocks / daily_prices / high_confidence_market_caps / sync_log。来源：fetch.py:440。"""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            code           TEXT PRIMARY KEY,
            secucode       TEXT UNIQUE,
            name           TEXT,
            market         TEXT,
            price_latest   REAL,
            history_start  DATE,
            history_end    DATE,
            is_delisted    INTEGER DEFAULT 0,
            created_at     DATETIME DEFAULT (datetime('now','localtime')),
            updated_at     DATETIME DEFAULT (datetime('now','localtime'))
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_prices (
            code        TEXT  NOT NULL,
            trade_date  DATE  NOT NULL,
            open        REAL,
            close       REAL,
            high        REAL,
            low         REAL,
            volume      REAL,
            amount      REAL,
            pct_change  REAL,
            turnover    REAL,
            PRIMARY KEY (code, trade_date)
        )
    """)
    ensure_daily_price_market_cap_columns(conn)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS high_confidence_market_caps (
            trade_date           DATE NOT NULL,
            code                 TEXT NOT NULL,
            market_cap_yi        REAL,
            float_market_cap_yi  REAL,
            source               TEXT,
            created_at           DATETIME DEFAULT (datetime('now','localtime')),
            updated_at           DATETIME DEFAULT (datetime('now','localtime')),
            PRIMARY KEY (trade_date, code)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            sync_type   TEXT,
            sync_date   DATE,
            total       INTEGER,
            success     INTEGER,
            failed      INTEGER,
            new_stocks  INTEGER,
            duration_s  REAL,
            note        TEXT,
            created_at  DATETIME DEFAULT (datetime('now','localtime'))
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dp_code_date ON daily_prices(code, trade_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dp_date      ON daily_prices(trade_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_hc_market_caps_date ON high_confidence_market_caps(trade_date)")
    conn.commit()


def ensure_daily_price_indexes(conn: sqlite3.Connection) -> None:
    """补 daily_prices 索引。来源：app.py:549。"""
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='daily_prices'"
    ).fetchone()
    if not row:
        return
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dp_date ON daily_prices(trade_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_dp_code_date ON daily_prices(code, trade_date)")
    conn.commit()


def ensure_kline_cache_table(conn: sqlite3.Connection) -> None:
    """分钟线缓存表。来源：app.py:184。"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS intraday_5m_cache (
            code        TEXT NOT NULL,
            trade_date  DATE NOT NULL,
            cutoff      TEXT NOT NULL,
            bars_json   TEXT NOT NULL,
            source      TEXT,
            created_at  DATETIME DEFAULT (datetime('now','localtime')),
            PRIMARY KEY (code, trade_date, cutoff)
        )
    """)
    conn.commit()


def ensure_high_confidence_tables(conn: sqlite3.Connection) -> None:
    """高置信结果缓存 + 市值快照。来源：app.py:203。"""
    ensure_daily_price_market_cap_columns(conn)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS high_confidence_scans (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date     DATE NOT NULL,
            rule_version   TEXT NOT NULL,
            row_count      INTEGER,
            status         TEXT,
            payload_json   TEXT,
            error          TEXT,
            created_at     DATETIME DEFAULT (datetime('now','localtime')),
            updated_at     DATETIME DEFAULT (datetime('now','localtime')),
            UNIQUE(trade_date, rule_version)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_hc_scans_date ON high_confidence_scans(trade_date)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS high_confidence_market_caps (
            trade_date           DATE NOT NULL,
            code                 TEXT NOT NULL,
            market_cap_yi        REAL,
            float_market_cap_yi  REAL,
            source               TEXT,
            created_at           DATETIME DEFAULT (datetime('now','localtime')),
            updated_at           DATETIME DEFAULT (datetime('now','localtime')),
            PRIMARY KEY (trade_date, code)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_hc_market_caps_date ON high_confidence_market_caps(trade_date)")
    conn.commit()


def ensure_momentum_tables(conn: sqlite3.Connection) -> None:
    """动量选股 runs / picks / pick_returns。来源：app.py:262。"""
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


def ensure_pattern_tables(conn: sqlite3.Connection) -> None:
    """形态扫描 runs / picks / progress。来源：app.py:342。"""
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


def ensure_surge_tables(conn: sqlite3.Connection) -> None:
    """次日大涨研究 batches / rows。来源：app.py:411。"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS surge_scan_batches (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date          DATE NOT NULL,
            status              TEXT NOT NULL,
            row_count           INTEGER,
            pre_trade_filter_count INTEGER,
            raw_signal_count    INTEGER,
            stock_count         INTEGER,
            history_days        INTEGER,
            big_threshold       REAL,
            min_samples         INTEGER,
            continuation_only   INTEGER,
            min_signal_pct      REAL,
            exclude_signal_limit INTEGER,
            elapsed_s           REAL,
            params_json         TEXT,
            error               TEXT,
            created_at          DATETIME DEFAULT (datetime('now','localtime')),
            updated_at          DATETIME DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS surge_scan_rows (
            id                         INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id                   INTEGER NOT NULL,
            trade_date                 DATE NOT NULL,
            code                       TEXT NOT NULL,
            name                       TEXT,
            secucode                   TEXT,
            pattern                    TEXT,
            raw_pattern                TEXT,
            coupling                   TEXT,
            signal_key                 TEXT,
            signal_count               INTEGER,
            close                      REAL,
            pct_change                 REAL,
            turnover                   REAL,
            amount_yi                  REAL,
            scan_score                 REAL,
            surge_score                REAL,
            surge_samples              INTEGER,
            next_touch_limit_rate      REAL,
            next_close_limit_rate      REAL,
            next_high_ge_big_rate      REAL,
            next_close_ge_big_rate     REAL,
            avg_next_high_gain_pct     REAL,
            median_next_high_gain_pct  REAL,
            avg_next_close_gain_pct    REAL,
            avg_next_open_gain_pct     REAL,
            max_next_high_gain_pct     REAL,
            signal_touch_limit         INTEGER,
            signal_close_limit         INTEGER,
            patterns                   TEXT,
            couplings                  TEXT,
            row_json                   TEXT,
            created_at                 DATETIME DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_surge_batches_date ON surge_scan_batches(trade_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_surge_rows_batch ON surge_scan_rows(batch_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_surge_rows_code ON surge_scan_rows(code)")
    batch_columns = {r[1] for r in conn.execute("PRAGMA table_info(surge_scan_batches)").fetchall()}
    if "exclude_signal_limit" not in batch_columns:
        conn.execute("ALTER TABLE surge_scan_batches ADD COLUMN exclude_signal_limit INTEGER")
    if "pre_trade_filter_count" not in batch_columns:
        conn.execute("ALTER TABLE surge_scan_batches ADD COLUMN pre_trade_filter_count INTEGER")
    columns = {r[1] for r in conn.execute("PRAGMA table_info(surge_scan_rows)").fetchall()}
    if "signal_touch_limit" not in columns:
        conn.execute("ALTER TABLE surge_scan_rows ADD COLUMN signal_touch_limit INTEGER")
    if "signal_close_limit" not in columns:
        conn.execute("ALTER TABLE surge_scan_rows ADD COLUMN signal_close_limit INTEGER")
    conn.commit()


def ensure_index_daily_stats(conn: sqlite3.Connection) -> None:
    """行业宽度日线净值表。来源：csi1000_timing.py:255 / index_stats.py。"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS index_daily_stats (
            index_code   TEXT  NOT NULL,
            trade_date   DATE  NOT NULL,
            score_sum    REAL,
            high_count   INTEGER,
            low_count    INTEGER,
            valid_count  INTEGER,
            total_count  INTEGER,
            net_value    REAL,
            PRIMARY KEY (index_code, trade_date)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ids_date ON index_daily_stats(trade_date)")
    conn.commit()


def ensure_csi1000_tables(conn: sqlite3.Connection) -> None:
    """中证1000择时：index_prices / signals / trades。来源：csi1000_timing.py:172。"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS index_prices (
            index_code  TEXT NOT NULL,
            trade_date  DATE NOT NULL,
            open        REAL,
            close       REAL,
            high        REAL,
            low         REAL,
            volume      REAL,
            amount      REAL,
            pct_change  REAL,
            turnover    REAL,
            PRIMARY KEY (index_code, trade_date)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_index_prices_date ON index_prices(trade_date)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS csi1000_timing_signals (
            trade_date      DATE PRIMARY KEY,
            signal          TEXT NOT NULL,
            trade_state     TEXT,
            action          TEXT NOT NULL,
            reason          TEXT,
            csi_close       REAL,
            csi_score       REAL,
            hs300_score     REAL,
            csi_score_ma3   REAL,
            hs300_score_ma3 REAL,
            vol_ratio_5_20  REAL,
            price_from_low10 REAL,
            drawdown_from_high10 REAL,
            pct_2d          REAL,
            payload_json    TEXT,
            created_at      DATETIME DEFAULT (datetime('now','localtime')),
            updated_at      DATETIME DEFAULT (datetime('now','localtime'))
        )
    """)
    signal_cols = {r[1] for r in conn.execute("PRAGMA table_info(csi1000_timing_signals)").fetchall()}
    if "trade_state" not in signal_cols:
        conn.execute("ALTER TABLE csi1000_timing_signals ADD COLUMN trade_state TEXT")
    conn.execute("""
        UPDATE csi1000_timing_signals
        SET trade_state = CASE
            WHEN signal = 'LONG' THEN '多1000'
            WHEN signal = 'SHORT' THEN '空1000'
            ELSE '空仓'
        END
        WHERE trade_state IS NULL OR trade_state = ''
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS csi1000_timing_trades (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_key         TEXT NOT NULL,
            direction       TEXT NOT NULL,
            entry_date      DATE NOT NULL,
            entry_price     REAL NOT NULL,
            exit_date       DATE,
            exit_price      REAL,
            exit_reason     TEXT,
            hold_days       INTEGER,
            return_pct      REAL,
            signal_date     DATE,
            entry_reason    TEXT,
            created_at      DATETIME DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_csi1000_trades_run_exit ON csi1000_timing_trades(run_key, exit_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_csi1000_trades_run_entry ON csi1000_timing_trades(run_key, entry_date)")
    conn.commit()
