"""
华创样例反推扫描器。

第一版目标不是一次完全复刻原软件，而是把可解释的 K 线形态规则和
MA/MACD/HMA 耦合规则跑起来，并和样例入选股票做命中对比。

典型用法：
  python hc_strategy_scanner.py --date 2026-06-17 --compare
  python hc_strategy_scanner.py --date 2026-06-05 --mode strict --top 80
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from indicator_enrich import DEFAULT_DB, DEFAULT_OUTPUT_DIR, add_indicators, load_histories


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_SELECTED = DEFAULT_OUTPUT_DIR / "hc_daily_long_all_enriched.csv"
PANEL_CACHE_VERSION = "shapev3td"


PATTERN_WEIGHTS = {
    "陷阱": 1.0,
    "捉腰带线": 1.2,
    "黄包车夫": 0.9,
    "风高浪大线": 0.9,
    "纺锤": 0.8,
    "短蜡烛": 0.7,
    "长蜡烛": 1.1,
    "十字": 0.8,
    "吞噬模式": 1.2,
    "收盘缺影线": 1.3,
    "光头光脚缺影线": 1.5,
    "母子线": 1.0,
    "锤头": 0.9,
    "倒锤头": 0.8,
    "长脚十字": 0.8,
    "十字星": 0.7,
    "蜻蜓十字形十字": 0.8,
    "墓碑十字倒十字": 0.7,
}

COUPLING_WEIGHTS = {
    "MA5": 0.8,
    "MA30": 1.0,
    "MA60": 1.3,
    "MACD": 1.1,
    "HMA": 1.2,
    "Fscore": 0.5,
    "Concentration": 0.5,
    "ash": 0.5,
}


def normalize_pattern(value) -> str:
    text = "" if pd.isna(value) else str(value).strip()
    for prefix in ("Candle ", "Pattern ", "Line ", "Doji ", "Marubozu ", "Top "):
        if text.startswith(prefix):
            return text[len(prefix):].strip()
    return text


def is_bj_code(code: str, secucode: str = "") -> bool:
    code = str(code).zfill(6)
    return code.startswith(("8", "9")) or str(secucode).upper().endswith(".BJ")


def infer_secucode(code: str, market: str | None = None) -> str:
    code = str(code).zfill(6)
    if market == "1" or code.startswith(("5", "6", "9")):
        return f"{code}.SH"
    return f"{code}.SZ"


def load_stock_universe(conn: sqlite3.Connection, include_bj: bool = False) -> pd.DataFrame:
    stocks = pd.read_sql_query(
        """
        SELECT code, secucode, name, market
        FROM stocks
        WHERE COALESCE(is_delisted, 0) = 0
        """,
        conn,
    )
    stocks["code"] = stocks["code"].astype(str).str.zfill(6)
    stocks["secucode"] = stocks.apply(
        lambda r: r["secucode"] if pd.notna(r["secucode"]) and str(r["secucode"]).strip()
        else infer_secucode(r["code"], r["market"]),
        axis=1,
    )
    if not include_bj:
        stocks = stocks[
            ~stocks.apply(lambda r: is_bj_code(r["code"], r["secucode"]), axis=1)
        ].copy()
    return stocks


def start_date_by_trading_days(conn: sqlite3.Connection, date: str, lookback_days: int) -> str:
    rows = pd.read_sql_query(
        """
        SELECT trade_date
        FROM daily_prices
        WHERE trade_date <= ?
        GROUP BY trade_date
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        conn,
        params=[date, lookback_days],
    )
    if rows.empty:
        raise RuntimeError(f"{date} 没有可用交易日")
    return rows["trade_date"].iloc[-1]


def prepare_scan_frame(
    conn: sqlite3.Connection,
    date: str,
    lookback_days: int,
    min_history: int,
    include_bj: bool,
) -> pd.DataFrame:
    stocks = load_stock_universe(conn, include_bj)
    start = start_date_by_trading_days(conn, date, lookback_days)
    histories = load_histories(conn, stocks["code"].tolist(), start, date)
    if histories.empty:
        raise RuntimeError(f"{date} 没有可扫描的日 K")

    histories["code"] = histories["code"].astype(str).str.zfill(6)
    counts = histories[histories["trade_date"] <= date].groupby("code").size()
    enough_codes = counts[counts >= min_history].index
    histories = histories[histories["code"].isin(enough_codes)].copy()
    if histories.empty:
        raise RuntimeError(f"{date} 没有满足 {min_history} 根历史 K 线的股票")

    enriched = []
    for _, group in histories.groupby("code", sort=False):
        item = add_indicators(group)
        item = add_shape_metrics(item)
        enriched.append(item)
    hist = pd.concat(enriched, ignore_index=True)
    today = hist[hist["trade_date"] == date].copy()
    today = today.merge(stocks, on="code", how="left")
    return today


def add_shape_metrics(hist: pd.DataFrame) -> pd.DataFrame:
    hist = hist.sort_values("trade_date").copy()
    high = hist["high"]
    low = hist["low"]
    open_ = hist["open"]
    close = hist["close"]
    day_range = (high - low).replace(0, np.nan)
    body = (close - open_).abs()
    upper = high - np.maximum(open_, close)
    lower = np.minimum(open_, close) - low

    hist["prev_open"] = open_.shift(1)
    hist["prev_high"] = high.shift(1)
    hist["prev_low"] = low.shift(1)
    hist["prev_close"] = close.shift(1)
    hist["prev2_open"] = open_.shift(2)
    hist["prev2_high"] = high.shift(2)
    hist["prev2_low"] = low.shift(2)
    hist["prev2_close"] = close.shift(2)
    hist["avg_body20"] = body.rolling(20).mean()
    hist["avg_range20"] = day_range.rolling(20).mean()
    hist["body"] = body
    hist["upper_shadow"] = upper
    hist["lower_shadow"] = lower
    hist["range"] = day_range
    hist["body_range_pct"] = body / day_range
    hist["upper_range_pct"] = upper / day_range
    hist["lower_range_pct"] = lower / day_range
    hist["range_close_pct"] = day_range / close.replace(0, np.nan) * 100
    hist["body_close_pct"] = body / close.replace(0, np.nan) * 100
    hist["is_up"] = close > open_
    hist["is_down"] = close < open_
    return hist


def trueish(series: pd.Series) -> pd.Series:
    return series.fillna(False).astype(bool)


def detect_patterns(df: pd.DataFrame, engine: str = "recall") -> dict[str, pd.Series]:
    eps = 1e-9
    body = df["body"]
    upper = df["upper_shadow"]
    lower = df["lower_shadow"]
    rng = df["range"]
    body_pct = df["body_range_pct"]
    upper_pct = df["upper_range_pct"]
    lower_pct = df["lower_range_pct"]
    avg_body = df["avg_body20"]
    avg_range = df["avg_range20"]

    if engine == "talib_like":
        doji = (body <= avg_body * 0.10) | (body_pct <= 0.10)
        small_body = (body <= avg_body * 0.60) | (body_pct <= 0.35)
        long_body = (body >= avg_body * 1.00) & (body_pct >= 0.45)
        short_line = (body <= avg_body * 0.50) & (rng <= avg_range * 0.80)
        long_line = (body >= avg_body * 1.00) & (rng >= avg_range * 0.80) & (body_pct >= 0.45)
        long_shadow_both = (upper >= body * 1.5) & (lower >= body * 1.5)
        prev_red = df["prev_close"] < df["prev_open"]
        prev_green = df["prev_close"] > df["prev_open"]
        engulf = (
            (df["is_up"] & prev_red & (df["open"] <= df["prev_close"]) & (df["close"] >= df["prev_open"]))
            | (df["is_down"] & prev_green & (df["open"] >= df["prev_close"]) & (df["close"] <= df["prev_open"]))
        )
        harami = (
            (np.maximum(df["open"], df["close"]) <= np.maximum(df["prev_open"], df["prev_close"]))
            & (np.minimum(df["open"], df["close"]) >= np.minimum(df["prev_open"], df["prev_close"]))
            & (df["body"] <= (df["prev_close"] - df["prev_open"]).abs() * 0.75)
        )
        inside_prev = (df["prev_high"] < df["prev2_high"]) & (df["prev_low"] > df["prev2_low"])
        hikkake = inside_prev & (
            ((df["high"] > df["prev_high"]) & (df["close"] < df["prev_high"]))
            | ((df["low"] < df["prev_low"]) & (df["close"] > df["prev_low"]))
        )
        three_outside = engulf & (
            (df["close"] > df["prev2_close"])
            | (df["close"] < df["prev2_close"])
        )
        return {
            "十字": doji,
            "十字星": doji & (df["range_close_pct"] <= 3.0),
            "长脚十字": doji & long_shadow_both,
            "黄包车夫": doji & long_shadow_both & (rng >= avg_range * 1.0),
            "风高浪大线": small_body & (upper >= body * 1.5) & (lower >= body * 1.5) & (rng >= avg_range * 1.0),
            "纺锤": small_body & (upper >= body) & (lower >= body),
            "短蜡烛": short_line,
            "长蜡烛": long_line,
            "捉腰带线": df["is_up"] & long_body & (lower_pct <= 0.10) & (upper_pct <= 0.30),
            "收盘缺影线": df["is_up"] & long_body & (upper_pct <= 0.05),
            "光头光脚缺影线": long_body & (upper_pct <= 0.05) & (lower_pct <= 0.05),
            "吞噬模式": engulf,
            "母子线": harami,
            "陷阱": hikkake,
            "锤头": small_body & (lower >= body * 2.0) & (upper <= body * 0.5),
            "倒锤头": small_body & (upper >= body * 2.0) & (lower <= body * 0.5),
            "蜻蜓十字形十字": doji & (lower_pct >= 0.60) & (upper_pct <= 0.10),
            "墓碑十字倒十字": doji & (upper_pct >= 0.60) & (lower_pct <= 0.10),
            "三外部上涨和下跌": three_outside,
        }

    doji = (body_pct <= 0.10) | (df["body_close_pct"] <= 0.20)
    small_body = body_pct <= 0.35
    long_upper_lower = (upper >= body * 1.5) & (lower >= body * 1.5)
    long_range = df["range_close_pct"] >= 2.5

    prev_red = df["prev_close"] < df["prev_open"]
    engulf = (
        prev_red
        & df["is_up"]
        & (df["open"] <= df["prev_close"])
        & (df["close"] >= df["prev_open"])
    )
    harami = (
        (np.maximum(df["open"], df["close"]) <= np.maximum(df["prev_open"], df["prev_close"]))
        & (np.minimum(df["open"], df["close"]) >= np.minimum(df["prev_open"], df["prev_close"]))
        & (df["body"] <= (df["prev_close"] - df["prev_open"]).abs() * 0.75)
    )
    false_break = (
        ((df["low"] < df["prev_low"]) & (df["close"] > df["prev_low"]) & df["is_up"])
        | ((df["high"] > df["prev_high"]) & (df["close"] < df["prev_high"]) & df["is_down"])
    )
    broad_bear_trap = (
        df["is_down"]
        & (df["range_close_pct"] >= 2.0)
        & (body_pct >= 0.30)
        & (df["close"] <= df["prev_close"] * 1.01)
    )
    broad_bull_trap = (
        df["is_up"]
        & (df["range_close_pct"] >= 2.0)
        & (body_pct >= 0.30)
        & (df["close"] >= df["prev_close"] * 0.99)
        & ((df["low"] <= df["prev_low"]) | (lower_pct >= 0.30))
    )
    trap = false_break | broad_bear_trap | broad_bull_trap

    return {
        "十字": doji,
        "十字星": doji & (df["range_close_pct"] <= 2.5),
        "长脚十字": doji & long_upper_lower & long_range,
        "黄包车夫": doji & long_upper_lower & (df["range_close_pct"] >= 3.0),
        "风高浪大线": small_body & (upper_pct >= 0.25) & (lower_pct >= 0.25) & long_range,
        "纺锤": small_body & (upper >= body + eps) & (lower >= body + eps),
        "短蜡烛": ((body <= avg_body * 0.85) | (rng <= avg_range * 0.75)) & (body_pct <= 0.55),
        "长蜡烛": ((body >= avg_body * 1.15) | (rng >= avg_range * 1.15)) & (body_pct >= 0.45),
        "捉腰带线": df["is_up"] & (body_pct >= 0.45) & (lower_pct <= 0.22) & (upper_pct <= 0.40),
        "收盘缺影线": df["is_up"] & (upper_pct <= 0.10) & (body_pct >= 0.40),
        "光头光脚缺影线": df["is_up"] & (upper_pct <= 0.06) & (lower_pct <= 0.06) & (body_pct >= 0.85),
        "吞噬模式": engulf,
        "母子线": harami,
        "陷阱": trap,
        "锤头": small_body & (lower >= body * 2.0) & (upper <= body * 1.2),
        "倒锤头": small_body & (upper >= body * 2.0) & (lower <= body * 1.2),
        "蜻蜓十字形十字": doji & (lower_pct >= 0.60) & (upper_pct <= 0.15),
        "墓碑十字倒十字": doji & (upper_pct >= 0.60) & (lower_pct <= 0.15),
    }


def detect_couplings(df: pd.DataFrame, mode: str) -> dict[str, tuple[pd.Series, str, str]]:
    if mode == "strict":
        return {
            "MA5": (df["close_ma5_dist_pct"].abs() <= 3, "close near MA5 +/-3%", "price"),
            "MA30": ((df["close_ma30_dist_pct"] < 0) & (df["ma30_slope_pct"] < 0), "close below falling MA30", "price"),
            "MA60": ((df["close_ma60_dist_pct"] <= -3) & (df["ma60_slope_pct"] < 0), "close at least 3% below falling MA60", "price"),
            "MACD": (df["macd_rising"] & (df["macd"] < 0), "MACD histogram rising below zero", "price"),
            "HMA": ((df["close_above_hma30"]) & (df["hma60_slope_pct"] < 0), "close above HMA30 while HMA60 falling", "price"),
            "Fscore": (df["macd_rising"] & (df["close_ma10_dist_pct"] > 0), "proxy: MACD rising and close above MA10", "proxy"),
            "Concentration": (df["close_ma30_dist_pct"] < 0, "proxy: close below MA30", "proxy"),
            "ash": (df["macd_rising"], "proxy: MACD rising", "proxy"),
        }

    return {
        "MA5": (df["close_ma5_dist_pct"].abs() <= 3, "close near MA5 +/-3%", "price"),
        "MA30": (df["close_ma30_dist_pct"] < 0, "close below MA30", "price"),
        "MA60": (df["close_ma60_dist_pct"] < 0, "close below MA60", "price"),
        "MACD": (df["macd_rising"], "MACD histogram rising", "price"),
        "HMA": (df["close_above_hma20"] | df["close_above_hma30"], "close above HMA20 or HMA30", "price"),
        "Fscore": (df["macd_rising"] | (df["close_ma10_dist_pct"] > 0), "proxy: MACD rising or close above MA10", "proxy"),
        "Concentration": (df["close_ma30_dist_pct"] < 0, "proxy: close below MA30", "proxy"),
        "ash": (df["macd_rising"], "proxy: MACD rising", "proxy"),
    }


def split_filter(value: str | None) -> set[str] | None:
    if not value:
        return None
    items = {item.strip() for item in value.split(",") if item.strip()}
    return items or None


def build_signals(
    today: pd.DataFrame,
    mode: str,
    top: int | None,
    patterns_filter: set[str] | None = None,
    couplings_filter: set[str] | None = None,
    exclude_proxy: bool = False,
    pattern_engine: str = "recall",
) -> pd.DataFrame:
    pattern_masks = detect_patterns(today, pattern_engine)
    coupling_masks = detect_couplings(today, mode)
    if patterns_filter:
        pattern_masks = {
            pattern: mask
            for pattern, mask in pattern_masks.items()
            if pattern in patterns_filter
        }
    if couplings_filter:
        coupling_masks = {
            family: item
            for family, item in coupling_masks.items()
            if family in couplings_filter
        }
    rows = []
    base_cols = [
        "trade_date", "code", "secucode", "name", "open", "high", "low", "close",
        "prev_close", "pct_change", "turnover", "amount", "range_close_pct", "body_range_pct",
        "close_ma5_dist_pct", "close_ma30_dist_pct", "close_ma60_dist_pct",
        "ma30_slope_pct", "ma60_slope_pct", "macd", "macd_rising",
        "close_hma20_dist_pct", "close_hma30_dist_pct",
    ]
    for pattern, p_mask in pattern_masks.items():
        p_mask = trueish(p_mask)
        if not p_mask.any():
            continue
        for family, (c_mask, rule, source) in coupling_masks.items():
            if exclude_proxy and source == "proxy":
                continue
            mask = p_mask & trueish(c_mask)
            if not mask.any():
                continue
            part = today.loc[mask, base_cols].copy()
            part["形态名称"] = pattern
            part["coupling_family"] = family
            part["耦合规则"] = rule
            part["规则来源"] = source
            part["scan_score"] = (
                PATTERN_WEIGHTS.get(pattern, 0.6) * 10
                + COUPLING_WEIGHTS.get(family, 0.5) * 10
                + part["pct_change"].fillna(0).clip(-5, 10) * 0.2
                + part["turnover"].fillna(0).clip(0, 20) * 0.05
            )
            rows.append(part)
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    out = out.sort_values(["scan_score", "pct_change"], ascending=[False, False])
    if top:
        out = out.head(top)
    return out


def load_selected_for_date(path: Path, date: str) -> pd.DataFrame:
    selected = pd.read_csv(path)
    selected["判断日期"] = pd.to_datetime(selected["判断日期"]).dt.strftime("%Y-%m-%d")
    selected["code"] = selected["资产代码"].astype(str).str.split(".").str[0].str.zfill(6)
    selected["归一形态名称"] = selected["形态名称"].map(normalize_pattern)
    selected["coupling_family"] = selected["耦合条件"].astype(str).str.split("-").str[-1]
    if "是否北交所" in selected.columns:
        selected = selected[~selected["是否北交所"].fillna(False).astype(bool)]
    else:
        selected = selected[
            ~selected["资产代码"].astype(str).str.endswith(".BJ")
            & ~selected["code"].str.startswith(("8", "9"))
        ]
    return selected[selected["判断日期"] == date].copy()


def compare_with_selected(scan: pd.DataFrame, selected: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    if scan.empty:
        return scan.copy(), {
            "selected_rows": len(selected),
            "selected_stocks": selected["code"].nunique() if not selected.empty else 0,
            "scan_rows": 0,
            "scan_stocks": 0,
            "stock_recall": 0.0,
            "exact_signal_recall": 0.0,
        }
    selected_stock = set(selected["code"])
    selected_exact = set(
        zip(selected["code"], selected["归一形态名称"], selected["coupling_family"])
    )
    out = scan.copy()
    out["命中入选股票"] = out["code"].isin(selected_stock)
    out["命中精确信号"] = [
        (code, pattern, family) in selected_exact
        for code, pattern, family in zip(out["code"], out["形态名称"], out["coupling_family"])
    ]

    hit_stocks = set(out.loc[out["命中入选股票"], "code"])
    hit_exact = set(
        zip(
            out.loc[out["命中精确信号"], "code"],
            out.loc[out["命中精确信号"], "形态名称"],
            out.loc[out["命中精确信号"], "coupling_family"],
        )
    )
    stats = {
        "selected_rows": len(selected),
        "selected_stocks": selected["code"].nunique(),
        "scan_rows": len(out),
        "scan_stocks": out["code"].nunique(),
        "hit_selected_stocks": len(hit_stocks),
        "hit_exact_signals": len(hit_exact),
        "stock_recall": len(hit_stocks) / selected["code"].nunique() if selected["code"].nunique() else 0.0,
        "exact_signal_recall": len(hit_exact) / len(selected_exact) if selected_exact else 0.0,
    }
    return out, stats


def print_summary(scan: pd.DataFrame, selected: pd.DataFrame | None, stats: dict | None) -> None:
    print(f"scan rows: {len(scan)}")
    print(f"scan stocks: {scan['code'].nunique() if not scan.empty else 0}")
    if not scan.empty:
        print()
        print("top patterns")
        print(scan["形态名称"].value_counts().head(12).to_string())
        print()
        print("top couplings")
        print(scan["coupling_family"].value_counts().to_string())
        print()
        cols = ["trade_date", "secucode", "name", "形态名称", "coupling_family", "close", "pct_change", "scan_score"]
        hit_cols = ["命中入选股票", "命中精确信号"]
        cols += [c for c in hit_cols if c in scan.columns]
        print("top candidates")
        print(scan[cols].head(30).to_string(index=False))
    if selected is not None and stats is not None:
        print()
        print("compare")
        for key, value in stats.items():
            if key.endswith("recall"):
                print(f"{key}: {value:.2%}")
            else:
                print(f"{key}: {value}")


def run(args) -> int:
    date = pd.to_datetime(args.date).strftime("%Y-%m-%d")
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(args.db)

    day_rows = pd.read_sql_query(
        "SELECT COUNT(DISTINCT code) AS rows FROM daily_prices WHERE trade_date = ?",
        conn,
        params=[date],
    )["rows"].iloc[0]
    print(f"{date} daily_prices rows: {day_rows}")
    if day_rows < args.warn_market_rows:
        print(f"warning: 当日日K覆盖只有 {day_rows} 只，全市场扫描可能不完整")

    cache_dir = out_dir / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    bj_part = "withbj" if args.include_bj else "nonbj"
    cache_path = cache_dir / (
        f"hc_scan_panel_{PANEL_CACHE_VERSION}_{date.replace('-', '')}_lb{args.lookback_days}"
        f"_min{args.min_history}_{bj_part}.pkl"
    )
    if args.cache and cache_path.exists():
        today = pd.read_pickle(cache_path)
        print(f"loaded cache {cache_path}")
    else:
        today = prepare_scan_frame(
            conn,
            date,
            args.lookback_days,
            args.min_history,
            args.include_bj,
        )
        if args.cache:
            today.to_pickle(cache_path)
            print(f"wrote cache {cache_path}")

    scan = build_signals(
        today,
        args.mode,
        args.top,
        patterns_filter=split_filter(args.patterns),
        couplings_filter=split_filter(args.couplings),
        exclude_proxy=args.exclude_proxy,
        pattern_engine=args.pattern_engine,
    )

    selected = None
    stats = None
    if args.compare:
        selected = load_selected_for_date(Path(args.selected), date)
        scan, stats = compare_with_selected(scan, selected)

    output = out_dir / f"hc_scan_{date.replace('-', '')}_{args.mode}.csv"
    scan.to_csv(output, index=False, encoding="utf-8-sig")
    print(f"wrote {output}")

    if args.compare and selected is not None:
        missing = selected.copy()
        hit_codes = set(scan.loc[scan.get("命中入选股票", False).astype(bool), "code"]) if not scan.empty else set()
        missing["扫描命中股票"] = missing["code"].isin(hit_codes)
        missing_output = out_dir / f"hc_scan_compare_{date.replace('-', '')}_{args.mode}.csv"
        missing.to_csv(missing_output, index=False, encoding="utf-8-sig")
        print(f"wrote {missing_output}")

    print_summary(scan, selected, stats)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="按反推规则扫描每日 K 线形态和指标耦合")
    parser.add_argument("--date", required=True, help="扫描日期，如 2026-06-17")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--selected", default=str(DEFAULT_SELECTED))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--mode", choices=["loose", "strict"], default="loose")
    parser.add_argument("--lookback-days", type=int, default=420)
    parser.add_argument("--min-history", type=int, default=120)
    parser.add_argument("--warn-market-rows", type=int, default=4500)
    parser.add_argument("--top", type=int, default=0, help="只输出前 N 条；0 表示全部输出")
    parser.add_argument("--compare", action="store_true", help="和样例入选清单对比")
    parser.add_argument("--include-bj", action="store_true", help="包含北交所")
    parser.add_argument("--patterns", help="只扫描指定形态，逗号分隔，如 捉腰带线,长蜡烛")
    parser.add_argument("--couplings", help="只扫描指定耦合族，逗号分隔，如 MA60,MACD,HMA")
    parser.add_argument("--exclude-proxy", action="store_true", help="排除 Fscore/Concentration/ash 等代理规则")
    parser.add_argument("--pattern-engine", choices=["recall", "talib_like"], default="recall")
    parser.add_argument("--cache", action=argparse.BooleanOptionalAction, default=True, help="缓存日度指标面板")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.top <= 0:
        args.top = None
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
