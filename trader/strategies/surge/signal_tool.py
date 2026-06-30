"""
华创 K 线信号样例解析与选股排序工具。

典型用法：
  python signal_tool.py analyze --examples lizi
  python signal_tool.py select --input lizi/【20260618】60%信号精选版.xlsx --examples lizi --cap-scope 百亿以上
  python signal_tool.py parse --examples lizi --out /tmp/hc_signals.csv
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from trader.core import config


BASE_DIR = config.BASE_DIR
DEFAULT_DB = config.DB_PATH
SIGNAL_EXTS = {".xlsx", ".xls"}


COLUMN_ALIASES = {
    "判断日期": "trade_date",
    "资产代码": "secucode",
    "资产名称": "name",
    "资产类别": "asset_type",
    "所属行业": "industry",
    "所属行业2": "industry2",
    "形态名称": "pattern",
    "耦合条件": "coupling",
    "上涨平均收益": "up_avg",
    "上涨次数": "up_count",
    "历史上涨次数": "up_count",
    "下跌平均收益": "down_avg",
    "下跌次数": "down_count",
    "历史下跌次数": "down_count",
    "形态总出现次数": "sample_count",
    "形态方向": "pattern_direction",
    "胜率": "win_rate",
    "盈亏比": "pl_ratio",
    "当日总市值": "market_cap_yi",
    "当日流通市值": "float_cap_yi",
}


@dataclass
class PriceCoverage:
    trade_date: str
    rows: int
    complete: bool


def iter_excel_files(path: str | Path) -> list[Path]:
    root = Path(path)
    if root.is_file():
        return [root]
    files = [
        p for p in root.rglob("*")
        if p.is_file() and p.suffix.lower() in SIGNAL_EXTS and not p.name.startswith("~$")
    ]
    return sorted(files, key=lambda p: str(p))


def filename_date(path: Path) -> str | None:
    text = path.name
    m = re.search(r"(20\d{2})[-年_/]?(0\d|1[0-2])[-月_/]?([0-3]\d)", text)
    if not m:
        return None
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"


def filename_threshold(path: Path) -> float | None:
    m = re.search(r"(\d+(?:\.\d+)?)%", path.name)
    return float(m.group(1)) / 100.0 if m else None


def normalize_date_value(value, fallback: str | None = None) -> str | None:
    if pd.isna(value):
        return fallback
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = int(value)
        if 19000101 <= number <= 20991231:
            return datetime.strptime(str(number), "%Y%m%d").strftime("%Y-%m-%d")
        if 20000 <= number <= 80000:
            return (
                pd.to_datetime(number, unit="D", origin="1899-12-30")
                .strftime("%Y-%m-%d")
            )
    text = str(value).strip()
    m = re.search(r"(20\d{2})[-年_/]?(0\d|1[0-2])[-月_/]?([0-3]\d)", text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.notna(parsed):
        return parsed.strftime("%Y-%m-%d")
    return fallback


def normalize_sheet_scope(sheet: str) -> tuple[str, str, bool]:
    direction = "全部"
    if "看多" in sheet:
        direction = "看多"
    elif "看空" in sheet:
        direction = "看空"

    cap_scope = "百亿以上" if "百亿以上" in sheet else "全部"
    selected = "精选" in sheet or sheet in ("个股精选信号-看多", "个股精选信号-看空")
    return direction, cap_scope, selected


def normalize_secucode(value) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if "." in text:
        code, market = text.split(".", 1)
        return f"{code.zfill(6)}.{market.upper()}"
    if text.isdigit():
        code = text.zfill(6)
        suffix = "SH" if code.startswith(("5", "6", "9")) else "SZ"
        return f"{code}.{suffix}"
    return text


def bare_code(secucode: str) -> str:
    return (secucode or "").split(".", 1)[0]


def normalize_pattern(value) -> str:
    text = "" if pd.isna(value) else str(value).strip()
    if text.startswith("名称"):
        text = text[2:].strip()
    if ("：" in text or ":" in text) and " " in text:
        tail = text.split()[-1].strip()
        if re.search(r"[\u4e00-\u9fff]", tail):
            text = tail
    return text


def text_column(df: pd.DataFrame, name: str) -> pd.Series:
    if name in df.columns:
        return df[name].fillna("").astype(str).str.strip()
    return pd.Series([""] * len(df), index=df.index)


def coupling_family(value) -> str:
    text = "" if pd.isna(value) else str(value).strip()
    if "-" not in text:
        return text
    return text.split("-", 1)[1]


def infer_direction(row, sheet_direction: str) -> str:
    if sheet_direction in ("看多", "看空"):
        return sheet_direction
    text = str(row.get("pattern_direction") or "")
    if "正" in text:
        return "看多"
    if "负" in text:
        return "看空"
    return sheet_direction


def normalize_signal_frame(df: pd.DataFrame, path: Path, sheet: str) -> pd.DataFrame:
    df = df.rename(columns={c: COLUMN_ALIASES.get(c, c) for c in df.columns}).copy()
    needed = {"secucode", "trade_date", "pattern", "coupling", "up_avg", "down_avg"}
    if not needed.issubset(df.columns):
        return pd.DataFrame()

    sheet_direction, cap_scope, selected = normalize_sheet_scope(sheet)
    fallback_date = filename_date(path)
    threshold = filename_threshold(path)

    out = pd.DataFrame(index=df.index)
    out["source_file"] = path.name
    out["source_path"] = str(path)
    out["sheet"] = sheet
    out["file_date"] = fallback_date
    out["threshold"] = threshold
    out["sheet_direction"] = sheet_direction
    out["cap_scope"] = cap_scope
    out["selected_sheet"] = selected
    if "trade_date" in df.columns:
        out["trade_date"] = df["trade_date"].map(lambda v: normalize_date_value(v, fallback_date))
    else:
        out["trade_date"] = fallback_date
    out["secucode"] = df["secucode"].map(normalize_secucode)
    out["code"] = out["secucode"].map(bare_code)
    out["name"] = text_column(df, "name")
    out["industry"] = text_column(df, "industry")
    out["industry2"] = text_column(df, "industry2")
    out["asset_type"] = text_column(df, "asset_type")
    out["pattern_raw"] = text_column(df, "pattern")
    out["pattern"] = df["pattern"].map(normalize_pattern)
    out["coupling"] = text_column(df, "coupling")
    out["coupling_family"] = out["coupling"].map(coupling_family)

    for col in ("up_avg", "up_count", "down_avg", "down_count", "sample_count",
                "win_rate", "pl_ratio", "market_cap_yi", "float_cap_yi"):
        if col in df.columns:
            out[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            out[col] = math.nan

    out["up_count"] = out["up_count"].fillna(0)
    out["down_count"] = out["down_count"].fillna(0)
    missing_sample = out["sample_count"].isna()
    out.loc[missing_sample, "sample_count"] = (
        out.loc[missing_sample, "up_count"] + out.loc[missing_sample, "down_count"]
    )

    missing_win = out["win_rate"].isna() & (out["sample_count"] > 0)
    out.loc[missing_win, "win_rate"] = (
        out.loc[missing_win, "up_count"] / out.loc[missing_win, "sample_count"]
    )

    missing_pl = out["pl_ratio"].isna()
    out.loc[missing_pl, "pl_ratio"] = (
        out.loc[missing_pl, "up_avg"].abs()
        / out.loc[missing_pl, "down_avg"].abs().replace(0, math.nan)
    )

    out["direction"] = [
        infer_direction(row, sheet_direction) for _, row in out.iterrows()
    ]
    out = out[out["trade_date"].notna() & (out["code"] != "")]
    return out.reset_index(drop=True)


def load_signal_excels(paths: Iterable[str | Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for item in paths:
        for path in iter_excel_files(item):
            try:
                xl = pd.ExcelFile(path)
            except Exception as exc:
                print(f"跳过无法读取的 Excel: {path} ({exc})")
                continue
            for sheet in xl.sheet_names:
                try:
                    df = pd.read_excel(path, sheet_name=sheet)
                except Exception as exc:
                    print(f"跳过无法读取的 sheet: {path.name}/{sheet} ({exc})")
                    continue
                normalized = normalize_signal_frame(df, path, sheet)
                if not normalized.empty:
                    frames.append(normalized)
    if not frames:
        return pd.DataFrame()
    frames = [frame.dropna(axis=1, how="all") for frame in frames if not frame.empty]
    data = pd.concat(frames, ignore_index=True)
    return data.drop_duplicates(
        subset=["source_path", "sheet", "trade_date", "secucode", "pattern", "coupling"]
    )


def connect_db(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def load_price_data(conn: sqlite3.Connection, dates: Iterable[str]) -> pd.DataFrame:
    dates = sorted({d for d in dates if d})
    if not dates:
        return pd.DataFrame()
    placeholders = ",".join("?" for _ in dates)
    rows = conn.execute(
        f"""
        SELECT code, trade_date, open, high, low, close, pct_change, amount, turnover
        FROM daily_prices
        WHERE trade_date IN ({placeholders})
        """,
        dates,
    ).fetchall()
    return pd.DataFrame([dict(r) for r in rows])


def load_price_coverage(conn: sqlite3.Connection, start_date: str, end_date: str) -> dict[str, PriceCoverage]:
    rows = conn.execute(
        """
        SELECT trade_date, COUNT(*) AS rows
        FROM daily_prices
        WHERE trade_date >= ? AND trade_date <= ?
        GROUP BY trade_date
        ORDER BY trade_date
        """,
        (start_date, end_date),
    ).fetchall()
    return {
        r["trade_date"]: PriceCoverage(
            trade_date=r["trade_date"],
            rows=int(r["rows"]),
            complete=int(r["rows"]) >= 4500,
        )
        for r in rows
    }


def load_next_trade_dates(conn: sqlite3.Connection, dates: Iterable[str]) -> dict[str, str]:
    unique_dates = sorted({d for d in dates if d})
    mapping: dict[str, str] = {}
    for d in unique_dates:
        row = conn.execute(
            """
            SELECT trade_date
            FROM daily_prices
            WHERE trade_date > ?
            GROUP BY trade_date
            HAVING COUNT(*) >= 50
            ORDER BY trade_date
            LIMIT 1
            """,
            (d,),
        ).fetchone()
        if row:
            mapping[d] = row["trade_date"]
    return mapping


def limit_rate_for_code(code: str, name: str = "") -> float:
    if "ST" in (name or "").upper():
        return 0.05
    if code.startswith(("8", "9")):
        return 0.30
    if code.startswith(("300", "301", "688", "689")):
        return 0.20
    return 0.10


def enrich_with_prices(signals: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
    if signals.empty:
        return signals.copy()

    out = signals.copy()
    dates = sorted(out["trade_date"].dropna().unique())
    if not dates:
        return out
    coverage = load_price_coverage(conn, min(dates), max(dates))
    px = load_price_data(conn, dates)
    if px.empty:
        return out

    px = px.rename(columns={
        "open": "same_open",
        "high": "same_high",
        "low": "same_low",
        "close": "same_close",
        "pct_change": "same_pct_change",
        "amount": "same_amount",
        "turnover": "same_turnover",
    })
    out = out.merge(px, on=["code", "trade_date"], how="left")
    out["price_rows_on_date"] = out["trade_date"].map(
        lambda d: coverage.get(d).rows if d in coverage else 0
    )
    out["price_complete_on_date"] = out["trade_date"].map(
        lambda d: bool(coverage.get(d).complete) if d in coverage else False
    )
    out["limit_rate"] = [
        limit_rate_for_code(code, name) for code, name in zip(out["code"], out["name"])
    ]
    prev_est = out["same_close"] / (1 + out["same_pct_change"] / 100.0)
    out["same_high_pct"] = (out["same_high"] / prev_est - 1) * 100.0
    out["same_touch_limit"] = out["same_high_pct"] >= out["limit_rate"] * 100.0 - 0.5
    out["same_close_limit"] = out["same_pct_change"] >= out["limit_rate"] * 100.0 - 0.5

    next_map = load_next_trade_dates(conn, dates)
    out["next_trade_date"] = out["trade_date"].map(next_map)
    next_dates = sorted({d for d in out["next_trade_date"].dropna()})
    if next_dates:
        npx = load_price_data(conn, next_dates)
        if not npx.empty:
            npx = npx.rename(columns={
                "trade_date": "next_trade_date",
                "open": "next_open",
                "high": "next_high",
                "low": "next_low",
                "close": "next_close",
                "pct_change": "next_pct_change",
                "amount": "next_amount",
                "turnover": "next_turnover",
            })
            out = out.merge(npx, on=["code", "next_trade_date"], how="left")
            next_prev = out["next_close"] / (1 + out["next_pct_change"] / 100.0)
            out["next_high_pct"] = (out["next_high"] / next_prev - 1) * 100.0
            out["next_touch_limit"] = out["next_high_pct"] >= out["limit_rate"] * 100.0 - 0.5
            out["next_close_limit"] = out["next_pct_change"] >= out["limit_rate"] * 100.0 - 0.5
    return out


def long_selected_all(signals: pd.DataFrame) -> pd.DataFrame:
    return signals[
        (signals["direction"] == "看多")
        & (signals["selected_sheet"])
        & (signals["cap_scope"] == "全部")
    ].copy()


def aggregate_hit_stats(examples: pd.DataFrame, min_samples: int = 3) -> dict[str, pd.DataFrame]:
    base = long_selected_all(examples)
    base = base[base["same_close"].notna()].copy()
    base = base.drop_duplicates(["trade_date", "secucode", "pattern", "coupling"])
    if base.empty:
        return {}

    def agg(keys: list[str]) -> pd.DataFrame:
        grouped = base.groupby(keys, dropna=False).agg(
            samples=("secucode", "count"),
            close_limit_rate=("same_close_limit", "mean"),
            touch_limit_rate=("same_touch_limit", "mean"),
            avg_same_pct=("same_pct_change", "mean"),
            avg_win_rate=("win_rate", "mean"),
            avg_pl_ratio=("pl_ratio", "mean"),
        ).reset_index()
        grouped = grouped[grouped["samples"] >= min_samples]
        return grouped.sort_values(
            ["close_limit_rate", "touch_limit_rate", "samples"],
            ascending=[False, False, False],
        )

    return {
        "pattern": agg(["pattern"]),
        "coupling": agg(["coupling_family"]),
        "combo": agg(["pattern", "coupling_family"]),
        "industry": agg(["industry"]),
    }


def metric_lookup(stats: pd.DataFrame, keys: list[str], value_cols: list[str]) -> dict[tuple, dict]:
    if stats is None or stats.empty:
        return {}
    lookup = {}
    for _, row in stats.iterrows():
        key = tuple(row[k] for k in keys)
        lookup[key] = {c: row[c] for c in value_cols if c in row}
    return lookup


def build_ranked_selection(
    candidates: pd.DataFrame,
    examples: pd.DataFrame,
    min_win: float,
    min_pl: float,
    min_samples: int,
    min_cap: float,
    limit: int,
) -> pd.DataFrame:
    rows = candidates[
        (candidates["direction"] == "看多")
        & (candidates["selected_sheet"])
        & (candidates["win_rate"] >= min_win)
        & (candidates["pl_ratio"] >= min_pl)
        & (candidates["sample_count"] >= min_samples)
    ].copy()
    if min_cap:
        rows = rows[(rows["market_cap_yi"].isna()) | (rows["market_cap_yi"] >= min_cap)]
    if rows.empty:
        return rows

    stats = aggregate_hit_stats(examples, min_samples=3)
    base = long_selected_all(examples)
    base = base[base["same_close"].notna()]
    base_close_rate = float(base["same_close_limit"].mean()) if not base.empty else 0.0
    base_touch_rate = float(base["same_touch_limit"].mean()) if not base.empty else 0.0

    pattern_lookup = metric_lookup(
        stats.get("pattern"), ["pattern"],
        ["samples", "close_limit_rate", "touch_limit_rate", "avg_same_pct"],
    )
    combo_lookup = metric_lookup(
        stats.get("combo"), ["pattern", "coupling_family"],
        ["samples", "close_limit_rate", "touch_limit_rate", "avg_same_pct"],
    )
    industry_lookup = metric_lookup(
        stats.get("industry"), ["industry"],
        ["samples", "close_limit_rate", "touch_limit_rate", "avg_same_pct"],
    )

    max_date = rows["trade_date"].dropna().max()
    previous = examples[
        (examples["direction"] == "看多")
        & (examples["selected_sheet"])
        & (examples["trade_date"] < max_date)
    ]
    repeat_counts = previous.groupby("secucode").size().to_dict()

    scores = []
    reasons_list = []
    for _, row in rows.iterrows():
        win_pct = row["win_rate"] * 100.0
        pl = row["pl_ratio"] if pd.notna(row["pl_ratio"]) else 0
        samples = row["sample_count"] if pd.notna(row["sample_count"]) else 0
        score = win_pct + min(pl, 3.0) * 8.0 + math.log1p(samples) * 2.0
        reasons = [
            f"胜率{win_pct:.1f}%",
            f"盈亏比{pl:.2f}",
            f"样本{int(samples)}",
        ]

        if row.get("threshold") and row["threshold"] >= 0.70:
            score += 5
            reasons.append("70%精选")
        if pd.notna(row.get("market_cap_yi")) and row["market_cap_yi"] >= 100:
            score += 3
            reasons.append("百亿市值")

        repeat = int(repeat_counts.get(row["secucode"], 0))
        if repeat:
            score += min(8, repeat * 2)
            reasons.append(f"历史重复{repeat}次")

        combo_key = (row["pattern"], row["coupling_family"])
        pattern_key = (row["pattern"],)
        industry_key = (row["industry"],)
        combo = combo_lookup.get(combo_key)
        pattern = pattern_lookup.get(pattern_key)
        industry = industry_lookup.get(industry_key)

        if combo:
            delta = combo["close_limit_rate"] - base_close_rate
            score += delta * 35 + (combo["touch_limit_rate"] - base_touch_rate) * 15
            reasons.append(
                f"组合封板率{combo['close_limit_rate'] * 100:.1f}%/样本{int(combo['samples'])}"
            )
        elif pattern:
            delta = pattern["close_limit_rate"] - base_close_rate
            score += delta * 24 + (pattern["touch_limit_rate"] - base_touch_rate) * 10
            reasons.append(
                f"形态封板率{pattern['close_limit_rate'] * 100:.1f}%/样本{int(pattern['samples'])}"
            )

        if industry and row["industry"]:
            score += (industry["close_limit_rate"] - base_close_rate) * 15
            reasons.append(
                f"行业封板率{industry['close_limit_rate'] * 100:.1f}%/样本{int(industry['samples'])}"
            )

        if row.get("same_close_limit") is True:
            reasons.append("本地日线已封板")
        elif row.get("same_touch_limit") is True:
            reasons.append("本地日线已触板")

        scores.append(round(score, 3))
        reasons_list.append(" / ".join(reasons))

    rows["strategy_score"] = scores
    rows["strategy_reasons"] = reasons_list
    rows["win_rate_pct"] = rows["win_rate"] * 100.0
    rows["same_limit_label"] = rows.apply(limit_label, axis=1)
    rows = rows.sort_values(
        ["strategy_score", "win_rate", "pl_ratio", "sample_count"],
        ascending=[False, False, False, False],
    )
    return rows.head(limit).reset_index(drop=True)


def limit_label(row) -> str:
    if bool(row.get("same_close_limit")):
        return "收盘涨停"
    if bool(row.get("same_touch_limit")):
        return "盘中触板"
    if pd.notna(row.get("same_pct_change")):
        return f"{row['same_pct_change']:.2f}%"
    return "无日线"


def print_table(df: pd.DataFrame, columns: list[str], limit: int | None = None) -> None:
    if df.empty:
        print("无结果")
        return
    view = df[columns].copy()
    if limit:
        view = view.head(limit)
    print(view.to_string(index=False))


def command_parse(args) -> int:
    signals = load_signal_excels(args.examples)
    if signals.empty:
        print("没有解析到信号")
        return 1
    if args.db:
        conn = connect_db(args.db)
        signals = enrich_with_prices(signals, conn)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        signals.to_csv(args.out, index=False, encoding="utf-8-sig")
        print(f"已输出 {len(signals)} 行: {args.out}")
    else:
        print(f"解析信号 {len(signals)} 行")
        print_table(
            signals,
            ["source_file", "sheet", "trade_date", "secucode", "name", "pattern", "coupling", "win_rate", "pl_ratio"],
            limit=args.limit,
        )
    return 0


def command_analyze(args) -> int:
    signals = load_signal_excels(args.examples)
    if signals.empty:
        print("没有解析到信号")
        return 1
    conn = connect_db(args.db)
    signals = enrich_with_prices(signals, conn)

    print(f"解析文件数: {signals['source_file'].nunique()}  信号行数: {len(signals)}")
    summary = signals.groupby(["source_file", "sheet"], dropna=False).agg(
        rows=("secucode", "count"),
        avg_win=("win_rate", "mean"),
        min_win=("win_rate", "min"),
        avg_pl=("pl_ratio", "mean"),
    ).reset_index()
    print("\n文件/sheet 概览:")
    print_table(summary, ["source_file", "sheet", "rows", "avg_win", "min_win", "avg_pl"])

    long_all = long_selected_all(signals)
    daily = long_all.groupby("trade_date", dropna=False).agg(
        candidates=("secucode", "count"),
        price_rows=("price_rows_on_date", "max"),
        priced=("same_close", lambda s: int(s.notna().sum())),
        touch_limit=("same_touch_limit", "sum"),
        close_limit=("same_close_limit", "sum"),
        avg_pct=("same_pct_change", "mean"),
    ).reset_index().sort_values("trade_date")
    print("\n看多精选-全部 同日表现:")
    print_table(daily, ["trade_date", "candidates", "price_rows", "priced", "touch_limit", "close_limit", "avg_pct"])

    stats = aggregate_hit_stats(signals, min_samples=args.min_group_samples)
    for name, title, cols in [
        ("pattern", "高命中形态", ["pattern", "samples", "close_limit_rate", "touch_limit_rate", "avg_same_pct"]),
        ("coupling", "高命中耦合族", ["coupling_family", "samples", "close_limit_rate", "touch_limit_rate", "avg_same_pct"]),
        ("combo", "高命中形态+耦合族", ["pattern", "coupling_family", "samples", "close_limit_rate", "touch_limit_rate", "avg_same_pct"]),
        ("industry", "高命中行业", ["industry", "samples", "close_limit_rate", "touch_limit_rate", "avg_same_pct"]),
    ]:
        print(f"\n{title}:")
        frame = stats.get(name, pd.DataFrame())
        print_table(frame, cols, limit=args.top)

    return 0


def command_select(args) -> int:
    candidates = load_signal_excels([args.input])
    if candidates.empty:
        print("输入文件没有解析到信号")
        return 1
    if args.cap_scope != "任意":
        candidates = candidates[candidates["cap_scope"] == args.cap_scope].copy()
    if args.direction != "任意":
        candidates = candidates[candidates["direction"] == args.direction].copy()

    examples = load_signal_excels(args.examples)
    if examples.empty:
        print("样例目录没有解析到信号")
        return 1

    if args.exclude_input_from_stats:
        input_path = str(Path(args.input))
        input_name = Path(args.input).name
        examples = examples[
            (examples["source_path"] != input_path)
            & (examples["source_file"] != input_name)
        ].copy()

    conn = connect_db(args.db)
    candidates = enrich_with_prices(candidates, conn)
    examples = enrich_with_prices(examples, conn)

    ranked = build_ranked_selection(
        candidates,
        examples,
        min_win=args.min_win,
        min_pl=args.min_pl,
        min_samples=args.min_samples,
        min_cap=args.min_cap,
        limit=args.limit,
    )
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        ranked.to_csv(args.out, index=False, encoding="utf-8-sig")
        print(f"已输出 {len(ranked)} 行: {args.out}")

    columns = [
        "secucode", "name", "industry", "pattern", "coupling", "win_rate_pct",
        "pl_ratio", "sample_count", "market_cap_yi", "strategy_score",
        "same_limit_label", "strategy_reasons",
    ]
    existing = [c for c in columns if c in ranked.columns]
    print_table(ranked, existing)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="解析华创 K 线信号样例并做候选股排序")
    sub = parser.add_subparsers(dest="command", required=True)

    p_parse = sub.add_parser("parse", help="解析 Excel 信号并可导出 CSV")
    p_parse.add_argument("--examples", nargs="+", default=["lizi"], help="Excel 文件或目录")
    p_parse.add_argument("--db", default=str(DEFAULT_DB), help="SQLite 数据库路径；留空则不补行情")
    p_parse.add_argument("--out", help="输出 CSV 路径")
    p_parse.add_argument("--limit", type=int, default=30)
    p_parse.set_defaults(func=command_parse)

    p_analyze = sub.add_parser("analyze", help="分析样例命中涨停的形态/耦合/行业")
    p_analyze.add_argument("--examples", nargs="+", default=["lizi"], help="Excel 文件或目录")
    p_analyze.add_argument("--db", default=str(DEFAULT_DB), help="SQLite 数据库路径")
    p_analyze.add_argument("--top", type=int, default=15)
    p_analyze.add_argument("--min-group-samples", type=int, default=3)
    p_analyze.set_defaults(func=command_analyze)

    p_select = sub.add_parser("select", help="对指定 Excel 的候选股做二次筛选和排序")
    p_select.add_argument("--input", required=True, help="待筛选 Excel")
    p_select.add_argument("--examples", nargs="+", default=["lizi"], help="用于学习命中统计的样例目录")
    p_select.add_argument("--db", default=str(DEFAULT_DB), help="SQLite 数据库路径")
    p_select.add_argument("--direction", choices=["看多", "看空", "任意"], default="看多")
    p_select.add_argument("--cap-scope", choices=["全部", "百亿以上", "任意"], default="百亿以上")
    p_select.add_argument("--min-win", type=float, default=0.70, help="最低胜率，小数")
    p_select.add_argument("--min-pl", type=float, default=1.0, help="最低盈亏比")
    p_select.add_argument("--min-samples", type=int, default=30, help="最低历史样本次数")
    p_select.add_argument("--min-cap", type=float, default=0.0, help="最低总市值，亿元")
    p_select.add_argument("--limit", type=int, default=30)
    p_select.add_argument("--out", help="输出 CSV 路径")
    p_select.add_argument(
        "--include-input-in-stats",
        dest="exclude_input_from_stats",
        action="store_false",
        help="学习样例统计时包含当前输入文件；默认排除，避免同日结果泄漏",
    )
    p_select.set_defaults(func=command_select, exclude_input_from_stats=True)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
