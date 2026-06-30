"""
Reverse-check whether visible lizi sample features can separate selected stocks.

This script intentionally avoids using the target date as training data. For a
target date, it trains a simple nearest-positive similarity score from earlier
lizi files, then reports how many current candidates remain when all target lizi
stocks are preserved.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
import warnings

import numpy as np
import pandas as pd

import app


FEATURE_SETS = {
    "kline": [
        "pct_change", "range_close_pct", "body_range_pct",
        "upper_range_pct", "lower_range_pct", "close_position_day_pct",
    ],
    "trend": [
        "close_ma5_dist_pct", "close_ma10_dist_pct", "close_ma30_dist_pct",
        "close_hma20_dist_pct", "close_hma30_dist_pct", "ma30_slope_pct",
    ],
    "all": [
        "pct_change", "range_close_pct", "body_range_pct",
        "upper_range_pct", "lower_range_pct", "close_position_day_pct",
        "close_ma5_dist_pct", "close_ma10_dist_pct", "close_ma30_dist_pct",
        "close_hma20_dist_pct", "close_hma30_dist_pct", "ma30_slope_pct",
        "amount_ratio20", "volume_ratio20",
    ],
}


def target_lizi_path(date: str) -> Path:
    key = date.replace("-", "")
    matches = sorted(Path("lizi").glob(f"*{key}*70%信号精选版*.xlsx"))
    if not matches:
        raise FileNotFoundError(f"lizi file not found for {date}")
    return matches[0]


def load_target_lizi(date: str) -> pd.DataFrame:
    rows = pd.read_excel(target_lizi_path(date), sheet_name="个股精选信号-看多-百亿以上")
    rows["code"] = rows["资产代码"].astype(str).str.extract(r"(\d{6})")[0].str.zfill(6)
    rows["signal_key"] = rows["耦合条件"].astype(str)
    return rows


def load_training_lizi(date: str) -> pd.DataFrame:
    date_key = date.replace("-", "")
    lizi_files = {
        p.name
        for p in Path("lizi").glob("*70%信号精选版*.xlsx")
        if date_key not in p.name
    }
    rows = pd.read_csv("outputs/hc_daily_long_100yi_indicators.csv")
    rows = rows[rows["来源文件"].isin(lizi_files)].copy()
    rows["code"] = rows["code"].astype(str).str.zfill(6)
    rows["signal_key"] = rows["耦合条件"].astype(str)
    return rows


def load_cached_candidates(date: str) -> pd.DataFrame:
    conn = sqlite3.connect(app.DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT payload_json
        FROM high_confidence_scans
        WHERE trade_date = ? AND rule_version = ?
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (date, app.HC_RULE_VERSION),
    ).fetchone()
    if not row:
        raise RuntimeError(f"no cached payload for {date} / {app.HC_RULE_VERSION}")
    rows = (json.loads(row["payload_json"]).get("groups") or [{}])[0].get("rows") or []
    out = pd.DataFrame(rows)
    out["code"] = out["code"].astype(str).str.zfill(6)
    return out


def load_panel(date: str) -> pd.DataFrame:
    key = date.replace("-", "")
    path = Path(f"outputs/cache/hc_scan_panel_shapev3td_{key}_lb400_min120_nonbj.pkl")
    if not path.exists():
        raise FileNotFoundError(path)
    panel = pd.read_pickle(path)
    panel["code"] = panel["code"].astype(str).str.zfill(6)
    if "close_position_day_pct" not in panel.columns:
        rng = (panel["high"] - panel["low"]).replace(0, np.nan)
        panel["close_position_day_pct"] = (panel["close"] - panel["low"]) / rng * 100
    return panel


def row_distance(train: pd.DataFrame, row: pd.Series, features: list[str]) -> float:
    x = train[features].astype(float)
    median = x.median()
    mad = (x - median).abs().median().replace(0, np.nan)
    scale = mad.fillna(x.std()).replace(0, np.nan).fillna(1.0)
    z_train = ((x - median) / scale).fillna(0).to_numpy(float)
    z = ((row[features].astype(float) - median) / scale).fillna(0).to_numpy(float)
    return float(np.sqrt(((z_train - z) ** 2).mean(axis=1)).min())


def score_candidates(train: pd.DataFrame, candidates: pd.DataFrame, features: list[str]) -> pd.Series:
    return pd.Series(
        [row_distance(train, row, features) for _, row in candidates.iterrows()],
        index=candidates.index,
    )


def run(args: argparse.Namespace) -> int:
    warnings.filterwarnings("ignore", category=FutureWarning)
    date = pd.to_datetime(args.date).strftime("%Y-%m-%d")
    train = load_training_lizi(date)
    target = load_target_lizi(date)
    candidates = load_cached_candidates(date)
    panel = load_panel(date)

    feature_cols = sorted({col for cols in FEATURE_SETS.values() for col in cols})
    candidates = candidates.merge(
        panel[["code", *[c for c in feature_cols if c in panel.columns]]],
        on="code",
        how="left",
    )
    for col in feature_cols:
        if col not in train.columns:
            train[col] = np.nan
        if col in candidates.columns:
            train[col] = pd.to_numeric(train[col], errors="coerce")
            candidates[col] = pd.to_numeric(candidates[col], errors="coerce")

    selected_codes = set(target["code"])
    print(f"date={date} train_rows={len(train)} candidates={len(candidates)} lizi={len(selected_codes)}")
    for name, features in FEATURE_SETS.items():
        features = [f for f in features if f in candidates.columns]
        scored = candidates.copy()
        scored["lizi_distance"] = score_candidates(train, scored, features)
        target_scores = scored.loc[scored["code"].isin(selected_codes), "lizi_distance"]
        threshold = float(target_scores.max())
        kept = scored[scored["lizi_distance"] <= threshold]
        kept_codes = set(kept["code"])
        print(
            f"{name}: threshold={threshold:.4f} "
            f"kept_rows={len(kept)} kept_stocks={len(kept_codes)} "
            f"overlap={len(kept_codes & selected_codes)}/{len(selected_codes)}"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reverse lizi visible-feature filters")
    parser.add_argument("--date", default="2026-06-22")
    return parser


if __name__ == "__main__":
    raise SystemExit(run(build_parser().parse_args()))
