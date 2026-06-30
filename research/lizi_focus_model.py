"""
Approximate lizi-style focus scoring.

The model is intentionally small and local: it trains a RandomForest classifier
from a calibration date where we have both local candidates and the lizi
selected Excel. The score is used as an extra focus layer after the existing
high-recall scanner, historical quality filter, and market-cap filter.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd

try:
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder
except ImportError:  # pragma: no cover - optional local scoring dependency
    ColumnTransformer = None
    RandomForestClassifier = None
    Pipeline = None
    OneHotEncoder = None


DEFAULT_CALIBRATION_DATE = "all_lizi"
DEFAULT_THRESHOLD = 0.25

NUMERIC_FEATURES = [
    "market_cap_missing",
    "hist_win_rate",
    "hist_samples",
    "hist_pl_ratio",
    "rank_score",
    "scan_score",
    "pct_change",
    "turnover",
    "amount_yi",
    "market_cap_yi",
    "signal_count",
    "pattern_count",
    "coupling_count",
]

CATEGORICAL_FEATURES = [
    "pattern",
    "coupling",
    "signal_key",
]


def _normalize_date(date: str) -> str:
    return pd.to_datetime(date).strftime("%Y-%m-%d")


def lizi_path_for_date(date: str, lizi_dir: str | Path = "lizi") -> Path | None:
    key = _normalize_date(date).replace("-", "")
    matches = sorted(Path(lizi_dir).glob(f"*{key}*70%信号精选版*.xlsx"))
    return matches[0] if matches else None


def lizi_dates(lizi_dir: str | Path = "lizi") -> list[str]:
    dates = []
    for path in sorted(Path(lizi_dir).glob("*70%信号精选版*.xlsx")):
        key = "".join(ch for ch in path.name if ch.isdigit())[:8]
        if len(key) != 8:
            continue
        try:
            dates.append(_normalize_date(key))
        except ValueError:
            continue
    return sorted(set(dates))


def calibration_dates(calibration_date: str | list[str] | tuple[str, ...], lizi_dir: str | Path) -> list[str]:
    if isinstance(calibration_date, (list, tuple, set)):
        return [_normalize_date(date) for date in calibration_date]
    if str(calibration_date).strip().lower() in {"all", "all_lizi", "*"}:
        return lizi_dates(lizi_dir)
    return [_normalize_date(str(calibration_date))]


def load_lizi_codes(date: str, lizi_dir: str | Path = "lizi") -> set[str]:
    path = lizi_path_for_date(date, lizi_dir)
    if path is None:
        return set()
    rows = pd.read_excel(path, sheet_name="个股精选信号-看多-百亿以上")
    codes = rows["资产代码"].astype(str).str.extract(r"(\d{6})")[0].dropna()
    return set(codes.astype(str).str.zfill(6))


def load_cached_rows(db_path: str | Path, date: str, rule_version: str) -> list[dict]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT payload_json
            FROM high_confidence_scans
            WHERE trade_date = ? AND rule_version = ? AND status = 'ok'
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (_normalize_date(date), rule_version),
        ).fetchone()
    finally:
        conn.close()
    if not row or not row["payload_json"]:
        return []
    payload = json.loads(row["payload_json"])
    return (payload.get("groups") or [{}])[0].get("rows") or []


def _frame(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["code"] = df["code"].astype(str).str.zfill(6)
    for col in NUMERIC_FEATURES:
        if col not in df.columns:
            df[col] = 0.0
        if col == "market_cap_missing":
            df[col] = df[col].fillna(False).astype(bool).astype(int)
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    for col in CATEGORICAL_FEATURES:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str)
    return df


def build_focus_model(
    db_path: str | Path,
    rule_version: str,
    calibration_date: str | list[str] | tuple[str, ...] = DEFAULT_CALIBRATION_DATE,
    lizi_dir: str | Path = "lizi",
    exclude_dates: str | list[str] | tuple[str, ...] | set[str] | None = None,
) -> Pipeline | None:
    if ColumnTransformer is None or RandomForestClassifier is None or Pipeline is None or OneHotEncoder is None:
        return None
    excluded = {
        _normalize_date(date)
        for date in ([exclude_dates] if isinstance(exclude_dates, str) else (exclude_dates or []))
    }
    frames = []
    for date in calibration_dates(calibration_date, lizi_dir):
        if _normalize_date(date) in excluded:
            continue
        rows = load_cached_rows(db_path, date, rule_version)
        df = _frame(rows)
        selected = load_lizi_codes(date, lizi_dir)
        if df.empty or not selected:
            continue
        df["calibration_date"] = _normalize_date(date)
        df["target"] = df["code"].isin(selected).astype(int)
        frames.append(df)
    if not frames:
        return None
    df = pd.concat(frames, ignore_index=True)
    if df["target"].nunique() < 2:
        return None
    preprocessor = ColumnTransformer([
        ("num", "passthrough", NUMERIC_FEATURES),
        ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
    ])
    model = RandomForestClassifier(
        n_estimators=500,
        max_depth=None,
        min_samples_leaf=1,
        class_weight="balanced",
        random_state=2,
    )
    pipeline = Pipeline([
        ("features", preprocessor),
        ("model", model),
    ])
    pipeline.fit(df[NUMERIC_FEATURES + CATEGORICAL_FEATURES], df["target"])
    return pipeline


def score_focus_rows(
    rows: list[dict],
    db_path: str | Path,
    rule_version: str,
    threshold: float = DEFAULT_THRESHOLD,
    calibration_date: str | list[str] | tuple[str, ...] = DEFAULT_CALIBRATION_DATE,
    lizi_dir: str | Path = "lizi",
    exclude_dates: str | list[str] | tuple[str, ...] | set[str] | None = None,
) -> tuple[list[dict], dict]:
    if not rows:
        return rows, {
            "focus_model_enabled": False,
            "focus_model_reason": "empty_rows",
        }
    model = build_focus_model(db_path, rule_version, calibration_date, lizi_dir, exclude_dates=exclude_dates)
    if model is None:
        return rows, {
            "focus_model_enabled": False,
            "focus_model_reason": "model_unavailable",
        }
    df = _frame(rows)
    scores = model.predict_proba(df[NUMERIC_FEATURES + CATEGORICAL_FEATURES])[:, 1]
    out = []
    for row, score in zip(rows, scores):
        item = dict(row)
        item["focus_score"] = round(float(score), 6)
        item["focus_selected"] = bool(score >= threshold)
        out.append(item)
    selected_rows = [row for row in out if row["focus_selected"]]
    return selected_rows, {
        "focus_model_enabled": True,
        "focus_model": "random_forest_v4_lizi_features_lodo",
        "focus_calibration_date": (
            "all_lizi"
            if str(calibration_date).strip().lower() in {"all", "all_lizi", "*"}
            else ",".join(calibration_dates(calibration_date, lizi_dir))
        ),
        "focus_excluded_dates": ",".join(
            sorted(
                _normalize_date(date)
                for date in ([exclude_dates] if isinstance(exclude_dates, str) else (exclude_dates or []))
            )
        ),
        "focus_threshold": threshold,
        "focus_input_rows": len(rows),
        "focus_output_rows": len(selected_rows),
    }
