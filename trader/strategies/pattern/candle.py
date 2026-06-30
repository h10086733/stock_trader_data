"""K 线形态基础工具：蜡烛图指标、四针/十字星检测、K 线图 SVG 生成。

来源：app.py 中以下函数（~3320–3560）：
  candle_metrics, is_small_body_candle, is_doji_candle, has_balanced_shadows,
  candle_body_mid, pct_distance, four_pin_levels, build_candlestick_chart,
  candle_shape, is_long_body, load_daily_histories_for_pattern,
  load_daily_histories_for_pattern_range。
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta

from trader.core.utils import clamp, chunked


def candle_metrics(row) -> dict | None:
    open_price = row["open"]
    close_price = row["close"]
    high = row["high"]
    low = row["low"]
    if not open_price or not close_price or not high or not low:
        return None
    base = close_price or open_price
    span = max(high - low, 0)
    body = abs(close_price - open_price)
    body_pct = body / base * 100.0 if base else None
    amp_pct = span / base * 100.0 if base else None
    body_range_pct = body / span * 100.0 if span else 0.0
    upper = high - max(open_price, close_price)
    lower = min(open_price, close_price) - low
    upper_pct = upper / span * 100.0 if span else 0.0
    lower_pct = lower / span * 100.0 if span else 0.0
    return {
        "body_pct": body_pct,
        "amp_pct": amp_pct,
        "body_range_pct": body_range_pct,
        "upper_pct": upper_pct,
        "lower_pct": lower_pct,
    }


def is_small_body_candle(row, params) -> bool:
    metrics = candle_metrics(row)
    if not metrics:
        return False
    return (
        metrics["body_pct"] <= params["max_body_pct"]
        and metrics["body_range_pct"] <= params["max_body_range_pct"]
        and metrics["amp_pct"] <= params["max_amp_pct"]
    )


def is_doji_candle(row, params) -> bool:
    metrics = candle_metrics(row)
    if not metrics:
        return False
    body_limit = min(params["doji_body_pct"], params["max_body_pct"])
    return (
        metrics["body_pct"] <= body_limit
        and metrics["body_range_pct"] <= params["max_body_range_pct"]
        and metrics["amp_pct"] <= params["max_amp_pct"]
    )


def has_balanced_shadows(row, params) -> bool:
    metrics = candle_metrics(row)
    if not metrics:
        return False
    min_shadow_pct = params.get("min_shadow_pct", 6.0)
    return (
        metrics["upper_pct"] >= min_shadow_pct
        and metrics["lower_pct"] >= min_shadow_pct
    )


def candle_body_mid(row):
    if not row["open"] or not row["close"]:
        return None
    return (row["open"] + row["close"]) / 2.0


def pct_distance(a, b, base):
    if a is None or b is None or not base:
        return None
    return abs(a - b) / base * 100.0


def four_pin_levels(rows, params):
    mids = [candle_body_mid(row) for row in rows]
    if any(mid is None for mid in mids):
        return None
    base = rows[-1]["close"]
    first_third_gap = pct_distance(mids[0], mids[2], base)
    second_fourth_gap = pct_distance(mids[1], mids[3], base)
    first_third_close_gap = pct_distance(rows[0]["close"], rows[2]["close"], base)
    second_fourth_close_gap = pct_distance(rows[1]["close"], rows[3]["close"], base)
    high_level = (mids[0] + mids[2]) / 2.0
    low_level = (mids[1] + mids[3]) / 2.0
    level_gap = (high_level - low_level) / base * 100.0 if base else None
    if any(v is None for v in [
        first_third_gap, second_fourth_gap, first_third_close_gap,
        second_fourth_close_gap, level_gap,
    ]):
        return None
    if first_third_gap > params["max_pair_distance"]:
        return None
    if second_fourth_gap > params["max_pair_distance"]:
        return None
    if first_third_close_gap > params["max_close_pair_distance"]:
        return None
    if second_fourth_close_gap > params["max_close_pair_distance"]:
        return None
    if level_gap < params["min_level_gap"]:
        return None
    if max(mids[1], mids[3]) >= min(mids[0], mids[2]):
        return None
    return {
        "first_third_gap": first_third_gap,
        "second_fourth_gap": second_fourth_gap,
        "first_third_close_gap": first_third_close_gap,
        "second_fourth_close_gap": second_fourth_close_gap,
        "level_gap": level_gap,
        "high_level": high_level,
        "low_level": low_level,
    }


def build_candlestick_chart(series, highlight: int = 5,
                             width: int = 360, height: int = 172) -> str:
    bars = [r for r in series if r["open"] and r["close"] and r["high"] and r["low"]]
    if len(bars) < 2:
        return ""

    pad_l, pad_r, pad_t, pad_b = 34, 8, 10, 22
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b
    low = min(r["low"] for r in bars)
    high = max(r["high"] for r in bars)
    span = high - low or 1

    def x_at(i):
        return pad_l + (i + 0.5) / len(bars) * plot_w

    def y_at(price):
        return pad_t + (high - price) / span * plot_h

    closes = [r["close"] for r in bars]
    ma20_pts = []
    ma40_pts = []
    for i in range(len(bars)):
        if i >= 19:
            ma20_pts.append((x_at(i), y_at(sum(closes[i - 19:i + 1]) / 20)))
        if i >= 39:
            ma40_pts.append((x_at(i), y_at(sum(closes[i - 39:i + 1]) / 40)))

    def polyline(points, color):
        if len(points) < 2:
            return ""
        pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
        return f'<polyline fill="none" stroke="{color}" stroke-width="1.5" points="{pts}"/>'

    candle_w = clamp(plot_w / len(bars) * 0.58, 2.0, 7.0)
    start_highlight = max(0, len(bars) - highlight)
    parts = [
        f'<svg viewBox="0 0 {width} {height}" width="100%" height="{height}" aria-hidden="true">',
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="transparent"/>',
    ]
    for tick in (0.25, 0.5, 0.75):
        y = pad_t + plot_h * tick
        parts.append(
            f'<line x1="{pad_l}" y1="{y:.1f}" x2="{width - pad_r}" y2="{y:.1f}" '
            f'stroke="#252a3a" stroke-width="1"/>'
        )
    parts.append(polyline(ma20_pts, "#d7a53f"))
    parts.append(polyline(ma40_pts, "#5c84e8"))

    for i, row in enumerate(bars):
        x = x_at(i)
        open_y = y_at(row["open"])
        close_y = y_at(row["close"])
        high_y = y_at(row["high"])
        low_y = y_at(row["low"])
        up = row["close"] >= row["open"]
        color = "#ff4d6a" if up else "#00c97a"
        if i >= start_highlight:
            parts.append(
                f'<rect x="{x - candle_w * .82:.1f}" y="{pad_t:.1f}" '
                f'width="{candle_w * 1.64:.1f}" height="{plot_h:.1f}" '
                f'fill="rgba(61,127,255,.10)"/>'
            )
        parts.append(
            f'<line x1="{x:.1f}" y1="{high_y:.1f}" x2="{x:.1f}" y2="{low_y:.1f}" '
            f'stroke="{color}" stroke-width="1.2"/>'
        )
        y = min(open_y, close_y)
        h = max(abs(close_y - open_y), 1.4)
        fill = color if not up else "transparent"
        parts.append(
            f'<rect x="{x - candle_w / 2:.1f}" y="{y:.1f}" width="{candle_w:.1f}" '
            f'height="{h:.1f}" fill="{fill}" stroke="{color}" stroke-width="1.2"/>'
        )

    last_date = bars[-1]["trade_date"][5:].replace("-", "/")
    first_date = bars[0]["trade_date"][5:].replace("-", "/")
    parts.append(
        f'<text x="{pad_l}" y="{height - 6}" fill="#697082" font-size="10">{first_date}</text>'
    )
    parts.append(
        f'<text x="{width - pad_r}" y="{height - 6}" fill="#697082" '
        f'font-size="10" text-anchor="end">{last_date}</text>'
    )
    parts.append(f'<text x="{pad_l}" y="9" fill="#d7a53f" font-size="9">MA20</text>')
    parts.append(f'<text x="{pad_l + 34}" y="9" fill="#5c84e8" font-size="9">MA40</text>')
    parts.append("</svg>")
    return "".join(parts)


# ── K 线形态分类 ─────────────────────────────────────────────────────────

def candle_shape(row) -> str:
    """返回 K 线形态标签，用于底部反转模式识别。"""
    open_price = row["open"]
    close_price = row["close"]
    high = row["high"]
    low = row["low"]
    if not all([open_price, close_price, high, low]):
        return "unknown"
    span = high - low
    body = abs(close_price - open_price)
    if span == 0:
        return "doji"
    body_ratio = body / span
    upper = high - max(open_price, close_price)
    lower = min(open_price, close_price) - low
    if body_ratio < 0.12:
        return "doji"
    bullish = close_price >= open_price
    upper_ratio = upper / span
    lower_ratio = lower / span
    if body_ratio >= 0.7:
        if lower_ratio >= 0.15 and bullish:
            return "bullish_hammer_long"
        if lower_ratio >= 0.15:
            return "hanging_man_long"
        return "marubozu_bull" if bullish else "marubozu_bear"
    if lower_ratio >= 0.45 and body_ratio <= 0.35:
        return "hammer" if bullish else "hanging_man"
    if upper_ratio >= 0.45 and body_ratio <= 0.35:
        return "inverted_hammer" if bullish else "shooting_star"
    if bullish:
        return "bullish_candle"
    return "bearish_candle"


def is_long_body(shape: str, min_body_pct: float = 0.8) -> bool:
    return shape in ("marubozu_bull", "marubozu_bear",
                     "bullish_hammer_long", "hanging_man_long")


# ── 日线历史加载（pattern 专用） ─────────────────────────────────────────

def load_daily_histories_for_pattern(conn, codes: list, trade_date: str,
                                      lookback_days: int) -> dict:
    """加载 trade_date 之前 lookback_days 天的日线序列，按 code 分组。"""
    histories = {code: [] for code in codes}
    start_dt = datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=lookback_days)
    start_date = start_dt.strftime("%Y-%m-%d")
    for batch in chunked(codes, 600):
        placeholders = ",".join("?" for _ in batch)
        rows = conn.execute(f"""
            SELECT code, trade_date, open, close, high, low, volume, amount,
                   pct_change, turnover
            FROM daily_prices
            WHERE code IN ({placeholders})
              AND trade_date >= ? AND trade_date <= ?
            ORDER BY code, trade_date
        """, batch + [start_date, trade_date]).fetchall()
        for row in rows:
            histories[row["code"]].append(row)
    return histories


def load_daily_histories_for_pattern_range(conn, codes: list,
                                            start_date: str, end_date: str,
                                            lookback_days: int,
                                            progress=None) -> dict:
    """加载区间回填所需历史日线（包含 lookback_days 窗口）。"""
    histories = {code: [] for code in codes}
    start_dt = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=lookback_days)
    history_start = start_dt.strftime("%Y-%m-%d")
    batches = list(chunked(codes, 600))
    for batch_index, batch in enumerate(batches, 1):
        placeholders = ",".join("?" for _ in batch)
        rows = conn.execute(f"""
            SELECT code, trade_date, open, close, high, low, volume, amount,
                   pct_change, turnover
            FROM daily_prices
            WHERE code IN ({placeholders})
              AND trade_date >= ? AND trade_date <= ?
            ORDER BY code, trade_date
        """, batch + [history_start, end_date]).fetchall()
        for row in rows:
            histories[row["code"]].append(row)
        if progress:
            progress(batch_index, len(batches), len(rows))
    return histories
