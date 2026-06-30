"""底部反转/四针形态检测：信号识别、历史统计、候选股评估。

来源：app.py ~3687-4395 的以下函数：
  candle_shape（底部用的扩展版本）、is_long_body、BOTTOM_PATTERN_GROUP_NAMES、
  bottom_reversal_context、bottom_pattern_group、bottom_pattern_allowed_names、
  detect_bottom_reversal、bottom_reversal_confirmation、bottom_reversal_weak_filter、
  bottom_reversal_signal_at、bottom_pattern_history_stats、bottom_pattern_history_bonus、
  evaluate_four_pin_candidate、evaluate_bottom_reversal_candidate、evaluate_pattern_candidate、
  apply_market_cap_filter。
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta

from trader.core.utils import clamp, chunked
from trader.data.realtime import position_in_range
from trader.strategies.pattern.candle import (
    candle_metrics,
    is_doji_candle,
    has_balanced_shadows,
    four_pin_levels,
    build_candlestick_chart,
)


# ── 形态标签（底部反转用的扩展版本） ─────────────────────────────────────

def candle_shape(row) -> dict | None:
    open_price = row["open"]
    close_price = row["close"]
    high = row["high"]
    low = row["low"]
    if not open_price or not close_price or not high or not low or high < low:
        return None
    span = high - low
    body = abs(close_price - open_price)
    upper = high - max(open_price, close_price)
    lower = min(open_price, close_price) - low
    base = close_price or open_price
    return {
        "open": open_price, "close": close_price, "high": high, "low": low,
        "span": span, "body": body,
        "upper": max(upper, 0), "lower": max(lower, 0),
        "body_pct": body / base * 100.0 if base else 0.0,
        "body_range_pct": body / span * 100.0 if span else 0.0,
        "mid": (open_price + close_price) / 2.0,
        "bull": close_price > open_price,
        "bear": close_price < open_price,
    }


def is_long_body(shape, min_body_pct: float = 0.8) -> bool:
    return shape and shape["body_pct"] >= min_body_pct and shape["body_range_pct"] >= 45


# ── 形态分组 ──────────────────────────────────────────────────────────────

BOTTOM_PATTERN_GROUP_NAMES = {
    "engulfing": ("看涨吞没",),
    "strong": ("看涨吞没", "曙光初现", "早晨之星"),
    "single": ("锤头线", "倒锤头线"),
    "all": ("看涨吞没", "曙光初现", "早晨之星", "锤头线", "倒锤头线"),
}
BOTTOM_STRONG_PATTERNS = ("看涨吞没", "曙光初现", "早晨之星")
BOTTOM_SINGLE_PIN_PATTERNS = ("锤头线", "倒锤头线")


def bottom_pattern_group(params) -> str:
    group = (params or {}).get("bottom_pattern_group")
    if group in BOTTOM_PATTERN_GROUP_NAMES:
        return group
    return "engulfing" if (params or {}).get("bottom_only_bullish_engulfing", 1) else "all"


def bottom_pattern_allowed_names(params) -> tuple:
    return BOTTOM_PATTERN_GROUP_NAMES[bottom_pattern_group(params)]


# ── 上下文验证 ────────────────────────────────────────────────────────────

def bottom_reversal_context(series, pattern_len, params):
    if len(series) < max(25, pattern_len + 10):
        return None
    last = series[-1]
    lookback = min(params["bottom_lookback_days"], len(series))
    window = series[-lookback:]
    highs = [r["high"] for r in window if r["high"] is not None]
    lows = [r["low"] for r in window if r["low"] is not None]
    closes = [r["close"] for r in window if r["close"] is not None]
    if not highs or not lows or not closes or last["close"] is None:
        return None
    range_low = min(lows)
    range_high = max(highs)
    bottom_position = position_in_range(last["close"], range_low, range_high)
    if bottom_position is None:
        return None
    bottom_position_pct = bottom_position * 100.0
    if bottom_position_pct > params["max_bottom_position"]:
        return None
    prior_window = series[max(0, len(series) - lookback):-pattern_len]
    pattern_rows = series[-pattern_len:]
    prior_highs = [r["high"] for r in prior_window if r["high"] is not None]
    pattern_lows = [r["low"] for r in pattern_rows if r["low"] is not None]
    if not prior_highs or not pattern_lows:
        return None
    prior_high = max(prior_highs)
    pattern_low = min(pattern_lows)
    prior_drop_pct = (prior_high - pattern_low) / prior_high * 100.0 if prior_high else 0.0
    if prior_drop_pct < params["min_prior_drop_pct"]:
        return None
    return {
        "bottom_position_pct": bottom_position_pct,
        "prior_drop_pct": prior_drop_pct,
        "range_low": range_low,
        "range_high": range_high,
    }


# ── 底部反转信号检测 ──────────────────────────────────────────────────────

def detect_bottom_reversal(series, params) -> dict | None:
    if len(series) < 3:
        return None
    s1 = candle_shape(series[-1])
    s2 = candle_shape(series[-2])
    s3 = candle_shape(series[-3])
    if not s1:
        return None

    body_limit = params["bottom_max_body_pct"]
    patterns = []
    close_position = position_in_range(s1["close"], s1["low"], s1["high"])
    close_position_pct = close_position * 100.0 if close_position is not None else 0

    if s1["span"] > 0 and s1["body_pct"] <= body_limit:
        body_ref = max(s1["body"], s1["close"] * 0.002)
        lower_ratio = s1["lower"] / body_ref if body_ref else 0
        upper_share = s1["upper"] / s1["span"] * 100.0
        lower_share = s1["lower"] / s1["span"] * 100.0
        if lower_ratio >= 3.0 and lower_share >= 60 and upper_share <= 12 and close_position_pct >= 65:
            patterns.append({
                "name": "锤头线", "days": 1, "score": 66,
                "reasons": ["底部锤头线", f"下影占比{lower_share:.0f}%",
                            f"下影/实体{lower_ratio:.1f}倍", f"实体{s1['body_pct']:.2f}%"],
            })
        upper_ratio = s1["upper"] / body_ref if body_ref else 0
        if upper_ratio >= 3.0 and upper_share >= 60 and lower_share <= 12 and close_position_pct >= 45:
            patterns.append({
                "name": "倒锤头线", "days": 1, "score": 62,
                "reasons": ["底部倒锤头线", f"上影占比{upper_share:.0f}%",
                            f"上影/实体{upper_ratio:.1f}倍", f"实体{s1['body_pct']:.2f}%"],
            })

    if s1 and s2 and s2["bear"] and s1["bull"]:
        if (is_long_body(s2) and s1["body_pct"] >= 0.8
                and s1["open"] <= s2["close"] * 1.002
                and s1["close"] >= s2["open"] * 0.998
                and s1["body"] >= s2["body"] * 1.05):
            patterns.append({
                "name": "看涨吞没", "days": 2, "score": 78,
                "reasons": ["底部看涨吞没", f"前阴实体{s2['body_pct']:.2f}%", f"后阳实体{s1['body_pct']:.2f}%"],
            })
        if (is_long_body(s2) and s1["close"] > s2["mid"]
                and s1["close"] <= s2["open"] * 0.998
                and s1["open"] <= s2["close"] * 1.002
                and s1["body"] >= s2["body"] * 0.5
                and close_position_pct >= 65):
            patterns.append({
                "name": "曙光初现", "days": 2, "score": 72,
                "reasons": ["底部曙光初现", "阳线收复前阴半分位", f"后阳实体{s1['body_pct']:.2f}%"],
            })

    if s1 and s2 and s3 and s3["bear"] and s1["bull"]:
        if (is_long_body(s3) and s2["body_pct"] <= body_limit and s2["body_range_pct"] <= 45
                and s1["close"] >= s3["mid"] and s1["close"] > s2["close"]
                and s1["body_pct"] >= 1.0 and s1["body"] >= s3["body"] * 0.45
                and s2["low"] <= min(s3["close"], s1["open"]) * 1.01
                and close_position_pct >= 65):
            patterns.append({
                "name": "早晨之星", "days": 3, "score": 84,
                "reasons": ["底部早晨之星", "第三根阳线收复首阴半分位", f"中间小实体{s2['body_pct']:.2f}%"],
            })

    if not patterns:
        return None
    allowed_names = bottom_pattern_allowed_names(params)
    patterns = [p for p in patterns if p["name"] in allowed_names]
    if not patterns:
        return None
    return max(patterns, key=lambda p: p["score"])


def bottom_reversal_confirmation(series, pattern, context, low_pattern,
                                  volume_ratio, ma5, ma5_slope_pct, ma20, params):
    last = series[-1]
    prev = series[-2] if len(series) >= 2 else None
    pct_change = last["pct_change"] if last["pct_change"] is not None else 0
    close_price = last["close"]
    if not close_price:
        return None

    rebound_pct = (close_price - low_pattern) / close_price * 100.0 if close_price else 0
    close_position = position_in_range(close_price, last["low"], last["high"])
    if close_position is None:
        return None
    close_position_pct = close_position * 100.0
    is_single_pin = pattern["name"] in BOTTOM_SINGLE_PIN_PATTERNS
    if not is_single_pin and close_position_pct < params["min_bottom_close_position"]:
        return None
    close_above_prev = bool(prev and prev["close"] is not None and close_price > prev["close"])
    if not is_single_pin and params.get("require_bottom_close_above_prev") and not close_above_prev:
        return None
    if not is_single_pin and pct_change < params.get("min_bottom_pct_change", -20.0):
        return None
    if not is_single_pin and rebound_pct < params.get("min_bottom_rebound_pct", 0.0):
        return None
    min_vr = params.get("min_bottom_volume_ratio") or 0
    if min_vr and (volume_ratio is None or volume_ratio < min_vr):
        return None
    max_vr = params.get("max_bottom_volume_ratio") or 0
    if not is_single_pin and max_vr and volume_ratio is not None and volume_ratio > max_vr:
        return None
    above_ma5 = ma5 is not None and close_price >= ma5
    if not is_single_pin and params.get("require_bottom_above_ma5") and not above_ma5:
        return None
    min_ma5_slope = params.get("min_bottom_ma5_slope_pct")
    if not is_single_pin and min_ma5_slope is not None and ma5_slope_pct is not None:
        if ma5_slope_pct < min_ma5_slope:
            return None
    above_ma20 = ma20 is not None and close_price >= ma20

    if pattern["name"] in BOTTOM_STRONG_PATTERNS:
        if pct_change < params.get("min_bottom_strong_gain_pct", 0.0):
            return None
        name = pattern["name"]
        if (name == "早晨之星" and close_position_pct < 60) or \
           (name == "看涨吞没" and close_position_pct < 60) or \
           (name == "曙光初现" and close_position_pct < 65):
            return None
    elif is_single_pin:
        prior5 = series[-6:-1]
        prior5_lows = [r["low"] for r in prior5 if r["low"] is not None]
        if pct_change < 0:
            return None
        if prior5_lows and last["low"] < min(prior5_lows):
            return None
        if volume_ratio is not None and volume_ratio > 5.0:
            return None

    confirm_reasons = [
        f"反弹{rebound_pct:.1f}%", f"当日涨幅{pct_change:.1f}%",
        f"收盘位{close_position_pct:.0f}%",
    ]
    if close_above_prev:
        confirm_reasons.append("高于前收")
    if volume_ratio is not None:
        confirm_reasons.append(f"量比{volume_ratio:.2f}")
    if above_ma5:
        confirm_reasons.append("收回MA5")
    if ma5_slope_pct is not None:
        confirm_reasons.append(f"MA5斜率{ma5_slope_pct:.1f}%")

    if params.get("require_bottom_confirm"):
        if is_single_pin:
            confirm_reasons += ["收盘不跌", "未破5日低点"]
            if volume_ratio is not None and volume_ratio <= 5.0:
                confirm_reasons.append("量能不过热")
            confirm_reasons.append("长影线放量")
            return {
                "rebound_pct": rebound_pct, "close_position_pct": close_position_pct,
                "close_above_prev": close_above_prev, "above_ma5": above_ma5,
                "ma5_slope_pct": ma5_slope_pct,
                "pin_low_break_5": False, "pin_volume_overheat": False,
                "reasons": confirm_reasons,
            }
        bullish_pattern = pattern["name"] in BOTTOM_STRONG_PATTERNS
        high_close = close_position_pct >= 70
        if not (above_ma20 or bullish_pattern or high_close):
            return None
        if above_ma20:
            confirm_reasons.append("收在MA20上方")
        elif bullish_pattern:
            confirm_reasons.append("组合反转确认")
        else:
            confirm_reasons.append("高位收盘确认")

    return {
        "rebound_pct": rebound_pct, "close_position_pct": close_position_pct,
        "close_above_prev": close_above_prev, "above_ma5": above_ma5,
        "ma5_slope_pct": ma5_slope_pct,
        "pin_low_break_5": None, "pin_volume_overheat": None,
        "reasons": confirm_reasons,
    }


def bottom_reversal_weak_filter(series, pattern_len, params, pattern_name=None):
    last = series[-1]
    close_price = last["close"]
    if close_price is None:
        return None
    lookback = min(params.get("bottom_new_low_lookback_days") or 20,
                   max(0, len(series) - pattern_len))
    prior_rows = series[max(0, len(series) - pattern_len - lookback):len(series) - pattern_len]
    prior_closes = [r["close"] for r in prior_rows if r["close"] is not None]
    prior_close_low = min(prior_closes) if prior_closes else None
    close_new_low = bool(prior_close_low is not None and close_price <= prior_close_low)
    if (pattern_name not in BOTTOM_SINGLE_PIN_PATTERNS
            and params.get("require_bottom_not_close_new_low") and close_new_low):
        return None
    close_lift_pct = (
        (close_price - prior_close_low) / prior_close_low * 100.0
        if prior_close_low else None
    )
    return {
        "close_new_low": close_new_low,
        "prior_close_low": prior_close_low,
        "close_lift_pct": close_lift_pct,
    }


def bottom_reversal_signal_at(series, params) -> dict | None:
    if len(series) < max(45, params["chart_bars"] // 2):
        return None
    last = series[-1]
    pattern = detect_bottom_reversal(series, params)
    if not pattern:
        return None
    context = bottom_reversal_context(series, pattern["days"], params)
    if not context:
        return None
    closes = [r["close"] for r in series if r["close"] is not None]
    prior_volume_rows = series[-21:-1]
    volumes = [r["volume"] for r in prior_volume_rows if r["volume"] is not None]
    if len(closes) < 40:
        return None
    ma5 = sum(closes[-5:]) / 5
    prev_ma5 = sum(closes[-6:-1]) / 5 if len(closes) >= 6 else None
    ma5_slope_pct = ((ma5 - prev_ma5) / prev_ma5 * 100.0) if prev_ma5 else None
    ma20 = sum(closes[-20:]) / 20
    ma40 = sum(closes[-40:]) / 40
    ma40_distance = abs(last["close"] - ma40) / ma40 * 100.0 if ma40 else None
    if params["max_ma40_distance"] and ma40_distance is not None:
        if ma40_distance > params["max_ma40_distance"]:
            return None
    pattern_rows = series[-pattern["days"]:]
    avg_volume20 = sum(volumes) / len(volumes) if volumes else None
    volume_ratio = (last["volume"] or 0) / avg_volume20 if avg_volume20 else None
    low_pattern = min(r["low"] for r in pattern_rows if r["low"] is not None)
    weak_meta = bottom_reversal_weak_filter(series, pattern["days"], params, pattern["name"])
    if not weak_meta:
        return None
    confirmation = bottom_reversal_confirmation(
        series, pattern, context, low_pattern, volume_ratio, ma5, ma5_slope_pct, ma20, params
    )
    if not confirmation:
        return None
    return {
        "pattern_name": pattern["name"],
        "pattern_days": pattern["days"],
        "volume_ratio": volume_ratio,
    }


# ── 历史统计 ──────────────────────────────────────────────────────────────

def bottom_pattern_history_stats(series, params, pattern_name) -> dict:
    hold_days = params.get("pattern_win_hold_days") or 1
    lookback_days = params.get("pattern_win_lookback_days") or 720
    target_pct = params.get("pattern_win_target_pct") or 3.0
    if len(series) < hold_days + 46:
        return {"sample_count": 0, "hold_days": hold_days, "target_pct": target_pct}

    try:
        cutoff_date = (
            datetime.strptime(series[-1]["trade_date"], "%Y-%m-%d")
            - timedelta(days=lookback_days)
        ).strftime("%Y-%m-%d")
    except (TypeError, ValueError):
        cutoff_date = None

    returns = []
    max_gains = []
    max_drawdowns = []
    wins = 0
    target_hits = 0
    end_limit = len(series) - hold_days - 1
    for idx in range(44, end_limit + 1):
        row = series[idx]
        if cutoff_date and row["trade_date"] < cutoff_date:
            continue
        signal = bottom_reversal_signal_at(series[:idx + 1], params)
        if not signal or signal["pattern_name"] != pattern_name:
            continue
        buy_price = row["close"]
        sell_price = series[idx + hold_days]["close"]
        if not buy_price or not sell_price:
            continue
        forward_rows = series[idx + 1:idx + hold_days + 1]
        highs = [r["high"] for r in forward_rows if r["high"] is not None]
        lows = [r["low"] for r in forward_rows if r["low"] is not None]
        return_pct = (sell_price - buy_price) / buy_price * 100.0
        max_gain_pct = (max(highs) - buy_price) / buy_price * 100.0 if highs else return_pct
        max_drawdown_pct = (min(lows) - buy_price) / buy_price * 100.0 if lows else return_pct
        returns.append(return_pct)
        max_gains.append(max_gain_pct)
        max_drawdowns.append(max_drawdown_pct)
        if return_pct > 0:
            wins += 1
        if max_gain_pct >= target_pct:
            target_hits += 1

    n = len(returns)
    if not n:
        return {"sample_count": 0, "hold_days": hold_days, "target_pct": target_pct}
    return {
        "sample_count": n, "hold_days": hold_days, "target_pct": target_pct,
        "win_rate_pct": round(wins / n * 100.0, 1),
        "target_rate_pct": round(target_hits / n * 100.0, 1),
        "avg_return_pct": round(sum(returns) / n, 2),
        "avg_max_gain_pct": round(sum(max_gains) / n, 2),
        "avg_max_drawdown_pct": round(sum(max_drawdowns) / n, 2),
    }


def bottom_pattern_history_bonus(stats) -> float:
    n = stats.get("sample_count") or 0
    if n < 3:
        return 0.0
    bonus = 0.0
    win_rate = stats.get("win_rate_pct")
    avg_return = stats.get("avg_return_pct")
    target_rate = stats.get("target_rate_pct")
    if win_rate is not None:
        bonus += clamp((win_rate - 50.0) / 50.0 * 8.0, -6.0, 8.0)
    if avg_return is not None:
        bonus += clamp(avg_return / 5.0 * 5.0, -5.0, 5.0)
    if target_rate is not None:
        bonus += clamp((target_rate - 35.0) / 65.0 * 4.0, -3.0, 4.0)
    return bonus


# ── 候选股评估 ────────────────────────────────────────────────────────────

def evaluate_four_pin_candidate(stock, series, params):
    if len(series) < max(45, params["chart_bars"] // 2):
        return None
    if series[-1]["trade_date"] != params["trade_date"]:
        return None
    last_four = series[-4:]
    if len(last_four) < 4:
        return None
    required_dates = params.get("required_pattern_dates") or []
    if required_dates and [row["trade_date"] for row in last_four] != required_dates:
        return None
    if not all(is_doji_candle(row, params) for row in last_four):
        return None
    shadowless_count = sum(1 for row in last_four if not has_balanced_shadows(row, params))
    if shadowless_count > params["max_shadowless_count"]:
        return None
    levels = four_pin_levels(last_four, params)
    if not levels:
        return None
    last = last_four[-1]
    if (last["amount"] or 0) < params["min_amount_wan"] * 10000:
        return None
    if params["min_turnover"] and (last["turnover"] is None or last["turnover"] < params["min_turnover"]):
        return None

    closes = [r["close"] for r in series if r["close"] is not None]
    volumes = [r["volume"] for r in series[-20:] if r["volume"] is not None]
    if len(closes) < 40:
        return None
    ma20 = sum(closes[-20:]) / 20
    ma40 = sum(closes[-40:]) / 40
    ma40_distance = abs(last["close"] - ma40) / ma40 * 100.0 if ma40 else None
    if params["max_ma40_distance"] and ma40_distance is not None:
        if ma40_distance > params["max_ma40_distance"]:
            return None

    metrics = [candle_metrics(row) for row in last_four]
    if any(m is None for m in metrics):
        return None
    avg_body = sum(m["body_pct"] for m in metrics) / len(metrics)
    avg_amp = sum(m["amp_pct"] for m in metrics) / len(metrics)
    max_body = max(m["body_pct"] for m in metrics)
    avg_volume20 = sum(volumes) / len(volumes) if volumes else None
    pattern_volume = sum((r["volume"] or 0) for r in last_four) / 4
    volume_ratio = pattern_volume / avg_volume20 if avg_volume20 else None
    high4 = max(r["high"] for r in last_four if r["high"] is not None)
    low4 = min(r["low"] for r in last_four if r["low"] is not None)
    range4_pct = (high4 - low4) / last["close"] * 100.0 if last["close"] else None
    closes20 = closes[-20:]
    ma40_score = (
        10 if not params["max_ma40_distance"]
        else clamp((params["max_ma40_distance"] - (ma40_distance or 0)) / max(params["max_ma40_distance"], 1) * 10, 0, 10)
    )
    score = (
        clamp((params["doji_body_pct"] - max_body) / params["doji_body_pct"] * 22, 0, 22)
        + clamp((params["max_amp_pct"] - avg_amp) / params["max_amp_pct"] * 14, 0, 14)
        + clamp((params["max_pair_distance"] - levels["first_third_gap"]) / params["max_pair_distance"] * 18, 0, 18)
        + clamp((params["max_pair_distance"] - levels["second_fourth_gap"]) / params["max_pair_distance"] * 18, 0, 18)
        + clamp((levels["level_gap"] - params["min_level_gap"]) / 1.8 * 10, 0, 10)
        + ma40_score
        + clamp((1.25 - (volume_ratio or 1.25)) / 1.25 * 8, 0, 8)
    )
    reasons = [
        "1/3在上2/4在下",
        f"1/3偏差{levels['first_third_gap']:.2f}%",
        f"2/4偏差{levels['second_fourth_gap']:.2f}%",
        f"1/3收差{levels['first_third_close_gap']:.2f}%",
        f"2/4收差{levels['second_fourth_close_gap']:.2f}%",
        f"高低差{levels['level_gap']:.2f}%",
    ]
    if shadowless_count:
        reasons.append(f"影线不完整{shadowless_count}根")
    if ma40_distance is not None:
        reasons.append(f"MA40距离{ma40_distance:.1f}%")
    if volume_ratio is not None and volume_ratio <= 0.85:
        reasons.append("缩量整理")
    if last["close"] >= ma20:
        reasons.append("收在MA20上方")

    chart_series = series[-params["chart_bars"]:]
    return {
        "code": stock["code"], "name": stock["name"] or "",
        "trade_date": last["trade_date"],
        "close": round(last["close"], 3) if last["close"] is not None else None,
        "pct": round(last["pct_change"], 2) if last["pct_change"] is not None else None,
        "amount_yi": round((last["amount"] or 0) / 100000000, 2),
        "turnover": round(last["turnover"], 2) if last["turnover"] is not None else None,
        "avg_body_pct": round(avg_body, 2), "doji_body_pct": round(max_body, 2),
        "avg_amp_pct": round(avg_amp, 2),
        "range5_pct": round(range4_pct, 2) if range4_pct is not None else None,
        "volume_ratio": round(volume_ratio, 2) if volume_ratio is not None else None,
        "first_third_gap": round(levels["first_third_gap"], 2),
        "second_fourth_gap": round(levels["second_fourth_gap"], 2),
        "first_third_close_gap": round(levels["first_third_close_gap"], 2),
        "second_fourth_close_gap": round(levels["second_fourth_close_gap"], 2),
        "level_gap": round(levels["level_gap"], 2),
        "shadowless_count": shadowless_count,
        "ma20": round(ma20, 3), "ma40": round(ma40, 3),
        "ma40_distance": round(ma40_distance, 2) if ma40_distance is not None else None,
        "score": round(score, 1), "reasons": " / ".join(reasons),
        "chart": build_candlestick_chart(chart_series, highlight=4),
        "bars": [
            {"trade_date": r["trade_date"], "open": r["open"], "close": r["close"],
             "high": r["high"], "low": r["low"], "volume": r["volume"],
             "amount": r["amount"], "pct_change": r["pct_change"], "turnover": r["turnover"]}
            for r in chart_series
        ],
    }


def evaluate_bottom_reversal_candidate(stock, series, params):
    if len(series) < max(45, params["chart_bars"] // 2):
        return None
    if series[-1]["trade_date"] != params["trade_date"]:
        return None
    last = series[-1]
    if (last["amount"] or 0) < params["min_amount_wan"] * 10000:
        return None
    if params["min_turnover"] and (last["turnover"] is None or last["turnover"] < params["min_turnover"]):
        return None
    pattern = detect_bottom_reversal(series, params)
    if not pattern:
        return None
    context = bottom_reversal_context(series, pattern["days"], params)
    if not context:
        return None
    closes = [r["close"] for r in series if r["close"] is not None]
    prior_volume_rows = series[-21:-1]
    volumes = [r["volume"] for r in prior_volume_rows if r["volume"] is not None]
    if len(closes) < 40:
        return None
    ma5 = sum(closes[-5:]) / 5
    prev_ma5 = sum(closes[-6:-1]) / 5 if len(closes) >= 6 else None
    ma5_slope_pct = ((ma5 - prev_ma5) / prev_ma5 * 100.0) if prev_ma5 else None
    ma20 = sum(closes[-20:]) / 20
    ma40 = sum(closes[-40:]) / 40
    ma40_distance = abs(last["close"] - ma40) / ma40 * 100.0 if ma40 else None
    if params["max_ma40_distance"] and ma40_distance is not None:
        if ma40_distance > params["max_ma40_distance"]:
            return None
    pattern_rows = series[-pattern["days"]:]
    metrics_list = [candle_metrics(row) for row in pattern_rows]
    if any(m is None for m in metrics_list):
        return None
    max_body = max(m["body_pct"] for m in metrics_list)
    avg_amp = sum(m["amp_pct"] for m in metrics_list) / len(metrics_list)
    avg_volume20 = sum(volumes) / len(volumes) if volumes else None
    pattern_volume = sum((r["volume"] or 0) for r in pattern_rows) / len(pattern_rows)
    pattern_volume_ratio = pattern_volume / avg_volume20 if avg_volume20 else None
    volume_ratio = (last["volume"] or 0) / avg_volume20 if avg_volume20 else None
    low_pattern = min(r["low"] for r in pattern_rows if r["low"] is not None)
    high_pattern = max(r["high"] for r in pattern_rows if r["high"] is not None)
    range_pct = (high_pattern - low_pattern) / last["close"] * 100.0 if last["close"] else None
    weak_meta = bottom_reversal_weak_filter(series, pattern["days"], params, pattern["name"])
    if not weak_meta:
        return None
    confirmation = bottom_reversal_confirmation(
        series, pattern, context, low_pattern, volume_ratio, ma5, ma5_slope_pct, ma20, params
    )
    if not confirmation:
        return None
    bottom_bonus = clamp(
        (params["max_bottom_position"] - context["bottom_position_pct"])
        / max(params["max_bottom_position"], 1) * 12, 0, 12,
    )
    drop_bonus = clamp((context["prior_drop_pct"] - params["min_prior_drop_pct"]) / 15 * 10, 0, 10)
    volume_bonus = clamp(((volume_ratio or 1.0) - 1.0) / 1.0 * 8, 0, 8)
    ma5_bonus = 4 if confirmation.get("above_ma5") else 0
    ma5_slope_bonus = clamp(((ma5_slope_pct or -1.0) + 1.0) / 3.0 * 4, 0, 4)
    ma_bonus = 5 if last["close"] >= ma20 else 0
    history_stats = bottom_pattern_history_stats(series, params, pattern["name"])
    history_bonus = bottom_pattern_history_bonus(history_stats)
    score = (pattern["score"] + bottom_bonus + drop_bonus + volume_bonus
             + ma5_bonus + ma5_slope_bonus + ma_bonus + history_bonus)

    reasons = list(pattern["reasons"])
    reasons.append(f"近{params['bottom_lookback_days']}日低位{context['bottom_position_pct']:.0f}%")
    reasons.append(f"前期回撤{context['prior_drop_pct']:.1f}%")
    reasons.extend(confirmation["reasons"])
    if last["close"] >= ma20:
        reasons.append("收在MA20上方")
    if ma40_distance is not None:
        reasons.append(f"MA40距离{ma40_distance:.1f}%")
    if weak_meta.get("close_lift_pct") is not None:
        reasons.append(f"脱离前低{weak_meta['close_lift_pct']:.1f}%")
    if history_stats.get("sample_count"):
        reasons.append(
            f"{history_stats['hold_days']}日胜率{history_stats.get('win_rate_pct', 0):.1f}%"
            f"/样本{history_stats['sample_count']}"
        )
    else:
        reasons.append(f"{history_stats['hold_days']}日历史样本不足")

    chart_series = series[-params["chart_bars"]:]
    return {
        "pattern_type": "bottom_reversal",
        "pattern_name": pattern["name"], "pattern_days": pattern["days"],
        "code": stock["code"], "name": stock["name"] or "",
        "trade_date": last["trade_date"],
        "close": round(last["close"], 3) if last["close"] is not None else None,
        "pct": round(last["pct_change"], 2) if last["pct_change"] is not None else None,
        "amount_yi": round((last["amount"] or 0) / 100000000, 2),
        "turnover": round(last["turnover"], 2) if last["turnover"] is not None else None,
        "avg_body_pct": round(sum(m["body_pct"] for m in metrics_list) / len(metrics_list), 2),
        "doji_body_pct": round(max_body, 2), "avg_amp_pct": round(avg_amp, 2),
        "range5_pct": round(range_pct, 2) if range_pct is not None else None,
        "volume_ratio": round(volume_ratio, 2) if volume_ratio is not None else None,
        "pattern_volume_ratio": round(pattern_volume_ratio, 2) if pattern_volume_ratio is not None else None,
        "pattern_win_sample_count": history_stats.get("sample_count", 0),
        "pattern_win_hold_days": history_stats.get("hold_days"),
        "pattern_win_rate_pct": history_stats.get("win_rate_pct"),
        "pattern_target_rate_pct": history_stats.get("target_rate_pct"),
        "pattern_avg_return_pct": history_stats.get("avg_return_pct"),
        "pattern_avg_max_gain_pct": history_stats.get("avg_max_gain_pct"),
        "pattern_avg_max_drawdown_pct": history_stats.get("avg_max_drawdown_pct"),
        "pattern_history_score_bonus": round(history_bonus, 1),
        "first_third_gap": None, "second_fourth_gap": None,
        "first_third_close_gap": None, "second_fourth_close_gap": None,
        "level_gap": None, "shadowless_count": None,
        "bottom_position_pct": round(context["bottom_position_pct"], 1),
        "prior_drop_pct": round(context["prior_drop_pct"], 2),
        "rebound_pct": round(confirmation["rebound_pct"], 2),
        "close_position_pct": round(confirmation["close_position_pct"], 1),
        "close_above_prev": confirmation["close_above_prev"],
        "above_ma5": confirmation["above_ma5"],
        "ma5": round(ma5, 3),
        "ma5_slope_pct": round(ma5_slope_pct, 2) if ma5_slope_pct is not None else None,
        "close_new_low": weak_meta["close_new_low"],
        "pin_low_break_5": confirmation.get("pin_low_break_5"),
        "pin_volume_overheat": confirmation.get("pin_volume_overheat"),
        "prior_close_low": round(weak_meta["prior_close_low"], 3) if weak_meta["prior_close_low"] is not None else None,
        "close_lift_pct": round(weak_meta["close_lift_pct"], 2) if weak_meta["close_lift_pct"] is not None else None,
        "ma20": round(ma20, 3), "ma40": round(ma40, 3),
        "ma40_distance": round(ma40_distance, 2) if ma40_distance is not None else None,
        "score": round(score, 1), "reasons": " / ".join(reasons),
        "chart": build_candlestick_chart(chart_series, highlight=pattern["days"]),
        "bars": [
            {"trade_date": r["trade_date"], "open": r["open"], "close": r["close"],
             "high": r["high"], "low": r["low"], "volume": r["volume"],
             "amount": r["amount"], "pct_change": r["pct_change"], "turnover": r["turnover"]}
            for r in chart_series
        ],
    }


def evaluate_pattern_candidate(stock, series, params):
    if params.get("pattern_type") == "bottom_reversal":
        return evaluate_bottom_reversal_candidate(stock, series, params)
    return evaluate_four_pin_candidate(stock, series, params)

