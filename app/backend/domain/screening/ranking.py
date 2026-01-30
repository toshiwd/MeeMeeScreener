from __future__ import annotations
import math
from typing import List, Optional, Tuple, Any

from app.utils.math_utils import _build_ma_series, _calc_slope, _compute_atr, _pct_change

def _get_config_value(config: dict, keys: list[str], default: Any) -> Any:
    current = config
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current

def _calc_body(open_: float, close: float) -> float:
    return abs(close - open_)

def _calc_lower_shadow(open_: float, low: float, close: float) -> float:
    return min(open_, close) - low

def _compute_volume_ratio(volumes: list[float], period: int, include_latest: bool) -> float | None:
    if period <= 0:
        return None
    if include_latest:
        if len(volumes) < period:
            return None
        window = volumes[-period:]
    else:
        if len(volumes) < period + 1:
            return None
        window = volumes[-period - 1:-1]
    avg = sum(window) / period if period else 0
    if avg <= 0:
        return None
    latest = volumes[-1]
    return latest / avg

def _count_streak(values: list[float], averages: list[float | None], direction: str) -> int | None:
    count = 0
    opposite = 0
    has_values = False
    for value, avg in zip(values, averages):
        if avg is None:
            continue
        has_values = True
        if direction == "up":
            if value > avg:
                count += 1
                opposite = 0
            elif value < avg:
                opposite += 1
                if opposite >= 2:
                    count = 0
            else:
                opposite = 0
        else:
            if value < avg:
                count += 1
                opposite = 0
            elif value > avg:
                opposite += 1
                if opposite >= 2:
                    count = 0
            else:
                opposite = 0
    return None if not has_values else count

def _calc_recent_bounds(highs: list[float], lows: list[float], lookback: int) -> tuple[float | None, float | None]:
    if not highs or not lows:
        return None, None
    if lookback <= 0:
        return max(highs), min(lows)
    window_highs = highs[-lookback:] if len(highs) >= lookback else highs
    window_lows = lows[-lookback:] if len(lows) >= lookback else lows
    return max(window_highs), min(window_lows)

def _normalize_daily_rows(rows: list[tuple], as_of: int | None) -> list[tuple]:
    by_date: dict[int, tuple] = {}
    for row in rows:
        if len(row) < 6:
            continue
        date_value = row[0]
        if date_value is None:
            continue
        date_int = int(date_value)
        if as_of is not None and date_int > as_of:
            continue
        by_date[date_int] = row
    return [by_date[key] for key in sorted(by_date.keys())]

def _normalize_monthly_rows(rows: list[tuple], as_of_month: int | None) -> list[tuple]:
    by_month: dict[int, tuple] = {}
    for row in rows:
        if len(row) < 5:
            continue
        month_value = row[0]
        if month_value is None:
            continue
        month_int = int(month_value)
        if as_of_month is not None and month_int > as_of_month:
            continue
        by_month[month_int] = row
    return [by_month[key] for key in sorted(by_month.keys())]

def _format_month_label(value: int | str | None) -> str | None:
    if value is None:
        return None
    try:
        raw = str(int(value)).zfill(6)
        year = int(raw[:4])
        month = int(raw[4:6])
        return f"{year:04d}-{month:02d}"
    except (ValueError, TypeError):
        return None

def _format_daily_label(value: int | None) -> str | None:
    if value is None:
        return None
    raw = str(int(value)).zfill(8)
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"

def _detect_body_box(monthly_rows: list[tuple], config: dict) -> dict | None:
    thresholds = _get_config_value(config, ["monthly", "thresholds"], {})
    min_months = int(thresholds.get("min_months", 3))
    max_months = int(thresholds.get("max_months", 14))
    max_range_pct = float(thresholds.get("max_range_pct", 0.2))
    wild_wick_pct = float(thresholds.get("wild_wick_pct", 0.1))

    bars: list[dict] = []
    for row in monthly_rows:
        if len(row) < 5:
            continue
        month_value, open_, high, low, close = row[:5]
        if month_value is None or open_ is None or high is None or low is None or close is None:
            continue
        body_high = max(float(open_), float(close))
        body_low = min(float(open_), float(close))
        bars.append(
            {
                "time": int(month_value),
                "open": float(open_),
                "high": float(high),
                "low": float(low),
                "close": float(close),
                "body_high": body_high,
                "body_low": body_low
            }
        )

    if len(bars) < min_months:
        return None

    bars.sort(key=lambda item: item["time"])
    max_months = min(max_months, len(bars))

    for length in range(max_months, min_months - 1, -1):
        window = bars[-length:]
        upper = max(item["body_high"] for item in window)
        lower = min(item["body_low"] for item in window)
        base = max(abs(lower), 1e-9)
        range_pct = (upper - lower) / base
        if range_pct > max_range_pct:
            continue
        wild = False
        for item in window:
            if item["high"] > upper * (1 + wild_wick_pct) or item["low"] < lower * (1 - wild_wick_pct):
                wild = True
                break
        return {
            "start": window[0]["time"],
            "end": window[-1]["time"],
            "upper": upper,
            "lower": lower,
            "months": length,
            "range_pct": range_pct,
            "wild": wild,
            "last_close": window[-1]["close"]
        }

    return None

def score_weekly_candidate(code: str, name: str, rows: list[tuple], config: dict, as_of: int | None) -> tuple[dict | None, dict | None, str | None]:
    rows = _normalize_daily_rows(rows, as_of)
    common = _get_config_value(config, ["common"], {})
    min_bars = int(common.get("min_daily_bars", 80))
    if len(rows) < min_bars:
        return None, None, "insufficient_daily_bars"

    dates = [int(row[0]) for row in rows]
    opens = [float(row[1]) for row in rows]
    highs = [float(row[2]) for row in rows]
    lows = [float(row[3]) for row in rows]
    closes = [float(row[4]) for row in rows]
    volumes = [float(row[5]) if row[5] is not None else 0.0 for row in rows]

    close = closes[-1] if closes else None
    if close is None:
        return None, None, "missing_close"

    ma7_series = _build_ma_series(closes, 7)
    ma20_series = _build_ma_series(closes, 20)
    ma60_series = _build_ma_series(closes, 60)
    ma100_series = _build_ma_series(closes, 100)
    ma200_series = _build_ma_series(closes, 200)

    ma7 = ma7_series[-1] if ma7_series else None
    ma20 = ma20_series[-1] if ma20_series else None
    ma60 = ma60_series[-1] if ma60_series else None
    ma100 = ma100_series[-1] if ma100_series else None
    ma200 = ma200_series[-1] if ma200_series else None
    if ma20 is None or ma60 is None:
        return None, None, "missing_ma"
    if ma100 is None or ma200 is None:
        return None, None, "missing_ma_long_term"

    slope_lookback = int(common.get("slope_lookback", 3))
    slope20 = _calc_slope(ma20_series, slope_lookback)
    slope100 = _calc_slope(ma100_series, slope_lookback)
    slope200 = _calc_slope(ma200_series, slope_lookback)

    atr_period = int(common.get("atr_period", 14))
    atr14 = _compute_atr(highs, lows, closes, atr_period)

    volume_period = int(common.get("volume_period", 20))
    include_latest = common.get("volume_ratio_mode", "exclude_latest") == "include_latest"
    volume_ratio = _compute_volume_ratio(volumes, volume_period, include_latest)

    up7 = _count_streak(closes, ma7_series, "up")
    down7 = _count_streak(closes, ma7_series, "down")

    trigger_lookback = int(common.get("trigger_lookback", 20))
    recent_high, recent_low = _calc_recent_bounds(highs, lows, trigger_lookback)
    break_up_pct = None
    break_down_pct = None
    if recent_high is not None and close:
        break_up_pct = max(0.0, (recent_high - close) / close * 100)
    if recent_low is not None and close:
        break_down_pct = max(0.0, (close - recent_low) / close * 100)

    weekly = _get_config_value(config, ["weekly"], {})
    weights = weekly.get("weights", {})
    thresholds = weekly.get("thresholds", {})
    down_weights = weekly.get("down_weights", {})
    down_thresholds = weekly.get("down_thresholds", {})
    max_reasons = int(common.get("max_reasons", 6))

    up_reasons: list[tuple[float, str]] = []
    down_reasons: list[tuple[float, str]] = []
    up_badges: list[str] = []
    down_badges: list[str] = []
    up_score = 0.0
    down_score = 0.0

    def push_reason(target: list[tuple[float, str]], weight: float, label: str):
        if weight:
            target.append((weight, label))

    def push_badge(target: list[str], label: str):
        if label and label not in target:
            target.append(label)

    if close > ma20 and ma20 > ma60:
        weight = float(weights.get("ma_alignment", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA20 > MA60")
        push_badge(up_badges, "MA整列")

    if ma60 > ma100:
        weight = float(weights.get("ma_alignment_100", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA60 > MA100")

    if ma100 > ma200:
        weight = float(weights.get("ma_alignment_200", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA100 > MA200")

    if close > ma100:
        weight = float(weights.get("obs_above_ma100", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA100より上")
    
    if close > ma200:
        weight = float(weights.get("obs_above_ma200", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA200より上")

    pull_min = int(thresholds.get("pullback_down7_min", 1))
    pull_max = int(thresholds.get("pullback_down7_max", 2))
    slope_min = float(thresholds.get("slope_min", 0))
    if close > ma20 and down7 is not None and pull_min <= down7 <= pull_max:
        if slope20 is None or slope20 >= slope_min:
            weight = float(weights.get("pullback_above_ma20", 0))
            up_score += weight
            push_reason(up_reasons, weight, f"MA20上で押し目（下{down7}本）")
            push_badge(up_badges, "押し目")

    vol_thresh = float(thresholds.get("volume_ratio", 1.5))
    if volume_ratio is not None and volume_ratio >= vol_thresh:
        weight = float(weights.get("volume_spike", 0))
        up_score += weight
        push_reason(up_reasons, weight, f"出来高増（20日比{volume_ratio:.2f}倍）")
        push_badge(up_badges, "出来高増")

    near_pct = float(thresholds.get("near_break_pct", 2.0))
    if break_up_pct is not None and break_up_pct <= near_pct:
        weight = float(weights.get("near_high_break", 0))
        up_score += weight
        push_reason(up_reasons, weight, f"高値ブレイク接近（{break_up_pct:.1f}%）")
        push_badge(up_badges, "高値接近")

    if slope20 is not None and slope20 >= slope_min:
        weight = float(weights.get("slope_up", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA20上向き")
        push_badge(up_badges, "MA上向き")

    if slope100 is not None and slope100 >= slope_min:
        weight = float(weights.get("slope_up_100", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA100上向き")

    if slope200 is not None and slope200 >= slope_min:
        weight = float(weights.get("slope_up_200", 0))
        up_score += weight
        push_reason(up_reasons, weight, "MA200上向き")

    big_candle = float(thresholds.get("big_candle_atr", 1.2))
    if atr14 is not None and abs(close - opens[-1]) >= atr14 * big_candle and close > opens[-1]:
        weight = float(weights.get("big_bull_candle", 0))
        up_score += weight
        push_reason(up_reasons, weight, "強い陽線")
        push_badge(up_badges, "陽線強")

    ma20_dist = float(thresholds.get("ma20_distance_pct", 2.0))
    if ma20:
        dist_pct = abs(close - ma20) / ma20 * 100
        if close >= ma20 and dist_pct <= ma20_dist:
            weight = float(weights.get("ma20_support", 0))
            up_score += weight
            push_reason(up_reasons, weight, f"MA20近接（{dist_pct:.1f}%）")
            push_badge(up_badges, "MA20近接")

    ma100_thresh = float(thresholds.get("ma100_distance_pct", 3.0))
    if close >= ma100:
        dist100 = abs(close - ma100) / ma100 * 100
        if dist100 <= ma100_thresh:
            weight = float(weights.get("ma100_support", 0))
            up_score += weight
            push_reason(up_reasons, weight, f"MA100近接（{dist100:.1f}%）")

    ma200_thresh = float(thresholds.get("ma200_distance_pct", 3.0))
    if close >= ma200:
        dist200 = abs(close - ma200) / ma200 * 100
        if dist200 <= ma200_thresh:
            weight = float(weights.get("ma200_support", 0))
            up_score += weight
            push_reason(up_reasons, weight, f"MA200近接（{dist200:.1f}%）")

    if close < ma20 and ma20 < ma60:
        weight = float(down_weights.get("ma_alignment", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA20 < MA60")
        push_badge(down_badges, "MA逆転")

    if ma60 < ma100:
        weight = float(down_weights.get("ma_alignment_100", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA60 < MA100")

    if ma100 < ma200:
        weight = float(down_weights.get("ma_alignment_200", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA100 < MA200")

    if close < ma100:
        weight = float(down_weights.get("obs_below_ma100", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA100より下")
    
    if close < ma200:
        weight = float(down_weights.get("obs_below_ma200", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA200より下")

    pull_min = int(down_thresholds.get("pullback_up7_min", 1))
    pull_max = int(down_thresholds.get("pullback_up7_max", 2))
    slope_max = float(down_thresholds.get("slope_max", 0))
    if close < ma20 and up7 is not None and pull_min <= up7 <= pull_max:
        if slope20 is None or slope20 <= slope_max:
            weight = float(down_weights.get("pullback_below_ma20", 0))
            down_score += weight
            push_reason(down_reasons, weight, f"MA20下で戻り（上{up7}本）")
            push_badge(down_badges, "戻り")

    vol_thresh = float(down_thresholds.get("volume_ratio", vol_thresh))
    if volume_ratio is not None and volume_ratio >= vol_thresh:
        weight = float(down_weights.get("volume_spike", 0))
        down_score += weight
        push_reason(down_reasons, weight, f"出来高増（20日比{volume_ratio:.2f}倍）")
        push_badge(down_badges, "出来高増")

    near_pct = float(down_thresholds.get("near_break_pct", near_pct))
    if break_down_pct is not None and break_down_pct <= near_pct:
        weight = float(down_weights.get("near_low_break", 0))
        down_score += weight
        push_reason(down_reasons, weight, f"安値ブレイク接近（{break_down_pct:.1f}%）")
        push_badge(down_badges, "安値接近")

    if slope20 is not None and slope20 <= slope_max:
        weight = float(down_weights.get("slope_down", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA20下向き")
        push_badge(down_badges, "MA下向き")

    if slope100 is not None and slope100 <= slope_max:
        weight = float(down_weights.get("slope_down_100", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA100下向き")

    if slope200 is not None and slope200 <= slope_max:
        weight = float(down_weights.get("slope_down_200", 0))
        down_score += weight
        push_reason(down_reasons, weight, "MA200下向き")

    big_candle = float(down_thresholds.get("big_candle_atr", big_candle))
    if atr14 is not None and abs(close - opens[-1]) >= atr14 * big_candle and close < opens[-1]:
        weight = float(down_weights.get("big_bear_candle", 0))
        down_score += weight
        push_reason(down_reasons, weight, "強い陰線")
        push_badge(down_badges, "陰線強")

    ma20_dist = float(down_thresholds.get("ma20_distance_pct", ma20_dist))
    if ma20:
        dist_pct = abs(close - ma20) / ma20 * 100
        if close <= ma20 and dist_pct <= ma20_dist:
            weight = float(down_weights.get("ma20_resistance", 0))
            down_score += weight
            push_reason(down_reasons, weight, f"MA20近接（{dist_pct:.1f}%）")
            push_badge(down_badges, "MA20近接")

    ma100_thresh = float(down_thresholds.get("ma100_distance_pct", 3.0))
    if close <= ma100:
        dist100 = abs(close - ma100) / ma100 * 100
        if dist100 <= ma100_thresh:
            weight = float(down_weights.get("ma100_resistance", 0))
            down_score += weight
            push_reason(down_reasons, weight, f"MA100近接（{dist100:.1f}%）")

    ma200_thresh = float(down_thresholds.get("ma200_distance_pct", 3.0))
    if close <= ma200:
        dist200 = abs(close - ma200) / ma200 * 100
        if dist200 <= ma200_thresh:
            weight = float(down_weights.get("ma200_resistance", 0))
            down_score += weight
            push_reason(down_reasons, weight, f"MA200近接（{dist200:.1f}%）")

    up_reasons.sort(key=lambda item: item[0], reverse=True)
    down_reasons.sort(key=lambda item: item[0], reverse=True)

    levels = {
        "close": close,
        "ma7": ma7,
        "ma20": ma20,
        "ma60": ma60,
        "atr14": atr14,
        "volume_ratio": volume_ratio
    }

    chart_hint = {
        "lines": {
            "ma20": ma20,
            "ma60": ma60,
            "ma100": ma100,
            "ma200": ma200,
            "recent_high": recent_high,
            "recent_low": recent_low
        }
    }

    as_of_label = _format_daily_label(dates[-1])
    series_bars = int(common.get("rank_series_bars", 60))
    series_rows = rows[-series_bars:] if series_bars > 0 else rows
    series = [
        [int(item[0]), float(item[1]), float(item[2]), float(item[3]), float(item[4])]
        for item in series_rows
    ]

    base = {
        "code": code,
        "name": name or code,
        "as_of": as_of_label,
        "levels": levels,
        "series": series,
        "distance_to_trigger": {
            "break_up_pct": break_up_pct,
            "break_down_pct": break_down_pct
        },
        "chart_hint": chart_hint
    }

    up_item = {
        **base,
        "total_score": round(up_score, 3),
        "reasons": [label for _, label in up_reasons[:max_reasons]],
        "badges": up_badges[:max_reasons]
    }
    down_item = {
        **base,
        "total_score": round(down_score, 3),
        "reasons": [label for _, label in down_reasons[:max_reasons]],
        "badges": down_badges[:max_reasons]
    }

    return up_item, down_item, None


def score_monthly_candidate(code: str, name: str, rows: list[tuple], config: dict, as_of_month: int | None) -> tuple[dict | None, str | None]:
    rows = _normalize_monthly_rows(rows, as_of_month)
    thresholds = _get_config_value(config, ["monthly", "thresholds"], {})
    min_months = int(thresholds.get("min_months", 3))
    if len(rows) < min_months:
        return None, "insufficient_monthly_bars"

    box = _detect_body_box(rows, config)
    if not box:
        return None, "no_box"

    weights = _get_config_value(config, ["monthly", "weights"], {})
    max_reasons = int(_get_config_value(config, ["common", "max_reasons"], 6))
    near_edge_pct = float(thresholds.get("near_edge_pct", 4.0))
    wild_penalty = float(weights.get("wild_box_penalty", 0))

    close = float(box["last_close"])
    upper = float(box["upper"])
    lower = float(box["lower"])
    break_up_pct = max(0.0, (upper - close) / close * 100) if close else None
    break_down_pct = max(0.0, (close - lower) / close * 100) if close else None
    edge_pct = None
    if break_up_pct is not None and break_down_pct is not None:
        edge_pct = min(break_up_pct, break_down_pct)

    reasons: list[tuple[float, str]] = []
    score = 0.0

    months = int(box["months"])
    weight_month = float(weights.get("box_months", 0))
    if weight_month:
        score += weight_month * months
        reasons.append((weight_month, f"箱の期間{months}か月"))

    if edge_pct is not None and edge_pct <= near_edge_pct:
        weight = float(weights.get("near_edge", 0))
        ratio = 1 - edge_pct / near_edge_pct if near_edge_pct else 1
        score += weight * ratio
        if break_up_pct is not None and break_down_pct is not None:
            if break_up_pct <= break_down_pct:
                reasons.append((weight, f"上抜けまで{break_up_pct:.1f}%"))
            else:
                reasons.append((weight, f"下抜けまで{break_down_pct:.1f}%"))

    if box["wild"] and wild_penalty:
        score += wild_penalty
        reasons.append((wild_penalty, "荒れ箱"))

    closes = [float(row[4]) for row in rows if len(row) >= 5 and row[4] is not None]
    ma7_series = _build_ma_series(closes, 7)
    ma20_series = _build_ma_series(closes, 20)
    ma60_series = _build_ma_series(closes, 60)
    ma7 = ma7_series[-1] if ma7_series else None
    ma20 = ma20_series[-1] if ma20_series else None
    ma60 = ma60_series[-1] if ma60_series else None

    # New Logic: MA Alignment for Monthly
    if ma7 and ma20 and ma60:
        if ma7 > ma20 and ma20 > ma60:
            w_order = float(weights.get("ma_order_7_20_60", 0))
            score += w_order
            reasons.append((w_order, "月足MA配列(7>20>60)"))

        # Simple slope using last 2 points
        s7 = ma7_series[-1] - ma7_series[-2] if len(ma7_series) > 1 else 0
        s20 = ma20_series[-1] - ma20_series[-2] if len(ma20_series) > 1 else 0
        if s7 > 0 and s20 > 0:
            w_slopes = float(weights.get("ma_slopes_up", 0))
            score += w_slopes
            reasons.append((w_slopes, "月足MA上昇"))

    reasons.sort(key=lambda item: item[0], reverse=True)

    levels = {
        "close": close,
        "ma7": ma7,
        "ma20": ma20,
        "ma60": ma60,
        "atr14": None
    }

    chart_hint = {
        "lines": {
            "box_upper": upper,
            "box_lower": lower,
            "ma20": ma20
        }
    }

    return {
        "code": code,
        "name": name or code,
        "as_of": _format_month_label(box["end"]),
        "total_score": round(score, 3),
        "reasons": [label for _, label in reasons[:max_reasons]],
        "levels": levels,
        "distance_to_trigger": {
            "break_up_pct": break_up_pct,
            "break_down_pct": break_down_pct
        },
        "box_info": {
            "box_start": _format_month_label(box["start"]),
            "box_end": _format_month_label(box["end"]),
            "box_upper_body": upper,
            "box_lower_body": lower,
            "box_months": months,
            "wild_box_flag": box["wild"],
            "range_pct": box["range_pct"]
        },
        "box_start": _format_month_label(box["start"]),
        "box_end": _format_month_label(box["end"]),
        "box_upper_body": upper,
        "box_lower_body": lower,
        "box_months": months,
        "wild_box_flag": box["wild"],
        "chart_hint": chart_hint
    }, None

def calc_short_a_score(
    closes: list[float],
    opens: list[float],
    lows: list[float],
    ma5_series: list[float | None],
    ma20_series: list[float | None],
    atr14: float | None,
    volumes: list[float],
    avg_volume: float | None,
    down7: int | None,
    highs: list[float]
) -> tuple[int, list[str], list[str]]:
    """
    A型: 反転確定ショート（20割れ2本 + 決定打B/G/M 2/3成立）
    Returns (score, reasons, badges)
    """
    if len(closes) < 3 or len(ma20_series) < 3 or ma20_series[-1] is None:
        return 0, [], []

    close = closes[-1]
    ma20 = ma20_series[-1]
    prev_close = closes[-2]
    prev_ma20 = ma20_series[-2] if len(ma20_series) >= 2 and ma20_series[-2] is not None else None

    # A型の必須条件
    # 1. 終値 < MA20（実体割れ扱い）
    if close >= ma20:
        return 0, [], []

    # 2. 直近2本のうち 2本連続で終値 < MA20（=「20割れ2本」）
    if prev_ma20 is None or prev_close >= prev_ma20:
        return 0, [], []

    # 3. 下げの決定打（B/G/M の 2/3成立）
    decisive_count = 0
    reasons: list[str] = []
    badges: list[str] = ["20割れ2本"]

    # B（大陰線）：|C−O| ≥ 0.8×ATR(14) かつ 下ヒゲ ≤ 0.25×実体
    b_condition = False
    if atr14 is not None and len(opens) >= 1:
        body = abs(close - opens[-1])
        lower_shadow = min(opens[-1], close) - lows[-1]
        if body >= 0.8 * atr14 and close < opens[-1]:  # Bearish candle
            if body > 0 and lower_shadow <= 0.25 * body:
                b_condition = True
                decisive_count += 1
                reasons.append("大陰線")

    # G（ギャップダウン）：GD幅 ≥ 0.5×ATR(14)
    g_condition = False
    if atr14 is not None and len(closes) >= 2:
        gap_down = closes[-2] - opens[-1]  # Previous close - current open
        if gap_down >= 0.5 * atr14:
            g_condition = True
            decisive_count += 1
            reasons.append("ギャップダウン")

    # M：終値 < MA5
    m_condition = False
    if ma5_series and len(ma5_series) >= 1 and ma5_series[-1] is not None:
        if close < ma5_series[-1]:
            m_condition = True
            decisive_count += 1
            reasons.append("MA5下")

    # B/G/Mの2/3成立が必須
    if decisive_count < 2:
        return 0, [], []

    badges.append("B/G/M")

    # ベーススコア: 70点
    score = 70

    # 加点
    if b_condition:
        score += 25  # B成立 +25

    if g_condition:
        score += 20  # G成立 +20
        if b_condition:
            score += 10  # B+Gならさらに +10

    if m_condition:
        score += 10  # M成立 +10

    # 出来高≥20日平均 +10
    if avg_volume is not None and len(volumes) >= 1 and avg_volume > 0:
        if volumes[-1] >= avg_volume:
            score += 10
            reasons.append("出来高増")

    # 直近10日安値を終値で更新 +10
    if len(lows) >= 10:
        recent_low = min(lows[-10:-1]) if len(lows) > 1 else lows[-1]
        if close < recent_low:
            score += 10
            reasons.append("安値更新")

    # 7下本数が1〜3本目 +5（下げ初動を優先）
    if down7 is not None and 1 <= down7 <= 3:
        score += 5
        reasons.append(f"下げ初動（{down7}本目）")

    # 減点
    # 終値がMA20から乖離（終値 < MA20 − 1.0×ATR） -15
    if atr14 is not None and close < ma20 - 1.0 * atr14:
        score -= 15
        reasons.append("MA20乖離大")

    badges.insert(0, "反転確定")
    return max(0, score), reasons, badges

def calc_short_b_score(
    closes: list[float],
    opens: list[float],
    lows: list[float],
    ma5_series: list[float | None],
    ma20_series: list[float | None],
    ma60_series: list[float | None],
    slope20: float | None,
    slope60: float | None,
    atr14: float | None,
    volumes: list[float],
    avg_volume: float | None,
    down20: int | None,
    ma7_series: list[float | None]
) -> tuple[int, list[str], list[str]]:
    """
    B型: 下落トレンドの戻り売り（MA60下向き + 戻り失速）
    Returns (score, reasons, badges)
    """
    if len(closes) < 5 or len(ma60_series) < 5 or ma60_series[-1] is None:
        return 0, [], []

    close = closes[-1]
    ma20 = ma20_series[-1] if ma20_series and ma20_series[-1] is not None else None
    ma60 = ma60_series[-1]

    # B型の必須条件
    # 1. MA60傾き < 0（下向き）
    if slope60 is None or slope60 >= 0:
        return 0, [], []

    # 2. 終値 < MA60
    if close >= ma60:
        return 0, [], []

    # 3. 終値 < MA20
    if ma20 is not None and close >= ma20:
        return 0, [], []

    # 4.「戻り失速」判定
    pullback_stall = False
    reasons: list[str] = []

    # 直近5本以内に終値がMA7〜MA20帯に接近→その後2本以内で終値<MA5
    ma7 = ma7_series[-1] if ma7_series and len(ma7_series) >= 1 and ma7_series[-1] is not None else None
    ma5 = ma5_series[-1] if ma5_series and len(ma5_series) >= 1 and ma5_series[-1] is not None else None

    if ma7 is not None and ma20 is not None and ma5 is not None:
        # Check if price approached MA7-MA20 band in last 5 bars
        for i in range(-5, 0):
            if abs(i) > len(closes) or abs(i) > len(ma7_series) or abs(i) > len(ma20_series):
                continue
            past_close = closes[i]
            past_ma7 = ma7_series[i] if ma7_series[i] is not None else None
            past_ma20 = ma20_series[i] if ma20_series[i] is not None else None
            if past_ma7 is not None and past_ma20 is not None:
                band_low = min(past_ma7, past_ma20)
                band_high = max(past_ma7, past_ma20)
                if band_low <= past_close <= band_high:
                    # Check if current close < MA5
                    if close < ma5:
                        pullback_stall = True
                        reasons.append("戻り失速")
                        break

    # Alternative: 陰線実体 + 翌日安値更新
    if not pullback_stall and len(closes) >= 2 and len(opens) >= 2:
        prev_bearish = closes[-2] < opens[-2]  # Previous bar was bearish
        low_break = lows[-1] < lows[-2] if len(lows) >= 2 else False
        if prev_bearish and low_break:
            pullback_stall = True
            reasons.append("陰線後安値更新")

    if not pullback_stall:
        return 0, [], []

    badges: list[str] = ["戻り売り"]

    # ベーススコア: 60点
    score = 60

    # 加点
    # MA20傾き < 0 +15
    if slope20 is not None and slope20 < 0:
        score += 15
        reasons.append("MA20下向き")

    # 20下本数が10本以上 +10
    if down20 is not None and down20 >= 10:
        score += 10
        reasons.append(f"下落明確（{down20}本）")

    # 前安値ラインを実体で割る（終値で前安値割れ） +20
    if len(lows) >= 11:
        prev_low = min(lows[-11:-1]) if len(lows) > 1 else lows[-1]
        if close < prev_low:
            score += 20
            reasons.append("前安値割れ")

    # 出来高≥20日平均 +10
    if avg_volume is not None and len(volumes) >= 1 and avg_volume > 0:
        if volumes[-1] >= avg_volume:
            score += 10
            reasons.append("出来高増")

    # 7MA上に戻しても1〜2本で失速（戻り弱） +10
    if ma7 is not None and len(closes) >= 3:
        was_above_ma7 = False
        for i in range(-3, -1):
            if abs(i) <= len(closes) and abs(i) <= len(ma7_series):
                past_close = closes[i]
                past_ma7 = ma7_series[i] if ma7_series[i] is not None else None
                if past_ma7 is not None and past_close > past_ma7:
                    was_above_ma7 = True
                    break
        if was_above_ma7 and close < ma7:
            score += 10
            reasons.append("戻り弱")

    # 減点
    # 末期（終値 < MA20 − 1.2×ATR） -30 (Z2は既にチェック済みだが、ここでもペナルティ)
    if ma20 is not None and atr14 is not None and close < ma20 - 1.2 * atr14:
        score -= 30
        reasons.append("末期警戒")

    return max(0, score), reasons, badges

def calc_regression_slope(values: list[float | None], window: int = 5) -> float | None:
    """Calculate regression slope over the last `window` values (simple difference average)."""
    if len(values) < window:
        return None
    recent = values[-window:]
    valid = [v for v in recent if v is not None]
    if len(valid) < 2:
        return None
    # Simple: average of consecutive differences
    diffs = [valid[i + 1] - valid[i] for i in range(len(valid) - 1)]
    return sum(diffs) / len(diffs) if diffs else None

def calc_range_bounds_with_mid(
    highs: list[float], lows: list[float], lookback: int
) -> tuple[float | None, float | None, float | None]:
    """Calculate (high, low, midpoint) for the range over `lookback` periods."""
    if not highs or not lows:
        return None, None, None
    window_highs = highs[-lookback:] if len(highs) >= lookback else highs
    window_lows = lows[-lookback:] if len(lows) >= lookback else lows
    range_high = max(window_highs)
    range_low = min(window_lows)
    mid = (range_high + range_low) / 2
    return range_high, range_low, mid

def check_short_prohibition_zones(
    close: float,
    ma20: float | None,
    ma60: float | None,
    slope20: float | None,
    slope60: float | None,
    atr14: float | None,
    range_mid: float | None,
    range_high: float | None,
    range_low: float | None
) -> tuple[str | None, int]:
    """
    Check prohibition zones for short selling.
    Returns (zone_name, penalty_score):
    - Z1: 上昇優位 -> ShortScore = 0 (force)
    - Z2: 末期下げ -> ShortScore = 0 (force)
    - Z3: レンジ中央 -> -30 penalty
    - None: No prohibition
    """
    if ma20 is None or ma60 is None:
        return None, 0

    # Z1: 上昇優位（ネットショート事故ゾーン）
    # 終値 > MA20 かつ MA20傾き > 0（上向き）
    # かつ（終値 > MA60 または MA60傾き > 0）
    if close > ma20 and (slope20 is not None and slope20 > 0):
        if close > ma60 or (slope60 is not None and slope60 > 0):
            return "Z1", -9999  # Force to 0

    # Z2: 末期下げ（利確・触らないゾーン）
    # 終値 < MA20 － 1.2×ATR(14)
    if atr14 is not None and close < ma20 - 1.2 * atr14:
        return "Z2", -9999  # Force to 0

    # Z3: レンジ中央（期待値薄）
    # 直近60日の高安の中点±15%に終値が位置
    if range_mid is not None and range_high is not None and range_low is not None:
        range_band = (range_high - range_low) * 0.15
        if range_mid - range_band <= close <= range_mid + range_band:
            return "Z3", -30  # Penalty

    return None, 0
