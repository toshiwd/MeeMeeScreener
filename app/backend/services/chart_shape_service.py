from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence


CHART_SHAPE_PATTERNS: dict[str, dict[str, str]] = {
    "gap_up_stall_fade": {
        "family": "gap_failure",
        "bias": "caution",
        "actionability": "avoid_chase",
        "description": "large gap up, then no extension and fade from the gap close",
    },
    "gap_up_upper_wick_failure": {
        "family": "gap_failure",
        "bias": "caution",
        "actionability": "avoid_chase",
        "description": "large gap up with upper-wick rejection near the event bar",
    },
    "breakout_pullback_fail": {
        "family": "breakout_failure",
        "bias": "caution",
        "actionability": "avoid_or_wait",
        "description": "recent breakout attempt failed back below the prior range",
    },
    "gap_up_hold_high": {
        "family": "gap_hold",
        "bias": "bullish_watch",
        "actionability": "watch_for_continuation",
        "description": "large gap up and price still holds near the gap close",
    },
    "gap_up_continuation": {
        "family": "momentum",
        "bias": "bullish_watch",
        "actionability": "watch_for_pullback_or_entry",
        "description": "large gap up followed by further high extension",
    },
    "tight_high_flag": {
        "family": "constructive_pause",
        "bias": "bullish_watch",
        "actionability": "watch_for_break",
        "description": "prior advance, then tight consolidation near the high",
    },
    "breakout_hold": {
        "family": "breakout_hold",
        "bias": "bullish_watch",
        "actionability": "watch_for_continuation",
        "description": "breakout above the prior range and still holding above it",
    },
    "sideways_range": {
        "family": "range",
        "bias": "neutral",
        "actionability": "wait",
        "description": "narrow range without directional evidence",
    },
    "wide_choppy_range": {
        "family": "range",
        "bias": "neutral",
        "actionability": "wait",
        "description": "wide range without directional evidence",
    },
    "steady_uptrend": {
        "family": "trend",
        "bias": "bullish_watch",
        "actionability": "watch",
        "description": "window closes higher without a dominant gap event",
    },
    "steady_downtrend": {
        "family": "trend",
        "bias": "caution",
        "actionability": "avoid_long",
        "description": "window closes lower without a dominant gap event",
    },
    "range_or_unclear": {
        "family": "range",
        "bias": "neutral",
        "actionability": "wait",
        "description": "no strong deterministic pattern matched",
    },
    "insufficient_data": {
        "family": "unknown",
        "bias": "unknown",
        "actionability": "wait",
        "description": "not enough valid bars to classify",
    },
}


@dataclass(frozen=True)
class Bar:
    date: int
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None


def _to_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return number


def _normalize_bar(row: Sequence[Any]) -> Bar | None:
    if len(row) < 5:
        return None
    date = row[0]
    try:
        date_key = int(date)
    except (TypeError, ValueError):
        return None
    open_ = _to_float(row[1])
    high = _to_float(row[2])
    low = _to_float(row[3])
    close = _to_float(row[4])
    volume = _to_float(row[5]) if len(row) >= 6 else None
    if open_ is None or high is None or low is None or close is None:
        return None
    if open_ <= 0 or high <= 0 or low <= 0 or close <= 0:
        return None
    return Bar(date=date_key, open=open_, high=high, low=low, close=close, volume=volume)


def _pct(value: float | None) -> float | None:
    return None if value is None else round(value * 100.0, 4)


def _pattern_meta(label: str) -> dict[str, str]:
    return CHART_SHAPE_PATTERNS.get(label, CHART_SHAPE_PATTERNS["range_or_unclear"])


def get_chart_shape_pattern_catalog() -> dict[str, dict[str, str]]:
    return {label: dict(payload) for label, payload in CHART_SHAPE_PATTERNS.items()}


def classify_daily_chart_shapes_by_window(
    rows: Sequence[Sequence[Any]],
    *,
    requested_windows: Sequence[int] = (10, 20, 60),
) -> dict[str, Any]:
    windows = []
    seen: set[int] = set()
    for raw_window in requested_windows:
        try:
            window = max(3, int(raw_window))
        except (TypeError, ValueError):
            continue
        if window in seen:
            continue
        seen.add(window)
        windows.append(window)
    if not windows:
        windows = [10, 20, 60]

    by_window = {}
    for window in windows:
        by_window[str(window)] = classify_daily_chart_shape(
            rows,
            requested_window=window,
            include_gap_events=window <= 10,
        )
    event_shape = by_window.get("10") or by_window[str(windows[0])]
    context_shape = by_window.get("20") or by_window[str(windows[min(1, len(windows) - 1)])]
    trend_shape = by_window.get("60") or by_window[str(windows[-1])]

    conflict_flags: list[str] = []
    if event_shape.get("bias") == "caution" and context_shape.get("bias") == "bullish_watch":
        conflict_flags.append("short_term_caution_but_context_bullish")
    if event_shape.get("bias") == "bullish_watch" and trend_shape.get("bias") == "caution":
        conflict_flags.append("short_term_bullish_but_medium_caution")
    if context_shape.get("shape_family") == "range" and event_shape.get("shape_family") in {"gap_failure", "breakout_failure"}:
        conflict_flags.append("event_failure_inside_range_context")

    return {
        "confirmed": any(bool(item.get("confirmed")) for item in by_window.values()),
        "windows": windows,
        "by_window": by_window,
        "event_shape": event_shape,
        "context_shape": context_shape,
        "trend_shape": trend_shape,
        "conflict_flags": conflict_flags,
    }


def _upper_wick_ratio(bar: Bar) -> float:
    span = max(bar.high - bar.low, 0.0)
    if span <= 0:
        return 0.0
    return (bar.high - max(bar.open, bar.close)) / span


def _body_direction(bar: Bar) -> int:
    if bar.close > bar.open:
        return 1
    if bar.close < bar.open:
        return -1
    return 0


def classify_daily_chart_shape(
    rows: Sequence[Sequence[Any]],
    *,
    requested_window: int = 10,
    gap_threshold_pct: float = 8.0,
    stall_band_pct: float = 6.0,
    fade_threshold_pct: float = -2.0,
    include_gap_events: bool = True,
) -> dict[str, Any]:
    bars = [bar for row in rows if (bar := _normalize_bar(row)) is not None]
    bars.sort(key=lambda bar: bar.date)
    window = max(3, int(requested_window or 10))
    if len(bars) < 3:
        meta = _pattern_meta("insufficient_data")
        return {
            "confirmed": False,
            "shape_label": "insufficient_data",
            "shape_family": meta["family"],
            "bias": meta["bias"],
            "actionability": meta["actionability"],
            "description": meta["description"],
            "window": window,
            "bar_count": len(bars),
            "reasons": ["need_at_least_3_valid_bars"],
            "metrics": {},
        }

    sliced = bars[-window:]
    prev_by_date = {bars[idx].date: bars[idx - 1] for idx in range(1, len(bars))}
    gap_candidates: list[dict[str, Any]] = []
    for bar in sliced:
        prev = prev_by_date.get(bar.date)
        if prev is None:
            continue
        gap = (bar.open / prev.close) - 1.0
        close_ret = (bar.close / prev.close) - 1.0
        if include_gap_events and gap * 100.0 >= gap_threshold_pct:
            gap_candidates.append({"bar": bar, "prev": prev, "gap": gap, "close_ret": close_ret})

    first = sliced[0]
    last = sliced[-1]
    start_to_last = (last.close / first.close) - 1.0
    close_values = [bar.close for bar in sliced]
    high_values = [bar.high for bar in sliced]
    low_values = [bar.low for bar in sliced]
    close_range = (max(close_values) / min(close_values)) - 1.0 if min(close_values) > 0 else None
    full_range = (max(high_values) / min(low_values)) - 1.0 if min(low_values) > 0 else None
    last_3 = sliced[-3:] if len(sliced) >= 3 else sliced
    last_3_ret = (last_3[-1].close / last_3[0].close) - 1.0 if len(last_3) >= 2 else None
    latest_intraday = (last.close / last.open) - 1.0
    latest_upper_wick_ratio = _upper_wick_ratio(last)
    prior_window = sliced[:-1]
    prior_high = max((bar.high for bar in prior_window), default=None)
    prior_low = min((bar.low for bar in prior_window), default=None)
    prior_close_high = max((bar.close for bar in prior_window), default=None)
    near_window_high = (last.close / max(high_values)) - 1.0 if max(high_values) > 0 else None

    label = "range_or_unclear"
    confidence = 0.45
    reasons: list[str] = []

    if gap_candidates:
        gap_info = max(gap_candidates, key=lambda item: item["gap"])
        gap_bar = gap_info["bar"]
        after_gap = [bar for bar in sliced if bar.date >= gap_bar.date]
        after_closes = [bar.close for bar in after_gap]
        post_gap_return = (last.close / gap_bar.close) - 1.0
        post_gap_high_runup = (max(bar.high for bar in after_gap) / gap_bar.close) - 1.0
        post_gap_close_range = (
            (max(after_closes) / min(after_closes)) - 1.0 if len(after_closes) >= 2 and min(after_closes) > 0 else None
        )
        gap_event_wick = _upper_wick_ratio(gap_bar)
        reasons.append("large_gap_up")
        if gap_event_wick >= 0.45 and _body_direction(gap_bar) <= 0:
            reasons.append("gap_event_upper_wick_rejection")
            label = "gap_up_upper_wick_failure"
            confidence = 0.8
        elif post_gap_return * 100.0 <= fade_threshold_pct:
            reasons.append("failed_to_extend_after_gap")
            label = "gap_up_stall_fade"
            confidence = 0.82
        elif post_gap_close_range is not None and post_gap_close_range * 100.0 <= stall_band_pct:
            reasons.append("post_gap_range_bound")
            label = "gap_up_hold_high" if post_gap_return * 100.0 >= -1.0 else "range_or_unclear"
            confidence = 0.72
        elif post_gap_high_runup * 100.0 >= stall_band_pct:
            reasons.append("post_gap_extension")
            label = "gap_up_continuation"
            confidence = 0.68
        metrics = {
            "gap_date": gap_bar.date,
            "gap_pct": _pct(gap_info["gap"]),
            "gap_close_return_pct": _pct(gap_info["close_ret"]),
            "post_gap_return_pct": _pct(post_gap_return),
            "post_gap_high_runup_pct": _pct(post_gap_high_runup),
            "post_gap_close_range_pct": _pct(post_gap_close_range),
            "gap_event_upper_wick_ratio": round(gap_event_wick, 4),
        }
    else:
        metrics = {}
        breakout_level = prior_high if prior_high is not None else prior_close_high
        if (
            breakout_level is not None
            and last.high > breakout_level * 1.01
            and last.close >= breakout_level
            and latest_intraday * 100.0 >= -1.5
        ):
            label = "breakout_hold"
            confidence = 0.7
            reasons.append("close_holds_above_prior_range")
        elif (
            breakout_level is not None
            and max(high_values) > breakout_level * 1.01
            and last.close < breakout_level
        ):
            label = "breakout_pullback_fail"
            confidence = 0.76
            reasons.append("breakout_failed_below_prior_range")
        elif (
            start_to_last * 100.0 >= stall_band_pct
            and close_range is not None
            and close_range * 100.0 <= stall_band_pct * 1.25
            and near_window_high is not None
            and near_window_high * 100.0 >= -3.0
        ):
            label = "tight_high_flag"
            confidence = 0.66
            reasons.append("tight_consolidation_near_high")
        elif start_to_last * 100.0 >= stall_band_pct:
            label = "steady_uptrend"
            confidence = 0.62
            reasons.append("window_close_gain")
        elif start_to_last * 100.0 <= -stall_band_pct:
            label = "steady_downtrend"
            confidence = 0.62
            reasons.append("window_close_loss")
        elif close_range is not None and close_range * 100.0 <= stall_band_pct:
            label = "sideways_range"
            confidence = 0.64
            reasons.append("narrow_close_range")
        elif full_range is not None and full_range * 100.0 >= stall_band_pct * 2:
            label = "wide_choppy_range"
            confidence = 0.58
            reasons.append("wide_range_without_direction")

    metrics.update(
        {
            "start_date": first.date,
            "end_date": last.date,
            "window_return_pct": _pct(start_to_last),
            "close_range_pct": _pct(close_range),
            "full_range_pct": _pct(full_range),
            "last_3_return_pct": _pct(last_3_ret),
            "latest_intraday_pct": _pct(latest_intraday),
            "latest_upper_wick_ratio": round(latest_upper_wick_ratio, 4),
            "prior_high": round(prior_high, 4) if prior_high is not None else None,
            "prior_low": round(prior_low, 4) if prior_low is not None else None,
        }
    )
    if last_3_ret is not None and last_3_ret * 100.0 <= fade_threshold_pct:
        reasons.append("recent_3_bar_fade")
    if latest_intraday * 100.0 <= fade_threshold_pct:
        reasons.append("latest_session_weak")
    if latest_upper_wick_ratio >= 0.45:
        reasons.append("latest_upper_wick")

    meta = _pattern_meta(label)

    return {
        "confirmed": True,
        "shape_label": label,
        "shape_family": meta["family"],
        "bias": meta["bias"],
        "actionability": meta["actionability"],
        "description": meta["description"],
        "confidence": confidence,
        "window": window,
        "bar_count": len(sliced),
        "reasons": reasons,
        "metrics": metrics,
    }
