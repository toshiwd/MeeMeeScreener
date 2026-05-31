from __future__ import annotations

import argparse
import json
import math
import sys
from statistics import mean
from typing import Any


def _num(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _bar_to_ohlc(row: Any) -> dict[str, float] | None:
    if isinstance(row, dict):
        values = {key: _num(row.get(key)) for key in ("o", "h", "l", "c")}
    elif isinstance(row, (list, tuple)) and len(row) >= 5:
        values = {"o": _num(row[1]), "h": _num(row[2]), "l": _num(row[3]), "c": _num(row[4])}
    else:
        return None
    if any(values[key] is None for key in ("o", "h", "l", "c")):
        return None
    return values  # type: ignore[return-value]


def _ma(values: list[float], window: int) -> float | None:
    if len(values) < window:
        return None
    return mean(values[-window:])


def _rolling_ma(values: list[float], window: int) -> list[float | None]:
    out: list[float | None] = []
    for index in range(len(values)):
        if index + 1 < window:
            out.append(None)
        else:
            out.append(mean(values[index + 1 - window : index + 1]))
    return out


def _position(close: float, low: float, high: float) -> float | None:
    if high <= low:
        return None
    return (close - low) / (high - low)


def _slope(values: list[float], window: int) -> float | None:
    if len(values) < window:
        return None
    start = values[-window]
    if start == 0:
        return None
    return (values[-1] / start) - 1.0


def _touch_count(rows: list[dict[str, float]], ma_values: list[float | None], lookback: int, near_pct: float) -> int:
    count = 0
    start = max(0, len(rows) - lookback)
    for index in range(start, len(rows)):
        ma = ma_values[index]
        if ma is None or ma == 0:
            continue
        low = float(rows[index]["l"])
        high = float(rows[index]["h"])
        close = float(rows[index]["c"])
        if low <= ma <= high or abs((close / ma) - 1.0) <= near_pct:
            count += 1
    return count


def _consecutive_above(closes: list[float], ma_values: list[float | None]) -> int:
    count = 0
    for close, ma in zip(reversed(closes), reversed(ma_values)):
        if ma is None or close <= ma:
            break
        count += 1
    return count


def _round_level_distance_pct(close: float) -> float | None:
    if close <= 0:
        return None
    magnitude = 10 ** max(0, int(math.log10(close)) - 1)
    level = round(close / magnitude) * magnitude
    if level <= 0:
        return None
    return abs((close / level) - 1.0)


def _scene_profile(shape_intent: str, entry_timing: str) -> dict[str, str]:
    if shape_intent.startswith("b_phase_"):
        if entry_timing == "b_phase_buy_breakout":
            return {
                "market_scene": "sideways_b_phase",
                "trade_side": "long",
                "action_bias": "buy_breakout",
                "risk_note": "range_breakout_can_fail_without_followthrough",
            }
        if entry_timing in {"b_phase_short_breakdown", "b_phase_short_rejection"}:
            return {
                "market_scene": "sideways_b_phase",
                "trade_side": "short",
                "action_bias": "sell_breakdown_or_rejection",
                "risk_note": "sideways_phase_requires_clear_level_break_or_ma_rejection",
            }
        return {
            "market_scene": "sideways_b_phase",
            "trade_side": "none",
            "action_bias": "wait_for_breakout_or_breakdown",
            "risk_note": "do_not_force_trade_inside_range",
        }

    if shape_intent.startswith("c_phase_"):
        if entry_timing in {"c_phase_hold_or_small_buy_on_5ma", "c_phase_buy_or_add_after_20ma_bounce"}:
            return {
                "market_scene": "uptrend_c_phase",
                "trade_side": "long",
                "action_bias": "hold_or_add_long",
                "risk_note": "reduce_if_5ma_breaks_and_exit_watch_if_20ma_breaks",
            }
        return {
            "market_scene": "uptrend_c_phase",
            "trade_side": "exit_or_watch_short",
            "action_bias": "reduce_long_or_wait",
            "risk_note": "uptrend_may_be_ending_after_ma_break",
        }

    if shape_intent.startswith("a_phase_"):
        if entry_timing == "a_phase_short_cover_or_recovery_watch":
            return {
                "market_scene": "downtrend_a_phase",
                "trade_side": "short_cover_watch",
                "action_bias": "avoid_new_short_or_reduce_short",
                "risk_note": "multi_day_20ma_hold_can_signal_recovery",
            }
        return {
            "market_scene": "downtrend_a_phase",
            "trade_side": "short",
            "action_bias": "sell_rebound_rejection_or_lower_low",
            "risk_note": "avoid_chasing_after_large_extension",
        }

    if shape_intent.startswith("crash_bottom_"):
        return {
            "market_scene": "crash_bottoming_phase",
            "trade_side": "long_probe_or_short_cover",
            "action_bias": "probe_bottom_only_after_ma_reclaim",
            "risk_note": "first_5ma_or_20ma_touch_is_warning_not_full_reversal",
        }

    if (
        shape_intent.startswith("crash_warning_")
        or shape_intent.startswith("crash_initial_")
        or shape_intent.startswith("mature_uptrend_")
        or shape_intent.startswith("crash_longterm_")
    ):
        if "trigger" in entry_timing or "short" in entry_timing:
            return {
                "market_scene": "crash_or_distribution_phase",
                "trade_side": "short",
                "action_bias": "sell_distribution_or_ma_break",
                "risk_note": "confirm_trigger_before_short_if_still_above_short_ma",
            }
        return {
            "market_scene": "crash_or_distribution_phase",
            "trade_side": "watch",
            "action_bias": "wait_for_short_trigger",
            "risk_note": "warning_shape_without_ma_break_is_not_entry_confirmation",
        }

    if shape_intent in {"failed_high_retest_7ma_break", "failed_high_retest_wait_for_7ma_break"}:
        return {
            "market_scene": "distribution_or_failed_retest",
            "trade_side": "short" if shape_intent.endswith("7ma_break") else "watch",
            "action_bias": "sell_failed_high_retest_after_7ma_break",
            "risk_note": "must_be_upper_zone_retest_not_late_lower_drift",
        }

    if shape_intent == "late_breakdown_chase":
        return {
            "market_scene": "extended_breakdown",
            "trade_side": "none",
            "action_bias": "avoid_chase",
            "risk_note": "already_far_from_range_high",
        }

    if shape_intent in {"range_lower_drift", "weak_below_ma"}:
        return {
            "market_scene": "weak_lower_range",
            "trade_side": "none",
            "action_bias": "wait",
            "risk_note": "weak_but_not_clean_short_trigger",
        }

    return {
        "market_scene": "neutral_or_unclassified",
        "trade_side": "none",
        "action_bias": "wait",
        "risk_note": "no_actionable_shape",
    }


def classify_shape_from_bars(bars: list[Any]) -> dict[str, Any]:
    rows = [_bar_to_ohlc(row) for row in bars]
    rows = [row for row in rows if row is not None]
    if len(rows) < 30:
        return {
            "confirmed": False,
            "reason": "insufficient_bars",
            "bar_count": len(rows),
            "shape_intent": "unknown",
            "is_try_fail_7ma_break": False,
            "is_mature_uptrend_crash_setup": False,
        }

    closes = [float(row["c"]) for row in rows]
    highs = [float(row["h"]) for row in rows]
    lows = [float(row["l"]) for row in rows]
    close = closes[-1]

    ma5 = _ma(closes, 5)
    ma7 = _ma(closes, 7)
    ma20 = _ma(closes, 20)
    ma60 = _ma(closes, 60)
    ma100 = _ma(closes, 100)
    ma300 = _ma(closes, 300)
    ma20_values = _rolling_ma(closes, 20)
    ma60_values = _rolling_ma(closes, 60)
    ma100_values = _rolling_ma(closes, 100)
    ma300_values = _rolling_ma(closes, 300)

    range_high = max(highs[-60:])
    range_low = min(lows[-60:])
    range20_high = max(highs[-20:]) if len(highs) >= 20 else max(highs)
    range20_low = min(lows[-20:]) if len(lows) >= 20 else min(lows)
    prior_range20_high = max(highs[-21:-1]) if len(highs) >= 21 else range20_high
    prior_range20_low = min(lows[-21:-1]) if len(lows) >= 21 else range20_low
    range120_high = max(highs[-120:]) if len(highs) >= 120 else max(highs)
    range120_low = min(lows[-120:]) if len(lows) >= 120 else min(lows)
    range_position = _position(close, range_low, range_high)
    range20_pct = (range20_high / range20_low - 1.0) if range20_low else None
    prior_20_return = (closes[-21] / closes[-41] - 1.0) if len(closes) >= 41 and closes[-41] else None
    latest_20_return = (close / closes[-21] - 1.0) if len(closes) >= 21 and closes[-21] else None
    sideways_b_phase = bool(range20_pct is not None and range20_pct <= 0.08 and abs(latest_20_return or 0.0) <= 0.05)

    prior_window = rows[-60:-5] if len(rows) >= 65 else rows[:-5]
    prior_high = max(float(row["h"]) for row in prior_window) if prior_window else max(highs[:-1])
    recent_high = max(highs[-5:])
    recent_high_offset_pct = (recent_high / prior_high) - 1.0 if prior_high else None
    close_drop_from_recent_high_pct = (close / recent_high) - 1.0 if recent_high else None
    drawdown_from_range_high_pct = (close / range_high) - 1.0 if range_high else None
    ma20_slope_10 = (ma20_values[-1] / ma20_values[-11] - 1.0) if len(ma20_values) >= 11 and ma20_values[-1] and ma20_values[-11] else None
    ma60_slope_10 = (ma60_values[-1] / ma60_values[-11] - 1.0) if len(ma60_values) >= 11 and ma60_values[-1] and ma60_values[-11] else None
    latest_open = float(rows[-1]["o"])
    latest_high = float(rows[-1]["h"])
    latest_low = float(rows[-1]["l"])
    latest_range = latest_high - latest_low
    latest_body = abs(close - latest_open)
    latest_upper_wick = latest_high - max(close, latest_open)
    upper_wick_ratio = (latest_upper_wick / latest_range) if latest_range > 0 else None
    upper_wick_body_ratio = (latest_upper_wick / latest_body) if latest_body > 0 else None
    round_level_distance_pct = _round_level_distance_pct(close)

    below5 = bool(ma5 is not None and close < ma5)
    below7 = bool(ma7 is not None and close < ma7)
    below20 = bool(ma20 is not None and close < ma20)
    below60 = bool(ma60 is not None and close < ma60)
    prior_close = closes[-2] if len(closes) >= 2 else None
    prior_ma5 = mean(closes[-6:-1]) if len(closes) >= 6 else None
    prior_ma7 = mean(closes[-8:-1]) if len(closes) >= 8 else None
    prior_ma20 = mean(closes[-21:-1]) if len(closes) >= 21 else None
    prior_ma300 = mean(closes[-301:-1]) if len(closes) >= 301 else None
    crossed_below5 = bool(prior_close is not None and prior_ma5 is not None and prior_close >= prior_ma5 and below5)
    crossed_below7 = bool(prior_close is not None and prior_ma7 is not None and prior_close >= prior_ma7 and below7)
    crossed_below20 = bool(prior_close is not None and prior_ma20 is not None and prior_close >= prior_ma20 and below20)
    below300 = bool(ma300 is not None and close < ma300)
    crossed_below300 = bool(prior_close is not None and prior_ma300 is not None and prior_close >= prior_ma300 and below300)
    crossed_above300_recent = False
    recent_below300_count = 0
    recent_above300_count = 0
    if len(closes) >= 305:
        for idx in range(len(closes) - 20, len(closes)):
            ma = ma300_values[idx]
            if ma is None:
                continue
            if closes[idx] < ma:
                recent_below300_count += 1
            if closes[idx] > ma:
                recent_above300_count += 1
        for idx in range(max(1, len(closes) - 20), len(closes)):
            prev_ma = ma300_values[idx - 1]
            cur_ma = ma300_values[idx]
            if prev_ma is not None and cur_ma is not None and closes[idx - 1] <= prev_ma and closes[idx] > cur_ma:
                crossed_above300_recent = True
                break
    above5 = bool(ma5 is not None and close > ma5)
    above20 = bool(ma20 is not None and close > ma20)
    touched5 = bool(ma5 is not None and latest_low <= ma5 <= latest_high)
    touched20 = bool(ma20 is not None and latest_low <= ma20 <= latest_high)
    crossed_above5 = bool(prior_close is not None and prior_ma5 is not None and prior_close <= prior_ma5 and above5)
    crossed_above20 = bool(prior_close is not None and prior_ma20 is not None and prior_close <= prior_ma20 and above20)
    consecutive_above5 = _consecutive_above(closes, _rolling_ma(closes, 5))
    consecutive_above20 = _consecutive_above(closes, ma20_values)
    tried_prior_high = bool(recent_high_offset_pct is not None and -0.04 <= recent_high_offset_pct <= 0.035)
    near_upper_half = bool(range_position is not None and range_position >= 0.52)
    near_lower_half = bool(range_position is not None and range_position < 0.52)
    failed_after_try = bool(close_drop_from_recent_high_pct is not None and close_drop_from_recent_high_pct <= -0.01)
    too_late_breakdown = bool(drawdown_from_range_high_pct is not None and drawdown_from_range_high_pct <= -0.08)
    near_round_level = bool(round_level_distance_pct is not None and round_level_distance_pct <= 0.012)
    long_upper_wick = bool(
        upper_wick_ratio is not None
        and upper_wick_body_ratio is not None
        and upper_wick_ratio >= 0.16
        and upper_wick_body_ratio >= 1.2
    )
    recent_peak_index = max(range(max(0, len(rows) - 60), len(rows)), key=lambda idx: highs[idx])
    bars_since_recent_peak = len(rows) - 1 - recent_peak_index
    pre_peak_start_index = max(0, recent_peak_index - 40)
    pre_peak_low = min(lows[pre_peak_start_index : recent_peak_index + 1]) if recent_peak_index >= pre_peak_start_index else range_low
    pre_peak_rise_pct = (highs[recent_peak_index] / pre_peak_low) - 1.0 if pre_peak_low else None
    post_peak_drop_pct = (close / highs[recent_peak_index]) - 1.0 if highs[recent_peak_index] else None
    peak_to_now_daily_drop_pct = (post_peak_drop_pct / bars_since_recent_peak) if bars_since_recent_peak > 0 and post_peak_drop_pct is not None else None
    mountain_top_reversal = bool(
        pre_peak_rise_pct is not None
        and post_peak_drop_pct is not None
        and peak_to_now_daily_drop_pct is not None
        and pre_peak_rise_pct >= 0.12
        and post_peak_drop_pct <= -0.06
        and peak_to_now_daily_drop_pct <= -0.006
        and bars_since_recent_peak <= 20
    )
    recent_downtrend_drop_pct = (close / max(highs[-60:]) - 1.0) if highs[-60:] else None
    bottoming_context = bool(recent_downtrend_drop_pct is not None and recent_downtrend_drop_pct <= -0.08)
    sideways_after_drop = bool(
        len(closes) >= 8
        and (max(highs[-8:]) / min(lows[-8:]) - 1.0 if min(lows[-8:]) else 99.0) <= 0.08
    )
    ma_rollover = bool(
        (ma20_slope_10 is not None and ma20_slope_10 <= -0.005)
        or (ma60_slope_10 is not None and ma60_slope_10 <= -0.002)
    )
    bullish_ma_stack_before_300_break = bool(
        ma5 is not None
        and ma20 is not None
        and ma60 is not None
        and ma100 is not None
        and ma300 is not None
        and ma20 > ma60 > ma100
        and ma300 > 0
    )
    ma300_resistance_rejection = bool(
        ma300 is not None
        and latest_high >= ma300 * 0.985
        and close < ma300
        and below300
        and recent_below300_count >= 3
    )
    ma300_false_reclaim = bool(
        ma300 is not None
        and crossed_above300_recent
        and crossed_below300
        and recent_above300_count >= 1
    )
    c_phase_context = bool(
        ma5 is not None
        and ma20 is not None
        and ma60 is not None
        and ma5 > ma20 > ma60
        and (latest_20_return or 0.0) >= 0.04
    )
    prior_ma5_for_context = mean(closes[-6:-1]) if len(closes) >= 6 else None
    prior_ma20_for_context = mean(closes[-21:-1]) if len(closes) >= 21 else None
    prior_ma60_for_context = mean(closes[-61:-1]) if len(closes) >= 61 else None
    c_phase_prior_context = bool(
        prior_ma5_for_context is not None
        and prior_ma20_for_context is not None
        and prior_ma60_for_context is not None
        and prior_ma5_for_context > prior_ma20_for_context > prior_ma60_for_context
    )
    c_phase_ma5_ride = bool(c_phase_context and close >= ma5 and consecutive_above5 >= 3)
    c_phase_ma20_touch_bounce = bool(
        (c_phase_context or c_phase_prior_context)
        and touched20
        and close >= ma20
        and not crossed_below20
    )
    c_phase_warning_5ma_break = bool((c_phase_context or c_phase_prior_context) and crossed_below5 and not below20)
    c_phase_end_20ma_break = bool((c_phase_context or c_phase_prior_context) and (crossed_below20 or (below20 and close < (prior_ma20_for_context or close))))
    b_phase_breakout_up = bool(sideways_b_phase and close > prior_range20_high * 1.005 and above5)
    b_phase_breakdown_down = bool(sideways_b_phase and close < prior_range20_low * 0.995 and below5)
    b_phase_after_drop = bool(prior_20_return is not None and prior_20_return <= -0.08 and sideways_b_phase)
    b_phase_after_rise = bool(prior_20_return is not None and prior_20_return >= 0.08 and sideways_b_phase)
    long_ma_resistance = bool(
        (ma60 is not None and latest_high >= ma60 * 0.985 and close < ma60)
        or (ma100 is not None and latest_high >= ma100 * 0.985 and close < ma100)
        or (ma300 is not None and latest_high >= ma300 * 0.985 and close < ma300)
    )
    downtrend_context = bool(
        ma20 is not None
        and ma60 is not None
        and ma20 < ma60
        and (ma100 is None or ma60 <= ma100 * 1.02)
        and (ma20_slope_10 is None or ma20_slope_10 <= 0.005)
    )
    a_phase_20ma_rejection = bool(downtrend_context and touched20 and close < ma20)
    a_phase_60ma_rejection = bool(downtrend_context and ma60 is not None and latest_high >= ma60 * 0.985 and close < ma60)
    a_phase_100ma_rejection = bool(downtrend_context and ma100 is not None and latest_high >= ma100 * 0.985 and close < ma100)
    a_phase_lower_low_break = bool(downtrend_context and close < prior_range20_low * 0.995 and below5)
    a_phase_ma5_inside_continuation = bool(
        downtrend_context
        and ma5 is not None
        and prior_close is not None
        and below5
        and close < prior_close
        and latest_high <= ma5 * 1.01
    )
    a_phase_recovery_20ma_hold = bool(downtrend_context and consecutive_above20 >= 3)
    a_phase_bottom_range_breakdown = bool(b_phase_after_drop and close < prior_range20_low * 0.995 and below5)
    b_phase_bounce_blocked = bool((b_phase_after_drop or b_phase_after_rise) and long_ma_resistance and below5)

    six_month_uptrend_pct = _slope(closes, 120)
    ma20_touch_count_60 = _touch_count(rows, ma20_values, 60, 0.01)
    ma60_touch_count_60 = _touch_count(rows, ma60_values, 60, 0.012)
    prior_ma60_touch_count_60 = _touch_count(rows[:-20], ma60_values[:-20], 60, 0.012) if len(rows) > 80 else 0
    mature_uptrend_range_pct = (range120_high / range120_low) - 1.0 if range120_low else None
    mature_uptrend = bool(
        range120_high > range120_low
        and (
            (six_month_uptrend_pct is not None and six_month_uptrend_pct >= 0.15)
            or (mature_uptrend_range_pct is not None and mature_uptrend_range_pct >= 0.22)
        )
    )
    ma_touch_fatigue = bool(ma20_touch_count_60 >= 5 or ma60_touch_count_60 >= 1)
    second_ma60_contact_or_break = bool(
        (prior_ma60_touch_count_60 >= 1 and (ma60_touch_count_60 >= 1 or below60)) or ma60_touch_count_60 >= 2
    )
    crash_setup = bool(
        mature_uptrend
        and ma_touch_fatigue
        and tried_prior_high
        and failed_after_try
        and (below5 or below7 or below20 or below60)
    )
    crash_warning_setup = bool(
        mature_uptrend
        and near_round_level
        and (
            long_upper_wick
            or (tried_prior_high and close_drop_from_recent_high_pct is not None and close_drop_from_recent_high_pct <= -0.008)
        )
    )

    if ma300_false_reclaim:
        shape_intent = "crash_longterm_300ma_false_reclaim_second_break"
    elif ma300_resistance_rejection:
        shape_intent = "crash_longterm_300ma_retest_rejection"
    elif crossed_below300 and (mature_uptrend or bullish_ma_stack_before_300_break):
        shape_intent = "crash_longterm_first_300ma_break"
    elif bottoming_context and consecutive_above20 >= 2 and close > ma20:
        shape_intent = "crash_bottom_confirmed_above_20ma"
    elif bottoming_context and crossed_above20:
        shape_intent = "crash_bottom_warning_first_20ma_break"
    elif bottoming_context and consecutive_above5 >= 2 and sideways_after_drop:
        shape_intent = "crash_bottom_warning_multi_day_above_5ma"
    elif bottoming_context and (crossed_above5 or touched5):
        shape_intent = "crash_bottom_warning_first_5ma_touch"
    elif tried_prior_high and near_upper_half and failed_after_try and below7 and not too_late_breakdown:
        shape_intent = "failed_high_retest_7ma_break"
    elif mountain_top_reversal and (tried_prior_high or near_round_level or near_upper_half):
        shape_intent = "crash_initial_mountain_failed_high_retest"
    elif mountain_top_reversal and long_upper_wick:
        shape_intent = "crash_initial_mountain_upper_wick"
    elif crash_warning_setup and long_upper_wick and not too_late_breakdown:
        shape_intent = "crash_warning_round_level_upper_wick"
    elif (
        crash_warning_setup
        and tried_prior_high
        and close_drop_from_recent_high_pct is not None
        and close_drop_from_recent_high_pct <= -0.008
        and not too_late_breakdown
    ):
        shape_intent = "crash_warning_round_level_failed_high_retest"
    elif crash_setup and second_ma60_contact_or_break and below60:
        shape_intent = "mature_uptrend_crash_setup_second_60ma_break"
    elif crash_setup and (below20 or below7 or below5):
        shape_intent = "mature_uptrend_failed_high_retest_distribution"
    elif a_phase_100ma_rejection:
        shape_intent = "a_phase_downtrend_100ma_rejection"
    elif a_phase_60ma_rejection:
        shape_intent = "a_phase_downtrend_60ma_rejection"
    elif a_phase_20ma_rejection:
        shape_intent = "a_phase_downtrend_20ma_rejection"
    elif a_phase_bottom_range_breakdown:
        shape_intent = "a_phase_bottom_range_breakdown_turn_short"
    elif a_phase_lower_low_break:
        shape_intent = "a_phase_downtrend_lower_low_break"
    elif a_phase_ma5_inside_continuation:
        shape_intent = "a_phase_downtrend_ma5_inside_continuation"
    elif a_phase_recovery_20ma_hold:
        shape_intent = "a_phase_downtrend_recovery_20ma_hold"
    elif c_phase_end_20ma_break:
        shape_intent = "c_phase_uptrend_end_20ma_break"
    elif c_phase_warning_5ma_break:
        shape_intent = "c_phase_uptrend_warning_5ma_break"
    elif c_phase_ma20_touch_bounce:
        shape_intent = "c_phase_uptrend_20ma_touch_bounce"
    elif c_phase_ma5_ride:
        shape_intent = "c_phase_uptrend_ma5_ride"
    elif b_phase_bounce_blocked:
        shape_intent = "b_phase_bounce_blocked_by_long_ma"
    elif b_phase_breakout_up:
        shape_intent = "b_phase_breakout_up"
    elif b_phase_breakdown_down:
        shape_intent = "b_phase_breakdown_down"
    elif sideways_b_phase:
        shape_intent = "b_phase_sideways_wait"
    elif tried_prior_high and near_upper_half and failed_after_try and not below7:
        shape_intent = "failed_high_retest_wait_for_7ma_break"
    elif too_late_breakdown:
        shape_intent = "late_breakdown_chase"
    elif near_lower_half and (below7 or below20):
        shape_intent = "range_lower_drift"
    elif below7 and below20:
        shape_intent = "weak_below_ma"
    else:
        shape_intent = "neutral"

    reasons: list[str] = []
    if tried_prior_high:
        reasons.append("recent_high_tried_prior_high")
    if near_upper_half:
        reasons.append("close_in_upper_half_of_60bar_range")
    if near_lower_half:
        reasons.append("close_not_in_upper_half_of_60bar_range")
    if failed_after_try:
        reasons.append("close_rejected_from_recent_high")
    if near_round_level:
        reasons.append("near_round_level")
    if long_upper_wick:
        reasons.append("long_upper_wick")
    if mountain_top_reversal:
        reasons.append("mountain_top_reversal")
    if ma_rollover:
        reasons.append("ma_rollover")
    if below5:
        reasons.append("close_below_ma5")
    if below7:
        reasons.append("close_below_ma7")
    if below20:
        reasons.append("close_below_ma20")
    if below60:
        reasons.append("close_below_ma60")
    if mature_uptrend:
        reasons.append("mature_uptrend_approx_6month")
    if ma_touch_fatigue:
        reasons.append("ma_touch_fatigue")
    if second_ma60_contact_or_break:
        reasons.append("second_60ma_contact_or_break")
    if too_late_breakdown:
        reasons.append("late_breakdown_from_range_high")
    if crossed_below300:
        reasons.append("crossed_below_ma300")
    if ma300_resistance_rejection:
        reasons.append("ma300_retest_rejection")
    if ma300_false_reclaim:
        reasons.append("ma300_false_reclaim_second_break")
    if bullish_ma_stack_before_300_break:
        reasons.append("bullish_ma_stack_before_300_break")
    if sideways_b_phase:
        reasons.append("sideways_b_phase")
    if b_phase_after_drop:
        reasons.append("b_phase_after_drop")
    if b_phase_after_rise:
        reasons.append("b_phase_after_rise")
    if long_ma_resistance:
        reasons.append("long_ma_resistance")
    if c_phase_context:
        reasons.append("c_phase_ma_alignment")
    if c_phase_ma5_ride:
        reasons.append("c_phase_riding_ma5")
    if c_phase_ma20_touch_bounce:
        reasons.append("c_phase_touch_ma20_bounce")
    if downtrend_context:
        reasons.append("a_phase_downtrend_context")
    if a_phase_20ma_rejection:
        reasons.append("a_phase_20ma_rejection")
    if a_phase_60ma_rejection:
        reasons.append("a_phase_60ma_rejection")
    if a_phase_100ma_rejection:
        reasons.append("a_phase_100ma_rejection")
    if a_phase_lower_low_break:
        reasons.append("a_phase_lower_low_break")
    if a_phase_ma5_inside_continuation:
        reasons.append("a_phase_ma5_inside_continuation")
    if a_phase_bottom_range_breakdown:
        reasons.append("a_phase_bottom_range_breakdown")
    if bottoming_context:
        reasons.append("bottoming_context_after_crash")
    if touched5:
        reasons.append("touch_ma5")
    if crossed_above5:
        reasons.append("crossed_above_ma5")
    if crossed_above20:
        reasons.append("crossed_above_ma20")
    if consecutive_above5 >= 2:
        reasons.append("multi_day_above_ma5")
    if consecutive_above20 >= 3:
        reasons.append("multi_day_above_ma20")
    if sideways_after_drop:
        reasons.append("sideways_after_drop")

    if shape_intent.startswith("crash_warning_") or shape_intent.startswith("crash_initial_"):
        if too_late_breakdown:
            entry_timing = "late_breakdown_avoid_chase"
        elif crossed_below20:
            entry_timing = "initial_short_trigger_20ma_break"
        elif crossed_below7:
            entry_timing = "initial_short_trigger_7ma_break"
        elif below7:
            entry_timing = "short_trigger_active_below_7ma"
        elif crossed_below5:
            entry_timing = "probe_trigger_5ma_break"
        elif below5:
            entry_timing = "probe_active_below_5ma_wait_7ma"
        else:
            entry_timing = "watch_wait_for_ma_break"
    elif shape_intent == "failed_high_retest_wait_for_7ma_break":
        entry_timing = "watch_wait_for_7ma_break"
    elif shape_intent == "failed_high_retest_7ma_break":
        entry_timing = "initial_short_trigger_7ma_break"
    elif shape_intent == "late_breakdown_chase":
        entry_timing = "late_breakdown_avoid_chase"
    elif shape_intent == "crash_longterm_first_300ma_break":
        entry_timing = "watch_for_300ma_retest"
    elif shape_intent == "crash_longterm_300ma_retest_rejection":
        entry_timing = "short_trigger_300ma_retest_rejection"
    elif shape_intent == "crash_longterm_300ma_false_reclaim_second_break":
        entry_timing = "short_trigger_300ma_false_reclaim_second_break"
    elif shape_intent == "b_phase_breakout_up":
        entry_timing = "b_phase_buy_breakout"
    elif shape_intent == "b_phase_breakdown_down":
        entry_timing = "b_phase_short_breakdown"
    elif shape_intent == "b_phase_bounce_blocked_by_long_ma":
        entry_timing = "b_phase_short_rejection"
    elif shape_intent == "b_phase_sideways_wait":
        entry_timing = "b_phase_wait_for_breakout_or_breakdown"
    elif shape_intent == "c_phase_uptrend_ma5_ride":
        entry_timing = "c_phase_hold_or_small_buy_on_5ma"
    elif shape_intent == "c_phase_uptrend_20ma_touch_bounce":
        entry_timing = "c_phase_buy_or_add_after_20ma_bounce"
    elif shape_intent == "c_phase_uptrend_warning_5ma_break":
        entry_timing = "c_phase_reduce_or_wait_after_5ma_break"
    elif shape_intent == "c_phase_uptrend_end_20ma_break":
        entry_timing = "c_phase_exit_or_short_watch_after_20ma_break"
    elif shape_intent == "a_phase_downtrend_20ma_rejection":
        entry_timing = "a_phase_short_on_20ma_rejection"
    elif shape_intent == "a_phase_downtrend_60ma_rejection":
        entry_timing = "a_phase_short_on_60ma_rejection"
    elif shape_intent == "a_phase_downtrend_100ma_rejection":
        entry_timing = "a_phase_short_on_100ma_rejection"
    elif shape_intent == "a_phase_bottom_range_breakdown_turn_short":
        entry_timing = "a_phase_turn_short_on_bottom_range_break"
    elif shape_intent == "a_phase_downtrend_lower_low_break":
        entry_timing = "a_phase_add_short_on_lower_low_break"
    elif shape_intent == "a_phase_downtrend_ma5_inside_continuation":
        entry_timing = "a_phase_hold_or_add_short_while_under_5ma"
    elif shape_intent == "a_phase_downtrend_recovery_20ma_hold":
        entry_timing = "a_phase_short_cover_or_recovery_watch"
    elif shape_intent == "crash_bottom_warning_first_5ma_touch":
        entry_timing = "bottom_probe_very_small_or_short_cover_watch"
    elif shape_intent == "crash_bottom_warning_multi_day_above_5ma":
        entry_timing = "bottom_probe_small"
    elif shape_intent == "crash_bottom_warning_first_20ma_break":
        entry_timing = "bottom_reversal_watch_for_multi_day_20ma_hold"
    elif shape_intent == "crash_bottom_confirmed_above_20ma":
        entry_timing = "bottom_reversal_candidate"
    else:
        entry_timing = "none"

    scene_profile = _scene_profile(shape_intent, entry_timing)

    return {
        "confirmed": True,
        "bar_count": len(rows),
        "shape_intent": shape_intent,
        "market_scene": scene_profile["market_scene"],
        "trade_side": scene_profile["trade_side"],
        "action_bias": scene_profile["action_bias"],
        "risk_note": scene_profile["risk_note"],
        "is_try_fail_7ma_break": shape_intent == "failed_high_retest_7ma_break",
        "is_mature_uptrend_crash_setup": shape_intent.startswith("mature_uptrend_"),
        "is_crash_warning_setup": shape_intent.startswith("crash_warning_") or shape_intent.startswith("crash_initial_"),
        "is_crash_bottoming_setup": shape_intent.startswith("crash_bottom_"),
        "is_crash_longterm_300ma_setup": shape_intent.startswith("crash_longterm_"),
        "is_b_phase_setup": shape_intent.startswith("b_phase_"),
        "is_c_phase_setup": shape_intent.startswith("c_phase_"),
        "is_a_phase_setup": shape_intent.startswith("a_phase_"),
        "entry_timing": entry_timing,
        "metrics": {
            "close": round(close, 4),
            "ma5": round(ma5, 4) if ma5 is not None else None,
            "ma7": round(ma7, 4) if ma7 is not None else None,
            "ma20": round(ma20, 4) if ma20 is not None else None,
            "ma60": round(ma60, 4) if ma60 is not None else None,
            "ma100": round(ma100, 4) if ma100 is not None else None,
            "ma300": round(ma300, 4) if ma300 is not None else None,
            "crossed_below5": crossed_below5,
            "crossed_below7": crossed_below7,
            "crossed_below20": crossed_below20,
            "crossed_below300": crossed_below300,
            "recent_below300_count_20": recent_below300_count,
            "recent_above300_count_20": recent_above300_count,
            "crossed_above5": crossed_above5,
            "crossed_above20": crossed_above20,
            "consecutive_above5": consecutive_above5,
            "consecutive_above20": consecutive_above20,
            "recent_downtrend_drop_pct": round(recent_downtrend_drop_pct, 4)
            if recent_downtrend_drop_pct is not None
            else None,
            "range20_pct": round(range20_pct, 4) if range20_pct is not None else None,
            "prior_20_return": round(prior_20_return, 4) if prior_20_return is not None else None,
            "latest_20_return": round(latest_20_return, 4) if latest_20_return is not None else None,
            "range_position_60": round(range_position, 4) if range_position is not None else None,
            "round_level_distance_pct": round(round_level_distance_pct, 4)
            if round_level_distance_pct is not None
            else None,
            "upper_wick_ratio": round(upper_wick_ratio, 4) if upper_wick_ratio is not None else None,
            "upper_wick_body_ratio": round(upper_wick_body_ratio, 4) if upper_wick_body_ratio is not None else None,
            "ma20_slope_10": round(ma20_slope_10, 4) if ma20_slope_10 is not None else None,
            "ma60_slope_10": round(ma60_slope_10, 4) if ma60_slope_10 is not None else None,
            "bars_since_recent_peak": bars_since_recent_peak,
            "pre_peak_rise_pct": round(pre_peak_rise_pct, 4) if pre_peak_rise_pct is not None else None,
            "post_peak_drop_pct": round(post_peak_drop_pct, 4) if post_peak_drop_pct is not None else None,
            "peak_to_now_daily_drop_pct": round(peak_to_now_daily_drop_pct, 4)
            if peak_to_now_daily_drop_pct is not None
            else None,
            "six_month_uptrend_pct": round(six_month_uptrend_pct, 4) if six_month_uptrend_pct is not None else None,
            "mature_uptrend_range_pct": round(mature_uptrend_range_pct, 4)
            if mature_uptrend_range_pct is not None
            else None,
            "ma20_touch_count_60": ma20_touch_count_60,
            "ma60_touch_count_60": ma60_touch_count_60,
            "prior_ma60_touch_count_60": prior_ma60_touch_count_60,
            "prior_high": round(prior_high, 4),
            "recent_high_5": round(recent_high, 4),
            "recent_high_offset_pct": round(recent_high_offset_pct, 4) if recent_high_offset_pct is not None else None,
            "close_drop_from_recent_high_pct": round(close_drop_from_recent_high_pct, 4)
            if close_drop_from_recent_high_pct is not None
            else None,
            "drawdown_from_range_high_pct": round(drawdown_from_range_high_pct, 4)
            if drawdown_from_range_high_pct is not None
            else None,
        },
        "definition": "a_phase: downtrend context; rebounds rejected at 20/60/100MA, lower-low breaks, and bottom-range breakdowns are short setups; multi-day 20MA hold is recovery warning. c/b/crash labels cover other regimes.",
        "reasons": reasons,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bars-json", help="JSON array of bars. If omitted, read stdin.")
    args = parser.parse_args()
    payload = args.bars_json if args.bars_json is not None else sys.stdin.read()
    bars = json.loads(payload)
    print(json.dumps(classify_shape_from_bars(bars), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
