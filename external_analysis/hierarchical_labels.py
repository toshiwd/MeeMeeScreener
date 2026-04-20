from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


HIERARCHICAL_LABEL_SCHEMA_VERSION = "tradex_monthly_shape_memory_hierarchical_label_v1"

MONTHLY_MA_PERIODS = (6, 12, 24)
WEEKLY_MA_PERIODS = (10, 30, 60)
DAILY_MA_PERIODS = (7, 20, 60)

PRICE_NEAR_THRESHOLD = {
    "monthly": 0.02,
    "weekly": 0.015,
    "daily": 0.01,
}

SLOPE_LOOKBACK = {
    "monthly": {6: 3, 12: 3, 24: 6},
    "weekly": {10: 4, 30: 8, 60: 12},
    "daily": {7: 5, 20: 10, 60: 20},
}

SLOPE_UP_THRESHOLD = {
    "monthly": {6: 0.03, 12: 0.03, 24: 0.04},
    "weekly": {10: 0.02, 30: 0.02, 60: 0.03},
    "daily": {7: 0.01, 20: 0.012, 60: 0.015},
}

SLOPE_DOWN_THRESHOLD = {
    "monthly": {6: -0.03, 12: -0.03, 24: -0.04},
    "weekly": {10: -0.02, 30: -0.02, 60: -0.03},
    "daily": {7: -0.01, 20: -0.012, 60: -0.015},
}

EXTENSION_THRESHOLD = {
    "monthly": 0.08,
    "weekly": 0.06,
    "daily": 0.04,
}

PERSISTENCE_CAP = 12
PERSISTENCE_LATE_THRESHOLD = 5
PERSISTENCE_PRE_THRESHOLD = 1
WICK_LONG_THRESHOLD = 0.35
BODY_SMALL_THRESHOLD = 0.25
GAP_THRESHOLD = 0.002

MONTHLY_MAIN_STATE_SCORE = {
    "monthly_up_pre": 62.0,
    "monthly_up_mid": 80.0,
    "monthly_up_top_warning": 56.0,
    "monthly_range_pre": 42.0,
    "monthly_range_mid": 50.0,
    "monthly_range_late": 60.0,
    "monthly_down_mid": 32.0,
    "monthly_down_bottom_warning": 24.0,
}
MONTHLY_PHASE_SCORE = {
    "monthly_up_pre": 42.0,
    "monthly_up_mid": 66.0,
    "monthly_up_top_warning": 84.0,
    "monthly_range_pre": 26.0,
    "monthly_range_mid": 46.0,
    "monthly_range_late": 66.0,
    "monthly_down_mid": 54.0,
    "monthly_down_bottom_warning": 86.0,
}
WEEKLY_MAIN_STATE_SCORE = {
    "weekly_up_early": 64.0,
    "weekly_up_mid": 78.0,
    "weekly_up_late": 70.0,
    "weekly_range_mid": 48.0,
    "weekly_range_late": 56.0,
    "weekly_down_early": 36.0,
    "weekly_down_mid": 28.0,
    "weekly_down_bottom_warning": 22.0,
}
WEEKLY_PHASE_SCORE = {
    "weekly_up_early": 40.0,
    "weekly_up_mid": 64.0,
    "weekly_up_late": 80.0,
    "weekly_range_mid": 30.0,
    "weekly_range_late": 54.0,
    "weekly_down_early": 36.0,
    "weekly_down_mid": 56.0,
    "weekly_down_bottom_warning": 86.0,
}
DAILY_MAIN_STATE_SCORE = {
    "daily_up_early": 66.0,
    "daily_up_mid": 78.0,
    "daily_up_top_warning": 60.0,
    "daily_range_mid": 50.0,
    "daily_range_late": 56.0,
    "daily_down_early": 36.0,
    "daily_down_mid": 28.0,
    "daily_down_bottom_warning": 22.0,
    "daily_reversal_up_candidate": 86.0,
    "daily_reversal_down_candidate": 16.0,
}
DAILY_PHASE_SCORE = {
    "daily_up_early": 42.0,
    "daily_up_mid": 66.0,
    "daily_up_top_warning": 84.0,
    "daily_range_mid": 32.0,
    "daily_range_late": 56.0,
    "daily_down_early": 34.0,
    "daily_down_mid": 56.0,
    "daily_down_bottom_warning": 86.0,
    "daily_reversal_up_candidate": 90.0,
    "daily_reversal_down_candidate": 12.0,
}

MONTHLY_PRIORITY = [
    "monthly_up_top_warning",
    "monthly_down_bottom_warning",
    "monthly_up_mid",
    "monthly_down_mid",
    "monthly_range_late",
    "monthly_range_mid",
    "monthly_range_pre",
    "monthly_up_pre",
]
WEEKLY_PRIORITY = [
    "weekly_down_bottom_warning",
    "weekly_up_late",
    "weekly_up_mid",
    "weekly_down_mid",
    "weekly_up_early",
    "weekly_down_early",
    "weekly_range_late",
    "weekly_range_mid",
]
DAILY_PRIORITY = [
    "daily_reversal_up_candidate",
    "daily_reversal_down_candidate",
    "daily_up_top_warning",
    "daily_down_bottom_warning",
    "daily_up_mid",
    "daily_up_early",
    "daily_down_mid",
    "daily_down_early",
    "daily_range_late",
    "daily_range_mid",
]

TRIGGER_ORDER = [
    "daily_gap_up_flag",
    "daily_gap_down_flag",
    "daily_engulfing_bull_flag",
    "daily_engulfing_bear_flag",
    "daily_reclaim_ma20_flag",
    "daily_lose_ma20_flag",
    "daily_long_lower_wick_flag",
    "daily_long_upper_wick_flag",
    "daily_small_body_flag",
]


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, (np.floating, np.integer)):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


def _text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, str):
        text = value.strip()
        return text or fallback
    text = str(value).strip()
    return text or fallback


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(fallback)
    if not math.isfinite(out):
        return float(fallback)
    return float(out)


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return float(max(low, min(high, value)))


def _normalize_state_state(value: str, mapping: dict[str, float], fallback: float = 50.0) -> float:
    return float(mapping.get(value, fallback))


def _price_vs_ma_state(close: float, ma: float, near_threshold: float) -> str:
    if not math.isfinite(close) or not math.isfinite(ma) or ma <= 0.0:
        return "near"
    delta = close / ma - 1.0
    if delta >= near_threshold:
        return "above"
    if delta <= -near_threshold:
        return "below"
    return "near"


def _slope_state(current: float, previous: float, up_threshold: float, down_threshold: float) -> str:
    if not math.isfinite(current) or not math.isfinite(previous) or previous <= 0.0:
        return "flat"
    delta = current / previous - 1.0
    if delta >= up_threshold:
        return "up"
    if delta <= down_threshold:
        return "down"
    return "flat"


def _streak_counts(mask: np.ndarray, *, cap: int = PERSISTENCE_CAP) -> np.ndarray:
    counts = np.zeros(int(mask.size), dtype=int)
    streak = 0
    for idx, flag in enumerate(mask.astype(bool, copy=False)):
        if flag:
            streak = min(int(cap), streak + 1)
        else:
            streak = 0
        counts[idx] = streak
    return counts


def _score_streak(count: int) -> float:
    return _clamp((min(int(count), PERSISTENCE_CAP) / float(PERSISTENCE_CAP)) * 100.0)


def _alignment_state(
    *,
    close_state_short: str,
    close_state_mid: str,
    close_state_long: str,
    slope_short: str,
    slope_mid: str,
) -> str:
    if close_state_short == "above" and close_state_mid == "above" and close_state_long == "above" and slope_short == "up" and slope_mid in {"up", "flat"}:
        return "bull_stack"
    if close_state_short == "below" and close_state_mid == "below" and close_state_long == "below" and slope_short == "down" and slope_mid in {"down", "flat"}:
        return "bear_stack"
    if close_state_short == "near" and close_state_mid == "near" and close_state_long == "near" and slope_short == "flat" and slope_mid == "flat":
        return "compressed"
    return "mixed"


def _select_label(candidate_scores: dict[str, float], priority: list[str], *, fallback: str) -> str:
    best_label = fallback
    best_score = float("-inf")
    best_rank = len(priority) + 1
    for label, score in candidate_scores.items():
        if score is None:
            continue
        if float(score) > best_score:
            best_score = float(score)
            best_label = label
            best_rank = priority.index(label) if label in priority else len(priority) + 1
            continue
        if float(score) == best_score:
            rank = priority.index(label) if label in priority else len(priority) + 1
            if rank < best_rank:
                best_label = label
                best_rank = rank
    return best_label


def _candle_metrics(open_price: float, high_price: float, low_price: float, close_price: float) -> dict[str, float]:
    candle_range = max(high_price - low_price, 1e-9)
    body_size = abs(close_price - open_price)
    upper_wick_size = max(0.0, high_price - max(open_price, close_price))
    lower_wick_size = max(0.0, min(open_price, close_price) - low_price)
    return {
        "body_ratio": _clamp((body_size / candle_range) * 100.0, 0.0, 100.0) / 100.0,
        "upper_wick_ratio": _clamp((upper_wick_size / candle_range) * 100.0, 0.0, 100.0) / 100.0,
        "lower_wick_ratio": _clamp((lower_wick_size / candle_range) * 100.0, 0.0, 100.0) / 100.0,
        "close_pos_in_range": _clamp(((close_price - low_price) / candle_range) * 100.0, 0.0, 100.0) / 100.0,
        "close_near_high": _clamp(((high_price - close_price) / candle_range) * 100.0, 0.0, 100.0) / 100.0,
        "close_near_low": _clamp(((close_price - low_price) / candle_range) * 100.0, 0.0, 100.0) / 100.0,
    }


def _main_state_score(label: str) -> float:
    if label.startswith("monthly_"):
        return float(MONTHLY_MAIN_STATE_SCORE.get(label, 50.0))
    if label.startswith("weekly_"):
        return float(WEEKLY_MAIN_STATE_SCORE.get(label, 50.0))
    if label.startswith("daily_"):
        return float(DAILY_MAIN_STATE_SCORE.get(label, 50.0))
    return 50.0


def _phase_score(label: str) -> float:
    if label.startswith("monthly_"):
        return float(MONTHLY_PHASE_SCORE.get(label, 50.0))
    if label.startswith("weekly_"):
        return float(WEEKLY_PHASE_SCORE.get(label, 50.0))
    if label.startswith("daily_"):
        return float(DAILY_PHASE_SCORE.get(label, 50.0))
    return 50.0


def _build_monthly_context_for_code(code_df: pd.DataFrame) -> pd.DataFrame:
    working = code_df.sort_values("date").reset_index(drop=True).copy()
    working["month_key"] = working["date_ts"].dt.year * 100 + working["date_ts"].dt.month
    monthly = (
        working.groupby("month_key", sort=True, as_index=False)
        .agg(
            code=("code", "first"),
            month_end_date=("date", "max"),
            month_end_ts=("date_ts", "max"),
            o=("o", "first"),
            h=("h", "max"),
            l=("l", "min"),
            c=("c", "last"),
            v=("v", "sum"),
            daily_row_count=("date", "count"),
        )
        .sort_values("month_key")
        .reset_index(drop=True)
    )
    close = monthly["c"].astype(float)
    monthly["ma6"] = close.rolling(6, min_periods=6).mean()
    monthly["ma12"] = close.rolling(12, min_periods=12).mean()
    monthly["ma24"] = close.rolling(24, min_periods=24).mean()
    monthly["ma6_prev3"] = monthly["ma6"].shift(3)
    monthly["ma12_prev3"] = monthly["ma12"].shift(3)
    monthly["ma24_prev6"] = monthly["ma24"].shift(6)
    monthly["price_vs_ma6_state"] = [
        _price_vs_ma_state(float(close_value), float(ma6_value), PRICE_NEAR_THRESHOLD["monthly"])
        for close_value, ma6_value in zip(monthly["c"], monthly["ma6"])
    ]
    monthly["price_vs_ma12_state"] = [
        _price_vs_ma_state(float(close_value), float(ma12_value), PRICE_NEAR_THRESHOLD["monthly"])
        for close_value, ma12_value in zip(monthly["c"], monthly["ma12"])
    ]
    monthly["price_vs_ma24_state"] = [
        _price_vs_ma_state(float(close_value), float(ma24_value), PRICE_NEAR_THRESHOLD["monthly"])
        for close_value, ma24_value in zip(monthly["c"], monthly["ma24"])
    ]
    monthly["ma12_slope_state"] = [
        _slope_state(float(current), float(previous), SLOPE_UP_THRESHOLD["monthly"][12], SLOPE_DOWN_THRESHOLD["monthly"][12])
        for current, previous in zip(monthly["ma12"], monthly["ma12_prev3"])
    ]
    monthly["ma6_slope_state"] = [
        _slope_state(float(current), float(previous), SLOPE_UP_THRESHOLD["monthly"][6], SLOPE_DOWN_THRESHOLD["monthly"][6])
        for current, previous in zip(monthly["ma6"], monthly["ma6_prev3"])
    ]
    monthly["ma24_slope_state"] = [
        _slope_state(float(current), float(previous), SLOPE_UP_THRESHOLD["monthly"][24], SLOPE_DOWN_THRESHOLD["monthly"][24])
        for current, previous in zip(monthly["ma24"], monthly["ma24_prev6"])
    ]
    monthly["alignment_state"] = [
        _alignment_state(
            close_state_short=short,
            close_state_mid=mid,
            close_state_long=long,
            slope_short=slope_short,
            slope_mid=slope_mid,
        )
        for short, mid, long, slope_short, slope_mid in zip(
            monthly["price_vs_ma6_state"],
            monthly["price_vs_ma12_state"],
            monthly["price_vs_ma24_state"],
            monthly["ma6_slope_state"],
            monthly["ma12_slope_state"],
        )
    ]
    monthly["month_body"] = (monthly["c"] - monthly["o"]).abs()
    monthly["month_range"] = (monthly["h"] - monthly["l"]).replace(0, np.nan)
    monthly["body_ratio"] = (monthly["month_body"] / monthly["month_range"]).fillna(0.0).clip(0.0, 1.0)
    monthly["upper_wick_ratio"] = ((monthly["h"] - monthly[["o", "c"]].max(axis=1)) / monthly["month_range"]).fillna(0.0).clip(0.0, 1.0)
    monthly["lower_wick_ratio"] = ((monthly[["o", "c"]].min(axis=1) - monthly["l"]) / monthly["month_range"]).fillna(0.0).clip(0.0, 1.0)
    monthly["above_ma12_count"] = _streak_counts((monthly["c"].to_numpy(dtype=float) > monthly["ma12"].fillna(np.inf).to_numpy(dtype=float)))
    monthly["below_ma12_count"] = _streak_counts((monthly["c"].to_numpy(dtype=float) < monthly["ma12"].fillna(-np.inf).to_numpy(dtype=float)))
    monthly["above_ma24_count"] = _streak_counts((monthly["c"].to_numpy(dtype=float) > monthly["ma24"].fillna(np.inf).to_numpy(dtype=float)))
    monthly["below_ma24_count"] = _streak_counts((monthly["c"].to_numpy(dtype=float) < monthly["ma24"].fillna(-np.inf).to_numpy(dtype=float)))

    records: list[dict[str, Any]] = []
    for idx, row in enumerate(monthly.itertuples(index=False)):
        monthly_price_vs_ma6_state = str(getattr(row, "price_vs_ma6_state"))
        monthly_price_vs_ma12_state = str(getattr(row, "price_vs_ma12_state"))
        monthly_price_vs_ma24_state = str(getattr(row, "price_vs_ma24_state"))
        monthly_ma6_slope_state = str(getattr(row, "ma6_slope_state"))
        monthly_ma12_slope_state = str(getattr(row, "ma12_slope_state"))
        monthly_ma24_slope_state = str(getattr(row, "ma24_slope_state"))
        monthly_alignment_state = str(getattr(row, "alignment_state"))
        close_value = float(row.c)
        ma6_value = float(row.ma6) if math.isfinite(float(row.ma6)) else close_value
        ma12_value = float(row.ma12) if math.isfinite(float(row.ma12)) else close_value
        ma24_value = float(row.ma24) if math.isfinite(float(row.ma24)) else close_value
        candle = _candle_metrics(float(row.o), float(row.h), float(row.l), float(row.c))
        extension_up = max(
            close_value / max(ma6_value, 1e-9) - 1.0,
            close_value / max(ma12_value, 1e-9) - 1.0,
            close_value / max(ma24_value, 1e-9) - 1.0,
        )
        extension_down = min(
            close_value / max(ma6_value, 1e-9) - 1.0,
            close_value / max(ma12_value, 1e-9) - 1.0,
            close_value / max(ma24_value, 1e-9) - 1.0,
        )
        persistence_above = max(int(row.above_ma12_count), int(row.above_ma24_count))
        persistence_below = max(int(row.below_ma12_count), int(row.below_ma24_count))
        bull_alignment = monthly_alignment_state == "bull_stack"
        bear_alignment = monthly_alignment_state == "bear_stack"
        compressed = monthly_alignment_state == "compressed"
        candidate_scores = {
            "monthly_up_pre": (
                48.0
                + (12.0 if monthly_price_vs_ma24_state in {"above", "near"} else 0.0)
                + (10.0 if monthly_ma24_slope_state == "up" else 0.0)
                + (8.0 if monthly_ma6_slope_state in {"up", "flat"} else 0.0)
                + (6.0 if persistence_above <= 2 else 0.0)
            ),
            "monthly_up_mid": (
                60.0
                + (15.0 if bull_alignment else 0.0)
                + (10.0 if monthly_ma6_slope_state == "up" else 0.0)
                + (10.0 if monthly_ma12_slope_state == "up" else 0.0)
                + (10.0 if monthly_ma24_slope_state in {"up", "flat"} else 0.0)
                + (5.0 if persistence_above >= 2 else 0.0)
            ),
            "monthly_up_top_warning": (
                70.0
                + (20.0 if extension_up >= EXTENSION_THRESHOLD["monthly"] else 0.0)
                + (10.0 if monthly_ma6_slope_state in {"flat", "down"} else 0.0)
                + (10.0 if monthly_ma12_slope_state in {"flat", "down"} else 0.0)
                + (10.0 if monthly_ma24_slope_state in {"flat", "down"} else 0.0)
                + (5.0 if candle["upper_wick_ratio"] >= WICK_LONG_THRESHOLD else 0.0)
            ),
            "monthly_range_pre": (
                36.0
                + (12.0 if monthly_price_vs_ma6_state == "near" or monthly_price_vs_ma12_state == "near" or monthly_price_vs_ma24_state == "near" else 0.0)
                + (8.0 if compressed else 0.0)
                + (6.0 if persistence_above <= 1 and persistence_below <= 1 else 0.0)
            ),
            "monthly_range_mid": (
                46.0
                + (12.0 if monthly_alignment_state == "mixed" else 0.0)
                + (8.0 if monthly_ma6_slope_state == "flat" and monthly_ma12_slope_state == "flat" else 0.0)
                + (8.0 if monthly_ma12_slope_state == "flat" and monthly_ma24_slope_state == "flat" else 0.0)
                + (4.0 if persistence_above <= 3 and persistence_below <= 3 else 0.0)
            ),
            "monthly_range_late": (
                54.0
                + (12.0 if monthly_alignment_state == "mixed" else 0.0)
                + (10.0 if max(persistence_above, persistence_below) >= PERSISTENCE_LATE_THRESHOLD else 0.0)
                + (6.0 if candle["body_ratio"] <= BODY_SMALL_THRESHOLD else 0.0)
            ),
            "monthly_down_mid": (
                58.0
                + (15.0 if bear_alignment else 0.0)
                + (10.0 if monthly_ma6_slope_state == "down" else 0.0)
                + (10.0 if monthly_ma12_slope_state == "down" else 0.0)
                + (10.0 if monthly_ma24_slope_state in {"down", "flat"} else 0.0)
                + (5.0 if persistence_below >= 2 else 0.0)
            ),
            "monthly_down_bottom_warning": (
                72.0
                + (18.0 if extension_down <= -EXTENSION_THRESHOLD["monthly"] else 0.0)
                + (12.0 if monthly_price_vs_ma6_state == "near" or monthly_price_vs_ma12_state == "near" else 0.0)
                + (10.0 if monthly_ma6_slope_state in {"flat", "up"} else 0.0)
                + (10.0 if monthly_ma12_slope_state in {"flat", "up"} else 0.0)
                + (8.0 if candle["lower_wick_ratio"] >= WICK_LONG_THRESHOLD else 0.0)
            ),
        }
        label = _select_label(candidate_scores, MONTHLY_PRIORITY, fallback="monthly_range_mid")
        records.append(
            {
                "code": str(row.code),
                "month_key": int(row.month_key),
                "month_end_date": int(row.month_end_date),
                "month_end_ts": pd.Timestamp(row.month_end_ts).isoformat(),
                "monthly_main_state": label,
                "monthly_price_vs_ma12_state": monthly_price_vs_ma12_state,
                "monthly_price_vs_ma24_state": monthly_price_vs_ma24_state,
                "monthly_price_vs_ma6_state": monthly_price_vs_ma6_state,
                "monthly_ma6_slope_state": monthly_ma6_slope_state,
                "monthly_ma12_slope_state": monthly_ma12_slope_state,
                "monthly_ma24_slope_state": monthly_ma24_slope_state,
                "monthly_alignment_state": monthly_alignment_state,
                "monthly_above_ma12_count": int(row.above_ma12_count),
                "monthly_below_ma12_count": int(row.below_ma12_count),
                "monthly_above_ma24_count": int(row.above_ma24_count),
                "monthly_below_ma24_count": int(row.below_ma24_count),
                "monthly_main_state_score": float(_main_state_score(label)),
                "monthly_phase_score": float(_phase_score(label)),
                "monthly_environment_score": float(
                    _clamp(
                        0.35 * _main_state_score(label)
                        + 0.25 * _normalize_state_state(monthly_alignment_state, {"bull_stack": 90.0, "mixed": 45.0, "compressed": 55.0, "bear_stack": 12.0})
                        + 0.20 * ((_normalize_state_state(monthly_price_vs_ma6_state, {"above": 80.0, "near": 50.0, "below": 20.0}) + _normalize_state_state(monthly_price_vs_ma12_state, {"above": 80.0, "near": 50.0, "below": 20.0}) + _normalize_state_state(monthly_price_vs_ma24_state, {"above": 80.0, "near": 50.0, "below": 20.0})) / 3.0)
                        + 0.20 * ((_normalize_state_state(monthly_ma6_slope_state, {"up": 80.0, "flat": 50.0, "down": 20.0}) + _normalize_state_state(monthly_ma12_slope_state, {"up": 80.0, "flat": 50.0, "down": 20.0}) + _normalize_state_state(monthly_ma24_slope_state, {"up": 80.0, "flat": 50.0, "down": 20.0})) / 3.0)
                    )
                ),
                "monthly_phase_score_raw": float(
                    _clamp(
                        0.55 * _phase_score(label)
                        + 0.20 * ((int(row.above_ma12_count) + int(row.above_ma24_count) + int(row.above_ma12_count)) / 3.0 / max(1, PERSISTENCE_CAP) * 100.0)
                        + 0.15 * candle["body_ratio"] * 100.0
                        + 0.10 * (100.0 if label.endswith("warning") else 0.0)
                    )
                ),
                "_monthly_candidate_scores": candidate_scores,
                "_monthly_extension_up": float(extension_up),
                "_monthly_extension_down": float(extension_down),
                "_monthly_body_ratio": float(candle["body_ratio"]),
                "_monthly_upper_wick_ratio": float(candle["upper_wick_ratio"]),
                "_monthly_lower_wick_ratio": float(candle["lower_wick_ratio"]),
            }
        )
    return pd.DataFrame(records)


def _build_weekly_context_for_code(code_df: pd.DataFrame) -> pd.DataFrame:
    working = code_df.sort_values("date").reset_index(drop=True).copy()
    working["week_period"] = working["date_ts"].dt.to_period("W-FRI")
    weekly = (
        working.groupby("week_period", sort=True, as_index=False)
        .agg(
            code=("code", "first"),
            week_end_date=("date", "max"),
            week_end_ts=("date_ts", "max"),
            o=("o", "first"),
            h=("h", "max"),
            l=("l", "min"),
            c=("c", "last"),
            v=("v", "sum"),
            daily_row_count=("date", "count"),
        )
        .sort_values("week_period")
        .reset_index(drop=True)
    )
    close = weekly["c"].astype(float)
    weekly["ma10"] = close.rolling(10, min_periods=10).mean()
    weekly["ma30"] = close.rolling(30, min_periods=30).mean()
    weekly["ma60"] = close.rolling(60, min_periods=60).mean()
    weekly["ma10_prev4"] = weekly["ma10"].shift(4)
    weekly["ma30_prev8"] = weekly["ma30"].shift(8)
    weekly["ma60_prev12"] = weekly["ma60"].shift(12)
    weekly["price_vs_ma10_state"] = [
        _price_vs_ma_state(float(close_value), float(ma10_value), PRICE_NEAR_THRESHOLD["weekly"])
        for close_value, ma10_value in zip(weekly["c"], weekly["ma10"])
    ]
    weekly["price_vs_ma30_state"] = [
        _price_vs_ma_state(float(close_value), float(ma30_value), PRICE_NEAR_THRESHOLD["weekly"])
        for close_value, ma30_value in zip(weekly["c"], weekly["ma30"])
    ]
    weekly["price_vs_ma60_state"] = [
        _price_vs_ma_state(float(close_value), float(ma60_value), PRICE_NEAR_THRESHOLD["weekly"])
        for close_value, ma60_value in zip(weekly["c"], weekly["ma60"])
    ]
    weekly["ma10_slope_state"] = [
        _slope_state(float(current), float(previous), SLOPE_UP_THRESHOLD["weekly"][10], SLOPE_DOWN_THRESHOLD["weekly"][10])
        for current, previous in zip(weekly["ma10"], weekly["ma10_prev4"])
    ]
    weekly["ma30_slope_state"] = [
        _slope_state(float(current), float(previous), SLOPE_UP_THRESHOLD["weekly"][30], SLOPE_DOWN_THRESHOLD["weekly"][30])
        for current, previous in zip(weekly["ma30"], weekly["ma30_prev8"])
    ]
    weekly["alignment_state"] = [
        _alignment_state(
            close_state_short=short,
            close_state_mid=mid,
            close_state_long=long,
            slope_short=slope_short,
            slope_mid=slope_mid,
        )
        for short, mid, long, slope_short, slope_mid in zip(
            weekly["price_vs_ma10_state"],
            weekly["price_vs_ma30_state"],
            weekly["price_vs_ma60_state"],
            weekly["ma10_slope_state"],
            weekly["ma30_slope_state"],
        )
    ]
    weekly["week_body"] = (weekly["c"] - weekly["o"]).abs()
    weekly["week_range"] = (weekly["h"] - weekly["l"]).replace(0, np.nan)
    weekly["body_ratio"] = (weekly["week_body"] / weekly["week_range"]).fillna(0.0).clip(0.0, 1.0)
    weekly["upper_wick_ratio"] = ((weekly["h"] - weekly[["o", "c"]].max(axis=1)) / weekly["week_range"]).fillna(0.0).clip(0.0, 1.0)
    weekly["lower_wick_ratio"] = ((weekly[["o", "c"]].min(axis=1) - weekly["l"]) / weekly["week_range"]).fillna(0.0).clip(0.0, 1.0)
    weekly["above_ma10_count"] = _streak_counts((weekly["c"].to_numpy(dtype=float) > weekly["ma10"].fillna(np.inf).to_numpy(dtype=float)))
    weekly["below_ma10_count"] = _streak_counts((weekly["c"].to_numpy(dtype=float) < weekly["ma10"].fillna(-np.inf).to_numpy(dtype=float)))
    weekly["above_ma30_count"] = _streak_counts((weekly["c"].to_numpy(dtype=float) > weekly["ma30"].fillna(np.inf).to_numpy(dtype=float)))
    weekly["below_ma30_count"] = _streak_counts((weekly["c"].to_numpy(dtype=float) < weekly["ma30"].fillna(-np.inf).to_numpy(dtype=float)))

    records: list[dict[str, Any]] = []
    for row in weekly.itertuples(index=False):
        weekly_price_vs_ma10_state = str(getattr(row, "price_vs_ma10_state"))
        weekly_price_vs_ma30_state = str(getattr(row, "price_vs_ma30_state"))
        weekly_price_vs_ma60_state = str(getattr(row, "price_vs_ma60_state"))
        weekly_ma10_slope_state = str(getattr(row, "ma10_slope_state"))
        weekly_ma30_slope_state = str(getattr(row, "ma30_slope_state"))
        weekly_alignment_state = str(getattr(row, "alignment_state"))
        close_value = float(row.c)
        ma10_value = float(row.ma10) if math.isfinite(float(row.ma10)) else close_value
        ma30_value = float(row.ma30) if math.isfinite(float(row.ma30)) else close_value
        ma60_value = float(row.ma60) if math.isfinite(float(row.ma60)) else close_value
        candle = _candle_metrics(float(row.o), float(row.h), float(row.l), float(row.c))
        extension_up = max(
            close_value / max(ma10_value, 1e-9) - 1.0,
            close_value / max(ma30_value, 1e-9) - 1.0,
            close_value / max(ma60_value, 1e-9) - 1.0,
        )
        extension_down = min(
            close_value / max(ma10_value, 1e-9) - 1.0,
            close_value / max(ma30_value, 1e-9) - 1.0,
            close_value / max(ma60_value, 1e-9) - 1.0,
        )
        persistence_above = max(int(row.above_ma10_count), int(row.above_ma30_count))
        persistence_below = max(int(row.below_ma10_count), int(row.below_ma30_count))
        bull_alignment = weekly_alignment_state == "bull_stack"
        bear_alignment = weekly_alignment_state == "bear_stack"
        compressed = weekly_alignment_state == "compressed"
        candidate_scores = {
            "weekly_up_early": (
                48.0
                + (12.0 if weekly_price_vs_ma10_state in {"above", "near"} else 0.0)
                + (10.0 if weekly_ma10_slope_state == "up" else 0.0)
                + (8.0 if weekly_price_vs_ma30_state != "below" else 0.0)
                + (6.0 if persistence_above <= 2 else 0.0)
            ),
            "weekly_up_mid": (
                60.0
                + (15.0 if bull_alignment else 0.0)
                + (10.0 if weekly_ma10_slope_state == "up" else 0.0)
                + (10.0 if weekly_ma30_slope_state in {"up", "flat"} else 0.0)
                + (5.0 if persistence_above >= 2 else 0.0)
            ),
            "weekly_up_late": (
                68.0
                + (16.0 if bull_alignment else 0.0)
                + (10.0 if extension_up >= EXTENSION_THRESHOLD["weekly"] else 0.0)
                + (10.0 if weekly_ma10_slope_state in {"flat", "down"} else 0.0)
                + (6.0 if candle["upper_wick_ratio"] >= WICK_LONG_THRESHOLD else 0.0)
            ),
            "weekly_range_mid": (
                44.0
                + (12.0 if weekly_alignment_state == "mixed" else 0.0)
                + (8.0 if compressed else 0.0)
                + (5.0 if persistence_above <= 3 and persistence_below <= 3 else 0.0)
            ),
            "weekly_range_late": (
                54.0
                + (12.0 if weekly_alignment_state == "mixed" else 0.0)
                + (10.0 if max(persistence_above, persistence_below) >= PERSISTENCE_LATE_THRESHOLD else 0.0)
                + (6.0 if candle["body_ratio"] <= BODY_SMALL_THRESHOLD else 0.0)
            ),
            "weekly_down_early": (
                48.0
                + (12.0 if weekly_price_vs_ma10_state in {"below", "near"} else 0.0)
                + (10.0 if weekly_ma10_slope_state == "down" else 0.0)
                + (8.0 if weekly_price_vs_ma30_state != "above" else 0.0)
                + (5.0 if persistence_below <= 2 else 0.0)
            ),
            "weekly_down_mid": (
                58.0
                + (15.0 if bear_alignment else 0.0)
                + (10.0 if weekly_ma10_slope_state == "down" else 0.0)
                + (10.0 if weekly_ma30_slope_state in {"down", "flat"} else 0.0)
                + (5.0 if persistence_below >= 2 else 0.0)
            ),
            "weekly_down_bottom_warning": (
                72.0
                + (18.0 if extension_down <= -EXTENSION_THRESHOLD["weekly"] else 0.0)
                + (12.0 if weekly_price_vs_ma10_state == "near" else 0.0)
                + (10.0 if weekly_ma10_slope_state in {"flat", "up"} else 0.0)
                + (8.0 if candle["lower_wick_ratio"] >= WICK_LONG_THRESHOLD else 0.0)
            ),
        }
        label = _select_label(candidate_scores, WEEKLY_PRIORITY, fallback="weekly_range_mid")
        records.append(
            {
                "code": str(row.code),
                "week_period": str(row.week_period),
                "week_end_date": int(row.week_end_date),
                "week_end_ts": pd.Timestamp(row.week_end_ts).isoformat(),
                "weekly_main_state": label,
                "weekly_price_vs_ma10_state": weekly_price_vs_ma10_state,
                "weekly_price_vs_ma30_state": weekly_price_vs_ma30_state,
                "weekly_price_vs_ma60_state": weekly_price_vs_ma60_state,
                "weekly_ma10_slope_state": weekly_ma10_slope_state,
                "weekly_ma30_slope_state": weekly_ma30_slope_state,
                "weekly_alignment_state": weekly_alignment_state,
                "weekly_above_ma10_count": int(row.above_ma10_count),
                "weekly_below_ma10_count": int(row.below_ma10_count),
                "weekly_above_ma30_count": int(row.above_ma30_count),
                "weekly_below_ma30_count": int(row.below_ma30_count),
                "weekly_main_state_score": float(_main_state_score(label)),
                "weekly_phase_score": float(_phase_score(label)),
                "weekly_trend_score": float(
                    _clamp(
                        0.40 * _main_state_score(label)
                        + 0.25 * _normalize_state_state(weekly_alignment_state, {"bull_stack": 90.0, "mixed": 45.0, "compressed": 55.0, "bear_stack": 12.0})
                        + 0.20 * ((_normalize_state_state(weekly_price_vs_ma10_state, {"above": 80.0, "near": 50.0, "below": 20.0}) + _normalize_state_state(weekly_price_vs_ma30_state, {"above": 80.0, "near": 50.0, "below": 20.0}) + _normalize_state_state(weekly_price_vs_ma60_state, {"above": 80.0, "near": 50.0, "below": 20.0})) / 3.0)
                        + 0.15 * ((_normalize_state_state(weekly_ma10_slope_state, {"up": 80.0, "flat": 50.0, "down": 20.0}) + _normalize_state_state(weekly_ma30_slope_state, {"up": 80.0, "flat": 50.0, "down": 20.0})) / 2.0)
                    )
                ),
                "weekly_phase_score_raw": float(
                    _clamp(
                        0.55 * _phase_score(label)
                        + 0.20 * ((int(row.above_ma10_count) + int(row.above_ma30_count)) / 2.0 / max(1, PERSISTENCE_CAP) * 100.0)
                        + 0.15 * candle["body_ratio"] * 100.0
                        + 0.10 * (100.0 if label.endswith("warning") else 0.0)
                    )
                ),
                "_weekly_candidate_scores": candidate_scores,
                "_weekly_extension_up": float(extension_up),
                "_weekly_extension_down": float(extension_down),
                "_weekly_body_ratio": float(candle["body_ratio"]),
                "_weekly_upper_wick_ratio": float(candle["upper_wick_ratio"]),
                "_weekly_lower_wick_ratio": float(candle["lower_wick_ratio"]),
            }
        )
    return pd.DataFrame(records)


def _build_daily_context_for_code(code_df: pd.DataFrame) -> pd.DataFrame:
    working = code_df.sort_values("date").reset_index(drop=True).copy()
    close = working["c"].astype(float)
    open_ = working["o"].astype(float)
    high = working["h"].astype(float)
    low = working["l"].astype(float)
    range_ = (high - low).replace(0, np.nan)
    body = (close - open_).abs()
    upper_wick = high - np.maximum(open_, close)
    lower_wick = np.minimum(open_, close) - low
    working["daily_ma7"] = close.rolling(7, min_periods=7).mean()
    working["daily_ma20"] = close.rolling(20, min_periods=20).mean()
    working["daily_ma60"] = close.rolling(60, min_periods=60).mean()
    working["daily_ma7_prev5"] = working["daily_ma7"].shift(5)
    working["daily_ma20_prev10"] = working["daily_ma20"].shift(10)
    working["daily_ma60_prev20"] = working["daily_ma60"].shift(20)
    working["daily_price_vs_ma7_state"] = [
        _price_vs_ma_state(float(close_value), float(ma7_value), PRICE_NEAR_THRESHOLD["daily"])
        for close_value, ma7_value in zip(working["c"], working["daily_ma7"])
    ]
    working["daily_price_vs_ma20_state"] = [
        _price_vs_ma_state(float(close_value), float(ma20_value), PRICE_NEAR_THRESHOLD["daily"])
        for close_value, ma20_value in zip(working["c"], working["daily_ma20"])
    ]
    working["daily_price_vs_ma60_state"] = [
        _price_vs_ma_state(float(close_value), float(ma60_value), PRICE_NEAR_THRESHOLD["daily"])
        for close_value, ma60_value in zip(working["c"], working["daily_ma60"])
    ]
    working["daily_ma7_slope_state"] = [
        _slope_state(float(current), float(previous), SLOPE_UP_THRESHOLD["daily"][7], SLOPE_DOWN_THRESHOLD["daily"][7])
        for current, previous in zip(working["daily_ma7"], working["daily_ma7_prev5"])
    ]
    working["daily_ma20_slope_state"] = [
        _slope_state(float(current), float(previous), SLOPE_UP_THRESHOLD["daily"][20], SLOPE_DOWN_THRESHOLD["daily"][20])
        for current, previous in zip(working["daily_ma20"], working["daily_ma20_prev10"])
    ]
    working["daily_alignment_state"] = [
        _alignment_state(
            close_state_short=short,
            close_state_mid=mid,
            close_state_long=long,
            slope_short=slope_short,
            slope_mid=slope_mid,
        )
        for short, mid, long, slope_short, slope_mid in zip(
            working["daily_price_vs_ma7_state"],
            working["daily_price_vs_ma20_state"],
            working["daily_price_vs_ma60_state"],
            working["daily_ma7_slope_state"],
            working["daily_ma20_slope_state"],
        )
    ]
    working["daily_body_ratio"] = (body / range_).fillna(0.0).clip(0.0, 1.0)
    working["daily_upper_wick_ratio"] = (upper_wick / range_).fillna(0.0).clip(0.0, 1.0)
    working["daily_lower_wick_ratio"] = (lower_wick / range_).fillna(0.0).clip(0.0, 1.0)
    working["daily_close_pos_in_range"] = ((close - low) / range_).fillna(0.0).clip(0.0, 1.0)
    working["daily_above_ma7_count"] = _streak_counts((working["c"].to_numpy(dtype=float) > working["daily_ma7"].fillna(np.inf).to_numpy(dtype=float)))
    working["daily_below_ma7_count"] = _streak_counts((working["c"].to_numpy(dtype=float) < working["daily_ma7"].fillna(-np.inf).to_numpy(dtype=float)))
    working["daily_above_ma20_count"] = _streak_counts((working["c"].to_numpy(dtype=float) > working["daily_ma20"].fillna(np.inf).to_numpy(dtype=float)))
    working["daily_below_ma20_count"] = _streak_counts((working["c"].to_numpy(dtype=float) < working["daily_ma20"].fillna(-np.inf).to_numpy(dtype=float)))

    records: list[dict[str, Any]] = []
    for idx, row in enumerate(working.itertuples(index=False)):
        prev = working.iloc[idx - 1] if idx > 0 else None
        prev2 = working.iloc[idx - 2] if idx > 1 else None
        daily_price_vs_ma7_state = str(getattr(row, "daily_price_vs_ma7_state"))
        daily_price_vs_ma20_state = str(getattr(row, "daily_price_vs_ma20_state"))
        daily_price_vs_ma60_state = str(getattr(row, "daily_price_vs_ma60_state"))
        daily_ma7_slope_state = str(getattr(row, "daily_ma7_slope_state"))
        daily_ma20_slope_state = str(getattr(row, "daily_ma20_slope_state"))
        daily_alignment_state = str(getattr(row, "daily_alignment_state"))
        close_value = float(row.c)
        ma7_value = float(row.daily_ma7) if math.isfinite(float(row.daily_ma7)) else close_value
        ma20_value = float(row.daily_ma20) if math.isfinite(float(row.daily_ma20)) else close_value
        ma60_value = float(row.daily_ma60) if math.isfinite(float(row.daily_ma60)) else close_value
        candle = _candle_metrics(float(row.o), float(row.h), float(row.l), float(row.c))
        extension_up = max(
            close_value / max(ma7_value, 1e-9) - 1.0,
            close_value / max(ma20_value, 1e-9) - 1.0,
            close_value / max(ma60_value, 1e-9) - 1.0,
        )
        extension_down = min(
            close_value / max(ma7_value, 1e-9) - 1.0,
            close_value / max(ma20_value, 1e-9) - 1.0,
            close_value / max(ma60_value, 1e-9) - 1.0,
        )
        persistence_above = max(int(row.daily_above_ma7_count), int(row.daily_above_ma20_count))
        persistence_below = max(int(row.daily_below_ma7_count), int(row.daily_below_ma20_count))
        bull_alignment = daily_alignment_state == "bull_stack"
        bear_alignment = daily_alignment_state == "bear_stack"
        compressed = daily_alignment_state == "compressed"
        prev_close = float(prev.c) if prev is not None else float(row.c)
        prev_open = float(prev.o) if prev is not None else float(row.o)
        prev_high = float(prev.h) if prev is not None else float(row.h)
        prev_low = float(prev.l) if prev is not None else float(row.l)
        prev_ma20 = float(prev.daily_ma20) if prev is not None and math.isfinite(float(prev.daily_ma20)) else ma20_value
        reclaim_ma20 = bool(prev is not None and math.isfinite(prev_ma20) and prev_close < prev_ma20 and close_value >= ma20_value)
        lose_ma20 = bool(prev is not None and math.isfinite(prev_ma20) and prev_close >= prev_ma20 and close_value < ma20_value)
        gap_up = bool(prev is not None and row.o >= (prev_close * (1.0 + GAP_THRESHOLD)))
        gap_down = bool(prev is not None and row.o <= (prev_close * (1.0 - GAP_THRESHOLD)))
        engulfing_bull = bool(
            prev is not None
            and close_value > float(row.o)
            and prev_close < prev_open
            and float(row.o) <= prev_close
            and close_value >= prev_open
            and candle["body_ratio"] >= 0.45
        )
        engulfing_bear = bool(
            prev is not None
            and close_value < float(row.o)
            and prev_close > prev_open
            and float(row.o) >= prev_close
            and close_value <= prev_open
            and candle["body_ratio"] >= 0.45
        )
        long_lower_wick = bool(candle["lower_wick_ratio"] >= WICK_LONG_THRESHOLD and candle["lower_wick_ratio"] >= candle["upper_wick_ratio"] * 1.2)
        long_upper_wick = bool(candle["upper_wick_ratio"] >= WICK_LONG_THRESHOLD and candle["upper_wick_ratio"] >= candle["lower_wick_ratio"] * 1.2)
        small_body = bool(candle["body_ratio"] <= BODY_SMALL_THRESHOLD)
        change_day_flag = bool(idx > 0 and str(getattr(row, "daily_alignment_state")) != str(getattr(working.iloc[idx - 1], "daily_alignment_state")))
        candidate_scores = {
            "daily_reversal_up_candidate": (
                78.0
                + (14.0 if reclaim_ma20 else 0.0)
                + (12.0 if engulfing_bull else 0.0)
                + (10.0 if long_lower_wick else 0.0)
                + (6.0 if daily_price_vs_ma20_state in {"below", "near"} else 0.0)
            ),
            "daily_reversal_down_candidate": (
                78.0
                + (14.0 if lose_ma20 else 0.0)
                + (12.0 if engulfing_bear else 0.0)
                + (10.0 if long_upper_wick else 0.0)
                + (6.0 if daily_price_vs_ma20_state in {"above", "near"} else 0.0)
            ),
            "daily_up_top_warning": (
                70.0
                + (16.0 if extension_up >= EXTENSION_THRESHOLD["daily"] else 0.0)
                + (10.0 if long_upper_wick else 0.0)
                + (8.0 if small_body else 0.0)
                + (5.0 if bull_alignment else 0.0)
            ),
            "daily_down_bottom_warning": (
                70.0
                + (16.0 if extension_down <= -EXTENSION_THRESHOLD["daily"] else 0.0)
                + (10.0 if long_lower_wick else 0.0)
                + (8.0 if small_body else 0.0)
                + (5.0 if bear_alignment else 0.0)
            ),
            "daily_up_mid": (
                60.0
                + (15.0 if bull_alignment else 0.0)
                + (10.0 if daily_ma7_slope_state == "up" else 0.0)
                + (10.0 if daily_ma20_slope_state in {"up", "flat"} else 0.0)
                + (5.0 if persistence_above >= 2 else 0.0)
            ),
            "daily_up_early": (
                50.0
                + (12.0 if daily_price_vs_ma7_state in {"above", "near"} else 0.0)
                + (10.0 if daily_ma7_slope_state == "up" else 0.0)
                + (8.0 if daily_price_vs_ma20_state != "below" else 0.0)
            ),
            "daily_down_mid": (
                60.0
                + (15.0 if bear_alignment else 0.0)
                + (10.0 if daily_ma7_slope_state == "down" else 0.0)
                + (10.0 if daily_ma20_slope_state in {"down", "flat"} else 0.0)
                + (5.0 if persistence_below >= 2 else 0.0)
            ),
            "daily_down_early": (
                50.0
                + (12.0 if daily_price_vs_ma7_state in {"below", "near"} else 0.0)
                + (10.0 if daily_ma7_slope_state == "down" else 0.0)
                + (8.0 if daily_price_vs_ma20_state != "above" else 0.0)
            ),
            "daily_range_mid": (
                46.0
                + (12.0 if daily_alignment_state == "mixed" else 0.0)
                + (8.0 if daily_price_vs_ma20_state == "near" else 0.0)
                + (5.0 if daily_ma7_slope_state == "flat" and daily_ma20_slope_state == "flat" else 0.0)
            ),
            "daily_range_late": (
                54.0
                + (12.0 if daily_alignment_state == "mixed" else 0.0)
                + (10.0 if max(persistence_above, persistence_below) >= PERSISTENCE_LATE_THRESHOLD else 0.0)
                + (6.0 if small_body else 0.0)
            ),
        }
        label = _select_label(candidate_scores, DAILY_PRIORITY, fallback="daily_range_mid")
        records.append(
            {
                "code": str(row.code),
                "date": int(row.date),
                "daily_main_state": label,
                "daily_price_vs_ma7_state": daily_price_vs_ma7_state,
                "daily_price_vs_ma20_state": daily_price_vs_ma20_state,
                "daily_price_vs_ma60_state": daily_price_vs_ma60_state,
                "daily_ma7_slope_state": daily_ma7_slope_state,
                "daily_ma20_slope_state": daily_ma20_slope_state,
                "daily_alignment_state": daily_alignment_state,
                "daily_above_ma7_count": int(row.daily_above_ma7_count),
                "daily_below_ma7_count": int(row.daily_below_ma7_count),
                "daily_above_ma20_count": int(row.daily_above_ma20_count),
                "daily_below_ma20_count": int(row.daily_below_ma20_count),
                "daily_gap_up_flag": gap_up,
                "daily_gap_down_flag": gap_down,
                "daily_engulfing_bull_flag": engulfing_bull,
                "daily_engulfing_bear_flag": engulfing_bear,
                "daily_reclaim_ma20_flag": reclaim_ma20,
                "daily_lose_ma20_flag": lose_ma20,
                "daily_long_lower_wick_flag": long_lower_wick,
                "daily_long_upper_wick_flag": long_upper_wick,
                "daily_small_body_flag": small_body,
                "daily_change_day_flag": change_day_flag,
                "daily_main_state_score": float(_main_state_score(label)),
                "daily_phase_score": float(_phase_score(label)),
                "daily_execution_score": float(
                    _clamp(
                        0.35 * _main_state_score(label)
                        + 0.20 * _normalize_state_state(daily_alignment_state, {"bull_stack": 90.0, "mixed": 45.0, "compressed": 55.0, "bear_stack": 12.0})
                        + 0.20 * ((_normalize_state_state(daily_price_vs_ma7_state, {"above": 80.0, "near": 50.0, "below": 20.0}) + _normalize_state_state(daily_price_vs_ma20_state, {"above": 80.0, "near": 50.0, "below": 20.0}) + _normalize_state_state(daily_price_vs_ma60_state, {"above": 80.0, "near": 50.0, "below": 20.0})) / 3.0)
                        + 0.15 * ((_normalize_state_state(daily_ma7_slope_state, {"up": 80.0, "flat": 50.0, "down": 20.0}) + _normalize_state_state(daily_ma20_slope_state, {"up": 80.0, "flat": 50.0, "down": 20.0})) / 2.0)
                        + 0.10 * ((35.0 if reclaim_ma20 else 0.0) + (30.0 if lose_ma20 else 0.0) + (20.0 if engulfing_bull or engulfing_bear else 0.0) + (15.0 if gap_up or gap_down else 0.0)) / 100.0 * 100.0
                    )
                ),
                "daily_phase_score_raw": float(
                    _clamp(
                        0.55 * _phase_score(label)
                        + 0.15 * ((int(row.daily_above_ma7_count) + int(row.daily_above_ma20_count)) / 2.0 / max(1, PERSISTENCE_CAP) * 100.0)
                        + 0.15 * candle["body_ratio"] * 100.0
                        + 0.15 * (100.0 if label.startswith("daily_reversal_") else 0.0)
                    )
                ),
                "trigger_signal_score": float(
                    _clamp(
                        (30.0 if gap_up or gap_down else 0.0)
                        + (40.0 if engulfing_bull or engulfing_bear else 0.0)
                        + (45.0 if reclaim_ma20 or lose_ma20 else 0.0)
                        + (20.0 if long_lower_wick or long_upper_wick else 0.0)
                        + (10.0 if small_body else 0.0)
                    )
                ),
                "_daily_candidate_scores": candidate_scores,
                "_daily_extension_up": float(extension_up),
                "_daily_extension_down": float(extension_down),
                "_daily_body_ratio": float(candle["body_ratio"]),
                "_daily_upper_wick_ratio": float(candle["upper_wick_ratio"]),
                "_daily_lower_wick_ratio": float(candle["lower_wick_ratio"]),
                "_daily_change_day_flag": bool(change_day_flag),
            }
        )
    return pd.DataFrame(records)


def _build_historical_similarity_scores(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    work = rows.sort_values(["sample_month", "code"]).copy()
    months = sorted(int(month) for month in work["sample_month"].dropna().unique().tolist())
    result_frames: list[pd.DataFrame] = []
    for month in months:
        month_frame = work[work["sample_month"] == month].copy()
        history = work[work["sample_month"] < month].copy()
        if history.empty:
            month_frame["monthly_label_history_rate"] = 0.5
            month_frame["weekly_label_history_rate"] = 0.5
            month_frame["daily_label_history_rate"] = 0.5
        else:
            rate_maps: dict[str, dict[str, float]] = {}
            for label_col in ("monthly_main_state", "weekly_main_state", "daily_main_state"):
                key_col = f"{label_col}_history_rate"
                by_state = history.groupby([label_col])["is_next_top10"].mean().to_dict()
                by_regime = history.groupby(["regime_tag", label_col])["is_next_top10"].mean().to_dict()
                values: list[float] = []
                for row in month_frame.itertuples(index=False):
                    state = str(getattr(row, label_col))
                    regime = str(getattr(row, "regime_tag"))
                    value = by_regime.get((regime, state), by_state.get(state, 0.5))
                    values.append(float(0.5 if pd.isna(value) else value))
                month_frame[key_col] = values
        month_frame["similarity_filter_score"] = (
            100.0
            * (
                0.4 * month_frame["monthly_main_state_history_rate"].fillna(0.5).astype(float)
                + 0.35 * month_frame["weekly_main_state_history_rate"].fillna(0.5).astype(float)
                + 0.25 * month_frame["daily_main_state_history_rate"].fillna(0.5).astype(float)
            )
        ).clip(0.0, 100.0)
        result_frames.append(month_frame)
    return pd.concat(result_frames, ignore_index=True)


def _build_label_dictionary() -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    main_states = {
        "monthly": [
            ("monthly_up_pre", "上昇前"),
            ("monthly_up_mid", "上昇中盤"),
            ("monthly_up_top_warning", "上昇天井警戒"),
            ("monthly_range_pre", "横ばい前"),
            ("monthly_range_mid", "横ばい中盤"),
            ("monthly_range_late", "横ばい終盤"),
            ("monthly_down_mid", "下落中盤"),
            ("monthly_down_bottom_warning", "下落底警戒"),
        ],
        "weekly": [
            ("weekly_up_early", "上昇初動"),
            ("weekly_up_mid", "上昇中盤"),
            ("weekly_up_late", "上昇終盤"),
            ("weekly_range_mid", "横ばい中盤"),
            ("weekly_range_late", "横ばい終盤"),
            ("weekly_down_early", "下落初動"),
            ("weekly_down_mid", "下落中盤"),
            ("weekly_down_bottom_warning", "下落底警戒"),
        ],
        "daily": [
            ("daily_up_early", "上昇初動"),
            ("daily_up_mid", "上昇中盤"),
            ("daily_up_top_warning", "上昇天井警戒"),
            ("daily_range_mid", "横ばい中盤"),
            ("daily_range_late", "横ばい終盤"),
            ("daily_down_early", "下落初動"),
            ("daily_down_mid", "下落中盤"),
            ("daily_down_bottom_warning", "下落底警戒"),
            ("daily_reversal_up_candidate", "上反転候補"),
            ("daily_reversal_down_candidate", "下反転候補"),
        ],
    }
    flag_entries = [
        ("monthly_change_day_flag", "monthly", "event_flag", "月足転換日"),
        ("weekly_change_day_flag", "weekly", "event_flag", "週足転換日"),
        ("daily_change_day_flag", "daily", "event_flag", "日足転換日"),
        ("daily_gap_up_flag", "daily", "trigger_flag", "ギャップアップ"),
        ("daily_gap_down_flag", "daily", "trigger_flag", "ギャップダウン"),
        ("daily_engulfing_bull_flag", "daily", "trigger_flag", "陽包み足"),
        ("daily_engulfing_bear_flag", "daily", "trigger_flag", "陰包み足"),
        ("daily_reclaim_ma20_flag", "daily", "trigger_flag", "MA20奪回"),
        ("daily_lose_ma20_flag", "daily", "trigger_flag", "MA20失陥"),
        ("daily_long_lower_wick_flag", "daily", "trigger_flag", "長下ヒゲ"),
        ("daily_long_upper_wick_flag", "daily", "trigger_flag", "長上ヒゲ"),
        ("daily_small_body_flag", "daily", "trigger_flag", "小陰陽線"),
    ]
    for timeframe, items in main_states.items():
        for label_id, jp_name in items:
            entries.append(
                {
                    "label_id": label_id,
                    "timeframe": timeframe,
                    "label_kind": "main_state",
                    "jp_name": jp_name,
                    "parent_group": f"{timeframe}_main_state",
                    "mutually_exclusive_group": f"{timeframe}_main_state",
                    "can_coexist_with": [
                        "monthly_change_day_flag",
                        "weekly_change_day_flag",
                        "daily_change_day_flag",
                        "daily_trigger_flags",
                    ],
                    "is_authoritative_output": True,
                    "notes": f"{timeframe} main state label",
                }
            )
    for label_id, timeframe, label_kind, jp_name in flag_entries:
        entries.append(
            {
                "label_id": label_id,
                "timeframe": timeframe,
                "label_kind": label_kind,
                "jp_name": jp_name,
                "parent_group": "change_day_event" if "change_day" in label_id else "daily_trigger_flags",
                "mutually_exclusive_group": None,
                "can_coexist_with": ["monthly_main_state", "weekly_main_state", "daily_main_state"],
                "is_authoritative_output": True,
                "notes": "event or trigger flag",
            }
        )
    return {
        "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
        "contract_name": "MA-aware hierarchical labeling contract",
        "label_count": int(len(entries)),
        "entries": entries,
    }


def _build_rules() -> dict[str, Any]:
    return {
        "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
        "ma_periods": {
            "monthly": list(MONTHLY_MA_PERIODS),
            "weekly": list(WEEKLY_MA_PERIODS),
            "daily": list(DAILY_MA_PERIODS),
        },
        "confirmation_status": {
            "monthly": "provisional",
            "weekly": "provisional",
            "daily": "confirmed",
        },
        "slope_threshold_definitions": {
            "monthly": {
                "lookback_bars": SLOPE_LOOKBACK["monthly"],
                "up_threshold": SLOPE_UP_THRESHOLD["monthly"],
                "down_threshold": SLOPE_DOWN_THRESHOLD["monthly"],
            },
            "weekly": {
                "lookback_bars": SLOPE_LOOKBACK["weekly"],
                "up_threshold": SLOPE_UP_THRESHOLD["weekly"],
                "down_threshold": SLOPE_DOWN_THRESHOLD["weekly"],
            },
            "daily": {
                "lookback_bars": SLOPE_LOOKBACK["daily"],
                "up_threshold": SLOPE_UP_THRESHOLD["daily"],
                "down_threshold": SLOPE_DOWN_THRESHOLD["daily"],
            },
        },
        "flat_threshold_definitions": {
            "price_vs_ma_near_threshold": PRICE_NEAR_THRESHOLD,
            "alignment_state_definitions": {
                "bull_stack": "price above short/mid/long MA with rising short and mid slope",
                "bear_stack": "price below short/mid/long MA with falling short and mid slope",
                "compressed": "price near short/mid/long MA with flat short and mid slope",
                "mixed": "everything else",
            },
        },
        "persistence_count_rules": {
            "cap": PERSISTENCE_CAP,
            "late_threshold": PERSISTENCE_LATE_THRESHOLD,
            "pre_threshold": PERSISTENCE_PRE_THRESHOLD,
            "representation": "raw capped count per MA plus bounded score bucket",
        },
        "candle_pattern_rules": {
            "gap_threshold": GAP_THRESHOLD,
            "engulfing_rule": "current body must engulf previous body and close in direction of the pattern",
            "long_wick_threshold": WICK_LONG_THRESHOLD,
            "small_body_threshold": BODY_SMALL_THRESHOLD,
        },
        "change_day_rules": {
            "definition": "main state differs from previous timeframe bar state",
            "monthly_change_day_flag": "monthly main state changed from prior monthly row",
            "weekly_change_day_flag": "weekly main state changed from prior weekly row",
            "daily_change_day_flag": "daily main state changed from prior daily row",
        },
        "main_state_assignment_rules": {
            "monthly": {
                "priority": MONTHLY_PRIORITY,
                "fallback": "monthly_range_mid",
                "labels": [
                    "monthly_up_pre",
                    "monthly_up_mid",
                    "monthly_up_top_warning",
                    "monthly_range_pre",
                    "monthly_range_mid",
                    "monthly_range_late",
                    "monthly_down_mid",
                    "monthly_down_bottom_warning",
                ],
            },
            "weekly": {
                "priority": WEEKLY_PRIORITY,
                "fallback": "weekly_range_mid",
                "labels": [
                    "weekly_up_early",
                    "weekly_up_mid",
                    "weekly_up_late",
                    "weekly_range_mid",
                    "weekly_range_late",
                    "weekly_down_early",
                    "weekly_down_mid",
                    "weekly_down_bottom_warning",
                ],
            },
            "daily": {
                "priority": DAILY_PRIORITY,
                "fallback": "daily_range_mid",
                "labels": [
                    "daily_up_early",
                    "daily_up_mid",
                    "daily_up_top_warning",
                    "daily_range_mid",
                    "daily_range_late",
                    "daily_down_early",
                    "daily_down_mid",
                    "daily_down_bottom_warning",
                    "daily_reversal_up_candidate",
                    "daily_reversal_down_candidate",
                ],
            },
        },
        "score_composition_rules": {
            "monthly_environment_score": {
                "state_weight": 0.35,
                "ma_weight": 0.25,
                "persistence_weight": 0.20,
                "trigger_weight": 0.20,
            },
            "monthly_phase_score": "phase progression derived from main state plus persistence",
            "weekly_trend_score": {
                "state_weight": 0.40,
                "ma_weight": 0.25,
                "persistence_weight": 0.20,
                "trigger_weight": 0.15,
            },
            "weekly_phase_score": "phase progression derived from main state plus persistence",
            "daily_execution_score": {
                "state_weight": 0.35,
                "ma_weight": 0.20,
                "persistence_weight": 0.20,
                "trigger_weight": 0.25,
            },
            "daily_phase_score": "phase progression derived from main state plus persistence",
            "change_day_score": {
                "monthly_change_day_flag": 30.0,
                "weekly_change_day_flag": 25.0,
                "daily_change_day_flag": 20.0,
                "daily_reclaim_ma20_flag": 10.0,
                "daily_lose_ma20_flag": 10.0,
                "daily_gap_up_flag": 5.0,
                "daily_gap_down_flag": 5.0,
                "daily_engulfing_bull_flag": 5.0,
                "daily_engulfing_bear_flag": 5.0,
            },
            "winner_promotion_score": {
                "state_label_weight": 0.45,
                "context_weight": 0.25,
                "similarity_filter_weight": 0.15,
                "trigger_weight": 0.15,
            },
            "loser_removal_score": {
                "state_label_weight": 0.45,
                "context_weight": 0.25,
                "similarity_filter_weight": 0.15,
                "trigger_weight": 0.15,
            },
        },
        "fallback_behavior": {
            "monthly": "monthly_range_mid with neutral scores",
            "weekly": "weekly_range_mid with neutral scores",
            "daily": "daily_range_mid with neutral scores",
            "trigger_flags": "false",
            "counts": 0,
            "scores": 50.0,
        },
    }


def _build_priority() -> dict[str, Any]:
    return {
        "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
        "timeframes": [
            {
                "timeframe": "monthly",
                "main_state_resolution_order": MONTHLY_PRIORITY,
                "event_flag_evaluation_order": [
                    "monthly_change_day_flag",
                    "weekly_change_day_flag",
                    "daily_change_day_flag",
                ],
                "trigger_flag_evaluation_order": TRIGGER_ORDER,
                "tie_break_logic": "compare candidate score first, then priority order, then prefer more directional states over range states",
                "insufficient_data_fallback_logic": "emit monthly_range_mid, zero persistence, all flags false, scores at 50",
            },
            {
                "timeframe": "weekly",
                "main_state_resolution_order": WEEKLY_PRIORITY,
                "event_flag_evaluation_order": [
                    "monthly_change_day_flag",
                    "weekly_change_day_flag",
                    "daily_change_day_flag",
                ],
                "trigger_flag_evaluation_order": TRIGGER_ORDER,
                "tie_break_logic": "compare candidate score first, then priority order, then prefer directional states over range states",
                "insufficient_data_fallback_logic": "emit weekly_range_mid, zero persistence, all flags false, scores at 50",
            },
            {
                "timeframe": "daily",
                "main_state_resolution_order": DAILY_PRIORITY,
                "event_flag_evaluation_order": [
                    "monthly_change_day_flag",
                    "weekly_change_day_flag",
                    "daily_change_day_flag",
                ],
                "trigger_flag_evaluation_order": TRIGGER_ORDER,
                "tie_break_logic": "compare candidate score first, then priority order, then prefer reversal and warning states over ordinary directional states",
                "insufficient_data_fallback_logic": "emit daily_range_mid, zero persistence, all flags false, scores at 50",
            },
        ],
    }


def _summarize_scores(rows: pd.DataFrame, score_columns: Iterable[str]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for column in score_columns:
        series = pd.to_numeric(rows[column], errors="coerce")
        summary[column] = {
            "count": int(series.count()),
            "mean": float(series.mean()) if series.count() else 0.0,
            "std": float(series.std(ddof=1)) if series.count() > 1 else 0.0,
            "min": float(series.min()) if series.count() else 0.0,
            "p25": float(series.quantile(0.25)) if series.count() else 0.0,
            "median": float(series.median()) if series.count() else 0.0,
            "p75": float(series.quantile(0.75)) if series.count() else 0.0,
            "max": float(series.max()) if series.count() else 0.0,
            "zero_count": int((series == 0).sum()) if series.count() else 0,
            "full_count": int((series >= 100).sum()) if series.count() else 0,
        }
    return summary


def _summarize_label_balance(rows: pd.DataFrame, label_col: str) -> dict[str, Any]:
    if rows.empty:
        return {}
    summary: dict[str, Any] = {}
    total = float(len(rows))
    for label, group in rows.groupby(label_col):
        summary[str(label)] = {
            "count": int(len(group)),
            "share": float(len(group) / total) if total else 0.0,
            "mean_next_month_return": float(pd.to_numeric(group["next_month_return"], errors="coerce").mean()) if len(group) else 0.0,
            "top10_rate": float(pd.to_numeric(group["is_next_top10"], errors="coerce").mean()) if len(group) else 0.0,
            "bottom10_rate": float(pd.to_numeric(group["is_next_bottom10"], errors="coerce").mean()) if len(group) else 0.0,
            "mean_top10_boundary_gap": float(pd.to_numeric(group["top10_boundary_gap"], errors="coerce").mean()) if len(group) else 0.0,
            "mean_winner_promotion_score": float(pd.to_numeric(group["winner_promotion_score"], errors="coerce").mean()) if "winner_promotion_score" in group.columns else 0.0,
            "mean_loser_removal_score": float(pd.to_numeric(group["loser_removal_score"], errors="coerce").mean()) if "loser_removal_score" in group.columns else 0.0,
        }
    return summary


def _state_family_summary(rows: pd.DataFrame) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for label_col in ("monthly_main_state", "weekly_main_state", "daily_main_state"):
        summary[label_col] = _summarize_label_balance(rows, label_col)
    return summary


def _regime_summary(rows: pd.DataFrame) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    if rows.empty:
        return summary
    total = float(len(rows))
    for regime, group in rows.groupby("regime_tag"):
        summary[str(regime)] = {
            "count": int(len(group)),
            "share": float(len(group) / total) if total else 0.0,
            "mean_next_month_return": float(pd.to_numeric(group["next_month_return"], errors="coerce").mean()) if len(group) else 0.0,
            "top10_rate": float(pd.to_numeric(group["is_next_top10"], errors="coerce").mean()) if len(group) else 0.0,
            "bottom10_rate": float(pd.to_numeric(group["is_next_bottom10"], errors="coerce").mean()) if len(group) else 0.0,
            "mean_boundary_gap": float(pd.to_numeric(group["top10_boundary_gap"], errors="coerce").mean()) if len(group) else 0.0,
            "dominant_monthly_main_state": str(group["monthly_main_state"].mode().iloc[0]) if not group["monthly_main_state"].mode().empty else "unknown",
            "dominant_weekly_main_state": str(group["weekly_main_state"].mode().iloc[0]) if not group["weekly_main_state"].mode().empty else "unknown",
            "dominant_daily_main_state": str(group["daily_main_state"].mode().iloc[0]) if not group["daily_main_state"].mode().empty else "unknown",
        }
    return summary


def _variant_score(
    rows: pd.DataFrame,
    *,
    variant: str,
) -> pd.Series:
    state_score = pd.to_numeric(rows["state_label_score"], errors="coerce").fillna(50.0)
    context_score = pd.to_numeric(rows["context_score"], errors="coerce").fillna(50.0)
    trigger_score = pd.to_numeric(rows["trigger_signal_score"], errors="coerce").fillna(50.0)
    similarity_score = pd.to_numeric(rows["similarity_filter_score"], errors="coerce").fillna(50.0)
    winner_score = pd.to_numeric(rows["winner_promotion_score"], errors="coerce").fillna(50.0)
    loser_score = pd.to_numeric(rows["loser_removal_score"], errors="coerce").fillna(50.0)
    baseline = pd.to_numeric(rows["rerank_score"], errors="coerce").fillna(50.0)

    if variant == "A":
        return baseline.clip(0.0, 100.0)
    if variant == "B":
        return (0.70 * baseline + 0.30 * state_score).clip(0.0, 100.0)
    if variant == "C":
        return (0.70 * baseline + 0.30 * context_score).clip(0.0, 100.0)
    if variant == "D":
        return (0.50 * baseline + 0.25 * state_score + 0.25 * context_score).clip(0.0, 100.0)
    if variant == "E":
        return (0.45 * baseline + 0.20 * state_score + 0.20 * context_score + 0.15 * trigger_score).clip(0.0, 100.0)
    if variant == "F":
        return (0.40 * baseline + 0.18 * state_score + 0.18 * context_score + 0.12 * trigger_score + 0.12 * similarity_score).clip(0.0, 100.0)
    if variant == "G":
        return (
            0.30 * baseline
            + 0.15 * state_score
            + 0.15 * context_score
            + 0.10 * trigger_score
            + 0.10 * similarity_score
            + 0.10 * winner_score
            + 0.10 * (100.0 - loser_score)
        ).clip(0.0, 100.0)
    raise ValueError(f"unknown_variant:{variant}")


def _variant_state_family_summary(rows: pd.DataFrame) -> dict[str, Any]:
    return {
        "monthly_main_state_mean": float(pd.to_numeric(rows["monthly_main_state_score"], errors="coerce").mean()) if "monthly_main_state_score" in rows.columns else 0.0,
        "weekly_main_state_mean": float(pd.to_numeric(rows["weekly_main_state_score"], errors="coerce").mean()) if "weekly_main_state_score" in rows.columns else 0.0,
        "daily_main_state_mean": float(pd.to_numeric(rows["daily_main_state_score"], errors="coerce").mean()) if "daily_main_state_score" in rows.columns else 0.0,
        "state_label_score_mean": float(pd.to_numeric(rows["state_label_score"], errors="coerce").mean()) if "state_label_score" in rows.columns else 0.0,
        "context_score_mean": float(pd.to_numeric(rows["context_score"], errors="coerce").mean()) if "context_score" in rows.columns else 0.0,
        "trigger_signal_score_mean": float(pd.to_numeric(rows["trigger_signal_score"], errors="coerce").mean()) if "trigger_signal_score" in rows.columns else 0.0,
        "similarity_filter_score_mean": float(pd.to_numeric(rows["similarity_filter_score"], errors="coerce").mean()) if "similarity_filter_score" in rows.columns else 0.0,
        "winner_promotion_score_mean": float(pd.to_numeric(rows["winner_promotion_score"], errors="coerce").mean()) if "winner_promotion_score" in rows.columns else 0.0,
        "loser_removal_score_mean": float(pd.to_numeric(rows["loser_removal_score"], errors="coerce").mean()) if "loser_removal_score" in rows.columns else 0.0,
    }


def _evaluate_variant(
    scored: pd.DataFrame,
    *,
    variant: str,
    top_k: int,
    candidate_pool_k: int,
) -> dict[str, Any]:
    working = scored.copy()
    working["variant_score"] = _variant_score(working, variant=variant)
    monthly_rows: list[dict[str, Any]] = []
    for month, month_frame in working.groupby("sample_month"):
        champion_ranked = month_frame.sort_values(["champion_score", "code"], ascending=[False, True]).copy()
        variant_ranked = month_frame.sort_values(["variant_score", "code"], ascending=[False, True]).copy()
        champion_top10 = champion_ranked.head(min(top_k, len(champion_ranked)))
        variant_top10 = variant_ranked.head(min(top_k, len(variant_ranked)))
        changed_top10_members = set(champion_top10["sample_id"]) ^ set(variant_top10["sample_id"])
        changed_top5_members = set(champion_ranked.head(min(5, len(champion_ranked)))["sample_id"]) ^ set(variant_ranked.head(min(5, len(variant_ranked)))["sample_id"])
        top10_boundary_champion = champion_ranked.iloc[min(top_k - 1, len(champion_ranked) - 1)] if len(champion_ranked) else None
        next_top10_boundary_champion = champion_ranked.iloc[min(top_k, len(champion_ranked) - 1)] if len(champion_ranked) > top_k else top10_boundary_champion
        top10_boundary_variant = variant_ranked.iloc[min(top_k - 1, len(variant_ranked) - 1)] if len(variant_ranked) else None
        next_top10_boundary_variant = variant_ranked.iloc[min(top_k, len(variant_ranked) - 1)] if len(variant_ranked) > top_k else top10_boundary_variant
        candidate_pool = variant_ranked.head(min(candidate_pool_k, len(variant_ranked)))
        champion_pool = champion_ranked.head(min(candidate_pool_k, len(champion_ranked)))
        removed_bad = champion_top10[(champion_top10["is_next_bottom10"] == 1) & (~champion_top10["sample_id"].isin(variant_top10["sample_id"]))]
        added_good = variant_top10[(variant_top10["is_next_top10"] == 1) & (~variant_top10["sample_id"].isin(champion_top10["sample_id"]))]
        monthly_rows.append(
            {
                "sample_month": int(month),
                "sample_count": int(len(month_frame)),
                "variant": variant,
                "champion_top10_hit_count": int(champion_top10["is_next_top10"].sum()) if len(champion_top10) else 0,
                "variant_top10_hit_count": int(variant_top10["is_next_top10"].sum()) if len(variant_top10) else 0,
                "oos_top10_uplift": float(variant_top10["is_next_top10"].sum() - champion_top10["is_next_top10"].sum()) if len(variant_top10) and len(champion_top10) else 0.0,
                "bad_pick_removal": int(len(removed_bad)),
                "changed_top10_members_count": int(len(changed_top10_members)),
                "changed_top5_members_count": int(len(changed_top5_members)),
                "changed_rank_count": int(
                    sum(
                        1
                        for sample_id in set(champion_pool["sample_id"]) | set(candidate_pool["sample_id"])
                        if int(champion_pool.set_index("sample_id").index.get_indexer([sample_id])[0]) if False else 0
                    )
                ),
                "winner_promotion_delta": float(variant_top10["next_month_return"].mean() - champion_top10["next_month_return"].mean()) if len(variant_top10) and len(champion_top10) else 0.0,
                "loser_removal_delta": float(len(removed_bad)),
                "champion_boundary_gap": float(top10_boundary_champion["next_month_return"] - next_top10_boundary_champion["next_month_return"]) if top10_boundary_champion is not None and next_top10_boundary_champion is not None else 0.0,
                "variant_boundary_gap": float(top10_boundary_variant["next_month_return"] - next_top10_boundary_variant["next_month_return"]) if top10_boundary_variant is not None and next_top10_boundary_variant is not None else 0.0,
                "boundary_improved": bool(
                    top10_boundary_variant is not None
                    and next_top10_boundary_variant is not None
                    and float(top10_boundary_variant["next_month_return"] - next_top10_boundary_variant["next_month_return"])
                    > float(top10_boundary_champion["next_month_return"] - next_top10_boundary_champion["next_month_return"])
                ),
                "state_family_contribution_summary": _variant_state_family_summary(variant_ranked.head(min(top_k, len(variant_ranked)))),
                "regime_tag": str(month_frame["regime_tag"].iloc[0]) if "regime_tag" in month_frame.columns and not month_frame.empty else "unknown",
            }
        )
    monthly_df = pd.DataFrame(monthly_rows)
    if monthly_df.empty:
        return {
            "variant": variant,
            "month_count": 0,
            "sample_count": 0,
            "oos_top10_uplift": 0.0,
            "oos_bad_pick_removal": 0.0,
            "changed_top10_members_count": 0.0,
            "changed_top5_members_count": 0.0,
            "changed_rank_count": 0.0,
            "top10_boundary_improved": False,
            "winner_promotion_delta": 0.0,
            "loser_removal_delta": 0.0,
            "state_family_contribution_summary": {},
            "regime_breakdown": {},
            "monthly_rows": [],
        }
    regime_breakdown: dict[str, Any] = {}
    for regime, regime_df in monthly_df.groupby("regime_tag"):
        regime_breakdown[str(regime)] = {
            "month_count": int(len(regime_df)),
            "mean_oos_top10_uplift": float(regime_df["oos_top10_uplift"].mean()),
            "mean_bad_pick_removal": float(regime_df["bad_pick_removal"].mean()),
            "mean_changed_top10_members_count": float(regime_df["changed_top10_members_count"].mean()),
            "mean_variant_boundary_gap": float(regime_df["variant_boundary_gap"].mean()),
        }
    boundary_improved = bool(monthly_df["boundary_improved"].mean() > 0.5)
    return {
        "variant": variant,
        "month_count": int(len(monthly_df)),
        "sample_count": int(monthly_df["sample_count"].sum()),
        "oos_top10_uplift": float(monthly_df["oos_top10_uplift"].mean()),
        "oos_bad_pick_removal": float(monthly_df["bad_pick_removal"].mean()),
        "changed_top10_members_count": float(monthly_df["changed_top10_members_count"].mean()),
        "changed_top5_members_count": float(monthly_df["changed_top5_members_count"].mean()),
        "changed_rank_count": float(monthly_df["changed_rank_count"].mean()),
        "top10_boundary_improved": boundary_improved,
        "winner_promotion_delta": float(monthly_df["winner_promotion_delta"].mean()),
        "loser_removal_delta": float(monthly_df["loser_removal_delta"].mean()),
        "state_family_contribution_summary": _variant_state_family_summary(variant_ranked.head(min(top_k, len(variant_ranked)))),
        "regime_breakdown": regime_breakdown,
        "monthly_rows": monthly_df.to_dict(orient="records"),
    }


def _build_ablation_compare(scored: pd.DataFrame, *, top_k: int, candidate_pool_k: int) -> dict[str, Any]:
    variants = ["A", "B", "C", "D", "E", "F", "G"]
    variant_payloads = {
        variant: _evaluate_variant(scored, variant=variant, top_k=top_k, candidate_pool_k=candidate_pool_k)
        for variant in variants
    }
    best_variant = max(variant_payloads.items(), key=lambda item: (float(item[1].get("oos_top10_uplift") or 0.0), float(item[1].get("winner_promotion_delta") or 0.0)))
    if float(best_variant[1].get("oos_top10_uplift") or 0.0) > 0.0 and bool(best_variant[1].get("top10_boundary_improved")):
        decision = "keep"
        reason = "stable_boundary_uplift_with_positive_top10_gain"
    elif float(best_variant[1].get("oos_top10_uplift") or 0.0) > 0.0:
        decision = "hold"
        reason = "positive_top10_gain_without_clear_boundary_improvement"
    else:
        decision = "drop"
        reason = "no_stable_oos_uplift"
    return {
        "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
        "top_k": int(top_k),
        "candidate_pool_k": int(candidate_pool_k),
        "variants": variant_payloads,
        "best_variant": best_variant[0],
        "decision": decision,
        "decision_reason_typed": reason,
    }


def build_hierarchical_label_artifacts(
    *,
    source_frame: pd.DataFrame,
    expanding_scored: pd.DataFrame,
    rolling_scored: pd.DataFrame,
    top_k: int,
    candidate_pool_k: int,
) -> dict[str, Any]:
    if expanding_scored.empty:
        empty_frame = pd.DataFrame()
        dictionary = _build_label_dictionary()
        rules = _build_rules()
        priority = _build_priority()
        summary = {
            "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
            "row_count": 0,
            "label_balance": {},
            "scores": {},
            "selection": "empty",
        }
        return {
            "dictionary": dictionary,
            "rules": rules,
            "priority": priority,
            "rows": empty_frame,
            "summary": summary,
            "score_summary": {},
            "effect_by_state": {},
            "effect_by_regime": {},
            "ablation_compare": {"schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION, "variants": {}},
        }

    # Build one hierarchical row frame off the expanding split, then merge the
    # derived contract back into both expanding and rolling frames for ablation.
    code_frames: list[pd.DataFrame] = []
    for _, code_df in source_frame.groupby("code", sort=True):
        monthly = _build_monthly_context_for_code(code_df)
        weekly = _build_weekly_context_for_code(code_df)
        daily = _build_daily_context_for_code(code_df)
        monthly["code"] = monthly["code"].astype(str)
        weekly["code"] = weekly["code"].astype(str)
        daily["code"] = daily["code"].astype(str)
        code_frames.append(pd.concat([monthly, weekly, daily], ignore_index=True, sort=False))

    monthly_rows = pd.concat(code_frames, ignore_index=True, sort=False)
    monthly_rows = monthly_rows.rename(columns={"code": "symbol"})

    expanding = expanding_scored.copy()
    expanding["symbol"] = expanding["code"].astype(str)
    expanding["as_of_month"] = expanding["sample_month"].astype(int)
    expanding = expanding.rename(
        columns={
            "feature_window_start_date": "source_window_start",
            "feature_window_end_date": "source_window_end",
        }
    )
    expanding = expanding.merge(
        monthly_rows[monthly_rows["month_key"].notna()].copy(),
        left_on=["symbol", "month_end_date"],
        right_on=["symbol", "month_end_date"],
        how="left",
        suffixes=("", "_hier"),
    )
    expanding = expanding.merge(
        monthly_rows[monthly_rows["week_period"].notna()].copy().rename(columns={"week_end_date": "week_asof_date"}),
        left_on=["symbol"],
        right_on=["symbol"],
        how="left",
        suffixes=("", "_week"),
    )
    expanding = expanding.merge(
        monthly_rows[monthly_rows["date"].notna()].copy().rename(columns={"date": "daily_asof_date"}),
        left_on=["symbol", "month_end_date"],
        right_on=["symbol", "daily_asof_date"],
        how="left",
        suffixes=("", "_day"),
    )
    if "week_end_date" in expanding.columns:
        expanding = expanding[expanding["week_end_date"].isna() | (expanding["week_end_date"] <= expanding["month_end_date"])].copy()
    if "week_end_date" in expanding.columns:
        expanding = expanding.sort_values(["symbol", "month_end_date", "week_end_date"]).groupby(["sample_id"], as_index=False).tail(1)

    # The above merge approach can create duplicates for weekly context; clean by sample_id.
    if "sample_id" in expanding.columns:
        expanding = expanding.sort_values(["sample_id", "week_end_date" if "week_end_date" in expanding.columns else "sample_id"]).groupby("sample_id", as_index=False).tail(1)

    # Reconstruct the public row contract from the expanding frame.
    rows = expanding_scored.merge(
        expanding[
            [
                "sample_id",
                "symbol",
                "as_of_month",
                "source_window_start",
                "source_window_end",
            ]
            + [col for col in expanding.columns if col in {
                "monthly_main_state",
                "monthly_price_vs_ma12_state",
                "monthly_price_vs_ma24_state",
                "monthly_ma12_slope_state",
                "monthly_ma24_slope_state",
                "monthly_alignment_state",
                "monthly_above_ma12_count",
                "monthly_below_ma12_count",
                "monthly_above_ma24_count",
                "monthly_below_ma24_count",
                "monthly_main_state_score",
                "monthly_phase_score",
                "monthly_environment_score",
                "monthly_phase_score_raw",
                "weekly_main_state",
                "weekly_price_vs_ma10_state",
                "weekly_price_vs_ma30_state",
                "weekly_price_vs_ma60_state",
                "weekly_ma10_slope_state",
                "weekly_ma30_slope_state",
                "weekly_alignment_state",
                "weekly_above_ma10_count",
                "weekly_below_ma10_count",
                "weekly_above_ma30_count",
                "weekly_below_ma30_count",
                "weekly_main_state_score",
                "weekly_phase_score",
                "weekly_trend_score",
                "weekly_phase_score_raw",
                "daily_main_state",
                "daily_price_vs_ma7_state",
                "daily_price_vs_ma20_state",
                "daily_price_vs_ma60_state",
                "daily_ma7_slope_state",
                "daily_ma20_slope_state",
                "daily_alignment_state",
                "daily_above_ma7_count",
                "daily_below_ma7_count",
                "daily_above_ma20_count",
                "daily_below_ma20_count",
                "daily_gap_up_flag",
                "daily_gap_down_flag",
                "daily_engulfing_bull_flag",
                "daily_engulfing_bear_flag",
                "daily_reclaim_ma20_flag",
                "daily_lose_ma20_flag",
                "daily_long_lower_wick_flag",
                "daily_long_upper_wick_flag",
                "daily_small_body_flag",
                "daily_change_day_flag",
                "daily_main_state_score",
                "daily_phase_score",
                "daily_execution_score",
                "daily_phase_score_raw",
                "trigger_signal_score",
                "_monthly_candidate_scores",
                "_weekly_candidate_scores",
                "_daily_candidate_scores",
                "_monthly_extension_up",
                "_monthly_extension_down",
                "_monthly_body_ratio",
                "_monthly_upper_wick_ratio",
                "_monthly_lower_wick_ratio",
                "_weekly_extension_up",
                "_weekly_extension_down",
                "_weekly_body_ratio",
                "_weekly_upper_wick_ratio",
                "_weekly_lower_wick_ratio",
                "_daily_extension_up",
                "_daily_extension_down",
                "_daily_body_ratio",
                "_daily_upper_wick_ratio",
                "_daily_lower_wick_ratio",
                "_daily_change_day_flag",
            }]
        ],
        on="sample_id",
        how="left",
        suffixes=("", "_hier"),
    )

    # Overwrite current split names with the contract names and add score layers.
    rows["symbol"] = rows["code"].astype(str)
    rows["as_of_month"] = rows["sample_month"].astype(int)
    rows["source_window_start"] = rows["source_window_start"].fillna(rows["feature_window_start_date"])
    rows["source_window_end"] = rows["source_window_end"].fillna(rows["feature_window_end_date"])
    rows["month_end_date"] = rows["month_end_date"].fillna(rows["month_end_date"])
    rows["monthly_change_day_flag"] = rows["monthly_main_state"].ne(rows.groupby("symbol")["monthly_main_state"].shift(1)).fillna(False)
    rows["weekly_change_day_flag"] = rows["weekly_main_state"].ne(rows.groupby("symbol")["weekly_main_state"].shift(1)).fillna(False)
    rows["daily_change_day_flag"] = rows["daily_change_day_flag"].fillna(False)
    rows["monthly_change_day_flag"] = rows["monthly_change_day_flag"].astype(bool)
    rows["weekly_change_day_flag"] = rows["weekly_change_day_flag"].astype(bool)
    rows["daily_change_day_flag"] = rows["daily_change_day_flag"].astype(bool)

    rows["state_label_score"] = (
        0.35 * pd.to_numeric(rows["monthly_main_state_score"], errors="coerce").fillna(50.0)
        + 0.35 * pd.to_numeric(rows["weekly_main_state_score"], errors="coerce").fillna(50.0)
        + 0.30 * pd.to_numeric(rows["daily_main_state_score"], errors="coerce").fillna(50.0)
    ).clip(0.0, 100.0)
    rows["context_score"] = (
        0.25 * pd.to_numeric(rows["monthly_environment_score"], errors="coerce").fillna(50.0)
        + 0.15 * pd.to_numeric(rows["monthly_phase_score_raw"], errors="coerce").fillna(50.0)
        + 0.20 * pd.to_numeric(rows["weekly_trend_score"], errors="coerce").fillna(50.0)
        + 0.10 * pd.to_numeric(rows["weekly_phase_score_raw"], errors="coerce").fillna(50.0)
        + 0.15 * pd.to_numeric(rows["daily_execution_score"], errors="coerce").fillna(50.0)
        + 0.05 * pd.to_numeric(rows["daily_phase_score_raw"], errors="coerce").fillna(50.0)
        + 0.10 * (
            30.0 * rows["monthly_change_day_flag"].astype(float)
            + 25.0 * rows["weekly_change_day_flag"].astype(float)
            + 20.0 * rows["daily_change_day_flag"].astype(float)
        )
    ).clip(0.0, 100.0)
    rows["trigger_signal_score"] = pd.to_numeric(rows["trigger_signal_score"], errors="coerce").fillna(50.0).clip(0.0, 100.0)
    rows["similarity_filter_score"] = pd.to_numeric(rows["similarity_filter_score"], errors="coerce").fillna(50.0).clip(0.0, 100.0)
    rows["winner_promotion_score"] = (
        0.45 * rows["state_label_score"]
        + 0.25 * rows["context_score"]
        + 0.15 * rows["similarity_filter_score"]
        + 0.15 * rows["trigger_signal_score"]
    ).clip(0.0, 100.0)
    rows["loser_removal_score"] = (
        0.45 * (100.0 - rows["state_label_score"])
        + 0.25 * (100.0 - rows["context_score"])
        + 0.15 * (100.0 - rows["similarity_filter_score"])
        + 0.15 * (100.0 - rows["trigger_signal_score"])
    ).clip(0.0, 100.0)

    rows["monthly_change_day_flag"] = rows["monthly_change_day_flag"].astype(bool)
    rows["weekly_change_day_flag"] = rows["weekly_change_day_flag"].astype(bool)
    rows["daily_change_day_flag"] = rows["daily_change_day_flag"].astype(bool)

    rows["symbol"] = rows["symbol"].astype(str)
    rows["as_of_month"] = pd.to_numeric(rows["as_of_month"], errors="coerce").fillna(0).astype(int)
    rows["source_window_start"] = pd.to_numeric(rows["source_window_start"], errors="coerce").fillna(rows["month_end_date"]).astype(int)
    rows["source_window_end"] = pd.to_numeric(rows["source_window_end"], errors="coerce").fillna(rows["month_end_date"]).astype(int)

    rows = _build_historical_similarity_scores(rows)
    rows["similarity_filter_score"] = pd.to_numeric(rows["similarity_filter_score"], errors="coerce").fillna(50.0).clip(0.0, 100.0)
    rows["winner_promotion_score"] = (
        0.45 * rows["state_label_score"]
        + 0.25 * rows["context_score"]
        + 0.15 * rows["similarity_filter_score"]
        + 0.15 * rows["trigger_signal_score"]
    ).clip(0.0, 100.0)
    rows["loser_removal_score"] = (
        0.45 * (100.0 - rows["state_label_score"])
        + 0.25 * (100.0 - rows["context_score"])
        + 0.15 * (100.0 - rows["similarity_filter_score"])
        + 0.15 * (100.0 - rows["trigger_signal_score"])
    ).clip(0.0, 100.0)

    rows["change_day_score"] = (
        30.0 * rows["monthly_change_day_flag"].astype(float)
        + 25.0 * rows["weekly_change_day_flag"].astype(float)
        + 20.0 * rows["daily_change_day_flag"].astype(float)
        + 10.0 * rows["daily_reclaim_ma20_flag"].astype(float)
        + 10.0 * rows["daily_lose_ma20_flag"].astype(float)
        + 5.0 * rows["daily_gap_up_flag"].astype(float)
        + 5.0 * rows["daily_gap_down_flag"].astype(float)
        + 5.0 * rows["daily_engulfing_bull_flag"].astype(float)
        + 5.0 * rows["daily_engulfing_bear_flag"].astype(float)
    ).clip(0.0, 100.0)

    output_columns = [
        "sample_id",
        "symbol",
        "as_of_month",
        "source_window_start",
        "source_window_end",
        "next_month_return",
        "next_month_return_rank",
        "next_month_rank_pct",
        "is_next_top10",
        "is_next_bottom10",
        "top10_boundary_gap",
        "monthly_main_state",
        "weekly_main_state",
        "daily_main_state",
        "monthly_change_day_flag",
        "weekly_change_day_flag",
        "daily_change_day_flag",
        "daily_gap_up_flag",
        "daily_gap_down_flag",
        "daily_engulfing_bull_flag",
        "daily_engulfing_bear_flag",
        "daily_reclaim_ma20_flag",
        "daily_lose_ma20_flag",
        "daily_long_lower_wick_flag",
        "daily_long_upper_wick_flag",
        "daily_small_body_flag",
        "monthly_price_vs_ma12_state",
        "monthly_price_vs_ma24_state",
        "monthly_ma12_slope_state",
        "monthly_ma24_slope_state",
        "monthly_alignment_state",
        "weekly_price_vs_ma10_state",
        "weekly_price_vs_ma30_state",
        "weekly_price_vs_ma60_state",
        "weekly_ma10_slope_state",
        "weekly_ma30_slope_state",
        "weekly_alignment_state",
        "daily_price_vs_ma7_state",
        "daily_price_vs_ma20_state",
        "daily_price_vs_ma60_state",
        "daily_ma7_slope_state",
        "daily_ma20_slope_state",
        "daily_alignment_state",
        "monthly_above_ma12_count",
        "monthly_below_ma12_count",
        "monthly_above_ma24_count",
        "monthly_below_ma24_count",
        "weekly_above_ma10_count",
        "weekly_below_ma10_count",
        "weekly_above_ma30_count",
        "weekly_below_ma30_count",
        "daily_above_ma7_count",
        "daily_below_ma7_count",
        "daily_above_ma20_count",
        "daily_below_ma20_count",
        "monthly_environment_score",
        "monthly_phase_score",
        "weekly_trend_score",
        "weekly_phase_score",
        "daily_execution_score",
        "daily_phase_score",
        "change_day_score",
        "state_label_score",
        "context_score",
        "trigger_signal_score",
        "similarity_filter_score",
        "winner_promotion_score",
        "loser_removal_score",
        "monthly_main_state_score",
        "weekly_main_state_score",
        "daily_main_state_score",
        "regime_tag",
    ]
    rows = rows[output_columns].copy()

    dictionary = _build_label_dictionary()
    rules = _build_rules()
    priority = _build_priority()
    score_columns = [
        "monthly_environment_score",
        "monthly_phase_score",
        "weekly_trend_score",
        "weekly_phase_score",
        "daily_execution_score",
        "daily_phase_score",
        "change_day_score",
        "state_label_score",
        "context_score",
        "trigger_signal_score",
        "similarity_filter_score",
        "winner_promotion_score",
        "loser_removal_score",
        "monthly_main_state_score",
        "weekly_main_state_score",
        "daily_main_state_score",
    ]
    summary = {
        "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
        "row_count": int(len(rows)),
        "symbol_count": int(rows["symbol"].nunique()),
        "month_count": int(rows["as_of_month"].nunique()),
        "label_balance": _state_family_summary(rows),
        "score_summary": _summarize_scores(rows, score_columns),
        "score_columns": score_columns,
        "labels_with_top10_share_over_50pct": [
            label
            for label, payload in _summarize_label_balance(rows, "monthly_main_state").items()
            if float(payload.get("top10_rate") or 0.0) >= 0.5
        ],
        "artifact_contract": {
            "dictionary": "hierarchical_label_dictionary.json",
            "rules": "hierarchical_label_rules.json",
            "priority": "hierarchical_label_priority.json",
            "rows": "monthly_labels_hierarchical.parquet",
        },
    }
    effect_by_state = _state_family_summary(rows)
    effect_by_regime = _regime_summary(rows)
    ablation_compare = _build_ablation_compare(
        expanding_scored.assign(
            monthly_main_state=rows["monthly_main_state"].values,
            weekly_main_state=rows["weekly_main_state"].values,
            daily_main_state=rows["daily_main_state"].values,
            monthly_environment_score=rows["monthly_environment_score"].values,
            monthly_phase_score=rows["monthly_phase_score"].values,
            weekly_trend_score=rows["weekly_trend_score"].values,
            weekly_phase_score=rows["weekly_phase_score"].values,
            daily_execution_score=rows["daily_execution_score"].values,
            daily_phase_score=rows["daily_phase_score"].values,
            change_day_score=rows["change_day_score"].values,
            state_label_score=rows["state_label_score"].values,
            context_score=rows["context_score"].values,
            trigger_signal_score=rows["trigger_signal_score"].values,
            similarity_filter_score=rows["similarity_filter_score"].values,
            winner_promotion_score=rows["winner_promotion_score"].values,
            loser_removal_score=rows["loser_removal_score"].values,
        ),
        top_k=top_k,
        candidate_pool_k=candidate_pool_k,
    )
    return {
        "dictionary": dictionary,
        "rules": rules,
        "priority": priority,
        "rows": rows,
        "summary": summary,
        "score_summary": summary["score_summary"],
        "effect_by_state": effect_by_state,
        "effect_by_regime": effect_by_regime,
        "ablation_compare": ablation_compare,
    }
