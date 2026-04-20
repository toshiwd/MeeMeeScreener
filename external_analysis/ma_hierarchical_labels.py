from __future__ import annotations

import json
import math
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

MAIN_STATE_SPECS: dict[str, list[tuple[str, str]]] = {
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

EVENT_FLAG_SPECS: list[tuple[str, str, str, str]] = [
    ("monthly_change_day_flag", "monthly", "change_day_event", "月次切替フラグ"),
    ("weekly_change_day_flag", "weekly", "change_day_event", "週次切替フラグ"),
    ("daily_change_day_flag", "daily", "change_day_event", "日次切替フラグ"),
]

TRIGGER_FLAG_SPECS: list[tuple[str, str, str, str]] = [
    ("daily_gap_up_flag", "daily", "daily_trigger_flags", "ギャップ上"),
    ("daily_gap_down_flag", "daily", "daily_trigger_flags", "ギャップ下"),
    ("daily_engulfing_bull_flag", "daily", "daily_trigger_flags", "強気包み足"),
    ("daily_engulfing_bear_flag", "daily", "daily_trigger_flags", "弱気包み足"),
    ("daily_reclaim_ma20_flag", "daily", "daily_trigger_flags", "MA20再奪還"),
    ("daily_lose_ma20_flag", "daily", "daily_trigger_flags", "MA20割れ"),
    ("daily_long_lower_wick_flag", "daily", "daily_trigger_flags", "長い下ヒゲ"),
    ("daily_long_upper_wick_flag", "daily", "daily_trigger_flags", "長い上ヒゲ"),
    ("daily_small_body_flag", "daily", "daily_trigger_flags", "小さい実体"),
]

_STATE_SCORE_MAP = {"above": 80.0, "near": 50.0, "below": 20.0}
_SLOPE_SCORE_MAP = {"up": 80.0, "flat": 50.0, "down": 20.0}
_ALIGNMENT_SCORE_MAP = {"bull_stack": 90.0, "compressed": 60.0, "mixed": 45.0, "bear_stack": 10.0}


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


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return float(max(low, min(high, float(value))))


def _safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(fallback)
    if not math.isfinite(out):
        return float(fallback)
    return float(out)


def _score_map(value: Any, mapping: dict[str, float], fallback: float = 50.0) -> float:
    return float(mapping.get(_text(value), fallback))


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


def _score_streak_series(values: pd.Series) -> pd.Series:
    series = pd.to_numeric(values, errors="coerce").fillna(0).clip(lower=0, upper=PERSISTENCE_CAP)
    return series.apply(lambda value: _score_streak(int(value)))


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
    }


def _select_label(candidate_scores: dict[str, float], priority: list[str], *, fallback: str) -> str:
    best_label = fallback
    best_score = float("-inf")
    best_rank = len(priority) + 1
    for label, score in candidate_scores.items():
        score = float(score)
        if score > best_score:
            best_label = label
            best_score = score
            best_rank = priority.index(label) if label in priority else len(priority) + 1
            continue
        if score == best_score:
            rank = priority.index(label) if label in priority else len(priority) + 1
            if rank < best_rank:
                best_label = label
                best_rank = rank
    return best_label


def _main_state_score(label: str) -> float:
    if label.startswith("monthly_"):
        return {
            "monthly_up_pre": 62.0,
            "monthly_up_mid": 80.0,
            "monthly_up_top_warning": 56.0,
            "monthly_range_pre": 42.0,
            "monthly_range_mid": 50.0,
            "monthly_range_late": 60.0,
            "monthly_down_mid": 32.0,
            "monthly_down_bottom_warning": 24.0,
        }.get(label, 50.0)
    if label.startswith("weekly_"):
        return {
            "weekly_up_early": 64.0,
            "weekly_up_mid": 78.0,
            "weekly_up_late": 70.0,
            "weekly_range_mid": 48.0,
            "weekly_range_late": 56.0,
            "weekly_down_early": 36.0,
            "weekly_down_mid": 28.0,
            "weekly_down_bottom_warning": 22.0,
        }.get(label, 50.0)
    if label.startswith("daily_"):
        return {
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
        }.get(label, 50.0)
    return 50.0


def _phase_score(label: str) -> float:
    if label.startswith("monthly_"):
        return {
            "monthly_up_pre": 42.0,
            "monthly_up_mid": 66.0,
            "monthly_up_top_warning": 84.0,
            "monthly_range_pre": 26.0,
            "monthly_range_mid": 46.0,
            "monthly_range_late": 66.0,
            "monthly_down_mid": 54.0,
            "monthly_down_bottom_warning": 86.0,
        }.get(label, 50.0)
    if label.startswith("weekly_"):
        return {
            "weekly_up_early": 40.0,
            "weekly_up_mid": 64.0,
            "weekly_up_late": 80.0,
            "weekly_range_mid": 30.0,
            "weekly_range_late": 54.0,
            "weekly_down_early": 36.0,
            "weekly_down_mid": 56.0,
            "weekly_down_bottom_warning": 86.0,
        }.get(label, 50.0)
    if label.startswith("daily_"):
        return {
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
        }.get(label, 50.0)
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
    monthly["month_end_date"] = pd.to_datetime(monthly["month_end_ts"], utc=True, errors="coerce").dt.strftime("%Y%m%d").astype("Int64")
    close = monthly["c"].astype(float)
    monthly["ma6"] = close.rolling(6, min_periods=6).mean()
    monthly["ma12"] = close.rolling(12, min_periods=12).mean()
    monthly["ma24"] = close.rolling(24, min_periods=24).mean()
    monthly["ma6_prev3"] = monthly["ma6"].shift(3)
    monthly["ma12_prev3"] = monthly["ma12"].shift(3)
    monthly["ma24_prev6"] = monthly["ma24"].shift(6)
    monthly["price_vs_ma6_state"] = [_price_vs_ma_state(float(c), float(m), PRICE_NEAR_THRESHOLD["monthly"]) for c, m in zip(monthly["c"], monthly["ma6"])]
    monthly["price_vs_ma12_state"] = [_price_vs_ma_state(float(c), float(m), PRICE_NEAR_THRESHOLD["monthly"]) for c, m in zip(monthly["c"], monthly["ma12"])]
    monthly["price_vs_ma24_state"] = [_price_vs_ma_state(float(c), float(m), PRICE_NEAR_THRESHOLD["monthly"]) for c, m in zip(monthly["c"], monthly["ma24"])]
    monthly["ma6_slope_state"] = [_slope_state(float(cur), float(prev), SLOPE_UP_THRESHOLD["monthly"][6], SLOPE_DOWN_THRESHOLD["monthly"][6]) for cur, prev in zip(monthly["ma6"], monthly["ma6_prev3"])]
    monthly["ma12_slope_state"] = [_slope_state(float(cur), float(prev), SLOPE_UP_THRESHOLD["monthly"][12], SLOPE_DOWN_THRESHOLD["monthly"][12]) for cur, prev in zip(monthly["ma12"], monthly["ma12_prev3"])]
    monthly["ma24_slope_state"] = [_slope_state(float(cur), float(prev), SLOPE_UP_THRESHOLD["monthly"][24], SLOPE_DOWN_THRESHOLD["monthly"][24]) for cur, prev in zip(monthly["ma24"], monthly["ma24_prev6"])]
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
    monthly["above_ma12_count"] = _streak_counts(monthly["c"].to_numpy(dtype=float) > monthly["ma12"].fillna(np.inf).to_numpy(dtype=float))
    monthly["below_ma12_count"] = _streak_counts(monthly["c"].to_numpy(dtype=float) < monthly["ma12"].fillna(-np.inf).to_numpy(dtype=float))
    monthly["above_ma24_count"] = _streak_counts(monthly["c"].to_numpy(dtype=float) > monthly["ma24"].fillna(np.inf).to_numpy(dtype=float))
    monthly["below_ma24_count"] = _streak_counts(monthly["c"].to_numpy(dtype=float) < monthly["ma24"].fillna(-np.inf).to_numpy(dtype=float))

    records: list[dict[str, Any]] = []
    for row in monthly.itertuples(index=False):
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
        extension_up = max(close_value / max(ma6_value, 1e-9) - 1.0, close_value / max(ma12_value, 1e-9) - 1.0, close_value / max(ma24_value, 1e-9) - 1.0)
        extension_down = min(close_value / max(ma6_value, 1e-9) - 1.0, close_value / max(ma12_value, 1e-9) - 1.0, close_value / max(ma24_value, 1e-9) - 1.0)
        persistence_above = max(int(row.above_ma12_count), int(row.above_ma24_count))
        persistence_below = max(int(row.below_ma12_count), int(row.below_ma24_count))
        bull_alignment = monthly_alignment_state == "bull_stack"
        bear_alignment = monthly_alignment_state == "bear_stack"
        compressed = monthly_alignment_state == "compressed"
        candidate_scores = {
            "monthly_up_pre": 48.0 + (12.0 if monthly_price_vs_ma24_state in {"above", "near"} else 0.0) + (10.0 if monthly_ma24_slope_state == "up" else 0.0) + (8.0 if monthly_ma6_slope_state in {"up", "flat"} else 0.0) + (6.0 if persistence_above <= 2 else 0.0),
            "monthly_up_mid": 60.0 + (15.0 if bull_alignment else 0.0) + (10.0 if monthly_ma6_slope_state == "up" else 0.0) + (10.0 if monthly_ma12_slope_state == "up" else 0.0) + (10.0 if monthly_ma24_slope_state in {"up", "flat"} else 0.0) + (5.0 if persistence_above >= 2 else 0.0),
            "monthly_up_top_warning": 70.0 + (20.0 if extension_up >= EXTENSION_THRESHOLD["monthly"] else 0.0) + (10.0 if monthly_ma6_slope_state in {"flat", "down"} else 0.0) + (10.0 if monthly_ma12_slope_state in {"flat", "down"} else 0.0) + (10.0 if monthly_ma24_slope_state in {"flat", "down"} else 0.0) + (5.0 if candle["upper_wick_ratio"] >= WICK_LONG_THRESHOLD else 0.0),
            "monthly_range_pre": 36.0 + (12.0 if monthly_price_vs_ma6_state == "near" or monthly_price_vs_ma12_state == "near" or monthly_price_vs_ma24_state == "near" else 0.0) + (8.0 if compressed else 0.0) + (6.0 if persistence_above <= 1 and persistence_below <= 1 else 0.0),
            "monthly_range_mid": 46.0 + (12.0 if monthly_alignment_state == "mixed" else 0.0) + (8.0 if monthly_ma6_slope_state == "flat" and monthly_ma12_slope_state == "flat" else 0.0) + (8.0 if monthly_ma12_slope_state == "flat" and monthly_ma24_slope_state == "flat" else 0.0) + (4.0 if persistence_above <= 3 and persistence_below <= 3 else 0.0),
            "monthly_range_late": 54.0 + (12.0 if monthly_alignment_state == "mixed" else 0.0) + (10.0 if max(persistence_above, persistence_below) >= PERSISTENCE_LATE_THRESHOLD else 0.0) + (6.0 if candle["body_ratio"] <= BODY_SMALL_THRESHOLD else 0.0),
            "monthly_down_mid": 58.0 + (15.0 if bear_alignment else 0.0) + (10.0 if monthly_ma6_slope_state == "down" else 0.0) + (10.0 if monthly_ma12_slope_state == "down" else 0.0) + (10.0 if monthly_ma24_slope_state in {"down", "flat"} else 0.0) + (5.0 if persistence_below >= 2 else 0.0),
            "monthly_down_bottom_warning": 72.0 + (18.0 if extension_down <= -EXTENSION_THRESHOLD["monthly"] else 0.0) + (12.0 if monthly_price_vs_ma6_state == "near" or monthly_price_vs_ma12_state == "near" else 0.0) + (10.0 if monthly_ma6_slope_state in {"flat", "up"} else 0.0) + (10.0 if monthly_ma12_slope_state in {"flat", "up"} else 0.0) + (8.0 if candle["lower_wick_ratio"] >= WICK_LONG_THRESHOLD else 0.0),
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
                "monthly_ma12_slope_state": monthly_ma12_slope_state,
                "monthly_ma24_slope_state": monthly_ma24_slope_state,
                "monthly_alignment_state": monthly_alignment_state,
                "monthly_above_ma12_count": int(row.above_ma12_count),
                "monthly_below_ma12_count": int(row.below_ma12_count),
                "monthly_above_ma24_count": int(row.above_ma24_count),
                "monthly_below_ma24_count": int(row.below_ma24_count),
                "monthly_main_state_score": float(_main_state_score(label)),
                "monthly_phase_score": float(_phase_score(label)),
                "monthly_phase_score_raw": float(
                    _clamp(
                        0.55 * _phase_score(label)
                        + 0.20 * ((int(row.above_ma12_count) + int(row.above_ma24_count)) / 2.0 / max(1, PERSISTENCE_CAP) * 100.0)
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
    weekly["week_end_date"] = pd.to_datetime(weekly["week_end_ts"], utc=True, errors="coerce").dt.strftime("%Y%m%d").astype("Int64")
    close = weekly["c"].astype(float)
    weekly["ma10"] = close.rolling(10, min_periods=10).mean()
    weekly["ma30"] = close.rolling(30, min_periods=30).mean()
    weekly["ma60"] = close.rolling(60, min_periods=60).mean()
    weekly["ma10_prev4"] = weekly["ma10"].shift(4)
    weekly["ma30_prev8"] = weekly["ma30"].shift(8)
    weekly["ma60_prev12"] = weekly["ma60"].shift(12)
    weekly["price_vs_ma10_state"] = [_price_vs_ma_state(float(c), float(m), PRICE_NEAR_THRESHOLD["weekly"]) for c, m in zip(weekly["c"], weekly["ma10"])]
    weekly["price_vs_ma30_state"] = [_price_vs_ma_state(float(c), float(m), PRICE_NEAR_THRESHOLD["weekly"]) for c, m in zip(weekly["c"], weekly["ma30"])]
    weekly["price_vs_ma60_state"] = [_price_vs_ma_state(float(c), float(m), PRICE_NEAR_THRESHOLD["weekly"]) for c, m in zip(weekly["c"], weekly["ma60"])]
    weekly["ma10_slope_state"] = [_slope_state(float(cur), float(prev), SLOPE_UP_THRESHOLD["weekly"][10], SLOPE_DOWN_THRESHOLD["weekly"][10]) for cur, prev in zip(weekly["ma10"], weekly["ma10_prev4"])]
    weekly["ma30_slope_state"] = [_slope_state(float(cur), float(prev), SLOPE_UP_THRESHOLD["weekly"][30], SLOPE_DOWN_THRESHOLD["weekly"][30]) for cur, prev in zip(weekly["ma30"], weekly["ma30_prev8"])]
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
    weekly["above_ma10_count"] = _streak_counts(weekly["c"].to_numpy(dtype=float) > weekly["ma10"].fillna(np.inf).to_numpy(dtype=float))
    weekly["below_ma10_count"] = _streak_counts(weekly["c"].to_numpy(dtype=float) < weekly["ma10"].fillna(-np.inf).to_numpy(dtype=float))
    weekly["above_ma30_count"] = _streak_counts(weekly["c"].to_numpy(dtype=float) > weekly["ma30"].fillna(np.inf).to_numpy(dtype=float))
    weekly["below_ma30_count"] = _streak_counts(weekly["c"].to_numpy(dtype=float) < weekly["ma30"].fillna(-np.inf).to_numpy(dtype=float))

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
        extension_up = max(close_value / max(ma10_value, 1e-9) - 1.0, close_value / max(ma30_value, 1e-9) - 1.0, close_value / max(ma60_value, 1e-9) - 1.0)
        extension_down = min(close_value / max(ma10_value, 1e-9) - 1.0, close_value / max(ma30_value, 1e-9) - 1.0, close_value / max(ma60_value, 1e-9) - 1.0)
        persistence_above = max(int(row.above_ma10_count), int(row.above_ma30_count))
        persistence_below = max(int(row.below_ma10_count), int(row.below_ma30_count))
        bull_alignment = weekly_alignment_state == "bull_stack"
        bear_alignment = weekly_alignment_state == "bear_stack"
        compressed = weekly_alignment_state == "compressed"
        candidate_scores = {
            "weekly_up_early": 56.0 + (10.0 if weekly_price_vs_ma10_state in {"above", "near"} else 0.0) + (8.0 if weekly_ma10_slope_state == "up" else 0.0) + (6.0 if weekly_price_vs_ma30_state != "below" else 0.0) + (4.0 if persistence_above <= 2 else 0.0) + (4.0 if weekly_price_vs_ma60_state != "below" else 0.0),
            "weekly_up_mid": 60.0 + (12.0 if bull_alignment else 0.0) + (8.0 if weekly_price_vs_ma10_state == "above" else 0.0) + (8.0 if weekly_ma10_slope_state == "up" else 0.0) + (8.0 if weekly_ma30_slope_state in {"up", "flat"} else 0.0) + (4.0 if persistence_above >= 2 else 0.0),
            "weekly_up_late": 62.0 + (14.0 if bull_alignment else 0.0) + (10.0 if extension_up >= EXTENSION_THRESHOLD["weekly"] else 0.0) + (8.0 if weekly_ma10_slope_state in {"flat", "down"} else 0.0) + (6.0 if candle["upper_wick_ratio"] >= WICK_LONG_THRESHOLD else 0.0) + (4.0 if weekly_price_vs_ma60_state == "above" else 0.0),
            "weekly_range_mid": 58.0 + (10.0 if weekly_alignment_state in {"mixed", "compressed"} else 0.0) + (8.0 if weekly_ma10_slope_state == "flat" and weekly_ma30_slope_state == "flat" else 0.0) + (6.0 if compressed else 0.0) + (4.0 if abs(persistence_above - persistence_below) <= 1 else 0.0),
            "weekly_range_late": 61.0 + (10.0 if weekly_alignment_state in {"mixed", "compressed"} else 0.0) + (8.0 if max(persistence_above, persistence_below) >= PERSISTENCE_LATE_THRESHOLD else 0.0) + (6.0 if candle["body_ratio"] <= BODY_SMALL_THRESHOLD else 0.0) + (4.0 if weekly_price_vs_ma30_state == "near" or weekly_price_vs_ma60_state == "near" else 0.0),
            "weekly_down_early": 56.0 + (10.0 if weekly_price_vs_ma10_state in {"below", "near"} else 0.0) + (8.0 if weekly_ma10_slope_state == "down" else 0.0) + (6.0 if weekly_price_vs_ma30_state != "above" else 0.0) + (4.0 if persistence_below <= 2 else 0.0) + (4.0 if weekly_price_vs_ma60_state != "above" else 0.0),
            "weekly_down_mid": 60.0 + (12.0 if bear_alignment else 0.0) + (8.0 if weekly_price_vs_ma10_state == "below" else 0.0) + (8.0 if weekly_ma10_slope_state == "down" else 0.0) + (8.0 if weekly_ma30_slope_state in {"down", "flat"} else 0.0) + (4.0 if persistence_below >= 2 else 0.0),
            "weekly_down_bottom_warning": 59.0 + (16.0 if extension_down <= -EXTENSION_THRESHOLD["weekly"] else 0.0) + (10.0 if weekly_price_vs_ma10_state == "near" or weekly_price_vs_ma30_state == "near" else 0.0) + (8.0 if weekly_ma10_slope_state in {"flat", "up"} else 0.0) + (8.0 if candle["lower_wick_ratio"] >= WICK_LONG_THRESHOLD else 0.0) + (4.0 if persistence_below >= 3 else 0.0),
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
    working["daily_price_vs_ma7_state"] = [_price_vs_ma_state(float(c), float(m), PRICE_NEAR_THRESHOLD["daily"]) for c, m in zip(working["c"], working["daily_ma7"])]
    working["daily_price_vs_ma20_state"] = [_price_vs_ma_state(float(c), float(m), PRICE_NEAR_THRESHOLD["daily"]) for c, m in zip(working["c"], working["daily_ma20"])]
    working["daily_price_vs_ma60_state"] = [_price_vs_ma_state(float(c), float(m), PRICE_NEAR_THRESHOLD["daily"]) for c, m in zip(working["c"], working["daily_ma60"])]
    working["daily_ma7_slope_state"] = [_slope_state(float(cur), float(prev), SLOPE_UP_THRESHOLD["daily"][7], SLOPE_DOWN_THRESHOLD["daily"][7]) for cur, prev in zip(working["daily_ma7"], working["daily_ma7_prev5"])]
    working["daily_ma20_slope_state"] = [_slope_state(float(cur), float(prev), SLOPE_UP_THRESHOLD["daily"][20], SLOPE_DOWN_THRESHOLD["daily"][20]) for cur, prev in zip(working["daily_ma20"], working["daily_ma20_prev10"])]
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
    working["daily_above_ma7_count"] = _streak_counts(working["c"].to_numpy(dtype=float) > working["daily_ma7"].fillna(np.inf).to_numpy(dtype=float))
    working["daily_below_ma7_count"] = _streak_counts(working["c"].to_numpy(dtype=float) < working["daily_ma7"].fillna(-np.inf).to_numpy(dtype=float))
    working["daily_above_ma20_count"] = _streak_counts(working["c"].to_numpy(dtype=float) > working["daily_ma20"].fillna(np.inf).to_numpy(dtype=float))
    working["daily_below_ma20_count"] = _streak_counts(working["c"].to_numpy(dtype=float) < working["daily_ma20"].fillna(-np.inf).to_numpy(dtype=float))

    records: list[dict[str, Any]] = []
    for idx, row in enumerate(working.itertuples(index=False)):
        prev = working.iloc[idx - 1] if idx > 0 else None
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
        extension_up = max(close_value / max(ma7_value, 1e-9) - 1.0, close_value / max(ma20_value, 1e-9) - 1.0, close_value / max(ma60_value, 1e-9) - 1.0)
        extension_down = min(close_value / max(ma7_value, 1e-9) - 1.0, close_value / max(ma20_value, 1e-9) - 1.0, close_value / max(ma60_value, 1e-9) - 1.0)
        persistence_above = max(int(row.daily_above_ma7_count), int(row.daily_above_ma20_count))
        persistence_below = max(int(row.daily_below_ma7_count), int(row.daily_below_ma20_count))
        bull_alignment = daily_alignment_state == "bull_stack"
        bear_alignment = daily_alignment_state == "bear_stack"
        compressed = daily_alignment_state == "compressed"
        prev_close = float(prev.c) if prev is not None else float(row.c)
        prev_open = float(prev.o) if prev is not None else float(row.o)
        prev_ma20 = float(prev.daily_ma20) if prev is not None and math.isfinite(float(prev.daily_ma20)) else ma20_value
        reclaim_ma20 = bool(prev is not None and math.isfinite(prev_ma20) and prev_close < prev_ma20 and close_value >= ma20_value)
        lose_ma20 = bool(prev is not None and math.isfinite(prev_ma20) and prev_close >= prev_ma20 and close_value < ma20_value)
        gap_up = bool(prev is not None and row.o >= (prev_close * (1.0 + GAP_THRESHOLD)))
        gap_down = bool(prev is not None and row.o <= (prev_close * (1.0 - GAP_THRESHOLD)))
        engulfing_bull = bool(prev is not None and close_value > float(row.o) and prev_close < prev_open and float(row.o) <= prev_close and close_value >= prev_open and candle["body_ratio"] >= 0.45)
        engulfing_bear = bool(prev is not None and close_value < float(row.o) and prev_close > prev_open and float(row.o) >= prev_close and close_value <= prev_open and candle["body_ratio"] >= 0.45)
        long_lower_wick = bool(candle["lower_wick_ratio"] >= WICK_LONG_THRESHOLD and candle["lower_wick_ratio"] >= candle["upper_wick_ratio"] * 1.2)
        long_upper_wick = bool(candle["upper_wick_ratio"] >= WICK_LONG_THRESHOLD and candle["upper_wick_ratio"] >= candle["lower_wick_ratio"] * 1.2)
        small_body = bool(candle["body_ratio"] <= BODY_SMALL_THRESHOLD)
        change_day_flag = bool(idx > 0 and str(getattr(row, "daily_alignment_state")) != str(getattr(working.iloc[idx - 1], "daily_alignment_state")))
        candidate_scores = {
            "daily_reversal_up_candidate": 78.0 + (14.0 if reclaim_ma20 else 0.0) + (12.0 if engulfing_bull else 0.0) + (10.0 if long_lower_wick else 0.0) + (6.0 if daily_price_vs_ma20_state in {"below", "near"} else 0.0),
            "daily_reversal_down_candidate": 78.0 + (14.0 if lose_ma20 else 0.0) + (12.0 if engulfing_bear else 0.0) + (10.0 if long_upper_wick else 0.0) + (6.0 if daily_price_vs_ma20_state in {"above", "near"} else 0.0),
            "daily_up_top_warning": 70.0 + (16.0 if extension_up >= EXTENSION_THRESHOLD["daily"] else 0.0) + (10.0 if long_upper_wick else 0.0) + (8.0 if small_body else 0.0) + (5.0 if bull_alignment else 0.0),
            "daily_down_bottom_warning": 70.0 + (16.0 if extension_down <= -EXTENSION_THRESHOLD["daily"] else 0.0) + (10.0 if long_lower_wick else 0.0) + (8.0 if small_body else 0.0) + (5.0 if bear_alignment else 0.0),
            "daily_up_mid": 60.0 + (15.0 if bull_alignment else 0.0) + (10.0 if daily_ma7_slope_state == "up" else 0.0) + (10.0 if daily_ma20_slope_state in {"up", "flat"} else 0.0) + (5.0 if persistence_above >= 2 else 0.0),
            "daily_up_early": 50.0 + (12.0 if daily_price_vs_ma7_state in {"above", "near"} else 0.0) + (10.0 if daily_ma7_slope_state == "up" else 0.0) + (8.0 if daily_price_vs_ma20_state != "below" else 0.0),
            "daily_down_mid": 60.0 + (15.0 if bear_alignment else 0.0) + (10.0 if daily_ma7_slope_state == "down" else 0.0) + (10.0 if daily_ma20_slope_state in {"down", "flat"} else 0.0) + (5.0 if persistence_below >= 2 else 0.0),
            "daily_down_early": 50.0 + (12.0 if daily_price_vs_ma7_state in {"below", "near"} else 0.0) + (10.0 if daily_ma7_slope_state == "down" else 0.0) + (8.0 if daily_price_vs_ma20_state != "above" else 0.0),
            "daily_range_mid": 46.0 + (12.0 if daily_alignment_state == "mixed" else 0.0) + (8.0 if daily_price_vs_ma20_state == "near" else 0.0) + (5.0 if daily_ma7_slope_state == "flat" and daily_ma20_slope_state == "flat" else 0.0),
            "daily_range_late": 54.0 + (12.0 if daily_alignment_state == "mixed" else 0.0) + (10.0 if max(persistence_above, persistence_below) >= PERSISTENCE_LATE_THRESHOLD else 0.0) + (6.0 if small_body else 0.0),
        }
        label = _select_label(candidate_scores, DAILY_PRIORITY, fallback="daily_range_mid")
        records.append(
            {
                "code": str(row.code),
                "date": int(pd.Timestamp(row.date_ts).strftime("%Y%m%d")),
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
            month_frame["monthly_main_state_history_rate"] = 0.5
            month_frame["weekly_main_state_history_rate"] = 0.5
            month_frame["daily_main_state_history_rate"] = 0.5
        else:
            for label_col in ("monthly_main_state", "weekly_main_state", "daily_main_state"):
                key_col = f"{label_col}_history_rate"
                by_state = history.groupby(label_col)["is_next_top10"].mean().to_dict()
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
    return pd.concat(result_frames, ignore_index=True, sort=False)


def _build_label_dictionary() -> dict[str, Any]:
    all_main_state_labels = [label_id for labels in MAIN_STATE_SPECS.values() for label_id, _ in labels]
    all_flag_labels = [label_id for label_id, *_ in EVENT_FLAG_SPECS + TRIGGER_FLAG_SPECS]
    entries: list[dict[str, Any]] = []
    for timeframe, specs in MAIN_STATE_SPECS.items():
        for label_id, jp_name in specs:
            entries.append(
                {
                    "label_id": label_id,
                    "timeframe": timeframe,
                    "label_kind": "main_state",
                    "jp_name": jp_name,
                    "parent_group": f"{timeframe}_main_state",
                    "mutually_exclusive_group": f"{timeframe}_main_state",
                    "can_coexist_with": all_flag_labels,
                    "is_authoritative_output": True,
                    "notes": f"Deterministic {timeframe} main state label.",
                }
            )
    for label_id, timeframe, parent_group, jp_name in EVENT_FLAG_SPECS:
        entries.append(
            {
                "label_id": label_id,
                "timeframe": timeframe,
                "label_kind": "event_flag",
                "jp_name": jp_name,
                "parent_group": parent_group,
                "mutually_exclusive_group": None,
                "can_coexist_with": all_main_state_labels + [flag for flag in all_flag_labels if flag != label_id],
                "is_authoritative_output": True,
                "notes": "Event flag that coexists with the main state labels.",
            }
        )
    for label_id, timeframe, parent_group, jp_name in TRIGGER_FLAG_SPECS:
        entries.append(
            {
                "label_id": label_id,
                "timeframe": timeframe,
                "label_kind": "trigger_flag",
                "jp_name": jp_name,
                "parent_group": parent_group,
                "mutually_exclusive_group": None,
                "can_coexist_with": all_main_state_labels + [flag for flag in all_flag_labels if flag != label_id],
                "is_authoritative_output": True,
                "notes": "Trigger flag that coexists with the main state labels.",
            }
        )
    return {
        "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
        "contract_name": "MA-aware hierarchical labeling contract",
        "label_count": int(len(entries)),
        "label_sets": {
            "monthly_main_state": [label_id for label_id, _ in MAIN_STATE_SPECS["monthly"]],
            "weekly_main_state": [label_id for label_id, _ in MAIN_STATE_SPECS["weekly"]],
            "daily_main_state": [label_id for label_id, _ in MAIN_STATE_SPECS["daily"]],
            "event_flags": [label_id for label_id, *_ in EVENT_FLAG_SPECS],
            "trigger_flags": [label_id for label_id, *_ in TRIGGER_FLAG_SPECS],
        },
        "entries": entries,
    }


def _build_rules() -> dict[str, Any]:
    return {
        "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
        "timeframe_role_contract": {"monthly": "environment", "weekly": "trend", "daily": "execution"},
        "ma_periods": {"monthly": list(MONTHLY_MA_PERIODS), "weekly": list(WEEKLY_MA_PERIODS), "daily": list(DAILY_MA_PERIODS)},
        "confirmed_ma_conventions": {"daily": list(DAILY_MA_PERIODS)},
        "provisional_ma_conventions": {"monthly": list(MONTHLY_MA_PERIODS), "weekly": list(WEEKLY_MA_PERIODS)},
        "slope_threshold_definitions": {
            timeframe: {"lookback_bars": SLOPE_LOOKBACK[timeframe], "up_threshold": SLOPE_UP_THRESHOLD[timeframe], "down_threshold": SLOPE_DOWN_THRESHOLD[timeframe]}
            for timeframe in ("monthly", "weekly", "daily")
        },
        "flat_threshold_definitions": {
            "price_vs_ma_near_threshold": PRICE_NEAR_THRESHOLD,
            "slope_flat_definition": "flat when slope delta is between up_threshold and down_threshold",
            "alignment_state_definitions": {
                "bull_stack": "price above short/mid/long MA with rising short slope and non-declining mid slope",
                "bear_stack": "price below short/mid/long MA with falling short slope and non-rising mid slope",
                "compressed": "price near all three MA lines with flat short and mid slope",
                "mixed": "all other combinations",
            },
        },
        "main_state_score_anchors": {
            "monthly": {
                "monthly_up_pre": 62.0,
                "monthly_up_mid": 80.0,
                "monthly_up_top_warning": 56.0,
                "monthly_range_pre": 42.0,
                "monthly_range_mid": 50.0,
                "monthly_range_late": 60.0,
                "monthly_down_mid": 32.0,
                "monthly_down_bottom_warning": 24.0,
            },
            "weekly": {
                "weekly_up_early": 56.0,
                "weekly_up_mid": 60.0,
                "weekly_up_late": 62.0,
                "weekly_range_mid": 58.0,
                "weekly_range_late": 61.0,
                "weekly_down_early": 56.0,
                "weekly_down_mid": 60.0,
                "weekly_down_bottom_warning": 59.0,
            },
            "daily": {
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
            },
        },
        "persistence_count_rules": {
            "cap": PERSISTENCE_CAP,
            "late_threshold": PERSISTENCE_LATE_THRESHOLD,
            "pre_threshold": PERSISTENCE_PRE_THRESHOLD,
            "bucket_labels": ["0", "1-2", "3-5", "6+"],
            "representation": "raw capped count per MA plus bounded persistence score",
        },
        "candle_pattern_rules": {
            "gap_threshold": GAP_THRESHOLD,
            "engulfing_rule": "current candle body must engulf previous body and close in the pattern direction",
            "long_wick_threshold": WICK_LONG_THRESHOLD,
            "small_body_threshold": BODY_SMALL_THRESHOLD,
        },
        "change_day_rules": {
            "definition": "flag turns true when the current timeframe main state differs from the previous bar in that timeframe",
            "monthly_change_day_flag": "current monthly main state differs from previous monthly main state for the symbol",
            "weekly_change_day_flag": "current weekly main state differs from previous weekly main state for the symbol",
            "daily_change_day_flag": "current daily main state differs from previous daily main state for the symbol",
        },
        "main_state_assignment_rules": {
            "monthly": {"labels": [label_id for label_id, _ in MAIN_STATE_SPECS["monthly"]], "priority": MONTHLY_PRIORITY, "fallback": "monthly_range_mid"},
            "weekly": {"labels": [label_id for label_id, _ in MAIN_STATE_SPECS["weekly"]], "priority": WEEKLY_PRIORITY, "fallback": "weekly_range_mid"},
            "daily": {"labels": [label_id for label_id, _ in MAIN_STATE_SPECS["daily"]], "priority": DAILY_PRIORITY, "fallback": "daily_range_mid"},
        },
        "score_composition_rules": {
            "monthly_environment_score": {"state_weight": 0.35, "ma_weight": 0.25, "persistence_weight": 0.20, "trigger_weight": 0.20, "range": "0..100"},
            "monthly_phase_score": "main-state progression score on a 0..100 scale",
            "weekly_trend_score": {"state_weight": 0.40, "ma_weight": 0.25, "persistence_weight": 0.20, "trigger_weight": 0.15, "range": "0..100"},
            "weekly_phase_score": "main-state progression score on a 0..100 scale",
            "daily_execution_score": {"state_weight": 0.35, "ma_weight": 0.20, "persistence_weight": 0.20, "trigger_weight": 0.25, "range": "0..100"},
            "daily_phase_score": "main-state progression score on a 0..100 scale",
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
            "winner_promotion_score": {"state_label_weight": 0.45, "context_weight": 0.25, "similarity_filter_weight": 0.15, "trigger_weight": 0.15},
            "loser_removal_score": {"state_label_weight": 0.45, "context_weight": 0.25, "similarity_filter_weight": 0.15, "trigger_weight": 0.15},
        },
        "fallback_behavior": {
            "monthly": "monthly_range_mid with neutral scores and false change flag",
            "weekly": "weekly_range_mid with neutral scores and false change flag",
            "daily": "daily_range_mid with neutral scores and false change flag",
            "trigger_flags": "false",
            "persistence_counts": 0,
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
                "event_flag_evaluation_order": [flag_id for flag_id, *_ in EVENT_FLAG_SPECS],
                "trigger_flag_evaluation_order": [flag_id for flag_id, *_ in TRIGGER_FLAG_SPECS],
                "tie_break_logic": "higher candidate score first, then priority order, then more directional states over range states",
                "insufficient_data_fallback_logic": "emit monthly_range_mid, neutral scores, zero persistence, false flags",
            },
            {
                "timeframe": "weekly",
                "main_state_resolution_order": WEEKLY_PRIORITY,
                "event_flag_evaluation_order": [flag_id for flag_id, *_ in EVENT_FLAG_SPECS],
                "trigger_flag_evaluation_order": [flag_id for flag_id, *_ in TRIGGER_FLAG_SPECS],
                "tie_break_logic": "higher candidate score first, then priority order, then directional states over range states",
                "insufficient_data_fallback_logic": "emit weekly_range_mid, neutral scores, zero persistence, false flags",
            },
            {
                "timeframe": "daily",
                "main_state_resolution_order": DAILY_PRIORITY,
                "event_flag_evaluation_order": [flag_id for flag_id, *_ in EVENT_FLAG_SPECS],
                "trigger_flag_evaluation_order": [flag_id for flag_id, *_ in TRIGGER_FLAG_SPECS],
                "tie_break_logic": "higher candidate score first, then priority order, then reversal and warning states over ordinary directional states",
                "insufficient_data_fallback_logic": "emit daily_range_mid, neutral scores, zero persistence, false flags",
            },
        ],
    }


REGIME_GATE_FULL_MONTHLY_STATES = {
    "monthly_up_mid",
    "monthly_up_top_warning",
    "monthly_range_late",
}

REGIME_GATE_FULL_WEEKLY_STATES = {
    "weekly_up_mid",
    "weekly_up_late",
    "weekly_range_late",
}

REGIME_GATE_VETO_MONTHLY_STATES = {
    "monthly_down_mid",
    "monthly_down_bottom_warning",
}

REGIME_GATE_VETO_WEEKLY_STATES = {
    "weekly_down_mid",
    "weekly_down_bottom_warning",
}

REGIME_GATE_MIN_ENVIRONMENT_SCORE = 45.0
REGIME_GATE_MIN_TREND_SCORE = 45.0


def _regime_gate_mode(row: pd.Series) -> str:
    monthly_main_state = _text(row.get("monthly_main_state"), fallback="monthly_range_mid")
    weekly_main_state = _text(row.get("weekly_main_state"), fallback="weekly_range_mid")
    monthly_environment_score = _safe_float(row.get("monthly_environment_score"), 50.0)
    weekly_trend_score = _safe_float(row.get("weekly_trend_score"), 50.0)
    winner_promotion_score = _safe_float(row.get("winner_promotion_score"), 50.0)
    loser_removal_score = _safe_float(row.get("loser_removal_score"), 50.0)
    full_rerank = (
        monthly_main_state in REGIME_GATE_FULL_MONTHLY_STATES
        and weekly_main_state in REGIME_GATE_FULL_WEEKLY_STATES
        and monthly_environment_score >= REGIME_GATE_MIN_ENVIRONMENT_SCORE
        and weekly_trend_score >= REGIME_GATE_MIN_TREND_SCORE
        and winner_promotion_score >= loser_removal_score
    )
    if full_rerank:
        return "full_rerank"
    veto_only = (
        loser_removal_score > winner_promotion_score
        and (
            monthly_main_state in REGIME_GATE_VETO_MONTHLY_STATES
            or weekly_main_state in REGIME_GATE_VETO_WEEKLY_STATES
            or monthly_environment_score < REGIME_GATE_MIN_ENVIRONMENT_SCORE
            or weekly_trend_score < REGIME_GATE_MIN_TREND_SCORE
        )
    )
    if veto_only:
        return "veto_only"
    return "suppressed"


def _apply_regime_gate(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    work = rows.copy()
    work["regime_gate_mode"] = work.apply(_regime_gate_mode, axis=1)
    work["hierarchical_ungated_score"] = (
        0.50 * pd.to_numeric(work["champion_score"], errors="coerce").fillna(50.0)
        + 0.20 * pd.to_numeric(work["winner_promotion_score"], errors="coerce").fillna(50.0)
        + 0.15 * pd.to_numeric(work["monthly_environment_score"], errors="coerce").fillna(50.0)
        + 0.10 * pd.to_numeric(work["weekly_trend_score"], errors="coerce").fillna(50.0)
        + 0.05 * (100.0 - pd.to_numeric(work["loser_removal_score"], errors="coerce").fillna(50.0))
    ).clip(0.0, 100.0)
    work["hierarchical_regime_gated_score"] = pd.to_numeric(work["champion_score"], errors="coerce").fillna(50.0)
    full_mask = work["regime_gate_mode"] == "full_rerank"
    veto_mask = work["regime_gate_mode"] == "veto_only"
    work.loc[full_mask, "hierarchical_regime_gated_score"] = work.loc[full_mask, "hierarchical_ungated_score"]
    work.loc[veto_mask, "hierarchical_regime_gated_score"] = (
        0.85 * pd.to_numeric(work.loc[veto_mask, "champion_score"], errors="coerce").fillna(50.0)
        + 0.15 * (100.0 - pd.to_numeric(work.loc[veto_mask, "loser_removal_score"], errors="coerce").fillna(50.0))
    ).clip(0.0, 100.0)
    work["regime_gate_active_flag"] = full_mask
    work["regime_gate_veto_only_flag"] = veto_mask
    work["regime_gate_suppressed_flag"] = ~(full_mask | veto_mask)
    return work


def _gate_mode_summary(rows: pd.DataFrame) -> dict[str, Any]:
    if rows.empty or "regime_gate_mode" not in rows.columns:
        return {}
    total = float(len(rows))
    summary: dict[str, Any] = {}
    for mode, group in rows.groupby("regime_gate_mode", dropna=False):
        summary[str(mode)] = {
            "count": int(len(group)),
            "share": float(len(group) / total) if total else 0.0,
            "mean_next_month_return": float(pd.to_numeric(group["next_month_return"], errors="coerce").mean()) if len(group) else 0.0,
            "top10_rate": float(pd.to_numeric(group["is_next_top10"], errors="coerce").mean()) if len(group) else 0.0,
            "bottom10_rate": float(pd.to_numeric(group["is_next_bottom10"], errors="coerce").mean()) if len(group) else 0.0,
            "mean_winner_promotion_score": float(pd.to_numeric(group["winner_promotion_score"], errors="coerce").mean()) if len(group) else 0.0,
            "mean_loser_removal_score": float(pd.to_numeric(group["loser_removal_score"], errors="coerce").mean()) if len(group) else 0.0,
        }
    return summary


def _build_regime_gate_rules() -> dict[str, Any]:
    return {
        "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
        "mode_contract": {
            "champion_only": "rank by champion_score only",
            "champion_plus_hierarchical_rerank_ungated": "rank by hierarchical_ungated_score on all samples",
            "champion_plus_hierarchical_rerank_regime_gated": "rank by hierarchical_regime_gated_score with regime gate modes",
        },
        "full_rerank_gate": {
            "monthly_main_state": sorted(REGIME_GATE_FULL_MONTHLY_STATES),
            "weekly_main_state": sorted(REGIME_GATE_FULL_WEEKLY_STATES),
            "monthly_environment_score_min": REGIME_GATE_MIN_ENVIRONMENT_SCORE,
            "weekly_trend_score_min": REGIME_GATE_MIN_TREND_SCORE,
            "promotion_condition": "winner_promotion_score >= loser_removal_score",
        },
        "veto_only_gate": {
            "monthly_main_state": sorted(REGIME_GATE_VETO_MONTHLY_STATES),
            "weekly_main_state": sorted(REGIME_GATE_VETO_WEEKLY_STATES),
            "promotion_condition": "loser_removal_score > winner_promotion_score",
            "outside_compatible_states": "champion_score is retained with loser-removal-only downweighting",
        },
        "suppressed_gate": {
            "default_mode": "champion_score",
            "fallback": "when neither full_rerank nor veto_only conditions are satisfied",
        },
        "score_composition": {
            "hierarchical_ungated_score": {
                "champion_score": 0.50,
                "winner_promotion_score": 0.20,
                "monthly_environment_score": 0.15,
                "weekly_trend_score": 0.10,
                "loser_removal_score": -0.05,
            },
            "hierarchical_regime_gated_score": {
                "full_rerank_mode": "hierarchical_ungated_score",
                "veto_only_mode": {
                    "champion_score": 0.85,
                    "loser_removal_score": -0.15,
                },
                "suppressed_mode": "champion_score",
            },
        },
        "fallback_behavior": {
            "insufficient_data": "retain champion_score",
            "missing_gate_fields": "retain champion_score",
        },
    }


def _build_regime_gate_compare(scored: pd.DataFrame, *, top_k: int, candidate_pool_k: int) -> dict[str, Any]:
    if scored.empty:
        return {
            "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
            "top_k": int(top_k),
            "candidate_pool_k": int(candidate_pool_k),
            "variants": {},
            "best_variant": "champion_only",
            "decision": "drop_regime_gated_variant",
            "decision_reason_typed": "insufficient_data",
            "authoritative_rollup_decision": "drop_regime_gated_variant",
        }
    gated_rows = _apply_regime_gate(scored)

    def _compare_for(score_col: str, *, gated_mode: bool) -> dict[str, Any]:
        monthly_rows: list[dict[str, Any]] = []
        for month, month_frame in gated_rows.groupby("sample_month", sort=True):
            champion_ranked = month_frame.sort_values(["champion_score", "code"], ascending=[False, True]).reset_index(drop=True)
            rerank_full_ranked = month_frame.sort_values([score_col, "code"], ascending=[False, True]).reset_index(drop=True)
            rerank_candidates = champion_ranked.head(min(candidate_pool_k, len(champion_ranked))).copy()
            rerank_ranked = rerank_candidates.sort_values([score_col, "code"], ascending=[False, True]).reset_index(drop=True)
            champion_top10 = champion_ranked.head(min(top_k, len(champion_ranked)))
            rerank_top10 = rerank_ranked.head(min(top_k, len(rerank_ranked)))
            champion_top5 = champion_ranked.head(min(5, len(champion_ranked)))
            rerank_top5 = rerank_ranked.head(min(5, len(rerank_ranked)))
            champion_top30 = champion_ranked.head(min(candidate_pool_k, len(champion_ranked)))
            rerank_top30 = rerank_full_ranked.head(min(candidate_pool_k, len(rerank_full_ranked)))
            champion_bottom10 = champion_ranked[champion_ranked["is_next_bottom10"] == 1]
            rerank_bottom10 = rerank_full_ranked[rerank_full_ranked["is_next_bottom10"] == 1]
            removed_bad = champion_top10[~champion_top10["sample_id"].isin(rerank_top10["sample_id"]) & (champion_top10["is_next_bottom10"] == 1)]
            added_good = rerank_top10[~rerank_top10["sample_id"].isin(champion_top10["sample_id"]) & (rerank_top10["is_next_top10"] == 1)]
            changed_members = set(champion_top10["sample_id"]) ^ set(rerank_top10["sample_id"])
            changed_top5_members = set(champion_top5["sample_id"]) ^ set(rerank_top5["sample_id"])
            champion_rank_map = {row.sample_id: int(idx + 1) for idx, row in enumerate(champion_top30.itertuples(index=False))}
            rerank_rank_map = {row.sample_id: int(idx + 1) for idx, row in enumerate(rerank_top30.itertuples(index=False))}
            changed_rank_count = sum(1 for sample_id in set(champion_rank_map) | set(rerank_rank_map) if champion_rank_map.get(sample_id) != rerank_rank_map.get(sample_id))
            top10_boundary_champion = champion_ranked.iloc[min(top_k - 1, len(champion_ranked) - 1)]
            next_top10_boundary_champion = champion_ranked.iloc[min(top_k, len(champion_ranked) - 1)] if len(champion_ranked) > top_k else top10_boundary_champion
            top10_boundary_rerank = rerank_ranked.iloc[min(top_k - 1, len(rerank_ranked) - 1)]
            next_top10_boundary_rerank = rerank_ranked.iloc[min(top_k, len(rerank_ranked) - 1)] if len(rerank_ranked) > top_k else top10_boundary_rerank
            champion_candidate_pool_top10_capture = int(champion_top30["is_next_top10"].sum())
            rerank_candidate_pool_top10_capture = int(rerank_top30["is_next_top10"].sum())
            champion_candidate_pool_bad_pick_removal = int(max(0, int(champion_bottom10["sample_id"].nunique()) - int(champion_top30["is_next_bottom10"].sum())))
            rerank_candidate_pool_bad_pick_removal = int(max(0, int(rerank_bottom10["sample_id"].nunique()) - int(rerank_top30["is_next_bottom10"].sum())))
            monthly_rows.append(
                {
                    "sample_month": int(month),
                    "sample_count": int(len(month_frame)),
                    "champion_candidate_pool_top10_capture": champion_candidate_pool_top10_capture,
                    "rerank_candidate_pool_top10_capture": rerank_candidate_pool_top10_capture,
                    "candidate_pool_top10_capture_delta": int(rerank_candidate_pool_top10_capture - champion_candidate_pool_top10_capture),
                    "champion_candidate_pool_bad_pick_removal": champion_candidate_pool_bad_pick_removal,
                    "rerank_candidate_pool_bad_pick_removal": rerank_candidate_pool_bad_pick_removal,
                    "candidate_pool_bad_pick_removal_delta": int(rerank_candidate_pool_bad_pick_removal - champion_candidate_pool_bad_pick_removal),
                    "top5_hit_count": int(rerank_top5["is_next_top10"].sum()),
                    "top5_hit_rate": float(rerank_top5["is_next_top10"].mean()) if len(rerank_top5) else 0.0,
                    "champion_top5_hit_count": int(champion_top5["is_next_top10"].sum()),
                    "champion_top5_hit_rate": float(champion_top5["is_next_top10"].mean()) if len(champion_top5) else 0.0,
                    "changed_top5_members_count": int(len(changed_top5_members)),
                    "champion_top5_boundary_score_gap": float(champion_ranked.iloc[min(4, len(champion_ranked) - 1)][["champion_score"]].iloc[0] - champion_ranked.iloc[min(5, len(champion_ranked) - 1)][["champion_score"]].iloc[0]) if len(champion_ranked) > 5 else float(champion_ranked.iloc[min(4, len(champion_ranked) - 1)]["champion_score"]),
                    "rerank_top5_boundary_score_gap": float(rerank_ranked.iloc[min(4, len(rerank_ranked) - 1)][score_col] - rerank_ranked.iloc[min(5, len(rerank_ranked) - 1)][score_col]) if len(rerank_ranked) > 5 else float(rerank_ranked.iloc[min(4, len(rerank_ranked) - 1)][score_col]),
                    "champion_top5_boundary_outcome_gap": float(champion_ranked.iloc[min(4, len(champion_ranked) - 1)]["next_month_return"] - champion_ranked.iloc[min(5, len(champion_ranked) - 1)]["next_month_return"]) if len(champion_ranked) > 5 else float(champion_ranked.iloc[min(4, len(champion_ranked) - 1)]["next_month_return"]),
                    "rerank_top5_boundary_outcome_gap": float(rerank_ranked.iloc[min(4, len(rerank_ranked) - 1)]["next_month_return"] - rerank_ranked.iloc[min(5, len(rerank_ranked) - 1)]["next_month_return"]) if len(rerank_ranked) > 5 else float(rerank_ranked.iloc[min(4, len(rerank_ranked) - 1)]["next_month_return"]),
                    "champion_top10_hit_count": int(champion_top10["is_next_top10"].sum()),
                    "rerank_top10_hit_count": int(rerank_top10["is_next_top10"].sum()),
                    "champion_top10_hit_rate": float(champion_top10["is_next_top10"].mean()) if len(champion_top10) else 0.0,
                    "rerank_top10_hit_rate": float(rerank_top10["is_next_top10"].mean()) if len(rerank_top10) else 0.0,
                    "champion_top10_mean_next_month_return": float(champion_top10["next_month_return"].mean()) if len(champion_top10) else 0.0,
                    "rerank_top10_mean_next_month_return": float(rerank_top10["next_month_return"].mean()) if len(rerank_top10) else 0.0,
                    "champion_top10_median_next_month_return": float(champion_top10["next_month_return"].median()) if len(champion_top10) else 0.0,
                    "rerank_top10_median_next_month_return": float(rerank_top10["next_month_return"].median()) if len(rerank_top10) else 0.0,
                    "bad_pick_removal_count": int(len(removed_bad)),
                    "good_pick_addition_count": int(len(added_good)),
                    "changed_top10_members_count": int(len(changed_members)),
                    "changed_rank_count": int(changed_rank_count),
                    "champion_top10_boundary_score_gap": float(top10_boundary_champion["champion_score"] - next_top10_boundary_champion["champion_score"]) if len(champion_ranked) > top_k else float(top10_boundary_champion["champion_score"]),
                    "rerank_top10_boundary_score_gap": float(top10_boundary_rerank[score_col] - next_top10_boundary_rerank[score_col]) if len(rerank_ranked) > top_k else float(top10_boundary_rerank[score_col]),
                    "champion_top10_boundary_outcome_gap": float(top10_boundary_champion["next_month_return"] - next_top10_boundary_champion["next_month_return"]) if len(champion_ranked) > top_k else float(top10_boundary_champion["next_month_return"]),
                    "rerank_top10_boundary_outcome_gap": float(top10_boundary_rerank["next_month_return"] - next_top10_boundary_rerank["next_month_return"]) if len(rerank_ranked) > top_k else float(top10_boundary_rerank["next_month_return"]),
                    "selection_divergence_reason": "top10_member_swap" if changed_members else "rank_reorder_inside_pool" if changed_rank_count else "no_divergence",
                    "regime_tag": _text(champion_ranked.iloc[0].regime_tag, fallback="mixed"),
                    "gate_mode": "gated" if gated_mode else "ungated",
                }
            )
        monthly_df = pd.DataFrame(monthly_rows)
        if monthly_df.empty:
            return {
                "month_count": 0,
                "sample_count": 0,
                "winner_promotion_delta": 0.0,
                "winner_promotion_score_delta": 0.0,
                "loser_removal_delta": 0.0,
                "candidate_pool_top10_capture": 0.0,
                "candidate_pool_top10_capture_delta": 0.0,
                "candidate_pool_bad_pick_removal": 0.0,
                "candidate_pool_bad_pick_removal_delta": 0.0,
                "final_top10_uplift": 0.0,
                "final_top10_bad_pick_removal": 0.0,
                "changed_top5_members_count": 0.0,
                "top5_boundary_score_gap": 0.0,
                "top5_boundary_outcome_gap": 0.0,
                "oos_top10_uplift": 0.0,
                "oos_bad_pick_removal": 0.0,
                "changed_top10_members_count": 0.0,
                "changed_rank_count": 0.0,
                "top10_boundary_score_gap": 0.0,
                "top10_boundary_outcome_gap": 0.0,
                "gate_activation_rate": 0.0,
                "gate_suppression_rate": 0.0,
                "gate_veto_only_rate": 0.0,
                "regime_breakdown": {},
                "regime_sign_stability": {},
                "monthly_rows": [],
            }
        regime_breakdown: dict[str, dict[str, float]] = {}
        for regime, regime_df in monthly_df.groupby("regime_tag", dropna=False):
            mean_oos_top10_uplift = float((regime_df["rerank_top10_hit_count"] - regime_df["champion_top10_hit_count"]).mean())
            regime_breakdown[str(regime)] = {
                "month_count": float(len(regime_df)),
                "mean_oos_top10_uplift": mean_oos_top10_uplift,
                "mean_top10_uplift": mean_oos_top10_uplift,
                "mean_bad_pick_removal": float(regime_df["bad_pick_removal_count"].mean()),
                "mean_changed_top10_members_count": float(regime_df["changed_top10_members_count"].mean()),
                "mean_candidate_pool_top10_capture": float(regime_df["rerank_candidate_pool_top10_capture"].mean()),
                "mean_candidate_pool_bad_pick_removal": float(regime_df["rerank_candidate_pool_bad_pick_removal"].mean()),
                "mean_top10_boundary_outcome_gap": float(regime_df["rerank_top10_boundary_outcome_gap"].mean()),
                "positive_month_share": float((regime_df["rerank_top10_hit_count"] - regime_df["champion_top10_hit_count"] > 0).mean()),
                "negative_month_share": float((regime_df["rerank_top10_hit_count"] - regime_df["champion_top10_hit_count"] < 0).mean()),
            }
            regime_breakdown[str(regime)]["sign_stability"] = "positive" if regime_breakdown[str(regime)]["positive_month_share"] >= 0.55 else "negative" if regime_breakdown[str(regime)]["negative_month_share"] >= 0.55 else "mixed"
        uplift = monthly_df["rerank_top10_hit_count"] - monthly_df["champion_top10_hit_count"]
        return {
            "month_count": int(len(monthly_df)),
            "sample_count": int(monthly_df["sample_count"].sum()),
            "winner_promotion_delta": float(monthly_df["rerank_top10_hit_rate"].sub(monthly_df["champion_top10_hit_rate"]).mean()),
            "winner_promotion_score_delta": float(monthly_df["rerank_top10_mean_next_month_return"].sub(monthly_df["champion_top10_mean_next_month_return"]).mean()),
            "loser_removal_delta": float((monthly_df["bad_pick_removal_count"] - monthly_df["good_pick_addition_count"]).mean()),
            "candidate_pool_top10_capture": float(monthly_df["rerank_candidate_pool_top10_capture"].mean()),
            "candidate_pool_top10_capture_delta": float(monthly_df["candidate_pool_top10_capture_delta"].mean()),
            "candidate_pool_bad_pick_removal": float(monthly_df["rerank_candidate_pool_bad_pick_removal"].mean()),
            "candidate_pool_bad_pick_removal_delta": float(monthly_df["candidate_pool_bad_pick_removal_delta"].mean()),
            "final_top10_uplift": float(uplift.mean()),
            "final_top10_bad_pick_removal": float(monthly_df["bad_pick_removal_count"].mean()),
            "changed_top5_members_count": float(monthly_df["changed_top5_members_count"].mean()),
            "top5_boundary_score_gap": float(monthly_df["rerank_top5_boundary_score_gap"].mean()),
            "top5_boundary_outcome_gap": float(monthly_df["rerank_top5_boundary_outcome_gap"].mean()),
            "oos_top10_uplift": float(uplift.mean()),
            "oos_bad_pick_removal": float(monthly_df["bad_pick_removal_count"].mean()),
            "changed_top10_members_count": float(monthly_df["changed_top10_members_count"].mean()),
            "changed_rank_count": float(monthly_df["changed_rank_count"].mean()),
            "top10_boundary_score_gap": float(monthly_df["rerank_top10_boundary_score_gap"].mean()),
            "top10_boundary_outcome_gap": float(monthly_df["rerank_top10_boundary_outcome_gap"].mean()),
            "positive_month_share": float((uplift > 0).mean()),
            "negative_month_share": float((uplift < 0).mean()),
            "monthly_uplift_std": float(uplift.std(ddof=1) if len(uplift) > 1 else 0.0),
            "selection_divergence_reason": "top10_member_swap" if float(monthly_df["changed_top10_members_count"].mean()) > 0.0 else "rank_reorder_inside_pool" if float(monthly_df["changed_rank_count"].mean()) > 0.0 else "no_divergence",
            "regime_breakdown": regime_breakdown,
            "regime_sign_stability": {
                regime: {
                    "positive_month_share": payload["positive_month_share"],
                    "negative_month_share": payload["negative_month_share"],
                    "sign_stability": payload["sign_stability"],
                }
                for regime, payload in regime_breakdown.items()
            },
            "boundary_bucket_effect": {},
            "monthly_rows": monthly_df.to_dict(orient="records"),
            "gate_activation_rate": float((gated_rows["regime_gate_mode"] == "full_rerank").mean()) if gated_mode and "regime_gate_mode" in gated_rows.columns else 0.0,
            "gate_suppression_rate": float((gated_rows["regime_gate_mode"] == "suppressed").mean()) if gated_mode and "regime_gate_mode" in gated_rows.columns else 0.0,
            "gate_veto_only_rate": float((gated_rows["regime_gate_mode"] == "veto_only").mean()) if gated_mode and "regime_gate_mode" in gated_rows.columns else 0.0,
            "gate_mode": "gated" if gated_mode else "ungated",
        }

    variants = {
        "champion_only": _compare_for("champion_score", gated_mode=False),
        "champion_plus_hierarchical_rerank_ungated": _compare_for("hierarchical_ungated_score", gated_mode=False),
        "champion_plus_hierarchical_rerank_regime_gated": _compare_for("hierarchical_regime_gated_score", gated_mode=True),
    }

    best_variant = max(variants.items(), key=lambda item: (float(item[1].get("oos_top10_uplift") or 0.0), float(item[1].get("winner_promotion_delta") or 0.0), float(item[1].get("loser_removal_delta") or 0.0)))
    gated_payload = variants["champion_plus_hierarchical_rerank_regime_gated"]
    ungated_payload = variants["champion_plus_hierarchical_rerank_ungated"]
    champion_payload = variants["champion_only"]
    gated_uplift = float(gated_payload.get("oos_top10_uplift") or 0.0)
    ungated_uplift = float(ungated_payload.get("oos_top10_uplift") or 0.0)
    champion_uplift = float(champion_payload.get("oos_top10_uplift") or 0.0)
    gated_boundary_improved = bool(gated_payload.get("top10_boundary_improved"))
    gated_winner_improved = float(gated_payload.get("winner_promotion_delta") or 0.0) > 0.0
    gated_loser_improved = float(gated_payload.get("loser_removal_delta") or 0.0) > 0.0
    gated_stable = float(gated_payload.get("positive_month_share") or 0.0) >= 0.55
    if gated_uplift > champion_uplift and gated_uplift >= ungated_uplift and gated_boundary_improved and gated_winner_improved and gated_stable:
        decision = "keep_regime_gated_variant"
        reason = "stable_uplift_with_boundary_gain"
    elif gated_uplift > champion_uplift or gated_uplift > 0.0 or gated_boundary_improved or gated_winner_improved or gated_loser_improved:
        decision = "hold_regime_gated_variant"
        reason = "partial_improvement_without_stable_boundary_gain"
    else:
        decision = "drop_regime_gated_variant"
        reason = "no_stable_oos_improvement"
    return {
        "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
        "top_k": int(top_k),
        "candidate_pool_k": int(candidate_pool_k),
        "variants": variants,
        "best_variant": best_variant[0],
        "decision": decision,
        "decision_reason_typed": reason,
        "authoritative_rollup_decision": decision,
    }


def _regime_gate_effect_by_regime(compare_payload: dict[str, Any]) -> dict[str, Any]:
    variants = compare_payload.get("variants", {}) if isinstance(compare_payload, dict) else {}
    gated = variants.get("champion_plus_hierarchical_rerank_regime_gated", {}) if isinstance(variants, dict) else {}
    ungated = variants.get("champion_plus_hierarchical_rerank_ungated", {}) if isinstance(variants, dict) else {}
    champion = variants.get("champion_only", {}) if isinstance(variants, dict) else {}
    regime_breakdown = gated.get("regime_breakdown", {}) if isinstance(gated, dict) else {}

    def _payload_metric(payload: dict[str, Any], *keys: str, fallback: float = 0.0) -> float:
        for key in keys:
            if key in payload:
                return float(payload.get(key) or 0.0)
        return float(fallback)

    effect: dict[str, Any] = {}
    for regime, gated_payload in regime_breakdown.items():
        ungated_payload = (ungated.get("regime_breakdown", {}) or {}).get(regime, {}) if isinstance(ungated, dict) else {}
        champion_payload = (champion.get("regime_breakdown", {}) or {}).get(regime, {}) if isinstance(champion, dict) else {}
        effect[str(regime)] = {
            "month_count": int(gated_payload.get("month_count") or 0),
            "champion_oos_top10_uplift": _payload_metric(champion_payload, "mean_oos_top10_uplift", "mean_top10_uplift"),
            "ungated_oos_top10_uplift": _payload_metric(ungated_payload, "mean_oos_top10_uplift", "mean_top10_uplift"),
            "gated_oos_top10_uplift": _payload_metric(gated_payload, "mean_oos_top10_uplift", "mean_top10_uplift"),
            "champion_bad_pick_removal": _payload_metric(champion_payload, "mean_bad_pick_removal", "mean_candidate_pool_bad_pick_removal"),
            "ungated_bad_pick_removal": _payload_metric(ungated_payload, "mean_bad_pick_removal", "mean_candidate_pool_bad_pick_removal"),
            "gated_bad_pick_removal": _payload_metric(gated_payload, "mean_bad_pick_removal", "mean_candidate_pool_bad_pick_removal"),
            "champion_changed_top10_members_count": _payload_metric(champion_payload, "mean_changed_top10_members_count"),
            "ungated_changed_top10_members_count": _payload_metric(ungated_payload, "mean_changed_top10_members_count"),
            "gated_changed_top10_members_count": _payload_metric(gated_payload, "mean_changed_top10_members_count"),
            "gated_positive_month_share": _payload_metric(gated_payload, "positive_month_share"),
            "gated_negative_month_share": _payload_metric(gated_payload, "negative_month_share"),
            "sign_stability": gated_payload.get("sign_stability", "mixed"),
        }
    return {
        "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
        "variants": {
            "champion_only": champion,
            "champion_plus_hierarchical_rerank_ungated": ungated,
            "champion_plus_hierarchical_rerank_regime_gated": gated,
        },
        "regimes": effect,
    }


def build_hierarchical_regime_gate_artifacts(
    *,
    scored: pd.DataFrame,
    top_k: int,
    candidate_pool_k: int,
) -> dict[str, Any]:
    compare = _build_regime_gate_compare(scored, top_k=top_k, candidate_pool_k=candidate_pool_k)
    return {
        "rules": _build_regime_gate_rules(),
        "compare": compare,
        "effect_by_regime": _regime_gate_effect_by_regime(compare),
    }


def _summarize_scores(rows: pd.DataFrame, score_columns: Iterable[str]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for column in score_columns:
        series = pd.to_numeric(rows[column], errors="coerce")
        count = int(series.count())
        summary[column] = {
            "count": count,
            "mean": float(series.mean()) if count else 0.0,
            "std": float(series.std(ddof=1)) if count > 1 else 0.0,
            "min": float(series.min()) if count else 0.0,
            "p25": float(series.quantile(0.25)) if count else 0.0,
            "median": float(series.median()) if count else 0.0,
            "p75": float(series.quantile(0.75)) if count else 0.0,
            "max": float(series.max()) if count else 0.0,
            "zero_count": int((series == 0).sum()) if count else 0,
            "full_count": int((series >= 100).sum()) if count else 0,
        }
    return summary


def _summarize_label_balance(rows: pd.DataFrame, label_col: str) -> dict[str, Any]:
    if rows.empty or label_col not in rows.columns:
        return {}
    summary: dict[str, Any] = {}
    total = float(len(rows))
    for label, group in rows.groupby(label_col, dropna=False):
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
    return {label_col: _summarize_label_balance(rows, label_col) for label_col in ("monthly_main_state", "weekly_main_state", "daily_main_state")}


def _regime_summary(rows: pd.DataFrame) -> dict[str, Any]:
    if rows.empty or "regime_tag" not in rows.columns:
        return {}
    summary: dict[str, Any] = {}
    total = float(len(rows))
    for regime, group in rows.groupby("regime_tag", dropna=False):
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


def _annotate_change_flag(frame: pd.DataFrame, *, code_col: str, order_col: str, state_col: str, flag_col: str) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    work = frame.sort_values([code_col, order_col]).copy()
    previous = work.groupby(code_col)[state_col].shift(1)
    work[flag_col] = previous.notna() & work[state_col].ne(previous)
    return work


def _fill_defaults(frame: pd.DataFrame, defaults: dict[str, Any]) -> pd.DataFrame:
    work = frame.copy()
    for column, default in defaults.items():
        if column not in work.columns:
            work[column] = default
        else:
            work[column] = work[column].fillna(default)
    return work


def _merge_exact_by_code(left: pd.DataFrame, right: pd.DataFrame, *, key: str) -> pd.DataFrame:
    if left.empty or right.empty:
        return left.copy()
    return left.merge(right, on=["code", key], how="left")


def _merge_asof_by_code(left: pd.DataFrame, right: pd.DataFrame, *, left_key: str, right_key: str) -> pd.DataFrame:
    if left.empty or right.empty:
        return left.copy()
    parts: list[pd.DataFrame] = []
    left_work = left.sort_values(["code", left_key]).copy()
    right_work = right.sort_values(["code", right_key]).copy()
    for code, left_group in left_work.groupby("code", sort=False):
        right_group = right_work[right_work["code"] == code]
        if right_group.empty:
            parts.append(left_group.copy())
            continue
        parts.append(
            pd.merge_asof(
                left_group.sort_values(left_key),
                right_group.sort_values(right_key),
                left_on=left_key,
                right_on=right_key,
                by="code",
                direction="backward",
            )
        )
    return pd.concat(parts, ignore_index=True, sort=False) if parts else left.copy()


def _variant_score(rows: pd.DataFrame, *, variant: str) -> pd.Series:
    baseline = pd.to_numeric(rows["rerank_score"], errors="coerce").fillna(50.0)
    state_score = pd.to_numeric(rows["state_label_score"], errors="coerce").fillna(50.0)
    context_score = pd.to_numeric(rows["context_score"], errors="coerce").fillna(50.0)
    trigger_score = pd.to_numeric(rows["trigger_signal_score"], errors="coerce").fillna(50.0)
    similarity_score = pd.to_numeric(rows["similarity_filter_score"], errors="coerce").fillna(50.0)
    winner_score = pd.to_numeric(rows["winner_promotion_score"], errors="coerce").fillna(50.0)
    loser_score = pd.to_numeric(rows["loser_removal_score"], errors="coerce").fillna(50.0)
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
        return (0.30 * baseline + 0.15 * state_score + 0.15 * context_score + 0.10 * trigger_score + 0.10 * similarity_score + 0.10 * winner_score + 0.10 * (100.0 - loser_score)).clip(0.0, 100.0)
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


def _evaluate_variant(scored: pd.DataFrame, *, variant: str, top_k: int, candidate_pool_k: int) -> dict[str, Any]:
    if scored.empty:
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
            "selection_divergence_reason": "empty",
            "state_family_contribution_summary": {},
            "regime_breakdown": {},
            "monthly_rows": [],
        }
    working = scored.copy()
    working["variant_score"] = _variant_score(working, variant=variant)
    monthly_rows: list[dict[str, Any]] = []
    top_selection_frames: list[pd.DataFrame] = []
    for month, month_frame in working.groupby("sample_month", sort=True):
        champion_ranked = month_frame.sort_values(["champion_score", "code"], ascending=[False, True]).reset_index(drop=True)
        variant_ranked = month_frame.sort_values(["variant_score", "code"], ascending=[False, True]).reset_index(drop=True)
        champion_top10 = champion_ranked.head(min(top_k, len(champion_ranked)))
        variant_top10 = variant_ranked.head(min(top_k, len(variant_ranked)))
        top_selection_frames.append(variant_top10.copy())
        champion_pool = champion_ranked.head(min(candidate_pool_k, len(champion_ranked)))
        variant_pool = variant_ranked.head(min(candidate_pool_k, len(variant_ranked)))
        champion_rank_map = {str(row.sample_id): int(idx + 1) for idx, row in enumerate(champion_ranked.itertuples(index=False))}
        variant_rank_map = {str(row.sample_id): int(idx + 1) for idx, row in enumerate(variant_ranked.itertuples(index=False))}
        changed_top10_members = set(champion_top10["sample_id"].astype(str)) ^ set(variant_top10["sample_id"].astype(str))
        changed_top5_members = set(champion_ranked.head(min(5, len(champion_ranked)))["sample_id"].astype(str)) ^ set(variant_ranked.head(min(5, len(variant_ranked)))["sample_id"].astype(str))
        candidate_union = set(champion_pool["sample_id"].astype(str)) | set(variant_pool["sample_id"].astype(str))
        changed_rank_count = sum(1 for sample_id in candidate_union if champion_rank_map.get(sample_id) != variant_rank_map.get(sample_id))
        top10_boundary_champion = champion_ranked.iloc[min(top_k - 1, len(champion_ranked) - 1)] if len(champion_ranked) else None
        next_top10_boundary_champion = champion_ranked.iloc[min(top_k, len(champion_ranked) - 1)] if len(champion_ranked) > top_k else top10_boundary_champion
        top10_boundary_variant = variant_ranked.iloc[min(top_k - 1, len(variant_ranked) - 1)] if len(variant_ranked) else None
        next_top10_boundary_variant = variant_ranked.iloc[min(top_k, len(variant_ranked) - 1)] if len(variant_ranked) > top_k else top10_boundary_variant
        champion_top10_hit_rate = float(pd.to_numeric(champion_top10["is_next_top10"], errors="coerce").mean()) if len(champion_top10) else 0.0
        variant_top10_hit_rate = float(pd.to_numeric(variant_top10["is_next_top10"], errors="coerce").mean()) if len(variant_top10) else 0.0
        champion_top10_return = float(pd.to_numeric(champion_top10["next_month_return"], errors="coerce").mean()) if len(champion_top10) else 0.0
        variant_top10_return = float(pd.to_numeric(variant_top10["next_month_return"], errors="coerce").mean()) if len(variant_top10) else 0.0
        removed_bad = champion_top10[(pd.to_numeric(champion_top10["is_next_bottom10"], errors="coerce") == 1) & (~champion_top10["sample_id"].isin(variant_top10["sample_id"]))]
        added_bad = variant_top10[(pd.to_numeric(variant_top10["is_next_bottom10"], errors="coerce") == 1) & (~variant_top10["sample_id"].isin(champion_top10["sample_id"]))]
        selection_divergence_reason = "no_divergence"
        if changed_top10_members:
            selection_divergence_reason = "top10_member_swap"
        elif changed_rank_count:
            selection_divergence_reason = "rank_reorder_inside_pool"
        monthly_rows.append(
            {
                "sample_month": int(month),
                "sample_count": int(len(month_frame)),
                "variant": variant,
                "champion_top10_hit_rate": champion_top10_hit_rate,
                "variant_top10_hit_rate": variant_top10_hit_rate,
                "champion_top10_return": champion_top10_return,
                "variant_top10_return": variant_top10_return,
                "oos_top10_uplift": float(variant_top10_return - champion_top10_return),
                "bad_pick_removal": int(len(removed_bad)),
                "changed_top10_members_count": int(len(changed_top10_members)),
                "changed_top5_members_count": int(len(changed_top5_members)),
                "changed_rank_count": int(changed_rank_count),
                "winner_promotion_delta": float(variant_top10_hit_rate - champion_top10_hit_rate),
                "loser_removal_delta": float(len(removed_bad) - len(added_bad)),
                "champion_boundary_gap": float(top10_boundary_champion["next_month_return"] - next_top10_boundary_champion["next_month_return"]) if top10_boundary_champion is not None and next_top10_boundary_champion is not None else 0.0,
                "variant_boundary_gap": float(top10_boundary_variant["next_month_return"] - next_top10_boundary_variant["next_month_return"]) if top10_boundary_variant is not None and next_top10_boundary_variant is not None else 0.0,
                "boundary_improved": bool(top10_boundary_variant is not None and next_top10_boundary_variant is not None and float(top10_boundary_variant["next_month_return"] - next_top10_boundary_variant["next_month_return"]) > float(top10_boundary_champion["next_month_return"] - next_top10_boundary_champion["next_month_return"])),
                "selection_divergence_reason": selection_divergence_reason,
                "state_family_contribution_summary": _variant_state_family_summary(variant_top10),
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
            "selection_divergence_reason": "empty",
            "state_family_contribution_summary": {},
            "regime_breakdown": {},
            "monthly_rows": [],
        }
    regime_breakdown: dict[str, Any] = {}
    for regime, regime_df in monthly_df.groupby("regime_tag", dropna=False):
        regime_breakdown[str(regime)] = {
            "month_count": int(len(regime_df)),
            "mean_oos_top10_uplift": float(regime_df["oos_top10_uplift"].mean()),
            "mean_bad_pick_removal": float(regime_df["bad_pick_removal"].mean()),
            "mean_changed_top10_members_count": float(regime_df["changed_top10_members_count"].mean()),
            "mean_variant_boundary_gap": float(regime_df["variant_boundary_gap"].mean()),
        }
    top_selected = pd.concat(top_selection_frames, ignore_index=True, sort=False) if top_selection_frames else pd.DataFrame()
    selection_reason = "top10_member_swap" if float(monthly_df["changed_top10_members_count"].mean()) > 0.0 else "rank_reorder_inside_pool" if float(monthly_df["changed_rank_count"].mean()) > 0.0 else "no_divergence"
    return {
        "variant": variant,
        "month_count": int(len(monthly_df)),
        "sample_count": int(monthly_df["sample_count"].sum()),
        "oos_top10_uplift": float(monthly_df["oos_top10_uplift"].mean()),
        "oos_bad_pick_removal": float(monthly_df["bad_pick_removal"].mean()),
        "changed_top10_members_count": float(monthly_df["changed_top10_members_count"].mean()),
        "changed_top5_members_count": float(monthly_df["changed_top5_members_count"].mean()),
        "changed_rank_count": float(monthly_df["changed_rank_count"].mean()),
        "top10_boundary_improved": bool(monthly_df["boundary_improved"].mean() > 0.5),
        "winner_promotion_delta": float(monthly_df["winner_promotion_delta"].mean()),
        "loser_removal_delta": float(monthly_df["loser_removal_delta"].mean()),
        "selection_divergence_reason": selection_reason,
        "state_family_contribution_summary": _state_family_summary(top_selected) if not top_selected.empty else {},
        "regime_breakdown": regime_breakdown,
        "monthly_rows": monthly_df.to_dict(orient="records"),
    }


def _build_ablation_compare(scored: pd.DataFrame, *, top_k: int, candidate_pool_k: int) -> dict[str, Any]:
    variants = ["A", "B", "C", "D", "E", "F", "G"]
    variant_payloads = {variant: _evaluate_variant(scored, variant=variant, top_k=top_k, candidate_pool_k=candidate_pool_k) for variant in variants}
    best_variant = max(variant_payloads.items(), key=lambda item: (float(item[1].get("oos_top10_uplift") or 0.0), float(item[1].get("winner_promotion_delta") or 0.0)))
    best_payload = best_variant[1]
    if float(best_payload.get("oos_top10_uplift") or 0.0) > 0.0 and bool(best_payload.get("top10_boundary_improved")) and float(best_payload.get("loser_removal_delta") or 0.0) >= 0.0:
        decision = "keep"
        reason = "stable_top10_uplift_with_boundary_gain"
    elif float(best_payload.get("oos_top10_uplift") or 0.0) > 0.0:
        decision = "hold"
        reason = "positive_top10_uplift_without_clear_boundary_improvement"
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
        "authoritative_rollup_decision": decision,
    }


def _build_hierarchical_rows(source_frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    monthly_frames: list[pd.DataFrame] = []
    weekly_frames: list[pd.DataFrame] = []
    daily_frames: list[pd.DataFrame] = []
    if source_frame.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    for _, code_df in source_frame.groupby("code", sort=True):
        monthly_frames.append(_annotate_change_flag(_build_monthly_context_for_code(code_df), code_col="code", order_col="month_end_date", state_col="monthly_main_state", flag_col="monthly_change_day_flag"))
        weekly_frames.append(_annotate_change_flag(_build_weekly_context_for_code(code_df), code_col="code", order_col="week_end_date", state_col="weekly_main_state", flag_col="weekly_change_day_flag"))
        daily_frames.append(_annotate_change_flag(_build_daily_context_for_code(code_df), code_col="code", order_col="date", state_col="daily_main_state", flag_col="daily_change_day_flag"))
    monthly = pd.concat(monthly_frames, ignore_index=True, sort=False) if monthly_frames else pd.DataFrame()
    weekly = pd.concat(weekly_frames, ignore_index=True, sort=False) if weekly_frames else pd.DataFrame()
    daily = pd.concat(daily_frames, ignore_index=True, sort=False) if daily_frames else pd.DataFrame()
    return monthly, weekly, daily


def _compute_hierarchical_scores(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return rows.copy()
    work = rows.copy()
    work["monthly_main_state_score"] = pd.to_numeric(work["monthly_main_state_score"], errors="coerce").fillna(50.0).clip(0.0, 100.0)
    work["weekly_main_state_score"] = pd.to_numeric(work["weekly_main_state_score"], errors="coerce").fillna(50.0).clip(0.0, 100.0)
    work["daily_main_state_score"] = pd.to_numeric(work["daily_main_state_score"], errors="coerce").fillna(50.0).clip(0.0, 100.0)
    work["monthly_phase_score"] = pd.to_numeric(work["monthly_phase_score"], errors="coerce").fillna(pd.to_numeric(work["monthly_phase_score_raw"], errors="coerce")).fillna(50.0).clip(0.0, 100.0)
    work["weekly_phase_score"] = pd.to_numeric(work["weekly_phase_score"], errors="coerce").fillna(pd.to_numeric(work["weekly_phase_score_raw"], errors="coerce")).fillna(50.0).clip(0.0, 100.0)
    work["daily_phase_score"] = pd.to_numeric(work["daily_phase_score"], errors="coerce").fillna(pd.to_numeric(work["daily_phase_score_raw"], errors="coerce")).fillna(50.0).clip(0.0, 100.0)
    work["monthly_phase_score_raw"] = pd.to_numeric(work["monthly_phase_score_raw"], errors="coerce").fillna(work["monthly_phase_score"]).clip(0.0, 100.0)
    work["weekly_phase_score_raw"] = pd.to_numeric(work["weekly_phase_score_raw"], errors="coerce").fillna(work["weekly_phase_score"]).clip(0.0, 100.0)
    work["daily_phase_score_raw"] = pd.to_numeric(work["daily_phase_score_raw"], errors="coerce").fillna(work["daily_phase_score"]).clip(0.0, 100.0)
    for column in ("monthly_change_day_flag", "weekly_change_day_flag", "daily_change_day_flag", "daily_gap_up_flag", "daily_gap_down_flag", "daily_engulfing_bull_flag", "daily_engulfing_bear_flag", "daily_reclaim_ma20_flag", "daily_lose_ma20_flag", "daily_long_lower_wick_flag", "daily_long_upper_wick_flag", "daily_small_body_flag"):
        work[column] = work[column].fillna(False).astype(bool)

    work["_monthly_state_evidence_subtotal"] = (0.60 * work["monthly_main_state_score"] + 0.40 * pd.to_numeric(work["monthly_alignment_state"].map(_ALIGNMENT_SCORE_MAP), errors="coerce").fillna(50.0)).clip(0.0, 100.0)
    work["_monthly_ma_evidence_subtotal"] = pd.to_numeric(((work["monthly_price_vs_ma12_state"].map(_STATE_SCORE_MAP) + work["monthly_price_vs_ma24_state"].map(_STATE_SCORE_MAP) + work["monthly_ma12_slope_state"].map(_SLOPE_SCORE_MAP) + work["monthly_ma24_slope_state"].map(_SLOPE_SCORE_MAP)) / 4.0), errors="coerce").fillna(50.0).clip(0.0, 100.0)
    work["_monthly_persistence_evidence_subtotal"] = ((_score_streak_series(work["monthly_above_ma12_count"]) + _score_streak_series(work["monthly_below_ma12_count"]) + _score_streak_series(work["monthly_above_ma24_count"]) + _score_streak_series(work["monthly_below_ma24_count"])) / 4.0).clip(0.0, 100.0)
    work["_monthly_trigger_evidence_subtotal"] = (work["monthly_change_day_flag"].astype(float) * 100.0).clip(0.0, 100.0)
    work["monthly_environment_score"] = (0.35 * work["_monthly_state_evidence_subtotal"] + 0.25 * work["_monthly_ma_evidence_subtotal"] + 0.20 * work["_monthly_persistence_evidence_subtotal"] + 0.20 * work["_monthly_trigger_evidence_subtotal"]).clip(0.0, 100.0)

    work["_weekly_state_evidence_subtotal"] = (0.60 * work["weekly_main_state_score"] + 0.40 * pd.to_numeric(work["weekly_alignment_state"].map(_ALIGNMENT_SCORE_MAP), errors="coerce").fillna(50.0)).clip(0.0, 100.0)
    work["_weekly_ma_evidence_subtotal"] = pd.to_numeric(((work["weekly_price_vs_ma10_state"].map(_STATE_SCORE_MAP) + work["weekly_price_vs_ma30_state"].map(_STATE_SCORE_MAP) + work["weekly_price_vs_ma60_state"].map(_STATE_SCORE_MAP) + work["weekly_ma10_slope_state"].map(_SLOPE_SCORE_MAP) + work["weekly_ma30_slope_state"].map(_SLOPE_SCORE_MAP)) / 5.0), errors="coerce").fillna(50.0).clip(0.0, 100.0)
    work["_weekly_persistence_evidence_subtotal"] = ((_score_streak_series(work["weekly_above_ma10_count"]) + _score_streak_series(work["weekly_below_ma10_count"]) + _score_streak_series(work["weekly_above_ma30_count"]) + _score_streak_series(work["weekly_below_ma30_count"])) / 4.0).clip(0.0, 100.0)
    work["_weekly_trigger_evidence_subtotal"] = (work["weekly_change_day_flag"].astype(float) * 100.0).clip(0.0, 100.0)
    work["weekly_trend_score"] = (0.40 * work["_weekly_state_evidence_subtotal"] + 0.25 * work["_weekly_ma_evidence_subtotal"] + 0.20 * work["_weekly_persistence_evidence_subtotal"] + 0.15 * work["_weekly_trigger_evidence_subtotal"]).clip(0.0, 100.0)

    work["_daily_state_evidence_subtotal"] = (0.60 * work["daily_main_state_score"] + 0.40 * pd.to_numeric(work["daily_alignment_state"].map(_ALIGNMENT_SCORE_MAP), errors="coerce").fillna(50.0)).clip(0.0, 100.0)
    work["_daily_ma_evidence_subtotal"] = pd.to_numeric(((work["daily_price_vs_ma7_state"].map(_STATE_SCORE_MAP) + work["daily_price_vs_ma20_state"].map(_STATE_SCORE_MAP) + work["daily_price_vs_ma60_state"].map(_STATE_SCORE_MAP) + work["daily_ma7_slope_state"].map(_SLOPE_SCORE_MAP) + work["daily_ma20_slope_state"].map(_SLOPE_SCORE_MAP)) / 5.0), errors="coerce").fillna(50.0).clip(0.0, 100.0)
    work["_daily_persistence_evidence_subtotal"] = ((_score_streak_series(work["daily_above_ma7_count"]) + _score_streak_series(work["daily_below_ma7_count"]) + _score_streak_series(work["daily_above_ma20_count"]) + _score_streak_series(work["daily_below_ma20_count"])) / 4.0).clip(0.0, 100.0)
    work["_daily_trigger_evidence_subtotal"] = (
        0.25 * (work["daily_gap_up_flag"].astype(float) + work["daily_gap_down_flag"].astype(float))
        + 0.30 * (work["daily_engulfing_bull_flag"].astype(float) + work["daily_engulfing_bear_flag"].astype(float))
        + 0.25 * (work["daily_reclaim_ma20_flag"].astype(float) + work["daily_lose_ma20_flag"].astype(float))
        + 0.15 * (work["daily_long_lower_wick_flag"].astype(float) + work["daily_long_upper_wick_flag"].astype(float))
        + 0.05 * work["daily_small_body_flag"].astype(float)
    ) * 100.0
    work["_daily_trigger_evidence_subtotal"] = work["_daily_trigger_evidence_subtotal"].clip(0.0, 100.0)
    work["daily_execution_score"] = (0.35 * work["_daily_state_evidence_subtotal"] + 0.20 * work["_daily_ma_evidence_subtotal"] + 0.20 * work["_daily_persistence_evidence_subtotal"] + 0.25 * work["_daily_trigger_evidence_subtotal"]).clip(0.0, 100.0)

    work["change_day_score"] = (
        30.0 * work["monthly_change_day_flag"].astype(float)
        + 25.0 * work["weekly_change_day_flag"].astype(float)
        + 20.0 * work["daily_change_day_flag"].astype(float)
        + 10.0 * work["daily_reclaim_ma20_flag"].astype(float)
        + 10.0 * work["daily_lose_ma20_flag"].astype(float)
        + 5.0 * work["daily_gap_up_flag"].astype(float)
        + 5.0 * work["daily_gap_down_flag"].astype(float)
        + 5.0 * work["daily_engulfing_bull_flag"].astype(float)
        + 5.0 * work["daily_engulfing_bear_flag"].astype(float)
    ).clip(0.0, 100.0)
    work["state_label_score"] = (0.35 * work["monthly_main_state_score"] + 0.35 * work["weekly_main_state_score"] + 0.30 * work["daily_main_state_score"]).clip(0.0, 100.0)
    work["context_score"] = (
        0.25 * work["monthly_environment_score"]
        + 0.15 * work["monthly_phase_score"]
        + 0.20 * work["weekly_trend_score"]
        + 0.10 * work["weekly_phase_score"]
        + 0.15 * work["daily_execution_score"]
        + 0.05 * work["daily_phase_score"]
        + 0.10 * work["change_day_score"]
    ).clip(0.0, 100.0)
    work = _build_historical_similarity_scores(work)
    work["similarity_filter_score"] = pd.to_numeric(work["similarity_filter_score"], errors="coerce").fillna(50.0).clip(0.0, 100.0)
    work["trigger_signal_score"] = pd.to_numeric(work["trigger_signal_score"], errors="coerce").fillna(50.0).clip(0.0, 100.0)
    work["winner_promotion_score"] = (0.45 * work["state_label_score"] + 0.25 * work["context_score"] + 0.15 * work["similarity_filter_score"] + 0.15 * work["trigger_signal_score"]).clip(0.0, 100.0)
    work["loser_removal_score"] = (0.45 * (100.0 - work["state_label_score"]) + 0.25 * (100.0 - work["context_score"]) + 0.15 * (100.0 - work["similarity_filter_score"]) + 0.15 * (100.0 - work["trigger_signal_score"])).clip(0.0, 100.0)
    return work


def build_hierarchical_label_artifacts(
    *,
    source_frame: pd.DataFrame,
    expanding_scored: pd.DataFrame,
    rolling_scored: pd.DataFrame,
    top_k: int,
    candidate_pool_k: int,
    leakage_check_status: str | None = None,
) -> dict[str, Any]:
    dictionary = _build_label_dictionary()
    rules = _build_rules()
    priority = _build_priority()
    if expanding_scored.empty:
        empty_rows = pd.DataFrame()
        summary = {
            "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
            "row_count": 0,
            "symbol_count": 0,
            "month_count": 0,
            "label_counts": {},
            "label_balance": {},
            "score_summary": {},
            "score_columns": [],
            "artifact_contract": {
                "dictionary": "hierarchical_label_dictionary.json",
                "rules": "hierarchical_label_rules.json",
                "priority": "hierarchical_label_priority.json",
                "rows": "monthly_labels_hierarchical.parquet",
            },
            "leakage_check_status": _text(leakage_check_status, fallback="unknown"),
        }
        return {
            "dictionary": dictionary,
            "rules": rules,
            "priority": priority,
            "rows": empty_rows,
            "summary": summary,
            "score_summary": {},
            "effect_by_state": {},
            "effect_by_regime": {},
            "ablation_compare": {"schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION, "variants": {}},
        }

    monthly_context, weekly_context, daily_context = _build_hierarchical_rows(source_frame)
    monthly_context = _fill_defaults(monthly_context, {
        "monthly_main_state": "monthly_range_mid",
        "monthly_price_vs_ma12_state": "near",
        "monthly_price_vs_ma24_state": "near",
        "monthly_ma12_slope_state": "flat",
        "monthly_ma24_slope_state": "flat",
        "monthly_alignment_state": "mixed",
        "monthly_above_ma12_count": 0,
        "monthly_below_ma12_count": 0,
        "monthly_above_ma24_count": 0,
        "monthly_below_ma24_count": 0,
        "monthly_main_state_score": 50.0,
        "monthly_phase_score": 50.0,
        "monthly_phase_score_raw": 50.0,
        "monthly_change_day_flag": False,
    })
    weekly_context = _fill_defaults(weekly_context, {
        "weekly_main_state": "weekly_range_mid",
        "weekly_price_vs_ma10_state": "near",
        "weekly_price_vs_ma30_state": "near",
        "weekly_price_vs_ma60_state": "near",
        "weekly_ma10_slope_state": "flat",
        "weekly_ma30_slope_state": "flat",
        "weekly_alignment_state": "mixed",
        "weekly_above_ma10_count": 0,
        "weekly_below_ma10_count": 0,
        "weekly_above_ma30_count": 0,
        "weekly_below_ma30_count": 0,
        "weekly_main_state_score": 50.0,
        "weekly_phase_score": 50.0,
        "weekly_phase_score_raw": 50.0,
        "weekly_change_day_flag": False,
    })
    daily_context = _fill_defaults(daily_context, {
        "daily_main_state": "daily_range_mid",
        "daily_price_vs_ma7_state": "near",
        "daily_price_vs_ma20_state": "near",
        "daily_price_vs_ma60_state": "near",
        "daily_ma7_slope_state": "flat",
        "daily_ma20_slope_state": "flat",
        "daily_alignment_state": "mixed",
        "daily_above_ma7_count": 0,
        "daily_below_ma7_count": 0,
        "daily_above_ma20_count": 0,
        "daily_below_ma20_count": 0,
        "daily_gap_up_flag": False,
        "daily_gap_down_flag": False,
        "daily_engulfing_bull_flag": False,
        "daily_engulfing_bear_flag": False,
        "daily_reclaim_ma20_flag": False,
        "daily_lose_ma20_flag": False,
        "daily_long_lower_wick_flag": False,
        "daily_long_upper_wick_flag": False,
        "daily_small_body_flag": False,
        "daily_change_day_flag": False,
        "daily_main_state_score": 50.0,
        "daily_phase_score": 50.0,
        "daily_phase_score_raw": 50.0,
        "trigger_signal_score": 50.0,
    })

    def enrich(scored_frame: pd.DataFrame) -> pd.DataFrame:
        base = scored_frame.copy()
        base["code"] = base["code"].astype(str)
        base["symbol"] = base["code"].astype(str)
        base["as_of_month"] = pd.to_numeric(base["sample_month"], errors="coerce").fillna(0).astype(int)
        base["source_window_start"] = pd.to_numeric(base["feature_window_start_date"], errors="coerce").fillna(0).astype(int)
        base["source_window_end"] = pd.to_numeric(base["feature_window_end_date"], errors="coerce").fillna(0).astype(int)
        base["month_end_date"] = pd.to_numeric(base["month_end_date"], errors="coerce").fillna(0).astype(int)
        base = _merge_exact_by_code(base, monthly_context, key="month_end_date")
        base = _merge_asof_by_code(base, weekly_context, left_key="month_end_date", right_key="week_end_date")
        base = _merge_asof_by_code(base, daily_context, left_key="month_end_date", right_key="date")
        base = _fill_defaults(base, {
            "monthly_change_day_flag": False,
            "weekly_change_day_flag": False,
            "daily_change_day_flag": False,
            "state_label_score": 50.0,
            "context_score": 50.0,
            "change_day_score": 50.0,
            "trigger_signal_score": 50.0,
            "similarity_filter_score": 50.0,
            "winner_promotion_score": 50.0,
            "loser_removal_score": 50.0,
        })
        base = _compute_hierarchical_scores(base)
        return base

    expanding_rows = enrich(expanding_scored)
    rolling_rows = enrich(rolling_scored)

    output_columns = [
        "sample_id",
        "code",
        "symbol",
        "sample_month",
        "as_of_month",
        "source_window_start",
        "source_window_end",
        "next_month_return",
        "next_month_return_rank",
        "next_month_rank_pct",
        "is_next_top10",
        "is_next_bottom10",
        "top10_boundary_gap",
        "champion_score",
        "rerank_score",
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
        "_monthly_state_evidence_subtotal",
        "_monthly_ma_evidence_subtotal",
        "_monthly_persistence_evidence_subtotal",
        "_monthly_trigger_evidence_subtotal",
        "_weekly_state_evidence_subtotal",
        "_weekly_ma_evidence_subtotal",
        "_weekly_persistence_evidence_subtotal",
        "_weekly_trigger_evidence_subtotal",
        "_daily_state_evidence_subtotal",
        "_daily_ma_evidence_subtotal",
        "_daily_persistence_evidence_subtotal",
        "_daily_trigger_evidence_subtotal",
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
    expanding_rows = expanding_rows[output_columns].copy()
    rolling_rows = rolling_rows[output_columns].copy()

    score_columns = [
        "_monthly_state_evidence_subtotal",
        "_monthly_ma_evidence_subtotal",
        "_monthly_persistence_evidence_subtotal",
        "_monthly_trigger_evidence_subtotal",
        "_weekly_state_evidence_subtotal",
        "_weekly_ma_evidence_subtotal",
        "_weekly_persistence_evidence_subtotal",
        "_weekly_trigger_evidence_subtotal",
        "_daily_state_evidence_subtotal",
        "_daily_ma_evidence_subtotal",
        "_daily_persistence_evidence_subtotal",
        "_daily_trigger_evidence_subtotal",
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
        "row_count": int(len(expanding_rows)),
        "symbol_count": int(expanding_rows["symbol"].nunique()),
        "month_count": int(expanding_rows["as_of_month"].nunique()),
        "label_counts": {
            "monthly_main_state": {str(label): int(count) for label, count in expanding_rows["monthly_main_state"].value_counts(dropna=False).items()},
            "weekly_main_state": {str(label): int(count) for label, count in expanding_rows["weekly_main_state"].value_counts(dropna=False).items()},
            "daily_main_state": {str(label): int(count) for label, count in expanding_rows["daily_main_state"].value_counts(dropna=False).items()},
        },
        "label_balance": _state_family_summary(expanding_rows),
        "score_summary": _summarize_scores(expanding_rows, score_columns),
        "score_columns": score_columns,
        "artifact_contract": {
            "dictionary": "hierarchical_label_dictionary.json",
            "rules": "hierarchical_label_rules.json",
            "priority": "hierarchical_label_priority.json",
            "rows": "monthly_labels_hierarchical.parquet",
        },
        "leakage_check_status": _text(leakage_check_status, fallback="unknown"),
    }
    effect_by_state = _state_family_summary(expanding_rows)
    effect_by_regime = _regime_summary(expanding_rows)
    expanding_compare = _build_ablation_compare(expanding_rows, top_k=top_k, candidate_pool_k=candidate_pool_k)
    rolling_compare = _build_ablation_compare(rolling_rows, top_k=top_k, candidate_pool_k=candidate_pool_k)
    ablation_compare = {
        "schema_version": HIERARCHICAL_LABEL_SCHEMA_VERSION,
        "top_k": int(top_k),
        "candidate_pool_k": int(candidate_pool_k),
        "modes": {"expanding": expanding_compare, "rolling": rolling_compare},
        "authoritative_mode": "expanding",
        "decision": expanding_compare["decision"],
        "decision_reason_typed": expanding_compare["decision_reason_typed"],
    }
    return {
        "dictionary": dictionary,
        "rules": rules,
        "priority": priority,
        "rows": expanding_rows,
        "summary": summary,
        "score_summary": summary["score_summary"],
        "effect_by_state": effect_by_state,
        "effect_by_regime": effect_by_regime,
        "ablation_compare": ablation_compare,
    }
