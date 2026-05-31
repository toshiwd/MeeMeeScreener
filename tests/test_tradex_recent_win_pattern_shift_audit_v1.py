from __future__ import annotations

import math

import pandas as pd

from scripts.tradex_recent_win_pattern_shift_audit_v1 import (
    _days_since_event,
    _future_any,
    _future_max,
    _future_min,
    _period,
    _streak_bool,
    pattern_shift_matrix,
    research_decision,
)


def test_period_contract() -> None:
    assert _period(2019) == "pre_recent_2019_2023"
    assert _period(2023) == "pre_recent_2019_2023"
    assert _period(2024) == "recent_2024_2026"
    assert _period(2026) == "recent_2024_2026"


def test_streak_and_days_since_are_past_only() -> None:
    cond = pd.Series([False, True, True, False, True])
    assert _streak_bool(cond).tolist() == [0, 1, 2, 0, 1]
    event = pd.Series([False, True, False, False, True])
    assert _days_since_event(event).tolist() == [pd.NA, 0, 1, 2, 0]


def test_future_helpers_start_after_anchor_row() -> None:
    values = pd.Series([10, 8, 9, 7])
    mins = _future_min(values, 2).tolist()
    maxes = _future_max(values, 2).tolist()
    assert mins[:3] == [8.0, 7.0, 7.0]
    assert maxes[:3] == [9.0, 9.0, 7.0]
    assert math.isnan(mins[3])
    assert math.isnan(maxes[3])
    cond = pd.Series([False, False, True, False])
    assert _future_any(cond, 2).tolist() == [True, True, False, False]


def test_pattern_shift_matrix_classifies_recent_and_decayed_features() -> None:
    decomp = pd.DataFrame(
        [
            {"feature_group": "trend_ma", "feature": "early_trend_flag", "period_bucket": "pre_recent_2019_2023", "effect_size_mean_diff": 0.0},
            {"feature_group": "trend_ma", "feature": "early_trend_flag", "period_bucket": "recent_2024_2026", "effect_size_mean_diff": 0.05},
            {"feature_group": "freshness", "feature": "mature_trend_flag", "period_bucket": "pre_recent_2019_2023", "effect_size_mean_diff": 0.05},
            {"feature_group": "freshness", "feature": "mature_trend_flag", "period_bucket": "recent_2024_2026", "effect_size_mean_diff": -0.01},
        ]
    )

    matrix = pattern_shift_matrix(decomp)
    classes = dict(zip(matrix["feature"], matrix["pattern_class"]))

    assert classes["early_trend_flag"] == "recent_winner_feature"
    assert classes["mature_trend_flag"] == "decayed_feature"


def test_research_decision_requires_recent_sample_gate() -> None:
    rows = pd.DataFrame(
        {
            "period_bucket": ["recent_2024_2026"] * 2,
            "ret20": [0.1, 0.2],
        }
    )
    matrix = pd.DataFrame(
        {
            "feature_group": ["trend_ma"],
            "pattern_class": ["recent_winner_feature"],
        }
    )

    decision = research_decision(rows, matrix, {"recommended_single_axis": "x"})

    assert decision["research_decision"] == "inconclusive"
    assert "fewer than 1000" in decision["reason_typed"][0]
