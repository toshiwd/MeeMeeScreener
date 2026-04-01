from __future__ import annotations

import pandas as pd

from scripts.monthly_box_time_window_study import (
    _add_time_window_features,
    _build_article_gate_assessment,
    _build_report,
)


def test_add_time_window_features_marks_month_start_end_and_day_windows() -> None:
    frame = pd.DataFrame(
        {
            "code": ["1001"] * 31,
            "dt": pd.date_range("2025-01-01", periods=31, freq="D"),
            "o": [100.0] * 31,
            "h": [101.0] * 31,
            "l": [99.0] * 31,
            "c": [100.5] * 31,
        }
    )

    actual = _add_time_window_features(frame)

    assert actual.loc[0, "timing_label"] == "month_start_1_3"
    assert actual.loc[8, "timing_label"] == "day9_window"
    assert actual.loc[16, "timing_label"] == "day17_window"
    assert actual.loc[25, "timing_label"] == "day26_window"
    assert actual.loc[30, "timing_label"] == "month_end_1_3"
    assert bool(actual.loc[8, "timing_gate"]) is True
    assert bool(actual.loc[4, "timing_gate"]) is False


def test_article_gate_assessment_picks_best_timing_label() -> None:
    events = pd.DataFrame(
        [
            {
                "phase": "bottom_entry",
                "ret_long_10d": 0.01,
                "ret_long_20d": 0.02,
                "mfe_20d": 0.04,
                "mae_20d": -0.03,
                "timing_label": "other",
                "timing_gate": False,
            },
            {
                "phase": "bottom_entry",
                "ret_long_10d": 0.03,
                "ret_long_20d": 0.06,
                "mfe_20d": 0.08,
                "mae_20d": -0.02,
                "timing_label": "month_start_1_3",
                "timing_gate": True,
            },
            {
                "phase": "breakout_entry",
                "ret_long_10d": 0.02,
                "ret_long_20d": 0.03,
                "mfe_20d": 0.05,
                "mae_20d": -0.01,
                "timing_label": "day17_window",
                "timing_gate": True,
            },
            {
                "phase": "breakout_entry",
                "ret_long_10d": -0.01,
                "ret_long_20d": -0.02,
                "mfe_20d": 0.01,
                "mae_20d": -0.04,
                "timing_label": "other",
                "timing_gate": False,
            },
        ]
    )

    actual = _build_article_gate_assessment(events)

    assert actual["bottom_entry"]["best_timing_label"] == "month_start_1_3"
    assert actual["bottom_entry"]["gate_minus_all_mean20"] is not None
    assert actual["breakout_entry"]["best_timing_label"] == "day17_window"


def test_report_contains_timing_sections() -> None:
    result = {
        "meta": {
            "db_paths": ["data/stocks.duckdb"],
            "date_min": "2020-01-01",
            "date_max": "2026-02-26",
            "events_total": 4,
            "round_trip_cost": 0.002,
            "timing_labels": ["month_start_1_3", "month_end_1_3", "day9_window", "day17_window", "day26_window", "other"],
        },
        "phase_summary": [
            {
                "phase": "bottom_entry",
                "n": 10,
                "mean": 0.02,
                "mean_ret20d": 0.03,
                "win_rate": 0.6,
                "profit_factor": 1.4,
                "mfe20d": 0.05,
                "mae20d": -0.03,
                "expected_yen_10d_1m": 20000,
                "expected_yen_20d_1m": 30000,
            }
        ],
        "timing_summary": [
            {
                "phase": "bottom_entry",
                "label": "all",
                "n": 10,
                "mean_ret10d": 0.02,
                "mean_ret20d": 0.03,
                "win_rate10d": 0.6,
                "profit_factor10d": 1.4,
                "mfe20d": 0.05,
                "mae20d": -0.03,
                "expected_yen_10d_1m": 20000,
                "expected_yen_20d_1m": 30000,
            },
            {
                "phase": "bottom_entry",
                "label": "timing_gate",
                "n": 4,
                "mean_ret10d": 0.03,
                "mean_ret20d": 0.04,
                "win_rate10d": 0.75,
                "profit_factor10d": 1.8,
                "mfe20d": 0.06,
                "mae20d": -0.02,
                "expected_yen_10d_1m": 30000,
                "expected_yen_20d_1m": 40000,
            },
        ],
        "box_time_combo_summary": [],
        "timing_period_summary": [],
        "article_gate_assessment": {
            "bottom_entry": {
                "all_mean20": 0.03,
                "all_pf20": 1.4,
                "gate_mean20": 0.04,
                "gate_pf20": 1.8,
                "month_start_mean20": 0.05,
                "month_end_mean20": 0.01,
                "day9_mean20": 0.02,
                "day17_mean20": 0.04,
                "day26_mean20": 0.00,
                "best_timing_label": "month_start_1_3",
                "best_timing_mean20": 0.05,
                "gate_minus_all_mean20": 0.01,
            }
        },
        "replay_examples": {"1605": [], "5541": []},
    }

    report = _build_report(result)

    assert "## Timing Gate Comparison" in report
    assert "## Timing Label Summary" in report
    assert "## Article Gate Assessment" in report
