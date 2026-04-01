from __future__ import annotations

import pandas as pd

from scripts.monthly_box_breakout_research import (
    CASE_COLUMNS,
    _build_report,
    _classify_failure_reason,
    _compute_box_month_index,
    _failed_breakout_position,
)


def test_compute_box_month_index_counts_from_box_start_month() -> None:
    start = pd.Period("2022-04", freq="M")
    apply_month = pd.Period("2022-08", freq="M")

    actual = _compute_box_month_index(start, apply_month)

    assert actual == 5


def test_failed_breakout_position_respects_20_day_window() -> None:
    in_window = pd.DataFrame(
        {
            "box_upper": [100.0] * 21,
            "c": [101.0] + [101.5] * 19 + [98.0],
            "support_break_day": [False] * 21,
            "week_support_hold": [True] * 21,
            "hit_dn5_before_up5_20d": [False] + [False] * 20,
        }
    )
    out_of_window = pd.DataFrame(
        {
            "box_upper": [100.0] * 22,
            "c": [101.0] + [101.5] * 20 + [98.0],
            "support_break_day": [False] * 22,
            "week_support_hold": [True] * 22,
            "hit_dn5_before_up5_20d": [False] + [False] * 21,
        }
    )

    assert _failed_breakout_position(in_window, 0) == 20
    assert _failed_breakout_position(out_of_window, 0) is None


def test_classify_failure_reason_uses_priority_order() -> None:
    breakout_row = {
        "box_upper": 100.0,
        "climactic_day": True,
        "week_climactic": False,
        "box_month_index": 11,
        "vol_bucket": "dry",
    }
    fail_row = {
        "c": 97.0,
        "support_break_day": True,
        "week_support_hold": False,
    }

    actual = _classify_failure_reason(breakout_row, fail_row)

    assert actual == "climactic_exhaustion"


def test_monthly_box_report_contains_required_sections() -> None:
    result = {
        "meta": {
            "db_paths": ["data/stocks.duckdb"],
            "date_min": "2020-01-01",
            "date_max": "2026-02-26",
            "events_total": 3,
            "round_trip_cost": 0.002,
        },
        "phase_summary": [
            {
                "phase": "breakout_entry",
                "n": 10,
                "mean": 0.03,
                "mean_ret20d": 0.05,
                "win_rate": 0.6,
                "mfe20d": 0.08,
                "mae20d": -0.04,
                "expected_yen_10d_1m": 30000,
                "expected_yen_20d_1m": 50000,
            }
        ],
        "month_index_summary": [
            {
                "phase": "breakout_entry",
                "box_month_bucket": "6-8",
                "n": 10,
                "mean": 0.03,
                "mean_ret20d": 0.05,
                "win_rate": 0.6,
            }
        ],
        "failure_summary": [
            {
                "failure_reason": "reentry_into_box",
                "n": 5,
                "mean_ret10d": -0.02,
                "mean_ret20d": -0.03,
                "win_rate10d": 0.2,
                "avoid_yen_10d_1m": 20000,
            }
        ],
        "bottom_entry_summary": [
            {
                "box_zone": "lower",
                "entry_style": "bottom_lower_wick",
                "n": 6,
                "mean_ret10d": 0.02,
                "mean_ret20d": 0.03,
                "win_rate10d": 0.5,
            }
        ],
        "pattern_summary": {
            "bottom_entry": {"pattern_2": [], "pattern_3": [], "shape_combo": []},
            "breakout_entry": {
                "pattern_2": [
                    {
                        "daily_pattern_2": "UL-N-GU-HB>UL-N-NG-HB",
                        "n": 7,
                        "mean_ret10d": 0.04,
                        "mean_ret20d": 0.06,
                        "win_rate10d": 0.71,
                    }
                ],
                "pattern_3": [],
                "shape_combo": [],
            },
            "failed_breakout_exit": {"pattern_2": [], "pattern_3": [], "shape_combo": []},
        },
        "replay_examples": {
            "1605": [
                {
                    "date": "2022-08-05",
                    "phase": "breakout_entry",
                    "box_month_index": 6,
                    "monthly_context": "box_upper_pressure",
                    "weekly_context": "up_support_intact",
                    "daily_pattern_2": "UL-N-GU-HB>UL-N-NG-HB",
                    "breakout_result": "successful_breakout",
                    "failure_reason": None,
                    "ret10d": 0.08,
                    "ret20d": 0.12,
                }
            ],
            "5541": [],
        },
    }

    report = _build_report(result)

    assert "# 月足ボックス breakout / failed breakout 研究" in report
    assert "## Phase Summary" in report
    assert "## Failure Summary" in report
    assert "## Replay Examples" in report
    assert "### 1605" in report
    assert "### 5541" in report
    assert CASE_COLUMNS[0] == "code"
