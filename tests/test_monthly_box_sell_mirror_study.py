from __future__ import annotations

import pandas as pd

from scripts.monthly_box_sell_mirror_study import (
    _build_phase_masks,
    _build_report,
    _classify_short_failure_reason,
    _classify_short_entry_style,
)


def test_build_phase_masks_detects_upper_rejection_and_lower_breakdown() -> None:
    frame = pd.DataFrame(
        {
            "code": ["1001", "1001", "1002"],
            "dt": pd.date_range("2025-01-01", periods=3, freq="D"),
            "o": [100.0, 100.0, 100.0],
            "h": [101.5, 100.4, 99.9],
            "l": [99.2, 94.0, 93.8],
            "c": [98.6, 94.2, 93.9],
            "range": [2.0, 6.4, 0.5],
            "body": [1.4, 5.8, 6.1],
            "bar_tag": ["DL-WU-NG-LB", "DL-N-GD-LB", "DL-N-NG-LB"],
            "box_active": [True, True, True],
            "box_zone": ["upper", "lower", "mid"],
            "box_upper": [100.0, 100.0, 100.0],
            "box_lower": [95.0, 95.0, 95.0],
            "week_slope": ["down", "down", "flat"],
            "week_climactic": [False, False, False],
            "dist_bucket": ["near", "near", "near"],
            "analysis_box_month_bucket": ["6-8", "6-8", "4-5"],
            "timing_label": ["other", "day17_window", "day9_window"],
            "support_break_day": [False, True, True],
            "lower_break_after_reclaim": [False, True, False],
        }
        )

    masks = _build_phase_masks(frame)

    assert bool(masks["upper_rejection_short"].iloc[0]) is True
    assert bool(masks["lower_breakdown_short"].iloc[1]) is True
    assert bool(masks["lower_breakdown_short"].iloc[2]) is True
    assert bool(masks["lower_breakdown_strict_short"].iloc[1]) is False
    assert bool(masks["lower_breakdown_strict_short"].iloc[2]) is True
    assert bool(masks["upper_rejection_short"].iloc[2]) is False


def test_classify_short_helpers_cover_failure_and_entry_styles() -> None:
    entry_row = {
        "box_upper": 100.0,
        "box_lower": 95.0,
        "climactic_day": True,
        "week_climactic": False,
        "box_month_index": 10,
        "vol_bucket": "dry",
    }
    fail_row = {"c": 103.0}

    assert _classify_short_failure_reason(entry_row, fail_row) == "climactic_exhaustion"
    assert _classify_short_entry_style(pd.Series({"phase": "upper_rejection_short", "bar_tag": "DL-WU-NG-LB"})) == "upper_reject_lb"
    assert _classify_short_entry_style(pd.Series({"phase": "lower_breakdown_short", "support_break_day": True, "bar_tag": "DL-N-GD-LB"})) == "lower_breakdown_support"
    assert _classify_short_entry_style(pd.Series({"phase": "lower_breakdown_short", "lower_break_after_reclaim": True, "bar_tag": "DL-N-GD-LB"})) == "lower_breakdown_reclaim"


def test_report_contains_sell_mirror_sections() -> None:
    result = {
        "meta": {
            "db_paths": ["data/stocks.duckdb"],
            "date_min": "2020-01-01",
            "date_max": "2026-02-26",
            "events_total": 4,
            "round_trip_cost": 0.002,
        },
        "phase_summary": [
            {
                "phase": "upper_rejection_short",
                "n": 10,
                "mean_ret5d": 0.01,
                "mean": 0.02,
                "mean_ret20d": 0.03,
                "win_rate": 0.6,
                "mfe20d": 0.05,
                "mae20d": 0.04,
                "expected_yen_5d_1m": 10000,
                "expected_yen_10d_1m": 20000,
                "expected_yen_20d_1m": 30000,
            }
        ],
        "month_index_summary": [
            {
                "phase": "upper_rejection_short",
                "box_month_bucket": "6-8",
                "n": 10,
                "mean": 0.02,
                "mean_ret20d": 0.03,
                "win_rate": 0.6,
            }
        ],
        "period_summary": [],
        "timing_summary": [
            {
                "phase": "upper_rejection_short",
                "label": "all",
                "n": 10,
                "mean_ret5d": 0.01,
                "mean": 0.02,
                "mean_ret20d": 0.03,
                "win_rate": 0.6,
            }
        ],
        "timing_period_summary": [],
        "box_time_combo_summary": [],
        "failure_summary": [
            {
                "failure_reason": "reclaim_into_box",
                "n": 5,
                "mean_ret10d": -0.02,
                "mean_ret20d": -0.03,
                "win_rate10d": 0.2,
                "avoid_yen_10d_1m": 20000,
            }
        ],
        "entry_summary": [
            {
                "box_zone": "upper",
                "entry_style": "upper_reject_lb",
                "n": 6,
                "mean_ret5d": 0.01,
                "mean_ret10d": 0.02,
                "mean_ret20d": 0.03,
                "win_rate10d": 0.5,
            }
        ],
        "pattern_effect_summary": {
            "upper_rejection_short": [
                {
                    "phase": "upper_rejection_short",
                    "kind": "entry_style",
                    "pattern": "upper_reject_lb",
                    "n": 6,
                    "mean_ret10d": 0.02,
                    "mean_ret20d": 0.03,
                    "win_rate10d": 0.5,
                    "profit_factor10d": 1.4,
                    "mfe20d": 0.04,
                    "mae20d": 0.02,
                }
            ],
            "lower_breakdown_short": [],
            "lower_breakdown_strict_short": [],
            "failed_box_short_exit": [],
        },
        "pattern_summary": {
            "upper_rejection_short": {"pattern_2": [], "pattern_3": [], "shape_combo": []},
            "lower_breakdown_short": {"pattern_2": [], "pattern_3": [], "shape_combo": []},
            "failed_box_short_exit": {"pattern_2": [], "pattern_3": [], "shape_combo": []},
        },
        "replay_examples": {"4661": [], "6976": [], "8136": []},
    }

    report = _build_report(result)

    assert "# 月足ボックス 売りミラー" in report
    assert "## Pattern Effect Summary" in report
    assert "## Timing Summary" in report
    assert "## Failure Summary" in report
    assert "## Replay Examples" in report
    assert "### 4661" in report
