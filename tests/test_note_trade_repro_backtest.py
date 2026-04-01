from __future__ import annotations

import pandas as pd

from scripts.month_end_shape_study import classify_5541_premise_bucket
from scripts.note_trade_repro_backtest import (
    _build_markdown_report,
    _classify_entry_case,
    _classify_path_quality,
)


def test_classify_5541_premise_bucket_distinguishes_breakout_retest_and_chase() -> None:
    frame = pd.DataFrame(
        [
            {
                "box_months": 6,
                "box_state": "breakout_up",
                "trend_bucket": "up",
                "cnt60_up": 40,
                "dist_bucket": "extended",
                "box_wild": False,
            },
            {
                "box_months": 5,
                "box_state": "box_upper",
                "trend_bucket": "stack_up",
                "cnt60_up": 55,
                "dist_bucket": "near",
                "box_wild": False,
            },
            {
                "box_months": 8,
                "box_state": "breakout_up",
                "trend_bucket": "up",
                "cnt60_up": 120,
                "dist_bucket": "overheat",
                "box_wild": False,
            },
            {
                "box_months": 6,
                "box_state": "box_upper",
                "trend_bucket": "mixed",
                "cnt60_up": 12,
                "dist_bucket": "near",
                "box_wild": False,
            },
        ]
    )

    actual = classify_5541_premise_bucket(frame).tolist()

    assert actual == [
        "5541_long_base_breakout",
        "5541_first_retest",
        "late_vertical_chase",
        "base_breakout_watch",
    ]


def test_note_trade_repro_entry_case_and_path_quality_classification() -> None:
    frame = pd.DataFrame(
        [
            {
                "premise_bucket": "5541_long_base_breakout",
                "week_slope": "up",
                "week_support_hold": True,
                "week_climactic": False,
                "box_zone": "upper",
                "day_pos_ma20": "below20",
                "cnt20_down": 2,
                "recent_breakout_15d": False,
                "touch_ma20": False,
                "cnt7_down": 1,
                "climactic_day": False,
                "dist_ma20": 0.01,
                "hit_up5_before_dn5_20d": True,
                "hit_dn5_before_up5_20d": False,
                "mae_20d": -0.03,
                "ret_long_10d": 0.08,
            },
            {
                "premise_bucket": "5541_long_base_breakout",
                "week_slope": "up",
                "week_support_hold": True,
                "week_climactic": False,
                "box_zone": "breakout",
                "day_pos_ma20": "above20",
                "cnt20_down": 0,
                "recent_breakout_15d": True,
                "touch_ma20": True,
                "cnt7_down": 2,
                "climactic_day": False,
                "dist_ma20": 0.04,
                "hit_up5_before_dn5_20d": False,
                "hit_dn5_before_up5_20d": False,
                "mae_20d": -0.02,
                "ret_long_10d": 0.03,
            },
            {
                "premise_bucket": "5541_long_base_breakout",
                "week_slope": "up",
                "week_support_hold": True,
                "week_climactic": False,
                "box_zone": "breakout",
                "day_pos_ma20": "above20",
                "cnt20_down": 0,
                "recent_breakout_15d": False,
                "touch_ma20": False,
                "cnt7_down": 0,
                "climactic_day": True,
                "dist_ma20": 0.16,
                "hit_up5_before_dn5_20d": False,
                "hit_dn5_before_up5_20d": True,
                "mae_20d": -0.11,
                "ret_long_10d": -0.06,
            },
        ]
    )

    entry_cases = _classify_entry_case(frame).tolist()
    path_quality = _classify_path_quality(frame).tolist()

    assert entry_cases == ["anticipatory_pilot", "first_support_add", "vertical_chase"]
    assert path_quality == ["clean_trend", "volatile_win", "failed_fast"]


def test_note_trade_repro_markdown_report_contains_new_sections() -> None:
    result = {
        "meta": {
            "db_paths": ["data/stocks.duckdb"],
            "codes": 12,
            "date_min": "2020-01-01",
            "date_max": "2026-02-26",
            "round_trip_cost": 0.002,
            "min_samples": 20,
            "study_rows": 100,
            "focus_code": "5541",
        },
        "replay": {
            "5541": [
                {
                    "date": "2025-12-29",
                    "entry_case": "anticipatory_pilot",
                    "premise_bucket": "5541_long_base_breakout",
                    "path_quality": "clean_trend",
                    "ret10d": 0.12,
                    "ret20d": 0.18,
                    "ret_climactic_partial": 0.14,
                    "ret_trend_break": 0.09,
                    "ret_time_stop": 0.05,
                }
            ]
        },
        "cross_section": {
            "premise_bucket": [
                {
                    "premise_bucket": "5541_long_base_breakout",
                    "n": 40,
                    "mean": 0.08,
                    "win_rate": 0.6,
                    "mfe20d": 0.14,
                    "mae20d": -0.05,
                    "up5_before_dn5_20d": 0.55,
                }
            ],
            "entry_case": [
                {
                    "entry_case": "anticipatory_pilot",
                    "n": 18,
                    "mean": 0.07,
                    "win_rate": 0.58,
                    "mfe20d": 0.13,
                    "mae20d": -0.05,
                }
            ],
        },
        "exit_case_stats": [
            {
                "exit_case": "trend_break",
                "n": 30,
                "mean": 0.06,
                "win_rate": 0.57,
                "avg_days": 7.4,
                "median_days": 6.0,
            }
        ],
        "exclusion_rules": [
            {
                "rule": "late_vertical_chase",
                "n": 14,
                "mean_ret10d": -0.03,
                "delta_vs_baseline": -0.05,
                "win_rate": 0.29,
            }
        ],
        "pattern_study": {"pattern_2": [], "pattern_3": [], "pattern_4": []},
    }

    report = _build_markdown_report(result)

    assert "# 5541型の再現性検証と早仕込み建玉研究" in report
    assert "## 5541の局面分解" in report
    assert "## 建玉3案の比較" in report
    assert "## 利確・撤退3案の比較" in report
    assert "## 失敗しやすい局面の除外条件" in report
    assert "anticipatory_pilot" in report
    assert "trend_break" in report
