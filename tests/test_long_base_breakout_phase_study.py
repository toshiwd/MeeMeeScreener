from __future__ import annotations

import pandas as pd

from scripts.long_base_breakout_phase_study import _build_report, _phase_start, _phase_summary_row


def test_phase_start_only_marks_first_day_of_contiguous_run() -> None:
    mask = pd.Series([False, True, True, False, True, True, True, False, True])
    codes = pd.Series(["1001", "1001", "1001", "1001", "1001", "1002", "1002", "1002", "1002"])

    actual = _phase_start(mask, codes).tolist()

    assert actual == [False, True, False, False, True, True, False, False, True]


def test_phase_summary_row_reports_takeprofit_avoidance_yen() -> None:
    frame = pd.DataFrame(
        {
            "ret_long_10d": [-0.02, -0.01, -0.03],
            "ret_long_20d": [-0.01, -0.02, -0.04],
            "mfe_20d": [0.02, 0.01, 0.03],
            "mae_20d": [-0.05, -0.06, -0.08],
            "hit_up5_before_dn5_20d": [False, False, False],
            "hit_dn5_before_up5_20d": [True, True, True],
        }
    )

    summary = _phase_summary_row("takeprofit", frame)

    assert summary["n"] == 3
    assert summary["mean"] < 0.0
    assert summary["avoid_yen_10d_1m"] > 0
    assert summary["avoid_yen_20d_1m"] > 0


def test_phase_report_contains_phase_sections() -> None:
    result = {
        "meta": {
            "db_paths": ["data/stocks.duckdb"],
            "focus_code": "5541",
            "date_min": "2020-01-01",
            "date_max": "2026-02-26",
            "capital_base_yen": 1000000,
            "round_trip_cost": 0.002,
            "events_total": 3,
        },
        "phase_summary": [
            {
                "phase": "entry",
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
        "period_summary": [
            {
                "phase": "entry",
                "period_bucket": "2023-2026",
                "n": 10,
                "mean": 0.03,
                "mean_ret20d": 0.05,
                "win_rate": 0.6,
            }
        ],
        "pattern_summary": {
            "entry": [
                {
                    "pattern_2": "UL-N-NG-HB>US-N-NG-IN",
                    "n": 12,
                    "mean_ret10d": 0.04,
                    "mean_ret20d": 0.06,
                    "win_rate10d": 0.66,
                    "dn5_before_up5_20d": 0.2,
                }
            ],
            "add": [],
            "takeprofit": [],
        },
        "replay": [
            {
                "date": "2025-02-28",
                "phase": "takeprofit",
                "premise_bucket": "5541_long_base_breakout",
                "pattern_2": "DL-N-NG-LB>DL-N-NG-LB",
                "ret10d": -0.02,
                "ret20d": -0.05,
                "mfe20d": 0.01,
                "mae20d": -0.08,
            }
        ],
    }

    report = _build_report(result)

    assert "# 5541型 Phase Study" in report
    assert "## Phase Summary" in report
    assert "## Similar Patterns" in report
    assert "## 5541 Replay" in report
    assert "takeprofit" in report
