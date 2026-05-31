from __future__ import annotations

import pandas as pd

from scripts.tradex_recent_topk_selected_loser_timing_decomposition_v1 import (
    classify_failures,
    delayed_summary,
    future_path_for_row,
)


def test_future_path_delayed_entry_improves_ret20() -> None:
    daily = pd.DataFrame(
        {
            "decision_ymd": [20240101 + i for i in range(30)],
            "close": [100, 90, 90, 90, 90, 90, *([100] * 24)],
            "low": [99, 89, 89, 89, 89, 89, *([99] * 24)],
            "high": [101, 91, 91, 91, 91, 91, *([101] * 24)],
            "ma7": [95] * 30,
            "ma20": [95] * 30,
        }
    )
    row = pd.Series({"code": "A", "decision_ymd": 20240101, "ret20_num": -0.1})

    out = future_path_for_row(row, {"A": daily})

    assert out["path_available"] is True
    assert out["delayed_t5_improves_ret20"] is True


def test_delayed_summary_has_loser_and_winner_rows() -> None:
    rows = pd.DataFrame(
        [
            {"loser20": True, "winner20": False, "delayed_t5_ret20": 0.01, "delayed_t5_improves_ret20": True, "delayed_t5_avoids_loser": True},
            {"loser20": False, "winner20": True, "delayed_t5_ret20": 0.05, "delayed_t5_improves_ret20": False, "delayed_t5_avoids_loser": True},
        ]
    )

    result = delayed_summary(rows)

    assert {"selected_loser", "selected_winner"} <= set(result["cohort"])


def test_classify_failures_marks_true_bad_candidate() -> None:
    rows = pd.DataFrame(
        [
            {
                "year": 2024,
                "loser20": True,
                "dist_ma20_pct": 0.0,
                "upper_wick_ratio": 0.0,
                "failed_high_update": False,
                "ret3": -0.01,
                "close_below_ma7": False,
                "close_below_ma20": False,
                "ma7_slope_down": False,
                "delayed_t3_improves_ret20": False,
                "delayed_t5_improves_ret20": False,
                "ma7_delayed_improves_ret20": False,
                "ma20_delayed_improves_ret20": False,
            }
        ]
    )

    result = classify_failures(rows)

    assert result.iloc[0]["failure_type"] == "true_bad_candidate"
