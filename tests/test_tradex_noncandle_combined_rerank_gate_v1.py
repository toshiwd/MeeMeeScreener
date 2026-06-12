from __future__ import annotations

from scripts.tradex_noncandle_combined_rerank_gate_v1 import _apply_cnt60, _apply_monthly_range


def test_combined_rerank_applies_monthly_range_after_cnt60_rank() -> None:
    rows = [
        {"dt": 20260605, "dir": "up", "rank": 1, "code": "A", "cnt60Up": 40.0, "monthlyRangeProb": 0.0, "forward_return_20": 0.01},
        {"dt": 20260605, "dir": "up", "rank": 2, "code": "B", "cnt60Up": 4.0, "monthlyRangeProb": 0.8, "forward_return_20": 0.02},
        {"dt": 20260605, "dir": "up", "rank": 3, "code": "C", "cnt60Up": 5.0, "monthlyRangeProb": 0.0, "forward_return_20": 0.03},
    ]

    cnt60_rows = _apply_cnt60(rows)
    combined_rows = _apply_monthly_range(cnt60_rows)
    by_code = {row["code"]: row for row in combined_rows}

    assert by_code["B"]["cnt60_rank"] == 1
    assert by_code["C"]["cnt60_rank"] == 2
    assert by_code["A"]["cnt60_rank"] == 3
    assert by_code["C"]["combined_rank"] == 1
    assert by_code["A"]["combined_rank"] == 2
    assert by_code["B"]["combined_rank"] == 3
    assert by_code["C"]["combined_rank_reason"] == "monthly_range_prob_rank_window_pass"
    assert by_code["B"]["combined_rank_reason"] == "monthly_range_prob_rank_window_demoted"


def test_combined_rerank_does_not_use_silent_fallback_for_missing_monthly_range() -> None:
    rows = [
        {"dt": 20260605, "dir": "up", "rank": 1, "code": "A", "cnt60Up": 4.0, "monthlyRangeProb": None, "forward_return_20": 0.01},
        {"dt": 20260605, "dir": "up", "rank": 2, "code": "B", "cnt60Up": 5.0, "monthlyRangeProb": 0.0, "forward_return_20": 0.02},
    ]

    combined_rows = _apply_monthly_range(_apply_cnt60(rows))
    by_code = {row["code"]: row for row in combined_rows}

    assert by_code["B"]["combined_rank"] == 1
    assert by_code["A"]["combined_rank"] == 2
    assert by_code["A"]["combined_rank_reason"] == "missing_monthly_range_prob_no_silent_fallback"
