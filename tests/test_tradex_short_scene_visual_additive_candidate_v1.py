from __future__ import annotations

from scripts.tradex_short_scene_visual_additive_candidate_v1 import _decision, _select_one_per_date, _topk_compare


def test_select_one_per_date_prefers_lower_ma20_slope() -> None:
    selected = _select_one_per_date(
        [
            {"dt": 20260401, "code": "2001", "ma20_slope_10": 0.02},
            {"dt": 20260401, "code": "2002", "ma20_slope_10": -0.03},
            {"dt": 20260402, "code": "2003", "ma20_slope_10": None},
        ]
    )

    assert selected[20260401]["code"] == "2002"
    assert selected[20260402]["code"] == "2003"


def test_topk_compare_adds_selected_candidate_and_branches() -> None:
    rows = [
        {"dt": 20260401, "code": f"100{i}", "tradePriorityScore": 1.0 - i * 0.01, "finalRank": i, "forward_return_20": -0.01}
        for i in range(1, 7)
    ]
    selected = {
        20260401: {
            "dt": 20260401,
            "code": "2001",
            "forward_return_20": 0.06,
            "ma20_slope_10": -0.02,
        }
    }

    compare = _topk_compare({20260401: rows}, selected, topk=5)

    assert compare["changed_member_count_total"] == 2
    assert compare["selected_additive_rows_in_topk"] == 1
    assert compare["additive_delta"]["forward_return_20_mean"] > 0


def test_additive_decision_drops_when_no_candidates() -> None:
    compare = {
        "top5": {"changed_member_count_total": 0, "additive_delta": {"forward_return_20_mean": 0.0}},
        "top10": {"additive_delta": {"forward_return_20_mean": 0.0}},
    }
    coverage = {"selected_additive_candidate_count": 0, "selected_additive_date_count": 0}

    assert _decision(compare, coverage) == {"judgment": "drop", "reason_type": "no_additive_candidates_selected"}
