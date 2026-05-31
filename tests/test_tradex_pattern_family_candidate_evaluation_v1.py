from __future__ import annotations

import pandas as pd

from scripts import tradex_pattern_family_candidate_evaluation_v1 as mod


def _rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20250101, "code": "1001", "family_a": True, "family_b": False, "ret5": 0.01, "ret20": 0.12, "winner_ret20_gt_10pct": True, "bad_ret20_lt_minus_5pct": False, "severe_ret20_lt_minus_10pct": False},
            {"as_of_date": 20250102, "code": "1002", "family_a": True, "family_b": True, "ret5": -0.01, "ret20": -0.08, "winner_ret20_gt_10pct": False, "bad_ret20_lt_minus_5pct": True, "severe_ret20_lt_minus_10pct": False},
            {"as_of_date": 20250103, "code": "1003", "family_a": False, "family_b": True, "ret5": 0.02, "ret20": 0.03, "winner_ret20_gt_10pct": False, "bad_ret20_lt_minus_5pct": False, "severe_ret20_lt_minus_10pct": False},
        ]
    )


def test_metric_computes_required_fields() -> None:
    m = mod.metric(_rows()[_rows()["family_a"]], {20250101, 20250102, 20250103})
    assert m["sample_count"] == 2
    assert m["zero_candidate_date_count"] == 1
    assert m["winner_rate_ret20_gt_10pct"] == 0.5
    assert m["bad_rate_ret20_lt_minus_5pct"] == 0.5
    assert m["outcome_coverage_rate"] == 1.0


def test_overlap_matrix_counts_pairwise_overlap() -> None:
    matrix = mod.overlap_matrix(_rows(), ["family_a", "family_b"])
    assert matrix["family_a"]["family_b"]["overlap_sample_count"] == 1
    assert matrix["family_a"]["family_b"]["left_overlap_rate"] == 0.5


def test_family_decision_marks_broad_reference_proxy_only() -> None:
    metrics = {mod.BROAD_REFERENCE_FLAG: {"outcome_coverage_rate": 1.0, "sample_count": 200000, "average_candidates_per_date": 100}}
    breadth = {"families": {mod.BROAD_REFERENCE_FLAG: {"is_broad_screen": True}}}
    decisions = mod.family_decisions(metrics, breadth)
    assert decisions[mod.BROAD_REFERENCE_FLAG]["decision"] == "reference_proxy_only"


def test_overall_decision_prefers_keep() -> None:
    decision, reasons = mod.overall_decision({"x": {"decision": "keep_for_next_family_pretest"}})
    assert decision == "family_candidate_keep_found"
    assert reasons


def test_overall_decision_detects_only_broad_or_reference() -> None:
    decision, _ = mod.overall_decision({"a": {"decision": "reference_proxy_only"}, "b": {"decision": "broad_low_quality_screen"}})
    assert decision == "only_broad_low_quality_or_reference_proxies"
