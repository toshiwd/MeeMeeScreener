from __future__ import annotations

import pandas as pd

from scripts import tradex_buyable_intersection_family_support_gate_v1 as mod


def _rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"as_of_date": 20250101, "code": "1001", "fresh_runtime_research_watch_rank": 1, "ret20": 0.1, "buy_entry_qualified": True, "variant_b_entry_qualified_top50": True},
            {"as_of_date": 20250701, "code": "1002", "fresh_runtime_research_watch_rank": 2, "ret20": -0.02, "buy_entry_qualified": True, "variant_b_entry_qualified_top50": True},
        ]
    )


def test_period_metrics_builds_half_year_buckets() -> None:
    periods = mod.period_metrics(_rows())
    assert "2025H1" in periods
    assert "2025H2" in periods


def test_date_concentration_reports_counts() -> None:
    concentration = mod.date_concentration(_rows())
    assert concentration["sample_count"] == 2
    assert concentration["date_count"] == 2
    assert concentration["max_candidates_per_date"] == 1


def test_current_projection_returns_latest_codes() -> None:
    projection = mod.current_projection(_rows())
    assert projection["latest_as_of_date"] == 20250701
    assert projection["current_candidate_codes"] == ["1002"]
    assert projection["buyable_selection_ready"] is False


def test_decide_keep_when_support_gate_passes() -> None:
    decision, decision_class, reasons = mod.decide({"support_gate_pass": True})
    assert decision == "intersection_family_ready_for_forward_paper_validation"
    assert decision_class == "KEEP"
    assert reasons
