from __future__ import annotations

import pandas as pd

from scripts import tradex_candidate_generation_rebuild_preflight_v1 as mod


def _rows() -> pd.DataFrame:
    rows = pd.DataFrame(
        [
            {"decision_date": 20250101, "code": "1001", "baseline_rank": 5, "path20_available": True, "ret20": 0.12, "primary_family": "a"},
            {"decision_date": 20250101, "code": "1002", "baseline_rank": 20, "path20_available": True, "ret20": 0.15, "primary_family": "b"},
            {"decision_date": 20250102, "code": "1003", "baseline_rank": 80, "path20_available": True, "ret20": -0.08, "primary_family": "c"},
            {"decision_date": 20250102, "code": "1004", "baseline_rank": 140, "path20_available": True, "ret20": 0.2, "primary_family": "d"},
        ]
    )
    return mod.normalize(rows)


def test_source_funnel_counts_rank_buckets_and_low_rank_winners() -> None:
    audit = mod.source_funnel_audit(_rows(), {"diagnostic_not_real_candidate_source": True})
    assert audit["rank_bucket_counts"]["rank_1_10"] == 1
    assert audit["rank_bucket_counts"]["rank_11_50"] == 1
    assert audit["rank_bucket_counts"]["rank_51_100"] == 1
    assert audit["rank_bucket_counts"]["rank_gt_100_or_missing"] == 1
    assert audit["winners_present_but_ranked_below_top10"] == 2


def test_feature_gap_classifies_outcomes_as_future_leaks() -> None:
    audit = mod.feature_contract_gap_audit(["ret20", "ma7_slope", "liquidity_flags_json"])
    assert audit["feature_groups"]["future_outcome_fields"]["classification"] == "forbidden_future_leak"
    assert audit["feature_groups"]["liquidity_fields"]["classification"] == "available_but_not_actionable"
    assert audit["feature_groups"]["earnings_exrights_fields"]["classification"] == "unavailable"


def test_candidate_source_coverage_reports_missing_fields() -> None:
    records, summary = mod.candidate_source_coverage(_rows(), ["decision_date", "code", "ret20"])
    by_field = {r["field"]: r for r in records}
    assert by_field["ret20"]["present_in_source"] is True
    assert by_field["baseline_score"]["present_in_source"] is False
    assert "baseline_score" in summary["fields_missing"]


def test_decide_prefers_pattern_family_rebuild_for_diagnostic_source() -> None:
    decision, reasons = mod.decide(
        {"diagnostic_not_real_candidate_source": True},
        {"feature_groups": {"liquidity_fields": {"classification": "available_but_not_actionable"}}},
        {"winners_present_but_ranked_below_top10": 10, "winner_rank_distribution": {"rank_1_10": 1}},
        {},
    )
    assert decision == "rebuild_pattern_family_source_rows"
    assert reasons


def test_recommended_contract_preserves_boundaries() -> None:
    contract = mod.recommended_rebuild_contract("rebuild_pattern_family_source_rows")
    assert contract["boundary"]["meemee_reflection"] is False
    assert contract["boundary"]["runtime_db_write"] is False
    assert any("family_b" in item for item in contract["candidate_family_definitions_to_preserve_as_frozen_seeds"])
