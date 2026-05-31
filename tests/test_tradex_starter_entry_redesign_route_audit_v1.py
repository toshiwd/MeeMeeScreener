from __future__ import annotations

import pandas as pd

from scripts import tradex_starter_entry_redesign_route_audit_v1 as mod


def test_feature_gap_audit_marks_sequence_and_external_gaps() -> None:
    audit = mod.feature_gap_audit()
    by_signal = {row["signal"]: row["classification"] for row in audit["gap_items"]}

    assert by_signal["chart_shape_n_wave_inverse_n"] == "requires_image_or_sequence_representation"
    assert by_signal["post_breakout_retest"] == "missing_derivable_from_daily_bars"
    assert by_signal["event_risk"] == "requires_external_data"


def test_redesign_options_include_family_split_and_chart_shape() -> None:
    options = mod.redesign_options()["options"]
    names = {row["name"] for row in options}

    assert "starter_entry_family_split_v1" in names
    assert "chart_shape_rerank_v1" in names
    assert "candidate_pool_rebuild_v1" in names


def test_decide_prefers_family_split_when_deep_good_and_repeated_failures() -> None:
    miss = pd.DataFrame(
        [
            {"starter_good_count": 100, "deep_ranked_starter_good_gt50": 45},
            {"starter_good_count": 100, "deep_ranked_starter_good_gt50": 35},
        ]
    )
    ledger = {
        "entries": [
            {"failure_flags": ["replacement_quality_negative"]},
            {"failure_flags": ["replacement_quality_negative"]},
            {"failure_flags": ["replacement_quality_negative"]},
        ]
    }

    decision = mod.decide(miss, mod.feature_gap_audit(), ledger)

    assert decision["research_decision"] == "candidate_family_split_needed"
    assert decision["meemee_reflectable_candidate"] is False


def test_decide_feature_expansion_when_family_evidence_is_weak() -> None:
    miss = pd.DataFrame([{"starter_good_count": 100, "deep_ranked_starter_good_gt50": 10}])
    ledger = {"entries": [{"failure_flags": []}]}

    decision = mod.decide(miss, mod.feature_gap_audit(), ledger)

    assert decision["research_decision"] == "feature_contract_expansion_needed"
