from __future__ import annotations

from scripts import tradex_buy_candidate_generation_surface_closure_v1 as mod


def test_missing_contract_decision_requires_new_contracts() -> None:
    missing = mod.missing_contract_decision({"feature_groups": {}}, {"fields": {"liquidity_event_fields": {"classification": "unavailable"}, "earnings_exrights_fields": {"classification": "unavailable"}}})
    assert missing["decision"] == "add_feature_contracts_before_continue"
    assert missing["missing_or_insufficient_contracts"]["asof_high_upside_score_contract"] is True


def test_next_contract_requirements_names_two_contracts() -> None:
    req = mod.next_contract_requirements()
    ids = [item["contract_id"] for item in req["required_before_more_family_generation"]]
    assert "asof_positive_selection_score_v1" in ids
    assert "actionable_event_liquidity_risk_v1" in ids


def test_decide_blocks_when_no_keep() -> None:
    decision, cls, reasons = mod.decide({"research_decision": "drop"}, {})
    assert decision == "add_feature_contracts_before_continue"
    assert cls == "BLOCKED"
    assert reasons


def test_decide_keep_passthrough() -> None:
    decision, cls, _ = mod.decide({"research_decision": "keep_for_next_stage"}, {})
    assert decision == "keep_for_next_stage"
    assert cls == "KEEP"


def test_failed_surface_evidence_keeps_interpretation() -> None:
    evidence = mod.failed_surface_evidence({}, {}, {"research_decision": "selectivity_edge_overlap_driven_close_or_redesign"})
    assert evidence["v1_selectivity"]["decision"] == "selectivity_edge_overlap_driven_close_or_redesign"
