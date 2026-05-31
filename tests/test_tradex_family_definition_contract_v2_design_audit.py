from __future__ import annotations

from scripts import tradex_family_definition_contract_v2_design_audit as mod


def test_proposed_contract_has_at_most_three_families_and_forbidden_terms() -> None:
    contract = mod.proposed_contract_v2()
    assert len(contract["families"]) == 3
    assert contract["families"][0]["family_id"] == "high_upside_contained_reserve_family_v2"
    assert "ret20" in contract["families"][0]["forbidden_terms_features"]


def test_validation_plan_has_required_stages() -> None:
    plan = mod.validation_plan(mod.proposed_contract_v2())
    first = plan["high_upside_contained_reserve_family_v2"]
    assert "stage_1_source_row_generation" in first
    assert "stage_5_optional_family_portfolio_pretest" in first


def test_decide_proceeds_when_required_features_available() -> None:
    gaps = {
        "atr_volatility_quality": {"available_status": "available_point_in_time"},
        "failed_high_upper_wick_candle_quality": {"available_status": "available_point_in_time"},
        "monthly_box_regime_quality": {"available_status": "available_point_in_time"},
        "weekly_regime_quality": {"available_status": "available_point_in_time"},
        "turnover_liquidity_proxies": {"available_status": "partial_available_via_volume_vs_20d_avg"},
    }
    decision, reasons = mod.decide(gaps, mod.proposed_contract_v2(), {"x": {"status": "closed"}})
    assert decision == "proceed_to_family_definition_v2_source_generation"
    assert reasons


def test_decide_adds_feature_contracts_when_required_missing() -> None:
    gaps = {
        "atr_volatility_quality": {"available_status": "unavailable"},
        "failed_high_upper_wick_candle_quality": {"available_status": "available_point_in_time"},
        "monthly_box_regime_quality": {"available_status": "available_point_in_time"},
        "weekly_regime_quality": {"available_status": "available_point_in_time"},
        "turnover_liquidity_proxies": {"available_status": "partial_available_via_volume_vs_20d_avg"},
    }
    decision, _ = mod.decide(gaps, mod.proposed_contract_v2(), {"x": {"status": "closed"}})
    assert decision == "add_feature_contracts_before_family_v2"


def test_feature_gap_priority_marks_event_optional_not_required() -> None:
    preflight = {"feature_groups": {"pattern_family_source_fields": {"classification": "available_point_in_time"}}}
    source = {"fields": {"liquidity_event_fields": {"classification": "unavailable"}}}
    gaps = mod.feature_gap_priority(preflight, source)
    assert gaps["liquidity_event_actionable_fields"]["required_before_family_v2_implementation"] is False
    assert gaps["atr_volatility_quality"]["required_before_family_v2_implementation"] is True
