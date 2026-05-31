from __future__ import annotations

from scripts import tradex_current_buyability_readiness_audit_v1 as mod


def test_decide_blocks_when_research_surface_is_outcome_lagged() -> None:
    decision, cls, reasons = mod.decide(
        {
            "latest_confirmed_bar_date": 20260520,
            "latest_scored_asof_date": 20260325,
            "outcome_lagged_research_surface": True,
        },
        {},
    )
    assert decision == "current_buyability_blocked_need_forward_candidate_surface"
    assert cls == "BLOCKED"
    assert reasons


def test_decide_blocks_when_no_selector_contract_ready() -> None:
    decision, cls, _ = mod.decide(
        {
            "latest_confirmed_bar_date": 20260520,
            "latest_scored_asof_date": 20260520,
            "outcome_lagged_research_surface": False,
        },
        {"x": {"usable_for_buyability": False}},
    )
    assert decision == "current_buyability_blocked_no_valid_selector"
    assert cls == "BLOCKED"


def test_decide_keep_when_contract_ready_and_current() -> None:
    decision, cls, _ = mod.decide(
        {
            "latest_confirmed_bar_date": 20260520,
            "latest_scored_asof_date": 20260520,
            "outcome_lagged_research_surface": False,
        },
        {"x": {"usable_for_buyability": True}},
    )
    assert decision == "current_buyability_pretest_ready"
    assert cls == "KEEP"


def test_missing_contracts_includes_forward_current_surface() -> None:
    contracts = mod.missing_contracts({"latest_confirmed_bar_date": 20260520})
    ids = [item["contract_id"] for item in contracts["missing_contracts"]]
    assert "forward_current_candidate_surface_v1" in ids
    assert "current_period_validation_protocol_v1" in ids
