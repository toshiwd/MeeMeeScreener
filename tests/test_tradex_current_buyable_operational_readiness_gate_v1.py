from __future__ import annotations

from scripts import tradex_current_buyable_operational_readiness_gate_v1 as mod


def _inputs(ret20_ready: bool = False, full_risk: bool = False) -> dict[str, object]:
    return {
        "forward_decision": {
            "research_decision": "forward_validation_pending_more_confirmed_bars",
            "runtime_db_write": False,
            "meemee_reflectable_candidate": False,
            "production_ranking_changed": False,
            "production_candidate_generator_changed": False,
            "active_gate_created": False,
            "validated_buy_count": 0,
        },
        "forward_summary": {
            "candidate_count": 2,
            "selected_codes": ["8086", "9831"],
            "selected_as_of_date": 20260520,
            "minimum_available_future_sessions": 20 if ret20_ready else 2,
            "ret5_all_candidates_ready": ret20_ready,
            "ret20_all_candidates_ready": ret20_ready,
            "status_counts": {"ret20_ready" if ret20_ready else "pending_ret5": 2},
        },
        "forward_metrics": {
            "ret20": {
                "mean_ret20": 0.05 if ret20_ready else None,
                "winner_rate_ret20_gt_10pct": 0.25 if ret20_ready else None,
                "bad_rate_ret20_lt_minus_5pct": 0.10 if ret20_ready else None,
                "severe_rate_ret20_lt_minus_10pct": 0.05 if ret20_ready else None,
            }
        },
        "invalidation_decision": {"research_decision": "invalidation_contract_ready_for_forward_tracking"},
        "invalidation_summary": {"complete_level_count": 2},
        "invalidation_source": {"feature_snapshot_complete": full_risk, "recent_swing_low_complete": True},
    }


def test_readiness_gate_blocks_when_forward_outcomes_are_pending() -> None:
    gate = mod.readiness_gate(_inputs())
    assert gate["freeze_gate_pass"] is True
    assert gate["ret5_maturity_gate_pass"] is False
    assert gate["operational_readiness_gate_pass"] is False


def test_blocking_contracts_names_forward_and_risk_blockers() -> None:
    inputs = _inputs()
    gate = mod.readiness_gate(inputs)
    blockers = mod.blocking_contracts(gate, inputs)
    contracts = {row["contract"] for row in blockers["blocking_contracts"]}
    assert "forward_ret5_confirmed_outcome" in contracts
    assert "forward_ret20_confirmed_outcome" in contracts
    assert "full_invalidation_risk_levels" in contracts


def test_readiness_gate_passes_only_when_ret20_and_full_risk_pass() -> None:
    gate = mod.readiness_gate(_inputs(ret20_ready=True, full_risk=True))
    assert gate["ret20_quality_gate_pass"] is True
    assert gate["full_risk_contract_gate_pass"] is True
    assert gate["operational_readiness_gate_pass"] is True


def test_repaired_ma_atr_source_counts_as_full_risk_contract() -> None:
    inputs = _inputs(ret20_ready=True, full_risk=False)
    inputs["invalidation_decision"] = {"research_decision": "invalidation_contract_repaired_full_levels_ready"}
    inputs["invalidation_source"] = {"ma20_complete": True, "atr14_complete": True, "recent_swing_low_complete": True}
    gate = mod.readiness_gate(inputs)
    assert gate["invalidation_tracking_gate_pass"] is True
    assert gate["full_risk_contract_gate_pass"] is True


def test_decide_blocked_when_contracts_are_missing() -> None:
    inputs = _inputs()
    gate = mod.readiness_gate(inputs)
    blockers = mod.blocking_contracts(gate, inputs)
    decision, decision_class, reasons = mod.decide(gate, blockers, {"no_lookahead_pass": True})
    assert decision == "operational_readiness_blocked_pending_forward_outcomes_or_full_risk_contract"
    assert decision_class == "BLOCKED"
    assert "forward_ret5_ret20_or_full_risk_contracts_missing" in reasons


def test_no_lookahead_accepts_repaired_invalidation_contract() -> None:
    inputs = _inputs()
    inputs["invalidation_decision"] = {"research_decision": "invalidation_contract_repaired_full_levels_ready"}
    audit = mod.no_lookahead_audit(inputs)
    assert audit["no_lookahead_pass"] is True


def test_v2_stop_atr2_source_counts_as_full_risk_contract() -> None:
    inputs = _inputs(ret20_ready=True, full_risk=False)
    inputs["invalidation_decision"] = {"research_decision": "invalidation_contract_v2_stop_atr2_ready_for_forward_tracking"}
    inputs["invalidation_source"] = {"atr14_complete": True}
    gate = mod.readiness_gate(inputs)
    assert gate["invalidation_tracking_gate_pass"] is True
    assert gate["full_risk_contract_gate_pass"] is True
    assert mod.no_lookahead_audit(inputs)["no_lookahead_pass"] is True
