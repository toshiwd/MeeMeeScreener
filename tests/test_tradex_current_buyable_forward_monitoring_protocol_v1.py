from __future__ import annotations

import pandas as pd

from scripts import tradex_current_buyable_forward_monitoring_protocol_v1 as mod


def test_monitoring_status_marks_ret_windows() -> None:
    sessions = pd.DataFrame(
        [
            {"code": "8086", "future_sessions": 5, "latest_bar": 20260527},
            {"code": "9831", "future_sessions": 6, "latest_bar": 20260528},
        ]
    )
    status = mod.monitoring_status(
        {"selected_as_of_date": 20260520, "selected_codes": ["8086", "9831"]},
        sessions,
        {"research_decision": "current_candidates_active_no_invalidation_hit"},
        {"research_decision": "operational_readiness_blocked_pending_forward_outcomes_or_full_risk_contract"},
    )
    assert status["ret5_ready_to_rerun"] is True
    assert status["ret20_ready_to_rerun"] is False


def test_blockers_include_pending_ret20_and_readiness_gate() -> None:
    status = {"ret5_ready_to_rerun": True, "ret20_ready_to_rerun": False}
    block = mod.blockers(status, {"production_ready": False, "research_decision": "blocked"})
    contracts = {row["contract"] for row in block["blocking_contracts"]}
    assert "forward_ret20_confirmed_outcome" in contracts
    assert "operational_readiness_gate" in contracts


def test_no_lookahead_accepts_pending_forward_and_active_tracking() -> None:
    audit = mod.no_lookahead_audit(
        {"research_decision": "forward_validation_pending_more_confirmed_bars"},
        {"research_decision": "current_candidates_active_no_invalidation_hit"},
    )
    assert audit["no_lookahead_pass"] is True
    assert audit["runtime_db_write"] is False


def test_decide_waits_when_ret5_not_ready() -> None:
    status = {"ret5_ready_to_rerun": False, "ret20_ready_to_rerun": False}
    decision, decision_class, reasons = mod.decide(status, {"blocking_contract_count": 1}, {"no_lookahead_pass": True})
    assert decision == "monitoring_protocol_wait_for_more_confirmed_bars"
    assert decision_class == "HOLD_UNDERPOWERED"
    assert "confirmed_future_sessions_below_ret5_window" in reasons
