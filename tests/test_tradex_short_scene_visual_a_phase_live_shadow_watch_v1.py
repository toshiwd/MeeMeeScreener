from __future__ import annotations

from scripts.tradex_short_scene_visual_a_phase_live_shadow_watch_v1 import _contract_checks, _decision


def _contract() -> dict:
    return {
        "authoritative_rollup_decision": "candidate_generation_contract_ready",
        "candidate_contract": {
            "paper_replay_ready": True,
            "owner": "TRADEX",
            "side": "sell",
            "meemee_reflectable": False,
        },
        "scope": {
            "meemee_ranking_changed": False,
            "meemee_ui_changed": False,
            "runtime_db_written": False,
            "silent_fallback_used": False,
            "research_fallback_used": False,
        },
    }


def test_contract_checks_accept_ready_contract() -> None:
    checks = _contract_checks(_contract())

    assert all(checks.values())


def test_decision_continues_live_shadow_when_candidate_selected() -> None:
    result = _decision(checks={name: True for name in _contract_checks(_contract())}, selected_count=1)

    assert result["judgment"] == "continue_live_shadow"
    assert result["forward_shadow_ready"] is True


def test_decision_holds_without_candidate_but_keeps_forward_shadow_ready() -> None:
    result = _decision(checks={name: True for name in _contract_checks(_contract())}, selected_count=0)

    assert result["judgment"] == "hold_no_live_candidate"
    assert result["forward_shadow_ready"] is True


def test_decision_blocks_invalid_contract() -> None:
    result = _decision(checks={"authoritative_contract_ready": False}, selected_count=1)

    assert result["judgment"] == "hold"
    assert result["forward_shadow_ready"] is False
    assert "authoritative_contract_ready" in result["blockers"]
