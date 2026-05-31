from __future__ import annotations

from scripts.tradex_scene_family_readiness_inventory_v1 import _next_axis, _scene_decision


def test_a_phase_ready_requires_keep_contract_and_live_shadow() -> None:
    sources = [
        {"exists": True, "artifact_complete": True, "decision": "keep"},
        {"exists": True, "artifact_complete": True, "decision": "candidate_generation_contract_ready"},
        {"exists": True, "artifact_complete": True, "decision": "hold_no_live_candidate"},
    ]

    decision = _scene_decision("a_phase_downtrend", sources)

    assert decision["readiness"] == "shadow_candidate_generation_ready"
    assert decision["judgment"] == "keep"


def test_non_a_keep_still_needs_contract() -> None:
    decision = _scene_decision("c_phase_uptrend", [{"exists": True, "artifact_complete": True, "decision": "keep"}])

    assert decision["readiness"] == "research_keep_needs_contract"
    assert decision["judgment"] == "hold"


def test_next_axis_prefers_probe_only_b_phase_first() -> None:
    rows = {
        "a_phase_downtrend": {"decision": {"readiness": "shadow_candidate_generation_ready"}},
        "b_phase_sideways": {"decision": {"readiness": "probe_only"}},
        "c_phase_uptrend": {"decision": {"readiness": "probe_only"}},
        "crash_or_bottoming": {"decision": {"readiness": "probe_only"}},
    }

    axis = _next_axis(rows)

    assert axis["selected_next_axis"] == "b_phase_sideways"
