from __future__ import annotations

import json
from pathlib import Path


OUTPUT_ROOT = Path(r"G:\Tradex\side_specific_high_recall_contract_design_v1")


def _latest_bundle() -> Path:
    bundles = [p for p in OUTPUT_ROOT.iterdir() if p.is_dir()]
    if not bundles:
        raise FileNotFoundError(f"no side-specific design bundles found under {OUTPUT_ROOT}")
    return max(bundles, key=lambda p: p.stat().st_mtime)


def _load_latest(name: str) -> dict:
    return json.loads((_latest_bundle() / name).read_text(encoding="utf-8"))


def test_latest_bundle_decision_is_side_specific_short_hold() -> None:
    decision = _load_latest("side_specific_high_recall_contract_design_v1_decision.json")
    recommendation = _load_latest("side_specific_high_recall_recommendation.json")

    assert decision["decision"] == "ready_to_build_side_specific_surface_with_short_hold"
    assert decision["status"] == "ready_to_build_side_specific_surface_with_short_hold"
    assert decision["best_next_action"] == "build_side_specific_surface_with_short_hold"
    assert recommendation["recommended_next_action"] == "build_side_specific_surface_with_short_hold"


def test_short_side_is_explicitly_research_hold() -> None:
    boundary = _load_latest("side_specific_evaluation_boundary_contract.json")
    short_contract = _load_latest("short_side_high_recall_contract.json")
    evidence = _load_latest("side_specific_evidence_audit.json")

    assert boundary["rules"]["combined_metrics_require_both_sides_passing_minimum_gates"] is True
    assert boundary["rules"]["mixed_side_keep_drop_decisions_allowed"] is False
    assert short_contract["role"] == "research_hold"
    assert short_contract["state"] == "diagnostic_only_pool"
    assert evidence["short"]["validation_role"] == "research_hold"
    assert evidence["short"]["selected"]["top10"]["top15_capture_rate"] == 0.0
