from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import tradex_short_cleanup_bottom_risk_family_closure_v1 as mod


@pytest.fixture(scope="module")
def closure_run(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, dict[str, object]]:
    out_root = tmp_path_factory.mktemp("short_cleanup_bottom_risk_family_closure")
    result = mod.run(source_root=mod.DEFAULT_SOURCE_ROOT, output_root=out_root)
    return Path(result["output_dir"]), result


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_closure_bundle_and_final_decision(closure_run: tuple[Path, dict[str, object]]) -> None:
    root, result = closure_run
    assert result["decision"] == "closed_as_research_drop"

    final = _read_json(root / "short_cleanup_bottom_risk_final_decision.json")
    assert final["decision"] == "closed_as_research_drop"
    assert final["final_decision"] == "closed_as_research_drop"
    assert final["production_candidate"] is False
    assert final["meemee_reflectable"] is False
    assert final["publish_allowed"] is False
    assert final["live_sell_signal_allowed"] is False
    assert final["production_state_changed"] is False
    assert final["meeMee_changed"] is False
    assert final["no_lookahead_pass"] is True
    assert final["lineage_stage_count"] == 10

    failure = _read_json(root / "short_cleanup_bottom_risk_failure_diagnosis.json")
    assert failure["decision"] == "closed_as_research_drop"
    assert failure["typed_reasons"] == [
        "soft_cost_dependency_too_high",
        "insufficient_clean_borrowable_sample",
        "paper_replay_blocked",
        "exposure_guard_damages_edge",
        "not_production_candidate",
        "meemee_reflectable_false",
    ]
    assert failure["production_candidate"] is False
    assert failure["meemee_reflectable"] is False
    assert failure["publish_allowed"] is False
    assert failure["live_sell_signal_allowed"] is False
    assert failure["production_state_changed"] is False
    assert failure["meeMee_changed"] is False
    assert failure["no_lookahead_pass"] is True

    blockers = failure["blockers"]
    assert [blocker["reason"] for blocker in blockers] == [
        "soft_cost_dependency_too_high",
        "insufficient_clean_borrowable_sample",
        "paper_replay_blocked",
        "exposure_guard_damages_edge",
        "not_production_candidate",
        "meemee_reflectable_false",
    ]

    metric = _read_json(root / "short_cleanup_bottom_risk_metric_rollup.json")
    assert metric["final_chain_status"] == "closed_as_research_drop"
    assert metric["borrow_decomposition"]["decision"] == "hold_due_to_insufficient_clean_borrowable_sample"
    assert metric["exposure_guard"]["decision"] == "drop_as_soft_cost_dependency_too_high"
    assert metric["exposure_guard"]["harmful_short_exposure_reduced"] is True
    assert metric["exposure_guard"]["edge_depends_on_soft_cost_names"] is True
    assert metric["borrow_decomposition"]["clean_borrowable_event_count"] == 2

    assert (root / "short_cleanup_bottom_risk_family_closure_contract.json").exists()
    assert (root / "short_cleanup_bottom_risk_lineage_summary.json").exists()
    assert (root / "short_cleanup_bottom_risk_metric_rollup.json").exists()
    assert (root / "short_cleanup_bottom_risk_failure_diagnosis.json").exists()
    assert (root / "short_cleanup_bottom_risk_final_decision.json").exists()
    assert (root / "no_lookahead_audit_rollup.json").exists()
    assert (root / "_ARTIFACT_COMPLETE.json").exists()


def test_lineage_order_and_no_lookahead_rollup(closure_run: tuple[Path, dict[str, object]]) -> None:
    root, _ = closure_run
    lineage = _read_json(root / "short_cleanup_bottom_risk_lineage_summary.json")
    assert lineage["lineage_stage_count"] == 10
    assert lineage["authoritative_artifact_roots"][0] == "G:\\Tradex\\entry_precision_short_audit_v1"
    assert lineage["authoritative_artifact_roots"][-1] == str(mod.DEFAULT_SOURCE_ROOT)
    assert [stage["decision"] for stage in lineage["stages"]] == [
        "hold",
        "drop",
        "drop",
        "hold_due_to_small_sample",
        "hold_until_unknown_horizon_completes",
        "keep_frozen_watch_candidate",
        "keep_for_stability_replay",
        "hold_due_to_borrow_proxy_gap",
        "hold_due_to_insufficient_clean_borrowable_sample",
        "drop_as_soft_cost_dependency_too_high",
    ]
    assert [stage["no_lookahead_pass"] for stage in lineage["stages"][:3]] == [None, None, None]
    assert all(stage["no_lookahead_pass"] is True for stage in lineage["stages"][3:])

    no_lookahead = _read_json(root / "no_lookahead_audit_rollup.json")
    assert no_lookahead["overall_no_lookahead_pass"] is True
    assert no_lookahead["available_stage_count"] == 7
    assert no_lookahead["passed_stage_count"] == 7
    assert all(
        stage["no_lookahead_pass"] is True
        for stage in no_lookahead["stages"]
        if stage["no_lookahead_available"]
    )
