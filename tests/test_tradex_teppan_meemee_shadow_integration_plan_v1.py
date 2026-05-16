from __future__ import annotations

import json
from pathlib import Path

from scripts import tradex_teppan_meemee_shadow_integration_plan_v1 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _manual_root(tmp_path: Path) -> Path:
    root = tmp_path / "manual"
    source_roots = {
        "pattern_discovery_root": str(tmp_path / "pattern"),
        "loss_guard_root": str(tmp_path / "guard"),
        "branching_probe_root": str(tmp_path / "probe"),
        "publish_review_gate_root": str(tmp_path / "gate"),
        "shadow_bundle_root": str(tmp_path / "gate" / "shadow_publish_bundle"),
    }
    _write_json(
        root / "manual_publish_review_decision.json",
        {
            "decision": "approve_publish_implementation_plan",
            "decision_reason": "review_gates_passed_with_feature_portability_planning_required",
            "source_roots": source_roots,
            "approved_for_runtime_mutation": False,
            "no_meemee_mutation": True,
            "no_runtime_duckdb_write": True,
            "no_production_registration": True,
            "production_ranking_changed": False,
        },
    )
    _write_json(
        root / "implementation_readiness_report.json",
        {
            "readiness_state": "ready_for_shadow_integration_plan",
            "approved_for_publish_implementation_plan": True,
            "approved_for_runtime_mutation": False,
        },
    )
    _write_json(
        root / "runtime_reflection_gap_report.json",
        {
            "gap_state": "bounded_implementation_planning_gap",
            "direct_runtime_reflection_allowed_now": False,
            "runtime_reflection_implementation_required": True,
        },
    )
    _write_json(
        root / "feature_portability_report.json",
        {
            "pass": True,
            "runtime_native_features": ["champion_rank", "champion_score", "runtime_ohlcv_history"],
            "portable_with_shadow_integration_plan": ["teppan_pattern_match", "teppan_guard_pass"],
            "features": [
                {
                    "feature": "champion_rank",
                    "portability": "native_runtime_input",
                    "available_in_current_meemee_runtime_ranking_generation": True,
                    "decision_time_safe": True,
                },
                {
                    "feature": "teppan_pattern_match",
                    "portability": "portable_with_shadow_integration_plan",
                    "available_in_current_meemee_runtime_ranking_generation": False,
                    "available_for_publish_review_contract": True,
                    "decision_time_safe": True,
                    "source_file_or_artifact": source_roots["pattern_discovery_root"],
                },
                {
                    "feature": "teppan_guard_pass",
                    "portability": "portable_with_shadow_integration_plan",
                    "available_in_current_meemee_runtime_ranking_generation": False,
                    "available_for_publish_review_contract": True,
                    "decision_time_safe": True,
                    "source_file_or_artifact": source_roots["loss_guard_root"],
                },
            ],
        },
    )
    _write_json(root / "blocker_or_approval_report.json", {"blockers": [], "decision": "approve_publish_implementation_plan"})
    _write_json(root / "next_axis_recommendation.json", {"next": "teppan_ranking_meemee_shadow_integration_plan_v1"})
    _write_json(root / "_ARTIFACT_COMPLETE.json", {"complete": True})
    return root


def _run(tmp_path: Path, manual_root: Path):
    return mod.run_teppan_ranking_meemee_shadow_integration_plan_v1(
        manual_review_root=manual_root,
        output_root=tmp_path / "out",
        run_id="shadow-plan",
    )


def test_shadow_integration_plan_approves_bounded_shadow_implementation(tmp_path: Path) -> None:
    manual_root = _manual_root(tmp_path)
    payload = _run(tmp_path, manual_root)

    assert payload["decision"] == mod.DECISION_APPROVE
    assert payload["shadow_integration_plan"]["active_runtime_ranking_change_allowed"] is False
    assert payload["shadow_integration_plan"]["runtime_duckdb_write_allowed"] is False
    assert payload["shadow_integration_plan"]["production_publish_registration_allowed"] is False
    assert payload["feature_materialization_plan"]["decision"] == "ready"
    assert payload["runtime_contract_gap_report"]["decision"] == "compatible_with_shadow_adapter_plan"
    assert payload["rank_storage_contract"]["record_shape"]["original_rank_is_recoverable"] is True
    assert payload["rank_storage_contract"]["record_shape"]["adjusted_rank_is_separate"] is True
    assert payload["rollback_plan"]["runtime_db_rollback_required"] is False
    assert payload["acceptance_criteria"]["decision"] == "pass"
    assert payload["implementation_change_list"]["decision"] == "ready_for_implementation_ticket"
    complete = json.loads((Path(payload["output_root"]) / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["complete"] is True
    assert complete["existing_artifacts"]["_ARTIFACT_COMPLETE.json"] is True
    for name in mod.REQUIRED_OUTPUTS:
        assert (Path(payload["output_root"]) / name).exists(), name


def test_shadow_integration_plan_holds_when_manual_review_not_approved(tmp_path: Path) -> None:
    manual_root = _manual_root(tmp_path)
    decision_path = manual_root / "manual_publish_review_decision.json"
    decision = json.loads(decision_path.read_text(encoding="utf-8"))
    decision["decision"] = "hold_for_feature_portability_gap"
    decision_path.write_text(json.dumps(decision, ensure_ascii=False), encoding="utf-8")

    payload = _run(tmp_path, manual_root)

    assert payload["decision"] == mod.DECISION_HOLD_CONTRACT
    assert "manual_publish_review_not_approved_for_implementation_plan" in payload["shadow_integration_plan"]["blockers"]
    assert payload["acceptance_criteria"]["decision"] == "blocked"


def test_shadow_integration_plan_holds_for_feature_materialization_gap(tmp_path: Path) -> None:
    manual_root = _manual_root(tmp_path)
    feature_path = manual_root / "feature_portability_report.json"
    feature = json.loads(feature_path.read_text(encoding="utf-8"))
    feature["features"].append({"feature": "teppan_guard_pass", "portability": "blocked"})
    feature_path.write_text(json.dumps(feature, ensure_ascii=False), encoding="utf-8")

    payload = _run(tmp_path, manual_root)

    assert payload["decision"] == mod.DECISION_HOLD_FEATURE
    assert "teppan_guard_pass" in payload["shadow_integration_plan"]["blockers"]
    assert payload["feature_materialization_plan"]["decision"] == "blocked"
