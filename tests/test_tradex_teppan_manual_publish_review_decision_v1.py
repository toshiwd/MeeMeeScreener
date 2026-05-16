from __future__ import annotations

import json
from pathlib import Path

from scripts import tradex_teppan_manual_publish_review_decision_v1 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _artifact_roots(tmp_path: Path) -> dict[str, Path]:
    pattern = tmp_path / "pattern"
    guard = tmp_path / "guard"
    probe = tmp_path / "probe"
    gate = tmp_path / "gate"
    shadow = gate / "shadow_publish_bundle"
    _write_json(
        pattern / "research_decision.json",
        {
            "authoritative_research_decision": "promising_patterns_found",
            "teppan_count": 0,
            "high_return_count": 85,
            "high_win_rate_count": 338,
            "silent_fallback_used": False,
        },
    )
    _write_json(pattern / "teppan_candidates.json", {"candidates": [{"no_lookahead_features": True}]})
    _write_json(
        guard / "research_decision.json",
        {
            "authoritative_research_decision": "keep",
            "candidate_local_decision": "keep_loss_guard_improves_risk_without_expectancy_harm",
            "primary_guard": {"delta_vs_baseline": {"severe_loss_rate20": -0.03}},
            "silent_fallback_used": False,
        },
    )
    _write_json(
        probe / "research_decision.json",
        {
            "decision": "keep",
            "candidate_local_decision": "keep",
            "session_aggregate_decision": "keep",
            "authoritative_research_decision": "teppan_ranking_branching_keep_candidate",
            "typed_reason": "same_condition_branching_helped_and_coverage_complete",
            "changed_top5_members_count": 276,
            "changed_top10_members_count": 146,
            "changed_rank_count": 1362,
            "production_ranking_changed": False,
            "meemee_reflectable": False,
            "publish_bundle_created": False,
            "silent_fallback_used": False,
        },
    )
    _write_json(
        probe / "compare.json",
        {
            "ranking_compare": {
                "top5": {"delta": {"avg_ret20": 0.001, "median_ret20": 0.0006, "severe_loss_rate20": -0.002}},
                "top10": {"delta": {"avg_ret20": 0.0007, "median_ret20": 0.0005, "severe_loss_rate20": -0.0002}},
            },
            "same_condition_contract": {"contract_hash": "same"},
            "silent_fallback_used": False,
        },
    )
    _write_json(probe / "ranking_coverage_audit.json", {"complete_champion_ranking_available": True, "complete_top20_decision_set_rate": 1.0})
    _write_json(probe / "branching_probe.json", {"changed_top5_members_count": 276, "changed_top10_members_count": 146})
    _write_json(probe / "_ARTIFACT_COMPLETE.json", {"complete": True})
    feature_rows = [
        {
            "feature": "champion_rank",
            "available_for_publish_review_contract": True,
            "available_in_current_meemee_runtime_ranking_generation": True,
            "decision_time_safe": True,
            "depends_on_future_label": False,
            "depends_on_research_only_mining_labels": False,
            "missing_fields": [],
        },
        {
            "feature": "champion_score",
            "available_for_publish_review_contract": True,
            "available_in_current_meemee_runtime_ranking_generation": True,
            "decision_time_safe": True,
            "depends_on_future_label": False,
            "depends_on_research_only_mining_labels": False,
            "missing_fields": [],
        },
        {
            "feature": "runtime_ohlcv_history",
            "available_for_publish_review_contract": True,
            "available_in_current_meemee_runtime_ranking_generation": True,
            "decision_time_safe": True,
            "depends_on_future_label": False,
            "depends_on_research_only_mining_labels": False,
            "missing_fields": [],
        },
        {
            "feature": "teppan_pattern_match",
            "available_for_publish_review_contract": True,
            "available_in_current_meemee_runtime_ranking_generation": False,
            "decision_time_safe": True,
            "depends_on_future_label": False,
            "depends_on_research_only_mining_labels": False,
            "missing_fields": [],
        },
        {
            "feature": "teppan_guard_pass",
            "available_for_publish_review_contract": True,
            "available_in_current_meemee_runtime_ranking_generation": False,
            "decision_time_safe": True,
            "depends_on_future_label": False,
            "depends_on_research_only_mining_labels": False,
            "missing_fields": [],
        },
    ]
    _write_json(
        gate / "publish_review_decision.json",
        {
            "decision": "pass_to_manual_review",
            "blockers": [],
            "reproducibility_pass": True,
            "anti_leakage_pass": True,
            "source_artifact_integrity_pass": True,
            "feature_availability_pass": True,
            "no_meemee_mutation": True,
            "production_ranking_changed": False,
            "meemee_reflectable_now": False,
            "shadow_bundle_root": str(shadow),
        },
    )
    _write_json(gate / "feature_availability_audit.json", {"pass": True, "features": feature_rows})
    _write_json(
        gate / "ranking_adjustment_contract.json",
        {
            "required_inputs": [
                "champion_rank",
                "champion_score",
                "runtime_ohlcv_history_up_to_anchor_date",
                "teppan_pattern_artifact",
                "teppan_loss_guard_artifact",
            ],
            "forbidden_inputs": ["forward_ret_20d", "future_return_labels"],
        },
    )
    _write_json(gate / "reproducibility_audit.json", {"matches_within_tolerance": True})
    _write_json(gate / "anti_leakage_recheck.json", {"pass": True})
    _write_json(gate / "meemee_exposure_assessment.json", {"allowed_future_meemee_exposure": ["adjusted_rank"], "forbidden_meemee_exposure": ["forward_ret_20d"]})
    _write_json(gate / "shadow_publish_bundle_manifest.json", {"bundle_status": "complete", "required_files_present": True})
    _write_json(gate / "_ARTIFACT_COMPLETE.json", {"complete": True, "shadow_bundle_root": str(shadow)})
    _write_json(
        shadow / "published_logic_artifact.json",
        {
            "logic_id": "teppan_ranking_branching_probe_v1",
            "logic_version": "static_teppan_guarded_soft_boost_v1",
            "scorer_type": "static_guarded_soft_boost",
            "required_inputs": [
                "champion_rank",
                "champion_score",
                "runtime_ohlcv_history_up_to_anchor_date",
                "teppan_pattern_artifact",
                "teppan_loss_guard_artifact",
            ],
            "forbidden_inputs": ["forward_ret_20d", "future_return_labels", "realized_topk_membership_labels"],
        },
    )
    _write_json(shadow / "published_logic_manifest.json", {"status": "candidate"})
    _write_json(shadow / "validation_summary.json", {"decision": "candidate", "metrics": {"changed_top5_members_count": 276}})
    _write_json(shadow / "source_artifact_refs.json", {"source_artifact_root": str(probe)})
    _write_json(shadow / "ranking_adjustment_contract.json", {"required_inputs": []})
    _write_json(shadow / "meemee_exposure_assessment.json", {"is_reflectable_to_meemee_now": False})
    _write_json(shadow / "bundle_manifest.json", {"bundle_status": "complete", "required_files_present": True})
    return {"pattern": pattern, "guard": guard, "probe": probe, "gate": gate}


def _run(tmp_path: Path, roots: dict[str, Path], **kwargs):
    return mod.run_manual_publish_review_decision_v1(
        pattern_root=roots["pattern"],
        guard_root=roots["guard"],
        branching_probe_root=roots["probe"],
        publish_review_gate_root=roots["gate"],
        output_root=tmp_path / "out",
        run_id="manual",
        **kwargs,
    )


def test_manual_review_approves_current_gate_shape_without_runtime_mutation(tmp_path: Path) -> None:
    roots = _artifact_roots(tmp_path)
    payload = _run(tmp_path, roots)

    assert payload["decision"] == mod.DECISION_APPROVE
    assert payload["manual_publish_review_decision"]["decision_reason"] == "review_gates_passed_with_feature_portability_planning_required"
    assert payload["manual_publish_review_decision"]["production_ranking_changed"] is False
    assert payload["manual_publish_review_decision"]["approved_for_runtime_mutation"] is False
    assert payload["feature_portability_report"]["portability_judgment"] == "bounded_planning_gap"
    assert "teppan_pattern_match" in payload["feature_portability_report"]["portable_with_shadow_integration_plan"]
    assert payload["next_axis_recommendation"]["next"] == "teppan_ranking_meemee_shadow_integration_plan_v1"
    complete = json.loads((Path(payload["output_root"]) / "_ARTIFACT_COMPLETE.json").read_text(encoding="utf-8"))
    assert complete["complete"] is True
    assert complete["existing_artifacts"]["_ARTIFACT_COMPLETE.json"] is True
    for name in mod.REQUIRED_OUTPUTS:
        assert (Path(payload["output_root"]) / name).exists(), name


def test_manual_review_holds_for_feature_portability_gap(tmp_path: Path) -> None:
    roots = _artifact_roots(tmp_path)
    feature_path = roots["gate"] / "feature_availability_audit.json"
    feature = json.loads(feature_path.read_text(encoding="utf-8"))
    for row in feature["features"]:
        if row["feature"] == "teppan_pattern_match":
            row["available_for_publish_review_contract"] = False
            row["missing_fields"] = ["teppan_pattern_artifact"]
    feature_path.write_text(json.dumps(feature, ensure_ascii=False), encoding="utf-8")

    payload = _run(tmp_path, roots)

    assert payload["decision"] == mod.DECISION_HOLD_PORTABILITY
    assert "feature_portability_hard_blocker" in payload["manual_publish_review_decision"]["blockers"]
    assert payload["next_axis_recommendation"]["next"] == "teppan_feature_portability_audit_v1"


def test_manual_review_holds_when_strict_teppan_zero_is_validation_blocker(tmp_path: Path) -> None:
    roots = _artifact_roots(tmp_path)
    payload = _run(tmp_path, roots, strict_teppan_zero_policy="validation_blocker")

    assert payload["decision"] == mod.DECISION_HOLD_VALIDATION
    assert "strict_teppan_count_zero_escalated_to_validation_blocker" in payload["manual_publish_review_decision"]["blockers"]
    assert payload["next_axis_recommendation"]["next"] == "teppan_branching_stability_validation_v1"


def test_manual_review_rejects_failed_reproducibility(tmp_path: Path) -> None:
    roots = _artifact_roots(tmp_path)
    gate_decision_path = roots["gate"] / "publish_review_decision.json"
    gate_decision = json.loads(gate_decision_path.read_text(encoding="utf-8"))
    gate_decision["reproducibility_pass"] = False
    gate_decision_path.write_text(json.dumps(gate_decision, ensure_ascii=False), encoding="utf-8")

    payload = _run(tmp_path, roots)

    assert payload["decision"] == mod.DECISION_REJECT
    assert "gate_reproducibility_pass_false" in payload["manual_publish_review_decision"]["blockers"]
    assert payload["next_axis_recommendation"]["next"] == "freeze_teppan_ranking_branching_probe_v1_as_research_keep_only"
