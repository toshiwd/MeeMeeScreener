from __future__ import annotations

from pathlib import Path

from app.backend.infra.files.config_repo import ConfigRepository
from scripts.tradex_champion_top5_capture_boundary_promoter_v1_registry_mirror_adapter import (
    ACTIVE_SELECTION_FIELDS,
    build_registry_mirror_adapter_artifacts,
    _capture_runtime_selection_snapshot,
    _write_output_bundle,
)


def _authoritative_context(tmp_path: Path) -> dict[str, object]:
    return {
        "source_candidate_root": str(tmp_path / "source"),
        "keep_freeze_root": str(tmp_path / "freeze"),
        "publish_review_gate_root": str(tmp_path / "gate"),
        "manual_review_root": str(tmp_path / "manual"),
        "planning_root": str(tmp_path / "planning"),
        "shadow_registration_root": str(tmp_path / "shadow"),
        "shadow_bundle_root": str(tmp_path / "bundle"),
        "shadow_registration": {
            "feature_fallback_contract": {
                "schema_version": "shadow",
                "candidate_id": "champion_top5_capture_boundary_promoter_v1",
                "decision_time_input": "path_value_score_v1",
                "adjusted_ranking_materialization": "not_run",
                "adjusted_ranking_reason": "shadow_registration_only",
                "policy": {
                    "reason_code": "missing_path_value_score_v1_no_promotion",
                    "fallback_to_future_labels": False,
                    "fallback_to_mining_artifacts": False,
                },
            },
            "rank_storage_contract": {
                "schema_version": "shadow",
                "candidate_id": "champion_top5_capture_boundary_promoter_v1",
                "adjusted_ranking_materialization": "not_run",
                "adjusted_ranking_reason": "shadow_registration_only",
                "record_shape": {
                    "original_rank": "integer",
                    "adjusted_rank": "integer",
                    "original_score": "number",
                    "adjusted_score": "number",
                    "path_value_score_v1": "number",
                    "promotion_applied": "boolean",
                    "promotion_reason_code": "string",
                    "source_candidate_id": "string",
                    "top5_boundary_before": "integer",
                    "top5_boundary_after": "integer",
                },
            },
            "meemee_exposure_contract": {
                "schema_version": "shadow",
                "candidate_id": "champion_top5_capture_boundary_promoter_v1",
                "allowed_fields": ["final adjusted rank", "source candidate id"],
                "forbidden_fields": ["raw promotion inventory", "future-return labels"],
                "future_exposure_only": True,
                "no_api_change": True,
                "no_ui_change": True,
            },
            "rollback_contract": {
                "schema_version": "shadow",
                "candidate_id": "champion_top5_capture_boundary_promoter_v1",
                "rollback_action": "remove_shadow_candidate_entry",
                "active_selection_preserved": True,
            },
            "review_semantics": {
                "manual_review_no_meemee_mutation": True,
                "manual_review_no_registry_mutation": True,
                "prior_review_confirms_no_product_mutation": True,
                "reproducibility_review_meeMee_mutation_field": False,
                "reproducibility_review_registry_mutation_field": False,
                "semantic_reading": "false means mutation was not detected; it is not a failure flag in that artifact.",
            },
        },
        "review_semantics": {
            "manual_review_no_meemee_mutation": True,
            "manual_review_no_registry_mutation": True,
            "prior_review_confirms_no_product_mutation": True,
            "reproducibility_review_meeMee_mutation_field": False,
            "reproducibility_review_registry_mutation_field": False,
            "semantic_reading": "false means mutation was not detected; it is not a failure flag in that artifact.",
        },
        "feature_audit": {
            "pass": True,
            "source_artifact_root": str(tmp_path / "source"),
        },
        "fallback_policy": {"policy": {"reason_code": "missing_path_value_score_v1_no_promotion"}},
        "rank_storage_contract": {"record_shape": {"original_rank": "integer", "adjusted_rank": "integer"}},
        "exposure_contract": {"allowed_fields": ["final adjusted rank"], "forbidden_fields": ["raw promotion inventory"]},
        "rollback_contract": {"rollback_action": "remove_shadow_candidate_entry"},
    }


def _runtime_snapshot(*, selected_logic_key: str = "logic_a:v1", default_logic_pointer: str = "logic_a:v1") -> dict[str, object]:
    return {
        "schema_version": "logic_selection_v1",
        "source_of_truth": "external_analysis",
        "registry_sync_state": "in_sync",
        "degraded": False,
        "registry_version": 4,
        "source_revision": "rv:4",
        "selected_logic_override": None,
        "default_logic_pointer": default_logic_pointer,
        "registry_default_logic_pointer": default_logic_pointer,
        "selected_logic_key": selected_logic_key,
        "selected_source": "default_logic_pointer",
        "resolved_source": "default_logic_pointer",
        "champion_logic_key": selected_logic_key,
        "challenger_logic_key": None,
        "challenger_logic_keys": [],
        "last_known_good_present": False,
        "last_known_good_artifact_uri": None,
        "safe_fallback_key": "builtin_safe_fallback",
        "shadow_only": True,
        "publish_candidate_allowed": True,
        "meeMee_reflect_allowed": False,
        "production_path_allowed": False,
    }


def test_registry_mirror_adapter_artifacts_are_inactive_and_narrow(tmp_path: Path) -> None:
    config_repo = ConfigRepository(str(tmp_path))
    authoritative = _authoritative_context(tmp_path)
    runtime_before = _runtime_snapshot()
    runtime_after = _runtime_snapshot()

    artifacts = build_registry_mirror_adapter_artifacts(
        authoritative=authoritative,
        config_repo=config_repo,
        runtime_before=runtime_before,
        runtime_after=runtime_after,
    )

    decision = artifacts["registry_mirror_adapter_decision"]
    invariance = artifacts["active_selection_invariance_check"]
    candidate = artifacts["non_active_candidate_entry"]

    assert decision["decision"] == "registered_inactive_registry_mirror"
    assert decision["active_selection_invariance_passed"] is True
    assert decision["rollback_simulation_passed"] is True
    assert invariance["pass"] is True
    assert invariance["changed_fields"] == []
    assert candidate["active"] is False
    assert candidate["activation_state"] == "shadow_review_candidate"
    assert candidate["registration_mode"] == "shadow_or_publish_candidate_registry_only"
    assert candidate["adjusted_ranking_materialization"] == "not_run"
    assert candidate["adjusted_ranking_reason"] == "non_active_registry_mirror_only"
    assert artifacts["fallback_contract_carry_forward"]["policy"]["reason_code"] == "missing_path_value_score_v1_no_promotion"
    assert artifacts["rank_storage_contract_carry_forward"]["record_shape"]["original_rank"] == "integer"
    assert "raw promotion inventory" in artifacts["meemee_exposure_contract_carry_forward"]["forbidden_fields"]
    assert artifacts["meemee_exposure_contract_carry_forward"]["future_exposure_only"] is True
    assert artifacts["rollback_contract_carry_forward"]["rollback_simulation"]["pass"] is True


def test_registry_mirror_adapter_preserves_active_selection_fields(tmp_path: Path) -> None:
    config_repo = ConfigRepository(str(tmp_path))
    artifacts = build_registry_mirror_adapter_artifacts(
        authoritative=_authoritative_context(tmp_path),
        config_repo=config_repo,
        runtime_before=_runtime_snapshot(),
        runtime_after=_runtime_snapshot(),
    )

    invariance = artifacts["active_selection_invariance_check"]
    assert invariance["pass"] is True
    assert invariance["changed_fields"] == []
    before = invariance["before"]
    after = invariance["after"]
    for field in ACTIVE_SELECTION_FIELDS:
        assert before[field] == after[field]


def test_registry_mirror_adapter_writes_complete_bundle(tmp_path: Path) -> None:
    output_root = tmp_path / "adapter"
    config_repo = ConfigRepository(str(tmp_path))
    artifacts = build_registry_mirror_adapter_artifacts(
        authoritative=_authoritative_context(tmp_path),
        config_repo=config_repo,
        runtime_before=_runtime_snapshot(),
        runtime_after=_runtime_snapshot(),
    )

    written = _write_output_bundle(output_root, artifacts)
    for name in written:
        assert (output_root / name).exists()
