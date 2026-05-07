from __future__ import annotations

import json
from pathlib import Path

from scripts.tradex_champion_top5_capture_boundary_promoter_v1_shadow_registration import (
    ACTIVE_SELECTION_FIELDS,
    build_shadow_registration_artifacts,
    _write_output_bundle,
)


def _snapshot(*, selected_logic_key: str = "logic_a:v1", default_logic_pointer: str = "logic_a:v1") -> dict[str, object]:
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


def _authoritative_context(tmp_path: Path) -> dict[str, object]:
    return {
        "source_candidate_root": str(tmp_path / "source"),
        "keep_freeze_root": str(tmp_path / "freeze"),
        "publish_review_gate_root": str(tmp_path / "gate"),
        "manual_review_root": str(tmp_path / "manual"),
        "planning_root": str(tmp_path / "planning"),
        "shadow_bundle_root": str(tmp_path / "bundle"),
        "feature_audit": {"pass": True, "source_artifact_root": str(tmp_path / "source")},
        "fallback_policy": {
            "policy": {
                "if_missing_any_rank1_to_rank20_candidate": "do_not_apply_promotion_for_that_decision_set",
                "ordering": "preserve_original_champion_ordering",
                "reason_code": "missing_path_value_score_v1_no_promotion",
                "impute": False,
                "fallback_to_future_labels": False,
                "fallback_to_mining_artifacts": False,
                "silent_promotion": False,
            }
        },
        "rank_storage_contract": {
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
        "exposure_contract": {
            "allowed_fields": ["final adjusted rank", "simple reason label", "source candidate id"],
            "forbidden_fields": ["raw promotion inventory", "future-return labels"],
            "future_exposure_only": True,
            "no_api_change": True,
            "no_ui_change": True,
            "consumer_surface": "MeeMee publish-state / runtime-selection views only",
        },
        "review_semantics": {"prior_review_confirms_no_product_mutation": True},
    }


def test_shadow_registration_artifacts_are_inactive_and_narrow(tmp_path: Path) -> None:
    output_root = tmp_path / "shadow"
    artifacts = build_shadow_registration_artifacts(
        authoritative=_authoritative_context(tmp_path),
        before_selection=_snapshot(),
        after_selection=_snapshot(),
    )

    decision = artifacts["shadow_registration_decision"]
    invariance = artifacts["active_selection_invariance_check"]
    candidate = artifacts["shadow_candidate_entry"]

    assert decision["decision"] == "registered_inactive_shadow"
    assert decision["active_selection_invariance_passed"] is True
    assert invariance["pass"] is True
    assert invariance["changed_fields"] == []
    assert candidate["active"] is False
    assert candidate["activation_state"] == "shadow_review_candidate"
    assert candidate["registration_mode"] == "shadow_or_publish_candidate_registry_only"
    assert candidate["fallback_policy"]["adjusted_ranking_materialization"] == "not_run"
    assert artifacts["rank_storage_contract"]["adjusted_ranking_materialization"] == "not_run"

    written = _write_output_bundle(output_root, artifacts)
    for name in written:
        assert (output_root / name).exists()


def test_shadow_registration_preserves_active_selection_fields() -> None:
    before = _snapshot(selected_logic_key="logic_a:v1", default_logic_pointer="logic_a:v1")
    after = _snapshot(selected_logic_key="logic_a:v1", default_logic_pointer="logic_a:v1")
    artifacts = build_shadow_registration_artifacts(
        authoritative=_authoritative_context(Path("C:/tmp")),
        before_selection=before,
        after_selection=after,
    )

    invariance = artifacts["active_selection_invariance_check"]
    assert invariance["pass"] is True
    assert invariance["changed_fields"] == []
    assert artifacts["registry_before_snapshot"]["selected_logic_key"] == "logic_a:v1"
    assert artifacts["registry_after_snapshot"]["selected_logic_key"] == "logic_a:v1"
    for field in ACTIVE_SELECTION_FIELDS:
        assert artifacts["registry_before_snapshot"][field] == artifacts["registry_after_snapshot"][field]


def test_shadow_registration_fallback_contract_is_non_silent() -> None:
    artifacts = build_shadow_registration_artifacts(
        authoritative=_authoritative_context(Path("C:/tmp")),
        before_selection=_snapshot(),
        after_selection=_snapshot(),
    )
    contract = artifacts["feature_fallback_contract"]

    assert contract["decision_time_input"] == "path_value_score_v1"
    assert contract["adjusted_ranking_materialization"] == "not_run"
    assert contract["adjusted_ranking_reason"] == "shadow_registration_only"
    assert contract["policy"]["reason_code"] == "missing_path_value_score_v1_no_promotion"
    assert contract["policy"]["fallback_to_future_labels"] is False
    assert contract["policy"]["fallback_to_mining_artifacts"] is False


def test_shadow_registration_rollback_contract_is_shadow_only() -> None:
    artifacts = build_shadow_registration_artifacts(
        authoritative=_authoritative_context(Path("C:/tmp")),
        before_selection=_snapshot(),
        after_selection=_snapshot(),
    )
    contract = artifacts["rollback_contract"]

    assert contract["rollback_action"] == "remove_shadow_candidate_entry"
    assert contract["active_selection_preserved"] is True
    assert contract["active_selection_fields_unchanged"] is True
    assert contract["rollback_can_touch_active_selection"] is False
    assert contract["rollback_materialization"] == "not_run"
    assert "remove or retire shadow metadata only" in contract["shadow_entry_cleanup"]


def test_shadow_registration_exposure_contract_is_restricted() -> None:
    artifacts = build_shadow_registration_artifacts(
        authoritative=_authoritative_context(Path("C:/tmp")),
        before_selection=_snapshot(),
        after_selection=_snapshot(),
    )
    contract = artifacts["meemee_exposure_contract"]

    assert contract["future_exposure_only"] is True
    assert contract["no_api_change"] is True
    assert contract["no_ui_change"] is True
    assert contract["consumer_surface"] == "MeeMee publish-state / runtime-selection views only"
    assert "raw promotion inventory" in contract["forbidden_fields"]
    assert "future-return labels" in contract["forbidden_fields"]
    assert "final adjusted rank" in contract["allowed_fields"]
