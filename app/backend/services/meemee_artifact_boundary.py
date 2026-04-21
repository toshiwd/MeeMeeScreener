from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any


class MeeMeeArtifactBucket(str, Enum):
    MEE_MEE_SAFE = "MeeMee-safe"
    TRADEX_ONLY = "TRADEX-only"
    BLOCKED_HOLD = "blocked / hold"


MEEMEE_SAFE_ARTIFACT_FILENAMES: tuple[str, ...] = (
    "chart_gallery_authoritative_adoption.json",
    "chart_data_provenance_contract.json",
    "chart_gallery_authoritative_overwrite_contract.json",
)

TRADEX_ONLY_ARTIFACT_FILENAMES: tuple[str, ...] = (
    "buy_surface_operational_validation_r11_gate_decision.json",
    "buy_surface_operational_validation_r11_r1_defensive.json",
    "buy_surface_operational_challenger_r9_r6_regime_scoped.json",
    "buy_judgment_policy_selection_r5_gate_decision.json",
    "buy_judgment_revision_r4_reclaim_quality_gate.json",
)

BLOCKED_HOLD_ARTIFACT_FILENAMES: tuple[str, ...] = (
    "authoritative_decision.action_precision.json",
    "authoritative_decision.long_weak_direction.json",
    "authoritative_decision.long_too_late_forward_confirm.json",
    "buy_surface_operational_validation_r7_r6_symbol_level.json",
    "boundary_aware_readiness_contract.json",
    "boundary_instrumentation_backlog.json",
)

_MEEMEE_SAFE_SET = set(MEEMEE_SAFE_ARTIFACT_FILENAMES)
_TRADEX_ONLY_SET = set(TRADEX_ONLY_ARTIFACT_FILENAMES)
_BLOCKED_HOLD_SET = set(BLOCKED_HOLD_ARTIFACT_FILENAMES)

DEFAULT_RESEARCH_INVENTORY_DIR = Path(__file__).resolve().parents[3] / "artifacts" / "research_inventory"


@dataclass(frozen=True)
class MeeMeeArtifactClassification:
    requested_name: str
    artifact_name: str
    bucket: MeeMeeArtifactBucket
    allowed: bool
    reason: str

    @property
    def is_safe(self) -> bool:
        return self.bucket == MeeMeeArtifactBucket.MEE_MEE_SAFE and self.allowed


def _text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_artifact_name(value: Any) -> str | None:
    text = _text(value)
    if not text:
        return None
    if "/" in text or "\\" in text:
        return None
    return Path(text).name


def classify_meemee_artifact(artifact_name: Any) -> MeeMeeArtifactClassification:
    requested_name = _text(artifact_name)
    normalized_name = _normalize_artifact_name(artifact_name)
    if not normalized_name:
        return MeeMeeArtifactClassification(
            requested_name=requested_name,
            artifact_name=requested_name,
            bucket=MeeMeeArtifactBucket.TRADEX_ONLY,
            allowed=False,
            reason="artifact name must be a bare filename",
        )
    if normalized_name in _MEEMEE_SAFE_SET:
        return MeeMeeArtifactClassification(
            requested_name=requested_name,
            artifact_name=normalized_name,
            bucket=MeeMeeArtifactBucket.MEE_MEE_SAFE,
            allowed=True,
            reason="explicit MeeMee allowlist entry",
        )
    if normalized_name in _BLOCKED_HOLD_SET:
        return MeeMeeArtifactClassification(
            requested_name=requested_name,
            artifact_name=normalized_name,
            bucket=MeeMeeArtifactBucket.BLOCKED_HOLD,
            allowed=False,
            reason="explicit blocked / hold entry",
        )
    if normalized_name in _TRADEX_ONLY_SET:
        return MeeMeeArtifactClassification(
            requested_name=requested_name,
            artifact_name=normalized_name,
            bucket=MeeMeeArtifactBucket.TRADEX_ONLY,
            allowed=False,
            reason="explicit TRADEX-only entry",
        )
    return MeeMeeArtifactClassification(
        requested_name=requested_name,
        artifact_name=normalized_name,
        bucket=MeeMeeArtifactBucket.TRADEX_ONLY,
        allowed=False,
        reason="artifact is not allowlisted for MeeMee",
    )


def is_meemee_safe_artifact(artifact_name: Any) -> bool:
    return classify_meemee_artifact(artifact_name).is_safe


def list_meemee_safe_artifacts(*, root_dir: str | Path | None = None) -> list[dict[str, Any]]:
    return [
        {
            "artifact_name": artifact_name,
            "bucket": MeeMeeArtifactBucket.MEE_MEE_SAFE.value,
            "allowed": True,
            "reason": "explicit MeeMee allowlist entry",
            "artifact_path": str((Path(root_dir) if root_dir is not None else DEFAULT_RESEARCH_INVENTORY_DIR) / artifact_name),
        }
        for artifact_name in MEEMEE_SAFE_ARTIFACT_FILENAMES
    ]


def resolve_meemee_artifact_path(
    artifact_name: Any,
    *,
    root_dir: str | Path | None = None,
) -> Path:
    classification = classify_meemee_artifact(artifact_name)
    if not classification.is_safe:
        raise PermissionError(
            f"mee mee artifact {classification.artifact_name!r} is not resolvable from MeeMee runtime paths"
        )
    root = Path(root_dir) if root_dir is not None else DEFAULT_RESEARCH_INVENTORY_DIR
    path = (root / classification.artifact_name).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"mee mee artifact {classification.artifact_name!r} is missing")
    return path


def load_meemee_artifact_json(
    artifact_name: Any,
    *,
    root_dir: str | Path | None = None,
) -> Any:
    path = resolve_meemee_artifact_path(artifact_name, root_dir=root_dir)
    return json.loads(path.read_text(encoding="utf-8"))


def _maybe_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _maybe_number(value: Any) -> float | int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = float(value)
        except Exception:
            return None
        return int(parsed) if parsed.is_integer() else parsed
    return None


def _maybe_dict(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _maybe_list(value: Any) -> list[Any] | None:
    return value if isinstance(value, list) else None


def to_meemee_candidate_view(candidate: dict[str, Any] | None) -> dict[str, Any]:
    bundle = _maybe_dict(candidate) or {}
    summary = _maybe_dict(bundle.get("validation_summary")) or {}
    metrics = _maybe_dict(summary.get("metrics")) or {}
    return {
        "candidate_id": _maybe_text(bundle.get("candidate_id")),
        "logic_key": _maybe_text(bundle.get("logic_key")),
        "logic_id": _maybe_text(bundle.get("logic_id")),
        "logic_version": _maybe_text(bundle.get("logic_version")),
        "logic_family": _maybe_text(bundle.get("logic_family")),
        "status": _maybe_text(bundle.get("status")),
        "validation_state": _maybe_text(bundle.get("validation_state")),
        "created_at": _maybe_text(bundle.get("created_at")),
        "updated_at": _maybe_text(bundle.get("updated_at")),
        "source_publish_id": _maybe_text(bundle.get("source_publish_id")),
        "readiness_pass": bool(metrics.get("readiness_pass")),
        "sample_count": _maybe_number(metrics.get("sample_count")),
        "expectancy_delta": _maybe_number(metrics.get("expectancy_delta")),
        "has_snapshot": bool(bundle.get("published_ranking_snapshot")),
        "surface_bucket": MeeMeeArtifactBucket.MEE_MEE_SAFE.value,
        "surface_reason": "MeeMee-safe candidate summary only; compare and ranking internals withheld",
    }


def to_meemee_runtime_selection_view(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    state = _maybe_dict(snapshot) or {}
    return {
        "schema_version": state.get("schema_version"),
        "snapshot_created_at": state.get("snapshot_created_at"),
        "selected_logic_override": state.get("selected_logic_override"),
        "default_logic_pointer": state.get("default_logic_pointer"),
        "registry_default_logic_pointer": state.get("registry_default_logic_pointer"),
        "champion_logic_key": state.get("champion_logic_key"),
        "challenger_logic_key": state.get("challenger_logic_key"),
        "challenger_logic_keys": _maybe_list(state.get("challenger_logic_keys")) or [],
        "logic_key": state.get("logic_key"),
        "selected_logic_key": state.get("selected_logic_key"),
        "selected_logic_id": state.get("selected_logic_id"),
        "selected_logic_version": state.get("selected_logic_version"),
        "artifact_uri": state.get("artifact_uri"),
        "override_present": state.get("override_present"),
        "last_known_good_present": state.get("last_known_good_present"),
        "last_known_good": state.get("last_known_good"),
        "last_known_good_artifact_uri": state.get("last_known_good_artifact_uri"),
        "safe_fallback_key": state.get("safe_fallback_key"),
        "available_logic_keys": _maybe_list(state.get("available_logic_keys")) or [],
        "selected_source": state.get("selected_source"),
        "resolved_source": state.get("resolved_source"),
        "selected_pointer_name": state.get("selected_pointer_name"),
        "matched_available": state.get("matched_available"),
        "validation_state": state.get("validation_state"),
        "validation_issues": _maybe_list(state.get("validation_issues")) or [],
        "notes": _maybe_list(state.get("notes")) or [],
        "catalog_default_logic_pointer": state.get("catalog_default_logic_pointer"),
        "source_of_truth": state.get("source_of_truth"),
        "registry_sync_state": state.get("registry_sync_state"),
        "degraded": state.get("degraded"),
        "last_sync_time": state.get("last_sync_time"),
        "registry_version": state.get("registry_version"),
        "source_revision": state.get("source_revision"),
        "bootstrap_rule": state.get("bootstrap_rule"),
        "external_registry_version": state.get("external_registry_version"),
        "local_mirror_version": state.get("local_mirror_version"),
        "mirror_schema_version": state.get("mirror_schema_version"),
        "mirror_normalized": state.get("mirror_normalized"),
        "candidate_backfill_last_run": _maybe_dict(state.get("candidate_backfill_last_run")),
        "snapshot_sweep_last_run": _maybe_dict(state.get("snapshot_sweep_last_run")),
        "non_promotable_legacy_count": state.get("non_promotable_legacy_count"),
        "maintenance_degraded": state.get("maintenance_degraded"),
        "maintenance_state": _maybe_dict(state.get("maintenance_state")),
        "operator_mutation_observability": _maybe_dict(state.get("operator_mutation_observability")),
        "shadow_integration_available": state.get("shadow_integration_available"),
        "shadow_only": state.get("shadow_only"),
        "shadow_integration_state": _maybe_dict(state.get("shadow_integration_state")),
        "shadow_rollout_boundary": _maybe_dict(state.get("shadow_rollout_boundary")),
        "shadow_verify": _maybe_dict(state.get("shadow_verify")),
        "shadow_monitoring_contract": _maybe_dict(state.get("shadow_monitoring_contract")),
        "shadow_integration_validation_issues": _maybe_list(state.get("shadow_integration_validation_issues")) or [],
        "shadow_integration": _maybe_dict(state.get("shadow_integration")),
    }


def to_meemee_publish_state_view(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    state = _maybe_dict(snapshot) or {}
    return {
        "source_of_truth": state.get("source_of_truth"),
        "registry_sync_state": state.get("registry_sync_state"),
        "degraded": state.get("degraded"),
        "last_sync_time": state.get("last_sync_time"),
        "bootstrap_rule": state.get("bootstrap_rule"),
        "default_logic_pointer": state.get("default_logic_pointer"),
        "champion_logic_key": state.get("champion_logic_key"),
        "challenger_logic_key": state.get("challenger_logic_key"),
        "challenger_logic_keys": _maybe_list(state.get("challenger_logic_keys")) or [],
        "previous_stable_champion_logic_key": state.get("previous_stable_champion_logic_key"),
        "external_registry_version": state.get("external_registry_version"),
        "local_mirror_version": state.get("local_mirror_version"),
        "mirror_schema_version": state.get("mirror_schema_version"),
        "mirror_normalized": state.get("mirror_normalized"),
        "candidate_backfill_last_run": _maybe_dict(state.get("candidate_backfill_last_run")),
        "snapshot_sweep_last_run": _maybe_dict(state.get("snapshot_sweep_last_run")),
        "non_promotable_legacy_count": state.get("non_promotable_legacy_count"),
        "maintenance_degraded": state.get("maintenance_degraded"),
        "maintenance_state": _maybe_dict(state.get("maintenance_state")),
        "operator_mutation_observability": _maybe_dict(state.get("operator_mutation_observability")),
    }


def to_meemee_publish_queue_view(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    state = _maybe_dict(snapshot) or {}
    return {
        "ok": True,
        "source_of_truth": state.get("source_of_truth"),
        "registry_sync_state": state.get("registry_sync_state"),
        "degraded": state.get("degraded"),
        "last_sync_time": state.get("last_sync_time"),
        "default_logic_pointer": state.get("default_logic_pointer"),
        "champion_logic_key": state.get("champion_logic_key"),
        "challenger_logic_keys": _maybe_list(state.get("challenger_logic_keys")) or [],
        "operator_mutation_observability": _maybe_dict(state.get("operator_mutation_observability")),
    }
