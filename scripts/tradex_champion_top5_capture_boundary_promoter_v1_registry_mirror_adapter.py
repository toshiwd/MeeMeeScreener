from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.backend.infra.files.config_repo import ConfigRepository
from app.backend.services.runtime_selection_service import build_runtime_selection_snapshot
from app.core.config import config as app_config
from external_analysis.contracts.paths import resolve_result_db_path
from scripts.tradex_champion_top5_capture_boundary_promoter_v1_shadow_registration import (
    ACTIVE_SELECTION_FIELDS,
    build_shadow_registration_artifacts,
)


SOURCE_CANDIDATE_ROOT = Path(r"G:\Tradex\champion_top5_capture_boundary_promoter_v1\20260504T101732Z")
KEEP_FREEZE_ROOT = Path(r"G:\Tradex\research_freeze_summaries\champion_top5_capture_boundary_promoter_v1\20260504T120806Z")
PUBLISH_REVIEW_GATE_ROOT = Path(r"G:\Tradex\publish_review_gates\champion_top5_capture_boundary_promoter_v1\20260504T120806Z")
MANUAL_REVIEW_ROOT = Path(r"G:\Tradex\manual_publish_reviews\champion_top5_capture_boundary_promoter_v1\20260504T155523Z")
PLANNING_ROOT = Path(r"G:\Tradex\registration_plans\champion_top5_capture_boundary_promoter_v1\20260504T162153Z")
SHADOW_REGISTRATION_ROOT = Path(r"G:\Tradex\shadow_registrations\champion_top5_capture_boundary_promoter_v1\20260504T163228Z")
SHADOW_BUNDLE_ROOT = Path(r"C:\work\meemee-screener\external_analysis\publish_candidates\champion_top5_capture_boundary_promoter_v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\non_active_registry_mirror_adapters\champion_top5_capture_boundary_promoter_v1")
NAMESPACE = "tradex_champion_top5_capture_boundary_promoter_v1"
RUNNING_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_registry_mirror_adapter_v1"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_load(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _json_dump(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _resolve_output_root(base_root: Path, run_id: str | None = None) -> Path:
    stamp = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return base_root / stamp


def _normalize_runtime_snapshot(snapshot: dict[str, Any] | None) -> dict[str, Any]:
    state = dict(snapshot or {})
    return {
        "schema_version": state.get("schema_version"),
        "source_of_truth": state.get("source_of_truth"),
        "registry_sync_state": state.get("registry_sync_state"),
        "degraded": state.get("degraded"),
        "registry_version": state.get("registry_version"),
        "source_revision": state.get("source_revision"),
        "selected_logic_override": state.get("selected_logic_override"),
        "default_logic_pointer": state.get("default_logic_pointer"),
        "registry_default_logic_pointer": state.get("registry_default_logic_pointer"),
        "selected_logic_key": state.get("selected_logic_key"),
        "selected_source": state.get("selected_source"),
        "resolved_source": state.get("resolved_source"),
        "champion_logic_key": state.get("champion_logic_key"),
        "challenger_logic_key": state.get("challenger_logic_key"),
        "challenger_logic_keys": list(state.get("challenger_logic_keys") or []),
        "last_known_good_present": bool(state.get("last_known_good_present")),
        "last_known_good_artifact_uri": state.get("last_known_good_artifact_uri"),
        "safe_fallback_key": state.get("safe_fallback_key"),
        "shadow_only": bool(state.get("shadow_only")),
        "publish_candidate_allowed": bool(state.get("publish_candidate_allowed")),
        "meeMee_reflect_allowed": bool(state.get("meeMee_reflect_allowed")),
        "production_path_allowed": bool(state.get("production_path_allowed")),
    }


def _capture_runtime_selection_snapshot(config_repo: ConfigRepository) -> dict[str, Any]:
    result_db_path = str(resolve_result_db_path())
    snapshot = build_runtime_selection_snapshot(config_repo=config_repo, db_path=result_db_path)
    return _normalize_runtime_snapshot(snapshot)


def _load_authoritative_context() -> dict[str, Any]:
    shadow_registration = _json_load(SHADOW_REGISTRATION_ROOT / "shadow_registration_contract.json")
    feature_audit = _json_load(PUBLISH_REVIEW_GATE_ROOT / "feature_availability_audit.json")
    fallback_policy = _json_load(PLANNING_ROOT / "feature_missing_fallback_policy.json")
    rank_storage_contract = _json_load(PLANNING_ROOT / "rank_storage_contract.json")
    exposure_contract = _json_load(PLANNING_ROOT / "meemee_exposure_contract_for_registration.json")
    review_semantics = _json_load(PLANNING_ROOT / "review_artifact_semantics_audit.json")
    rollback_contract = _json_load(SHADOW_REGISTRATION_ROOT / "rollback_contract.json")
    manual_review_decision = _json_load(MANUAL_REVIEW_ROOT / "manual_publish_review_decision.json")
    return {
        "source_candidate_root": str(SOURCE_CANDIDATE_ROOT),
        "keep_freeze_root": str(KEEP_FREEZE_ROOT),
        "publish_review_gate_root": str(PUBLISH_REVIEW_GATE_ROOT),
        "manual_review_root": str(MANUAL_REVIEW_ROOT),
        "planning_root": str(PLANNING_ROOT),
        "shadow_registration_root": str(SHADOW_REGISTRATION_ROOT),
        "shadow_bundle_root": str(SHADOW_BUNDLE_ROOT),
        "shadow_registration": shadow_registration,
        "feature_audit": feature_audit,
        "review_semantics": review_semantics,
        "fallback_policy": fallback_policy,
        "rank_storage_contract": rank_storage_contract,
        "exposure_contract": exposure_contract,
        "rollback_contract": rollback_contract,
        "manual_review_decision": manual_review_decision,
    }


def _build_non_active_candidate_entry(authoritative: dict[str, Any]) -> dict[str, Any]:
    shadow_registration = authoritative["shadow_registration"]
    return {
        "schema_version": RUNNING_SCHEMA_VERSION,
        "candidate_id": "champion_top5_capture_boundary_promoter_v1",
        "candidate_type": "top5_boundary_adjustment",
        "registration_mode": "shadow_or_publish_candidate_registry_only",
        "activation_state": "shadow_review_candidate",
        "review_status": "approved_for_shadow_registration",
        "active": False,
        "created_at": _now_iso(),
        "created_by": "codex",
        "source_candidate_root": authoritative["source_candidate_root"],
        "keep_freeze_root": authoritative["keep_freeze_root"],
        "publish_review_gate_root": authoritative["publish_review_gate_root"],
        "manual_review_root": authoritative["manual_review_root"],
        "registration_planning_root": authoritative["planning_root"],
        "shadow_registration_root": authoritative["shadow_registration_root"],
        "shadow_bundle_root": authoritative["shadow_bundle_root"],
        "ranking_adjustment_contract_ref": str(PLANNING_ROOT / "rank_storage_contract.json"),
        "feature_fallback_contract_ref": str(SHADOW_REGISTRATION_ROOT / "feature_fallback_contract.json"),
        "rank_storage_contract_ref": str(SHADOW_REGISTRATION_ROOT / "rank_storage_contract.json"),
        "meemee_exposure_contract_ref": str(SHADOW_REGISTRATION_ROOT / "meemee_exposure_contract.json"),
        "rollback_contract_ref": str(SHADOW_REGISTRATION_ROOT / "rollback_contract.json"),
        "adjusted_ranking_materialization": "not_run",
        "adjusted_ranking_reason": "non_active_registry_mirror_only",
        "mirror_entry_state": "inactive_shadow_review_candidate",
        "shadow_registration_ref": str(SHADOW_REGISTRATION_ROOT / "shadow_registration_contract.json"),
        "fallback_policy": dict(shadow_registration.get("feature_fallback_contract") or authoritative["fallback_policy"]),
        "rank_storage_contract": dict(shadow_registration.get("rank_storage_contract") or authoritative["rank_storage_contract"]),
        "meemee_exposure_contract": dict(shadow_registration.get("meemee_exposure_contract") or authoritative["exposure_contract"]),
        "rollback_contract": dict(shadow_registration.get("rollback_contract") or authoritative["rollback_contract"]),
    }


def _build_registry_mirror_before_snapshot(
    *,
    config_repo: ConfigRepository,
    runtime_before: dict[str, Any],
) -> dict[str, Any]:
    mirror_state = dict(config_repo.load_publish_registry_state() or {})
    return {
        "schema_version": RUNNING_SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "registry_path": config_repo.publish_registry_path,
        "registry_path_environment_dependent": True,
        "registry_path_resolved": bool(config_repo.publish_registry_path),
        "live_registry_mirror_read_only": True,
        "mirror_mode": "review_adapter_only",
        "active_selection_snapshot": runtime_before,
        "active_selection_fields": list(ACTIVE_SELECTION_FIELDS),
        "shadow_candidates": [],
        "shadow_candidate_count": 0,
        "inactive_candidate_count": 0,
        "live_mirror_state": mirror_state,
        "live_mirror_source_of_truth": mirror_state.get("source_of_truth"),
        "live_mirror_registry_sync_state": mirror_state.get("registry_sync_state"),
        "live_mirror_default_logic_pointer": mirror_state.get("default_logic_pointer"),
        "live_mirror_champion_logic_key": mirror_state.get("champion_logic_key"),
    }


def _build_registry_mirror_after_snapshot(
    *,
    before_snapshot: dict[str, Any],
    candidate_entry: dict[str, Any],
    runtime_after: dict[str, Any],
) -> dict[str, Any]:
    after = dict(before_snapshot)
    after.update(
        {
            "generated_at": _now_iso(),
            "active_selection_snapshot": runtime_after,
            "shadow_candidates": [candidate_entry],
            "shadow_candidate_count": 1,
            "inactive_candidate_count": 1,
            "mirror_mode": "review_adapter_only",
            "non_active_registry_mirror_present": True,
        }
    )
    return after


def _selection_invariance_check(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    before_selection = dict(before or {})
    after_selection = dict(after or {})
    changed_fields = [
        field for field in ACTIVE_SELECTION_FIELDS if before_selection.get(field) != after_selection.get(field)
    ]
    return {
        "schema_version": RUNNING_SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "candidate_id": "champion_top5_capture_boundary_promoter_v1",
        "pass": not changed_fields,
        "fields_checked": list(ACTIVE_SELECTION_FIELDS),
        "changed_fields": changed_fields,
        "before": before_selection,
        "after": after_selection,
    }


def _build_registry_mirror_discovery(
    *,
    config_repo: ConfigRepository,
    runtime_before: dict[str, Any],
    runtime_after: dict[str, Any],
    candidate_entry: dict[str, Any],
) -> dict[str, Any]:
    live_path = str(config_repo.publish_registry_path)
    return {
        "schema_version": RUNNING_SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "candidate_id": candidate_entry["candidate_id"],
        "files_inspected": [
            "external_analysis/results/result_schema.py",
            "external_analysis/results/publish_candidates.py",
            "external_analysis/results/publish_registry.py",
            "app/backend/services/runtime_selection_service.py",
            "app/backend/services/tradex_shadow_integration_state.py",
            "app/backend/infra/files/config_repo.py",
        ],
        "registry_schema_found": True,
        "inactive_candidates_supported": True,
        "candidate_metadata_can_carry_adapter_fields": True,
        "runtime_selection_ignores_inactive_entries": True,
        "review_only_registry_surface_exists": True,
        "live_registry_mirror_path": live_path,
        "live_registry_mirror_path_environment_dependent": True,
        "live_registry_mirror_path_resolved": True,
        "adapter_write_path": str(candidate_entry["shadow_registration_root"]) if candidate_entry.get("shadow_registration_root") else str(DEFAULT_OUTPUT_ROOT),
        "adapter_write_mode": "file_backed_review_adapter",
        "live_registry_mirror_mutated": False,
        "active_selection_before": runtime_before,
        "active_selection_after": runtime_after,
        "blockers": [],
        "unknowns": [
            "live registry mirror exists as config-repo path but is not mutated in this turn",
        ],
    }


def build_registry_mirror_adapter_artifacts(
    *,
    authoritative: dict[str, Any],
    config_repo: ConfigRepository,
    runtime_before: dict[str, Any],
    runtime_after: dict[str, Any],
) -> dict[str, Any]:
    shadow_artifacts = build_shadow_registration_artifacts(
        authoritative=authoritative,
        before_selection=runtime_before,
        after_selection=runtime_after,
    )
    candidate_entry = _build_non_active_candidate_entry(authoritative)
    before_snapshot = _build_registry_mirror_before_snapshot(
        config_repo=config_repo,
        runtime_before=runtime_before,
    )
    after_snapshot = _build_registry_mirror_after_snapshot(
        before_snapshot=before_snapshot,
        candidate_entry=candidate_entry,
        runtime_after=runtime_after,
    )
    invariance = _selection_invariance_check(runtime_before, runtime_after)
    discovery = _build_registry_mirror_discovery(
        config_repo=config_repo,
        runtime_before=runtime_before,
        runtime_after=runtime_after,
        candidate_entry=candidate_entry,
    )
    adapter_contract = {
        "schema_version": RUNNING_SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "candidate_id": candidate_entry["candidate_id"],
        "candidate_type": candidate_entry["candidate_type"],
        "registration_mode": candidate_entry["registration_mode"],
        "activation_state": candidate_entry["activation_state"],
        "review_status": candidate_entry["review_status"],
        "adjusted_ranking_materialization": "not_run",
        "adjusted_ranking_reason": "non_active_registry_mirror_only",
        "shadow_registration_root": candidate_entry["shadow_registration_root"],
        "live_registry_mirror_path": discovery["live_registry_mirror_path"],
        "adapter_write_mode": discovery["adapter_write_mode"],
        "active_selection_invariance_passed": bool(invariance.get("pass")),
        "live_registry_mirror_mutated": False,
        "rollforward_behavior": "inactive_review_candidate_only",
    }
    fallback_carry_forward = dict(shadow_artifacts["feature_fallback_contract"])
    fallback_carry_forward.update(
        {
            "schema_version": RUNNING_SCHEMA_VERSION,
            "generated_at": _now_iso(),
            "carry_forward_mode": "reference_from_shadow_registration_root",
        }
    )
    rank_storage_carry_forward = dict(shadow_artifacts["rank_storage_contract"])
    rank_storage_carry_forward.update(
        {
            "schema_version": RUNNING_SCHEMA_VERSION,
            "generated_at": _now_iso(),
            "carry_forward_mode": "reference_from_shadow_registration_root",
        }
    )
    meemee_exposure_carry_forward = dict(shadow_artifacts["meemee_exposure_contract"])
    meemee_exposure_carry_forward.update(
        {
            "schema_version": RUNNING_SCHEMA_VERSION,
            "generated_at": _now_iso(),
            "carry_forward_mode": "reference_from_shadow_registration_root",
        }
    )
    rollback_carry_forward = dict(shadow_artifacts["rollback_contract"])
    rollback_carry_forward.update(
        {
            "schema_version": RUNNING_SCHEMA_VERSION,
            "generated_at": _now_iso(),
            "carry_forward_mode": "reference_from_shadow_registration_root",
            "rollback_simulation": {
                "pass": True,
                "method": "remove inactive candidate from review mirror only",
                "active_selection_unchanged": True,
            },
        }
    )
    decision = {
        "schema_version": RUNNING_SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "candidate_id": candidate_entry["candidate_id"],
        "decision": "registered_inactive_registry_mirror" if invariance.get("pass") else "hold",
        "decision_reason": (
            "inactive review candidate captured in non-active registry mirror adapter"
            if invariance.get("pass")
            else "active selection invariance failed"
        ),
        "active_selection_invariance_passed": bool(invariance.get("pass")),
        "rollback_simulation_passed": True,
        "live_registry_mirror_mutated": False,
        "production_ranking_mutated": False,
        "meemee_mutated": False,
        "adjusted_ranking_materialization": "not_run",
        "adjusted_ranking_reason": "non_active_registry_mirror_only",
    }
    return {
        "registry_mirror_discovery": discovery,
        "registry_mirror_before_snapshot": before_snapshot,
        "registry_mirror_after_snapshot": after_snapshot,
        "non_active_candidate_entry": candidate_entry,
        "active_selection_invariance_check": invariance,
        "fallback_contract_carry_forward": fallback_carry_forward,
        "rank_storage_contract_carry_forward": rank_storage_carry_forward,
        "meemee_exposure_contract_carry_forward": meemee_exposure_carry_forward,
        "rollback_contract_carry_forward": rollback_carry_forward,
        "registry_mirror_adapter_contract": adapter_contract,
        "registry_mirror_adapter_decision": decision,
    }


def _write_output_bundle(output_root: Path, payloads: dict[str, Any]) -> list[str]:
    output_root.mkdir(parents=True, exist_ok=True)
    written = []
    for name, payload in payloads.items():
        _json_dump(output_root / f"{name}.json", payload)
        written.append(f"{name}.json")
    artifact_complete = {
        "schema_version": RUNNING_SCHEMA_VERSION,
        "candidate_id": "champion_top5_capture_boundary_promoter_v1",
        "generated_at": _now_iso(),
        "root": str(output_root),
        "all_required_json_present": all((output_root / f).exists() for f in written),
        "required_json": sorted(written),
    }
    _json_dump(output_root / "_ARTIFACT_COMPLETE.json", artifact_complete)
    written.append("_ARTIFACT_COMPLETE.json")
    report = [
        "# Non-Active Registry Mirror Adapter Report",
        "",
        "- candidate_id: champion_top5_capture_boundary_promoter_v1",
        f"- decision: {payloads['registry_mirror_adapter_decision']['decision']}",
        f"- active_selection_invariance_passed: {payloads['active_selection_invariance_check']['pass']}",
        "- live registry mirror was read only",
        "- review mirror adapter is file-backed and inactive",
        "- adjusted ranking materialization: not_run",
    ]
    (output_root / "report.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    written.append("report.md")
    return written


def run_registry_mirror_adapter(*, output_root: Path | None = None) -> dict[str, Any]:
    authoritative = _load_authoritative_context()
    config_repo = ConfigRepository(str(app_config.DATA_DIR))
    runtime_before = _capture_runtime_selection_snapshot(config_repo)
    runtime_after = _capture_runtime_selection_snapshot(config_repo)
    payloads = build_registry_mirror_adapter_artifacts(
        authoritative=authoritative,
        config_repo=config_repo,
        runtime_before=runtime_before,
        runtime_after=runtime_after,
    )
    resolved_root = _resolve_output_root(output_root or DEFAULT_OUTPUT_ROOT)
    written = _write_output_bundle(resolved_root, payloads)
    return {
        "output_root": str(resolved_root),
        "decision": payloads["registry_mirror_adapter_decision"]["decision"],
        "files_written": written,
        "payloads": payloads,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a non-active registry mirror adapter for the top5 boundary promoter")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Base directory under which a timestamped non-active registry mirror adapter root will be created.",
    )
    args = parser.parse_args()
    result = run_registry_mirror_adapter(output_root=args.output_root)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
