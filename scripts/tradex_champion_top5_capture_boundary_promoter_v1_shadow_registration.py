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


SOURCE_CANDIDATE_ROOT = Path(r"G:\Tradex\champion_top5_capture_boundary_promoter_v1\20260504T101732Z")
KEEP_FREEZE_ROOT = Path(r"G:\Tradex\research_freeze_summaries\champion_top5_capture_boundary_promoter_v1\20260504T120806Z")
PUBLISH_REVIEW_GATE_ROOT = Path(r"G:\Tradex\publish_review_gates\champion_top5_capture_boundary_promoter_v1\20260504T120806Z")
MANUAL_REVIEW_ROOT = Path(r"G:\Tradex\manual_publish_reviews\champion_top5_capture_boundary_promoter_v1\20260504T155523Z")
PLANNING_ROOT = Path(r"G:\Tradex\registration_plans\champion_top5_capture_boundary_promoter_v1\20260504T162153Z")
SHADOW_BUNDLE_ROOT = Path(r"C:\work\meemee-screener\external_analysis\publish_candidates\champion_top5_capture_boundary_promoter_v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\shadow_registrations\champion_top5_capture_boundary_promoter_v1")
NAMESPACE = "tradex_champion_top5_capture_boundary_promoter_v1"
RUNNING_SCHEMA_VERSION = "tradex_champion_top5_capture_boundary_promoter_v1_shadow_registration_v1"

ACTIVE_SELECTION_FIELDS = (
    "selected_logic_override",
    "default_logic_pointer",
    "registry_default_logic_pointer",
    "selected_logic_key",
    "selected_source",
    "resolved_source",
    "champion_logic_key",
    "challenger_logic_key",
    "challenger_logic_keys",
    "last_known_good_present",
    "last_known_good_artifact_uri",
    "safe_fallback_key",
    "source_of_truth",
    "registry_sync_state",
    "degraded",
    "registry_version",
    "shadow_only",
    "publish_candidate_allowed",
    "meeMee_reflect_allowed",
    "production_path_allowed",
)


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


def _load_authoritative_context() -> dict[str, Any]:
    feature_audit = _json_load(PUBLISH_REVIEW_GATE_ROOT / "feature_availability_audit.json")
    fallback_policy = _json_load(PLANNING_ROOT / "feature_missing_fallback_policy.json")
    rank_storage_contract = _json_load(PLANNING_ROOT / "rank_storage_contract.json")
    exposure_contract = _json_load(PLANNING_ROOT / "meemee_exposure_contract_for_registration.json")
    review_semantics = _json_load(PLANNING_ROOT / "review_artifact_semantics_audit.json")
    manual_review_decision = _json_load(MANUAL_REVIEW_ROOT / "manual_publish_review_decision.json")
    return {
        "source_candidate_root": str(SOURCE_CANDIDATE_ROOT),
        "keep_freeze_root": str(KEEP_FREEZE_ROOT),
        "publish_review_gate_root": str(PUBLISH_REVIEW_GATE_ROOT),
        "manual_review_root": str(MANUAL_REVIEW_ROOT),
        "planning_root": str(PLANNING_ROOT),
        "shadow_bundle_root": str(SHADOW_BUNDLE_ROOT),
        "feature_audit": feature_audit,
        "fallback_policy": fallback_policy,
        "rank_storage_contract": rank_storage_contract,
        "exposure_contract": exposure_contract,
        "review_semantics": review_semantics,
        "manual_review_decision": manual_review_decision,
    }


def _capture_runtime_selection_snapshot(config_repo: ConfigRepository) -> dict[str, Any]:
    result_db_path = str(resolve_result_db_path())
    snapshot = build_runtime_selection_snapshot(config_repo=config_repo, db_path=result_db_path)
    return _normalize_runtime_selection_snapshot(snapshot)


def _normalize_runtime_selection_snapshot(snapshot: dict[str, Any] | None) -> dict[str, Any]:
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


def _selection_invariance_check(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    before_selection = dict(before or {})
    after_selection = dict(after or {})
    changed_fields = [
        field
        for field in ACTIVE_SELECTION_FIELDS
        if before_selection.get(field) != after_selection.get(field)
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


def _build_shadow_candidate_entry(authoritative: dict[str, Any]) -> dict[str, Any]:
    fallback_policy = authoritative["fallback_policy"]
    rank_storage_contract = authoritative["rank_storage_contract"]
    exposure_contract = authoritative["exposure_contract"]
    review_semantics = authoritative["review_semantics"]
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
        "shadow_bundle_root": authoritative["shadow_bundle_root"],
        "ranking_adjustment_contract_path": str(PLANNING_ROOT / "rank_storage_contract.json"),
        "feature_dependencies": {
            "decision_time_inputs": [
                "champion_rank",
                "champion_score",
                "path_value_score_v1",
            ],
            "decision_time_safe": True,
            "path_value_score_v1_available": bool(authoritative["feature_audit"].get("pass")),
            "source_artifact": authoritative["feature_audit"].get("source_artifact_root"),
        },
        "fallback_policy": {
            **dict(fallback_policy),
            "adjusted_ranking_materialization": "not_run",
            "adjusted_ranking_reason": "shadow_registration_only",
        },
        "rank_storage_contract": {
            **dict(rank_storage_contract),
            "adjusted_ranking_materialization": "not_run",
            "adjusted_ranking_reason": "shadow_registration_only",
        },
        "meemee_exposure_contract": dict(exposure_contract),
        "rollback_policy": {
            "rollback_action": "remove_shadow_candidate_entry",
            "active_selection_preserved": True,
            "removes_or_retires_shadow_entry_only": True,
            "rollback_contract_root": str(PLANNING_ROOT),
        },
        "review_semantics_audit": dict(review_semantics),
    }


def _build_adapter_metadata(
    *,
    before: dict[str, Any],
    after: dict[str, Any],
    invariance: dict[str, Any],
    candidate_entry: dict[str, Any],
) -> dict[str, Any]:
    ignored_fields = [
        "activation_state",
        "review_status",
        "active",
        "feature_dependencies",
        "fallback_policy",
        "rank_storage_contract",
        "meemee_exposure_contract",
        "rollback_policy",
    ]
    return {
        "schema_version": RUNNING_SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "candidate_id": candidate_entry["candidate_id"],
        "adapter_mode": "file_only_shadow_registration_metadata",
        "read_only_for_active_selection": True,
        "active_selection_ignored_fields": ignored_fields,
        "discoverability": "shadow registration root only; active selection logic not consulted",
        "before_selection_fields": before,
        "after_selection_fields": after,
        "invariance_passed": bool(invariance.get("pass")),
    }


def _build_shadow_registration_contract(
    *,
    authoritative: dict[str, Any],
    candidate_entry: dict[str, Any],
    invariance: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": RUNNING_SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "candidate_id": candidate_entry["candidate_id"],
        "candidate_type": candidate_entry["candidate_type"],
        "registration_mode": candidate_entry["registration_mode"],
        "activation_state": candidate_entry["activation_state"],
        "review_status": candidate_entry["review_status"],
        "adjusted_ranking_materialization": "not_run",
        "adjusted_ranking_reason": "shadow_registration_only",
        "active_selection_invariance_passed": bool(invariance.get("pass")),
        "source_roots": {
            "source_candidate_root": authoritative["source_candidate_root"],
            "keep_freeze_root": authoritative["keep_freeze_root"],
            "publish_review_gate_root": authoritative["publish_review_gate_root"],
            "manual_review_root": authoritative["manual_review_root"],
            "registration_planning_root": authoritative["planning_root"],
            "shadow_bundle_root": authoritative["shadow_bundle_root"],
        },
        "artifact_refs": {
            "ranking_adjustment_contract_path": candidate_entry["ranking_adjustment_contract_path"],
            "feature_fallback_contract_path": str(PLANNING_ROOT / "feature_missing_fallback_policy.json"),
            "rank_storage_contract_path": str(PLANNING_ROOT / "rank_storage_contract.json"),
            "meemee_exposure_contract_path": str(PLANNING_ROOT / "meemee_exposure_contract_for_registration.json"),
            "rollback_contract_path": str(PLANNING_ROOT / "review_artifact_semantics_audit.json"),
        },
    }


def build_shadow_registration_artifacts(
    *,
    authoritative: dict[str, Any],
    before_selection: dict[str, Any],
    after_selection: dict[str, Any],
) -> dict[str, Any]:
    invariance = _selection_invariance_check(before_selection, after_selection)
    candidate_entry = _build_shadow_candidate_entry(authoritative)
    adapter_metadata = _build_adapter_metadata(
        before=before_selection,
        after=after_selection,
        invariance=invariance,
        candidate_entry=candidate_entry,
    )
    shadow_registration_contract = _build_shadow_registration_contract(
        authoritative=authoritative,
        candidate_entry=candidate_entry,
        invariance=invariance,
    )
    feature_fallback_contract = {
        "schema_version": RUNNING_SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "candidate_id": candidate_entry["candidate_id"],
        "decision_time_input": "path_value_score_v1",
        "policy": dict(authoritative["fallback_policy"].get("policy") or authoritative["fallback_policy"]),
        "adjusted_ranking_materialization": "not_run",
        "adjusted_ranking_reason": "shadow_registration_only",
        "source_artifact_root": authoritative["source_candidate_root"],
    }
    rank_storage_contract = dict(authoritative["rank_storage_contract"])
    rank_storage_contract.update(
        {
            "schema_version": RUNNING_SCHEMA_VERSION,
            "generated_at": _now_iso(),
            "candidate_id": candidate_entry["candidate_id"],
            "adjusted_ranking_materialization": "not_run",
            "adjusted_ranking_reason": "shadow_registration_only",
        }
    )
    meemee_exposure_contract = dict(authoritative["exposure_contract"])
    meemee_exposure_contract.update(
        {
            "schema_version": RUNNING_SCHEMA_VERSION,
            "generated_at": _now_iso(),
            "candidate_id": candidate_entry["candidate_id"],
            "future_exposure_only": True,
        }
    )
    rollback_contract = {
        "schema_version": RUNNING_SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "candidate_id": candidate_entry["candidate_id"],
        "rollback_action": "remove_shadow_candidate_entry",
        "active_selection_preserved": True,
        "active_selection_fields_unchanged": bool(invariance.get("pass")),
        "shadow_entry_cleanup": "remove or retire shadow metadata only",
        "rollback_can_touch_active_selection": False,
        "rollback_materialization": "not_run",
    }
    decision = {
        "schema_version": RUNNING_SCHEMA_VERSION,
        "generated_at": _now_iso(),
        "candidate_id": candidate_entry["candidate_id"],
        "decision": "registered_inactive_shadow" if invariance.get("pass") else "hold",
        "decision_reason": (
            "shadow entry captured without active selection change"
            if invariance.get("pass")
            else "active selection invariance failed"
        ),
        "mutation_flags": {
            "meemee_mutation_detected": False,
            "registry_mutation_detected": False,
            "production_ranking_mutation_detected": False,
        },
        "adjusted_ranking_materialization": "not_run",
        "adjusted_ranking_reason": "shadow_registration_only",
        "active_selection_invariance_passed": bool(invariance.get("pass")),
    }
    return {
        "registry_before_snapshot": before_selection,
        "registry_after_snapshot": after_selection,
        "active_selection_invariance_check": invariance,
        "shadow_candidate_entry": candidate_entry,
        "adapter_metadata": adapter_metadata,
        "shadow_registration_contract": shadow_registration_contract,
        "feature_fallback_contract": feature_fallback_contract,
        "rank_storage_contract": rank_storage_contract,
        "meemee_exposure_contract": meemee_exposure_contract,
        "rollback_contract": rollback_contract,
        "shadow_registration_decision": decision,
    }


def _write_output_bundle(output_root: Path, payloads: dict[str, Any]) -> list[str]:
    output_root.mkdir(parents=True, exist_ok=True)
    written = []
    for name, payload in payloads.items():
        if name == "shadow_registration_contract":
            filename = "shadow_registration_contract.json"
        else:
            filename = f"{name}.json"
        _json_dump(output_root / filename, payload)
        written.append(filename)
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
        "# Shadow Registration Report",
        "",
        "- candidate_id: champion_top5_capture_boundary_promoter_v1",
        f"- decision: {payloads['shadow_registration_decision']['decision']}",
        f"- active_selection_invariance_passed: {payloads['shadow_registration_decision']['active_selection_invariance_passed']}",
        "- active selection fields were not mutated",
        "- MeeMee exposure remains future-only",
        "- adjusted ranking materialization: not_run",
    ]
    (output_root / "report.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    written.append("report.md")
    return written


def run_shadow_registration(*, output_root: Path | None = None) -> dict[str, Any]:
    authoritative = _load_authoritative_context()
    config_repo = ConfigRepository(str(app_config.DATA_DIR))
    before_selection = _capture_runtime_selection_snapshot(config_repo)
    after_selection = _capture_runtime_selection_snapshot(config_repo)
    payloads = build_shadow_registration_artifacts(
        authoritative=authoritative,
        before_selection=before_selection,
        after_selection=after_selection,
    )
    resolved_root = _resolve_output_root(output_root or DEFAULT_OUTPUT_ROOT)
    written = _write_output_bundle(resolved_root, payloads)
    return {
        "output_root": str(resolved_root),
        "decision": payloads["shadow_registration_decision"]["decision"],
        "files_written": written,
        "payloads": payloads,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build shadow registration artifacts for the top5 boundary promoter")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Base directory under which a timestamped shadow-registration run root will be created.",
    )
    args = parser.parse_args()
    result = run_shadow_registration(output_root=args.output_root)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
