from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.backend.infra.files.config_repo import (
    ConfigRepository,
    LAST_KNOWN_GOOD_ARTIFACT_SCHEMA_VERSION,
    LOGIC_SELECTION_SCHEMA_VERSION,
)
from external_analysis.results.publish import load_published_logic_catalog
from external_analysis.results.publish_candidates import load_publish_candidate_maintenance_state
from external_analysis.results.publish_registry import load_publish_registry_state as load_external_publish_registry_state
from shared.contracts.logic_selection import (
    DEFAULT_LOGIC_POINTER_NAME,
    LAST_KNOWN_GOOD_ARTIFACT_NAME,
    SELECTED_LOGIC_OVERRIDE_NAME,
)
from shared.runtime_selection import SAFE_FALLBACK_SOURCE, resolve_runtime_logic_selection
from app.backend.services.publish_registry_sync_service import inspect_publish_registry_sync

_SAFE_FALLBACK_KEY = "builtin_safe_fallback"
_VALIDATION_OK = "ok"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _split_logic_key(logic_key: str | None) -> tuple[str | None, str | None]:
    text = _normalize_text(logic_key)
    if not text:
        return None, None
    if ":" in text:
        logic_id, logic_version = text.split(":", 1)
        logic_id = _normalize_text(logic_id)
        logic_version = _normalize_text(logic_version)
        return logic_id, logic_version
    return text, None


def _logic_key_from_parts(logic_id: str | None, logic_version: str | None) -> str | None:
    logic_id = _normalize_text(logic_id)
    logic_version = _normalize_text(logic_version)
    if logic_id and logic_version:
        return f"{logic_id}:{logic_version}"
    return logic_id


def _resolve_artifact_path(locator: str | None, *, base_dir: str | None = None) -> str | None:
    text = _normalize_text(locator)
    if not text:
        return None
    candidate = Path(text).expanduser()
    if candidate.exists():
        return str(candidate.resolve())
    if not candidate.is_absolute() and base_dir:
        base_candidate = (Path(base_dir).expanduser() / candidate).resolve()
        if base_candidate.exists():
            return str(base_candidate)
    return None


def _checksum_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_catalog_entry(
    entry: dict[str, Any],
    *,
    config_repo: ConfigRepository,
) -> dict[str, Any]:
    logic_id = _normalize_text(entry.get("logic_id"))
    logic_version = _normalize_text(entry.get("logic_version"))
    logic_key = _logic_key_from_parts(logic_id, logic_version)
    artifact_uri = _normalize_text(entry.get("logic_artifact_uri") or entry.get("artifact_uri"))
    checksum = _normalize_text(entry.get("logic_artifact_checksum") or entry.get("checksum"))
    artifact_path = _resolve_artifact_path(artifact_uri, base_dir=config_repo.data_dir)
    manifest_payload = entry.get("logic_manifest")
    manifest_logic_id = _normalize_text(manifest_payload.get("logic_id")) if isinstance(manifest_payload, dict) else None
    manifest_logic_version = _normalize_text(manifest_payload.get("logic_version")) if isinstance(manifest_payload, dict) else None
    manifest_key = _logic_key_from_parts(manifest_logic_id, manifest_logic_version) if manifest_payload else None

    reasons: list[str] = []
    if not logic_key:
        reasons.append("missing_logic_key")
    if logic_id and logic_version and logic_key != f"{logic_id}:{logic_version}":
        reasons.append("logic_key_mismatch")
    if manifest_payload and manifest_key != logic_key:
        reasons.append("manifest_mismatch")
    if not artifact_uri:
        reasons.append("missing_artifact_uri")
    if artifact_uri and not artifact_path:
        reasons.append("artifact_missing")
    if artifact_path and checksum:
        actual_checksum = _checksum_file(artifact_path)
        if actual_checksum.lower() != checksum.lower():
            reasons.append("checksum_mismatch")

    validation_state = _VALIDATION_OK if not reasons else reasons[0]
    normalized = {
        "logic_id": logic_id,
        "logic_version": logic_version,
        "logic_key": logic_key,
        "logic_family": _normalize_text(entry.get("logic_family")),
        "status": _normalize_text(entry.get("status")) or "published",
        "artifact_uri": artifact_uri,
        "artifact_path": artifact_path,
        "artifact_checksum": checksum,
        "logic_manifest": manifest_payload if isinstance(manifest_payload, dict) else None,
        "published_at": _normalize_text(entry.get("published_at")),
        "default_logic_pointer": _normalize_text(entry.get("default_logic_pointer")),
        "validation_state": validation_state,
        "validation_reasons": reasons,
        "source": "publish_catalog",
    }
    return normalized


def _validate_last_known_good_state(
    state: dict[str, Any] | str | None,
    *,
    config_repo: ConfigRepository,
) -> dict[str, Any] | None:
    if state is None:
        return None
    if isinstance(state, str):
        # Backward compatibility only. Old scalar values are treated as invalid
        # unless they can be matched to a current validated catalog entry.
        logic_id, logic_version = _split_logic_key(state)
        return {
            "logic_id": logic_id,
            "logic_version": logic_version,
            "logic_key": _logic_key_from_parts(logic_id, logic_version),
            "artifact_uri": None,
            "artifact_path": None,
            "artifact_checksum": None,
            "published_at": None,
            "captured_at": None,
            "resolved_source": LAST_KNOWN_GOOD_ARTIFACT_NAME,
            "manifest_ref": None,
            "validation_state": "legacy_scalar_invalid",
            "validation_reasons": ["legacy_scalar_format"],
        }
    logic_key = _normalize_text(state.get("logic_key"))
    logic_id = _normalize_text(state.get("logic_id"))
    logic_version = _normalize_text(state.get("logic_version"))
    if not logic_key:
        logic_key = _logic_key_from_parts(logic_id, logic_version)
    artifact_uri = _normalize_text(state.get("artifact_uri"))
    artifact_path = _resolve_artifact_path(state.get("artifact_path") or artifact_uri, base_dir=config_repo.data_dir)
    checksum = _normalize_text(state.get("artifact_checksum"))
    reasons: list[str] = []
    if not logic_key:
        reasons.append("missing_logic_key")
    if not artifact_path:
        reasons.append("artifact_missing")
    elif checksum:
        actual_checksum = _checksum_file(artifact_path)
        if actual_checksum.lower() != checksum.lower():
            reasons.append("checksum_mismatch")
    validation_state = _VALIDATION_OK if not reasons else reasons[0]
    return {
        "logic_id": logic_id,
        "logic_version": logic_version,
        "logic_key": logic_key,
        "artifact_uri": artifact_uri,
        "artifact_path": artifact_path,
        "artifact_checksum": checksum,
        "published_at": _normalize_text(state.get("published_at")),
        "captured_at": _normalize_text(state.get("captured_at")),
        "resolved_source": _normalize_text(state.get("resolved_source")) or LAST_KNOWN_GOOD_ARTIFACT_NAME,
        "manifest_ref": _normalize_text(state.get("manifest_ref")),
        "source_artifact_path": artifact_path,
        "validation_state": validation_state,
        "validation_reasons": reasons,
        "source": "local_last_known_good",
    }


def _catalog_validation_summary(
    entries: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]]]:
    available: list[dict[str, Any]] = []
    keys: list[str] = []
    issues: list[dict[str, Any]] = []
    for entry in entries:
        if entry["validation_state"] == _VALIDATION_OK and entry.get("logic_key"):
            available.append(entry)
            keys.append(str(entry["logic_key"]))
        else:
            issues.append(
                {
                    "logic_key": entry.get("logic_key"),
                    "artifact_uri": entry.get("artifact_uri"),
                    "validation_state": entry.get("validation_state"),
                    "validation_reasons": entry.get("validation_reasons", []),
                }
            )
    return available, keys, issues


def _current_logic_selection_state(config_repo: ConfigRepository) -> dict[str, Any]:
    state = config_repo.load_logic_selection_state()
    return state if isinstance(state, dict) else {}


def _resolved_result_db_path(db_path: str | None = None) -> str | None:
    text = _normalize_text(db_path)
    if text:
        return text
    return _normalize_text(os.getenv("MEEMEE_RESULT_DB_PATH"))


def _build_resolution_snapshot(
    *,
    config_repo: ConfigRepository,
    local_state: dict[str, Any],
    catalog_manifest: list[dict[str, Any]],
    catalog_default_logic_pointer: str | None,
    publish_registry: dict[str, Any],
    lkg_state: dict[str, Any] | None,
    selection_issues: list[dict[str, Any]],
) -> dict[str, Any]:
    available_manifest, available_keys, catalog_issues = _catalog_validation_summary(catalog_manifest)
    available_lookup = {str(entry["logic_key"]): entry for entry in available_manifest if entry.get("logic_key")}

    override_raw = _normalize_text(local_state.get(SELECTED_LOGIC_OVERRIDE_NAME))
    registry_default_pointer = _normalize_text(publish_registry.get("default_logic_pointer"))
    default_pointer = (
        registry_default_pointer
        or _normalize_text(catalog_default_logic_pointer)
        or _normalize_text(local_state.get(DEFAULT_LOGIC_POINTER_NAME))
    )
    lkg_valid = lkg_state if lkg_state and lkg_state.get("validation_state") == _VALIDATION_OK else None
    lkg_key = _normalize_text(lkg_valid.get("logic_key")) if lkg_valid else None
    champion = publish_registry.get("champion") if isinstance(publish_registry.get("champion"), dict) else None
    challengers = publish_registry.get("challengers") if isinstance(publish_registry.get("challengers"), list) else []
    challenger = challengers[0] if challengers else (publish_registry.get("challenger") if isinstance(publish_registry.get("challenger"), dict) else None)
    champion_key = _normalize_text(champion.get("logic_key")) if champion else None
    challenger_key = _normalize_text(challenger.get("logic_key")) if challenger else None
    challenger_keys = [str(entry.get("logic_key")) for entry in challengers if isinstance(entry, dict) and entry.get("logic_key")]
    if lkg_key and lkg_key not in available_keys:
        available_keys.append(lkg_key)
        available_manifest.append(lkg_valid)
        available_lookup[lkg_key] = lkg_valid

    resolution = resolve_runtime_logic_selection(
        selected_logic_override=override_raw if override_raw in available_keys else None,
        default_logic_pointer=default_pointer if default_pointer in available_keys else None,
        last_known_good=lkg_key if lkg_key in available_keys else None,
        available_logic_keys=available_keys,
        safe_fallback_key=_SAFE_FALLBACK_KEY,
    )

    selected_key = resolution["selected_logic_key"]
    selected_entry = available_lookup.get(selected_key or "") if selected_key else None
    selected_source = resolution["selected_source"]
    if selected_source == SAFE_FALLBACK_SOURCE:
        validation_state = "safe_fallback"
    elif selected_source == SELECTED_LOGIC_OVERRIDE_NAME:
        validation_state = "override_valid"
    elif selected_source == DEFAULT_LOGIC_POINTER_NAME:
        validation_state = "default_valid"
    elif selected_source == LAST_KNOWN_GOOD_ARTIFACT_NAME:
        validation_state = "last_known_good_valid"
    else:
        validation_state = "unresolved"

    selected_logic_id = _normalize_text(selected_entry.get("logic_id")) if selected_entry else None
    selected_logic_version = _normalize_text(selected_entry.get("logic_version")) if selected_entry else None
    artifact_uri = _normalize_text(selected_entry.get("artifact_uri")) if selected_entry else None
    selected_manifest = dict(selected_entry) if selected_entry else None

    if selected_source == LAST_KNOWN_GOOD_ARTIFACT_NAME and lkg_valid:
        artifact_uri = _normalize_text(lkg_valid.get("artifact_uri")) or artifact_uri
        selected_logic_id = _normalize_text(lkg_valid.get("logic_id")) or selected_logic_id
        selected_logic_version = _normalize_text(lkg_valid.get("logic_version")) or selected_logic_version
        selected_manifest = dict(lkg_valid)

    if override_raw and override_raw not in available_keys:
        selection_issues.append(
            {
                "field": "selected_logic_override",
                "value": override_raw,
                "issue": "logic_key_not_available",
            }
        )
    if default_pointer and default_pointer not in available_keys:
        selection_issues.append(
            {
                "field": "default_logic_pointer",
                "value": default_pointer,
                "issue": "logic_key_not_available",
            }
        )
    if lkg_state and not lkg_valid:
        selection_issues.append(
            {
                "field": "last_known_good",
                "value": _normalize_text(lkg_state.get("logic_key")),
                "issue": lkg_state.get("validation_state") or "invalid",
            }
        )

    return {
        "schema_version": LOGIC_SELECTION_SCHEMA_VERSION,
        "snapshot_created_at": _now_iso(),
        "selected_logic_override": override_raw,
        "default_logic_pointer": default_pointer,
        "registry_default_logic_pointer": registry_default_pointer,
        "champion_logic_key": champion_key,
        "challenger_logic_key": challenger_key,
        "challenger_logic_keys": challenger_keys,
        "challengers": challengers,
        "logic_key": selected_key,
        "selected_logic_key": selected_key,
        "selected_logic_id": selected_logic_id,
        "selected_logic_version": selected_logic_version,
        "artifact_uri": artifact_uri,
        "selected_manifest": selected_manifest,
        "override_present": bool(override_raw),
        "last_known_good_present": bool(lkg_valid),
        "last_known_good": lkg_valid,
        "last_known_good_artifact_uri": _normalize_text(lkg_valid.get("artifact_uri")) if lkg_valid else None,
        "safe_fallback_key": _SAFE_FALLBACK_KEY,
        "available_logic_manifest": available_manifest,
        "available_logic_keys": available_keys,
        "selected_source": selected_source,
        "resolved_source": selected_source,
        "selected_pointer_name": resolution["selected_pointer_name"],
        "matched_available": resolution["matched_available"],
        "validation_state": validation_state,
        "validation_issues": selection_issues + catalog_issues,
        "notes": resolution["notes"],
        "catalog_default_logic_pointer": catalog_default_logic_pointer,
        "publish_registry": publish_registry,
        "publish_registry_state": {
            **publish_registry,
            "champion_logic_key": champion_key,
            "challenger_logic_key": challenger_key,
            "challenger_logic_keys": challenger_keys,
            "challengers": challengers,
            "default_logic_pointer": default_pointer,
            "registry_default_logic_pointer": registry_default_pointer,
            "previous_champion_logic_key": _normalize_text(publish_registry.get("previous_champion_logic_key")),
            "previous_stable_champion_logic_key": _normalize_text(publish_registry.get("previous_stable_champion_logic_key")),
            "bootstrap_rule": _normalize_text(publish_registry.get("bootstrap_rule")),
        },
        "catalog": {
            "available_logic_manifest": available_manifest,
            "available_logic_keys": available_keys,
            "default_logic_pointer": default_pointer,
            "validation_issues": catalog_issues,
        },
        "resolution": resolution,
        "resolution_order": [
            SELECTED_LOGIC_OVERRIDE_NAME,
            DEFAULT_LOGIC_POINTER_NAME,
            LAST_KNOWN_GOOD_ARTIFACT_NAME,
            SAFE_FALLBACK_SOURCE,
        ],
    }


def build_runtime_selection_snapshot(
    *,
    config_repo: ConfigRepository,
    db_path: str | None = None,
) -> dict[str, Any]:
    local_state = _current_logic_selection_state(config_repo)
    sync = inspect_publish_registry_sync(
        config_repo=config_repo,
        db_path=db_path,
        external_loader=load_external_publish_registry_state,
    )
    external_registry = sync["external_state"]
    local_registry = sync["local_mirror_normalized"]
    if sync["source_of_truth"] == "external_analysis" and sync["external_has_content"]:
        publish_registry = external_registry
    elif sync["local_has_content"]:
        publish_registry = local_registry
    else:
        publish_registry = {}
    registry_source_of_truth = sync["source_of_truth"]
    registry_sync_state = sync["registry_sync_state"]
    registry_degraded = bool(sync["degraded"])
    last_sync_time = sync["last_sync_time"]
    publish_catalog = load_published_logic_catalog(db_path=_resolved_result_db_path(db_path))
    raw_catalog_manifest = list(publish_catalog.get("available_logic_manifest") or [])
    catalog_default_logic_pointer = _normalize_text(publish_catalog.get("default_logic_pointer"))
    valid_catalog_entries = [_validate_catalog_entry(entry, config_repo=config_repo) for entry in raw_catalog_manifest]
    lkg_state = _validate_last_known_good_state(local_state.get(LAST_KNOWN_GOOD_ARTIFACT_NAME), config_repo=config_repo)
    maintenance_state = load_publish_candidate_maintenance_state(db_path=_resolved_result_db_path(db_path))
    selection_issues: list[dict[str, Any]] = []
    snapshot = _build_resolution_snapshot(
        config_repo=config_repo,
        local_state=local_state,
        catalog_manifest=valid_catalog_entries,
        catalog_default_logic_pointer=catalog_default_logic_pointer,
        publish_registry=publish_registry if isinstance(publish_registry, dict) else {},
        lkg_state=lkg_state,
        selection_issues=selection_issues,
    )
    snapshot["publish_registry_state"] = {
        **snapshot.get("publish_registry_state", {}),
        "source_of_truth": registry_source_of_truth,
        "registry_sync_state": registry_sync_state,
        "degraded": registry_degraded,
        "last_sync_time": last_sync_time,
        "registry_version": sync["external_registry_version"] if registry_source_of_truth == "external_analysis" else sync["local_mirror_version"],
        "external_registry_version": sync["external_registry_version"],
        "local_mirror_version": sync["local_mirror_version"],
        "mirror_schema_version": sync["mirror_schema_version"],
        "mirror_normalized": sync["mirror_normalized"],
        "source_revision": publish_registry.get("source_revision"),
    }
    snapshot["source_of_truth"] = registry_source_of_truth
    snapshot["registry_sync_state"] = registry_sync_state
    snapshot["degraded"] = registry_degraded
    snapshot["last_sync_time"] = last_sync_time
    snapshot["registry_version"] = sync["external_registry_version"] if registry_source_of_truth == "external_analysis" else sync["local_mirror_version"]
    snapshot["source_revision"] = publish_registry.get("source_revision")
    snapshot["publish_registry"] = publish_registry
    publish_challengers = publish_registry.get("challengers") if isinstance(publish_registry.get("challengers"), list) else []
    snapshot["challengers"] = publish_challengers
    snapshot["challenger_logic_keys"] = [
        str(entry.get("logic_key"))
        for entry in publish_challengers
        if isinstance(entry, dict) and entry.get("logic_key")
    ]
    snapshot["bootstrap_rule"] = _normalize_text(publish_registry.get("bootstrap_rule"))
    snapshot["external_registry_version"] = sync["external_registry_version"]
    snapshot["local_mirror_version"] = sync["local_mirror_version"]
    snapshot["mirror_schema_version"] = sync["mirror_schema_version"]
    snapshot["mirror_normalized"] = sync["mirror_normalized"]
    snapshot["ops_fallback_enabled"] = bool(maintenance_state.get("ops_fallback_enabled"))
    snapshot["ops_fallback_last_used_at"] = maintenance_state.get("ops_fallback_last_used_at")
    snapshot["ops_fallback_hit_count"] = int(maintenance_state.get("ops_fallback_hit_count") or 0)
    snapshot["candidate_backfill_last_run"] = maintenance_state.get("candidate_backfill_last_run")
    snapshot["snapshot_sweep_last_run"] = maintenance_state.get("snapshot_sweep_last_run")
    snapshot["maintenance_degraded"] = bool(maintenance_state.get("maintenance_degraded"))
    snapshot["maintenance_state"] = maintenance_state
    return snapshot


def _build_last_known_good_payload(
    *,
    snapshot: dict[str, Any],
) -> dict[str, Any] | None:
    selected_manifest = snapshot.get("selected_manifest")
    if not isinstance(selected_manifest, dict):
        return None
    if snapshot.get("selected_source") == SAFE_FALLBACK_SOURCE:
        return None
    if selected_manifest.get("validation_state") != _VALIDATION_OK:
        return None
    artifact_path = _normalize_text(selected_manifest.get("artifact_path"))
    if not artifact_path or not Path(artifact_path).exists():
        return None
    checksum = _normalize_text(selected_manifest.get("artifact_checksum"))
    if checksum and _checksum_file(artifact_path).lower() != checksum.lower():
        return None
    logic_id = _normalize_text(selected_manifest.get("logic_id"))
    logic_version = _normalize_text(selected_manifest.get("logic_version"))
    logic_key = _normalize_text(selected_manifest.get("logic_key")) or _logic_key_from_parts(logic_id, logic_version)
    if not logic_key:
        return None
    return {
        "schema_version": LAST_KNOWN_GOOD_ARTIFACT_SCHEMA_VERSION,
        "logic_id": logic_id,
        "logic_version": logic_version,
        "logic_key": logic_key,
        "artifact_uri": selected_manifest.get("artifact_uri"),
        "artifact_checksum": checksum,
        "published_at": selected_manifest.get("published_at"),
        "captured_at": _now_iso(),
        "resolved_source": snapshot.get("resolved_source"),
        "manifest_ref": logic_key,
        "source_artifact_path": artifact_path,
    }


def capture_last_known_good_if_eligible(
    *,
    config_repo: ConfigRepository,
    snapshot: dict[str, Any] | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    resolved_snapshot = snapshot or build_runtime_selection_snapshot(config_repo=config_repo, db_path=_resolved_result_db_path(db_path))
    if resolved_snapshot.get("validation_state") not in {"override_valid", "default_valid", "last_known_good_valid"}:
        return {"ok": False, "captured": False, "reason": "selection_not_eligible"}
    payload = _build_last_known_good_payload(snapshot=resolved_snapshot)
    if not payload:
        return {"ok": False, "captured": False, "reason": "last_known_good_payload_invalid"}

    current_state = _current_logic_selection_state(config_repo)
    existing_lkg = _validate_last_known_good_state(current_state.get(LAST_KNOWN_GOOD_ARTIFACT_NAME), config_repo=config_repo)
    if existing_lkg and existing_lkg.get("validation_state") == _VALIDATION_OK:
        if (
            existing_lkg.get("logic_key") == payload["logic_key"]
            and existing_lkg.get("artifact_checksum") == payload["artifact_checksum"]
            and existing_lkg.get("artifact_path")
            and Path(str(existing_lkg["artifact_path"])).exists()
        ):
            return {
                "ok": True,
                "captured": False,
                "reason": "already_current",
                "last_known_good": existing_lkg,
            }

    artifact_source_path = str(payload["source_artifact_path"])
    captured_at = payload["captured_at"]
    artifact_path = config_repo.resolve_last_known_good_artifact_path(
        logic_key=str(payload["logic_key"]),
        checksum=str(payload["artifact_checksum"] or "nochecksum"),
        captured_at=captured_at.replace(":", "").replace("-", ""),
    )
    os_parent = Path(artifact_path).parent
    os_parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(artifact_source_path, artifact_path)
    stored_checksum = _checksum_file(artifact_path)

    if payload.get("artifact_checksum") and stored_checksum.lower() != str(payload["artifact_checksum"]).lower():
        raise RuntimeError("last_known_good_checksum_mismatch")

    last_known_good_meta = {
        "logic_id": payload["logic_id"],
        "logic_version": payload["logic_version"],
        "logic_key": payload["logic_key"],
        "artifact_uri": payload["artifact_uri"],
        "artifact_path": artifact_path,
        "artifact_checksum": stored_checksum,
        "published_at": payload["published_at"],
        "captured_at": captured_at,
        "resolved_source": payload["resolved_source"],
        "manifest_ref": payload["manifest_ref"],
        "validation_state": _VALIDATION_OK,
        "validation_reasons": [],
        "source": "local_last_known_good",
    }
    current_state["schema_version"] = LOGIC_SELECTION_SCHEMA_VERSION
    current_state[LAST_KNOWN_GOOD_ARTIFACT_NAME] = last_known_good_meta
    config_repo.save_logic_selection_state(current_state)
    config_repo.append_audit_event(
        {
            "event_type": "last_known_good_captured",
            "previous_logic_key": _normalize_text(existing_lkg.get("logic_key")) if existing_lkg else None,
            "new_logic_key": last_known_good_meta["logic_key"],
            "changed_at": captured_at,
            "source": "confirmed_only_analysis_path",
            "reason": "confirmed_only_read",
            "artifact_uri": last_known_good_meta["artifact_uri"],
            "artifact_path": artifact_path,
            "artifact_checksum": stored_checksum,
        }
    )
    return {
        "ok": True,
        "captured": True,
        "reason": "captured",
        "artifact_path": artifact_path,
        "last_known_good": last_known_good_meta,
    }


def _resolve_catalog_entry_by_key(
    *,
    config_repo: ConfigRepository,
    logic_key: str | None,
    db_path: str | None = None,
) -> dict[str, Any] | None:
    snapshot = build_runtime_selection_snapshot(config_repo=config_repo, db_path=_resolved_result_db_path(db_path))
    for entry in snapshot.get("available_logic_manifest") or []:
        if entry.get("logic_key") == logic_key and entry.get("validation_state") == _VALIDATION_OK:
            return entry
    return None


def validate_selected_logic_override(
    *,
    config_repo: ConfigRepository,
    selected_logic_override: str | None,
    db_path: str | None = None,
) -> dict[str, Any]:
    logic_key = _normalize_text(selected_logic_override)
    if not logic_key:
        return {"ok": False, "reason": "logic_key_required"}
    entry = _resolve_catalog_entry_by_key(config_repo=config_repo, logic_key=logic_key, db_path=db_path)
    if not entry:
        return {"ok": False, "reason": "logic_key_not_available", "logic_key": logic_key}
    artifact_path = _normalize_text(entry.get("artifact_path"))
    if not artifact_path or not Path(artifact_path).exists():
        return {"ok": False, "reason": "artifact_missing", "logic_key": logic_key}
    checksum = _normalize_text(entry.get("artifact_checksum"))
    if checksum and _checksum_file(artifact_path).lower() != checksum.lower():
        return {"ok": False, "reason": "checksum_mismatch", "logic_key": logic_key}
    logic_id = _normalize_text(entry.get("logic_id"))
    logic_version = _normalize_text(entry.get("logic_version"))
    if _logic_key_from_parts(logic_id, logic_version) != logic_key:
        return {"ok": False, "reason": "manifest_mismatch", "logic_key": logic_key}
    manifest_payload = entry.get("logic_manifest") if isinstance(entry.get("logic_manifest"), dict) else None
    if manifest_payload:
        manifest_key = _logic_key_from_parts(
            _normalize_text(manifest_payload.get("logic_id")),
            _normalize_text(manifest_payload.get("logic_version")),
        )
        if manifest_key != logic_key:
            return {"ok": False, "reason": "manifest_mismatch", "logic_key": logic_key}
        manifest_uri = _normalize_text(manifest_payload.get("artifact_uri"))
        if manifest_uri and manifest_uri != _normalize_text(entry.get("artifact_uri")):
            return {"ok": False, "reason": "manifest_mismatch", "logic_key": logic_key}
        manifest_checksum = _normalize_text(manifest_payload.get("checksum"))
        if manifest_checksum and checksum and manifest_checksum.lower() != checksum.lower():
            return {"ok": False, "reason": "manifest_mismatch", "logic_key": logic_key}
    return {
        "ok": True,
        "reason": None,
        "logic_key": logic_key,
        "logic_id": logic_id,
        "logic_version": logic_version,
        "artifact_uri": entry.get("artifact_uri"),
        "artifact_path": artifact_path,
        "artifact_checksum": checksum,
        "manifest": entry,
    }


def set_selected_logic_override(
    *,
    config_repo: ConfigRepository,
    selected_logic_override: str,
    source: str,
    reason: str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    validation = validate_selected_logic_override(
        config_repo=config_repo,
        selected_logic_override=selected_logic_override,
        db_path=db_path,
    )
    if not validation.get("ok"):
        return validation

    current = _current_logic_selection_state(config_repo)
    previous_value = _normalize_text(current.get(SELECTED_LOGIC_OVERRIDE_NAME))
    new_value = _normalize_text(validation.get("logic_key"))
    if previous_value == new_value:
        snapshot = build_runtime_selection_snapshot(config_repo=config_repo, db_path=_resolved_result_db_path(db_path))
        return {
            "ok": True,
            "changed": False,
            "validation": validation,
            "snapshot": snapshot,
        }

    current["schema_version"] = LOGIC_SELECTION_SCHEMA_VERSION
    current[SELECTED_LOGIC_OVERRIDE_NAME] = new_value
    config_repo.save_logic_selection_state(current)
    config_repo.append_audit_event(
        {
            "event_type": "selected_logic_override_changed",
            "previous_logic_key": previous_value,
            "new_logic_key": new_value,
            "changed_at": _now_iso(),
            "source": source,
            "reason": reason,
            "action": "set",
            "artifact_uri": validation.get("artifact_uri"),
            "artifact_checksum": validation.get("artifact_checksum"),
        }
    )
    snapshot = build_runtime_selection_snapshot(config_repo=config_repo, db_path=_resolved_result_db_path(db_path))
    return {
        "ok": True,
        "changed": True,
        "validation": validation,
        "snapshot": snapshot,
    }


def clear_selected_logic_override(
    *,
    config_repo: ConfigRepository,
    source: str,
    reason: str | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    current = _current_logic_selection_state(config_repo)
    previous_value = _normalize_text(current.get(SELECTED_LOGIC_OVERRIDE_NAME))
    if previous_value is None:
        snapshot = build_runtime_selection_snapshot(config_repo=config_repo, db_path=_resolved_result_db_path(db_path))
        return {"ok": True, "changed": False, "snapshot": snapshot}

    current["schema_version"] = LOGIC_SELECTION_SCHEMA_VERSION
    current[SELECTED_LOGIC_OVERRIDE_NAME] = None
    config_repo.save_logic_selection_state(current)
    config_repo.append_audit_event(
        {
            "event_type": "selected_logic_override_changed",
            "previous_logic_key": previous_value,
            "new_logic_key": None,
            "changed_at": _now_iso(),
            "source": source,
            "reason": reason,
            "action": "clear",
        }
    )
    snapshot = build_runtime_selection_snapshot(config_repo=config_repo, db_path=_resolved_result_db_path(db_path))
    return {"ok": True, "changed": True, "snapshot": snapshot}
