from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.backend.infra.files.config_repo import ConfigRepository, LOGIC_SELECTION_SCHEMA_VERSION
from external_analysis.results.publish import load_published_logic_catalog
from shared.contracts.logic_selection import (
    DEFAULT_LOGIC_POINTER_NAME,
    LAST_KNOWN_GOOD_ARTIFACT_NAME,
    SELECTED_LOGIC_OVERRIDE_NAME,
)
from shared.runtime_selection import SAFE_FALLBACK_SOURCE, resolve_runtime_logic_selection

_SAFE_FALLBACK_KEY = "builtin_safe_fallback"


def _normalize_state_value(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalize_logic_key(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalize_last_known_good(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        logic_key = _normalize_logic_key(value.get("logic_key"))
        if not logic_key:
            logic_id = _normalize_logic_key(value.get("logic_id"))
            logic_version = _normalize_logic_key(value.get("logic_version"))
            if logic_id and logic_version:
                logic_key = f"{logic_id}:{logic_version}"
        artifact_uri = _normalize_logic_key(value.get("artifact_uri"))
        return {
            "logic_key": logic_key,
            "logic_id": _normalize_logic_key(value.get("logic_id")),
            "logic_version": _normalize_logic_key(value.get("logic_version")),
            "artifact_uri": artifact_uri,
            "artifact_checksum": _normalize_logic_key(value.get("artifact_checksum")),
            "manifest_ref": _normalize_logic_key(value.get("manifest_ref")),
            "stored_at": _normalize_logic_key(value.get("stored_at")),
        }
    text = _normalize_state_value(value)
    if not text:
        return None
    return {
        "logic_key": text,
        "logic_id": None,
        "logic_version": None,
        "artifact_uri": None,
        "artifact_checksum": None,
        "manifest_ref": None,
        "stored_at": None,
    }


def _manifest_lookup(manifests: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for manifest in manifests:
        logic_key = _normalize_logic_key(manifest.get("logic_key"))
        if logic_key:
            lookup[logic_key] = manifest
    return lookup


def _fallback_snapshot(
    *,
    local_state: dict[str, Any],
    publish_catalog: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": LOGIC_SELECTION_SCHEMA_VERSION,
        "snapshot_created_at": datetime.now(timezone.utc).isoformat(),
        "selected_logic_override": _normalize_state_value(local_state.get(SELECTED_LOGIC_OVERRIDE_NAME)),
        "default_logic_pointer": _normalize_state_value(publish_catalog.get("default_logic_pointer")),
        "last_known_good": _normalize_last_known_good(local_state.get(LAST_KNOWN_GOOD_ARTIFACT_NAME)),
        "last_known_good_artifact_uri": None,
        "safe_fallback_key": _SAFE_FALLBACK_KEY,
        "available_logic_manifest": list(publish_catalog.get("available_logic_manifest") or []),
        "available_logic_keys": list(publish_catalog.get("available_logic_keys") or []),
        "resolution": None,
        "selected_logic_key": None,
        "selected_logic_id": None,
        "selected_logic_version": None,
        "artifact_uri": None,
        "selected_source": "unresolved",
        "resolved_source": "unresolved",
        "selected_pointer_name": None,
        "matched_available": False,
        "notes": ["runtime_selection_bootstrap_failed"],
        "catalog_default_logic_pointer": _normalize_state_value(publish_catalog.get("default_logic_pointer")),
        "catalog": publish_catalog,
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
    snapshot_created_at = datetime.now(timezone.utc).isoformat()
    local_state = config_repo.load_logic_selection_state()
    publish_catalog = load_published_logic_catalog(db_path=db_path)
    available_logic_manifest = list(publish_catalog.get("available_logic_manifest") or [])
    available_logic_lookup = _manifest_lookup(available_logic_manifest)
    available_logic_keys = list(publish_catalog.get("available_logic_keys") or [])

    selected_logic_override = _normalize_state_value(local_state.get(SELECTED_LOGIC_OVERRIDE_NAME))
    default_logic_pointer = _normalize_state_value(local_state.get(DEFAULT_LOGIC_POINTER_NAME))
    catalog_default_logic_pointer = _normalize_state_value(publish_catalog.get("default_logic_pointer"))
    if catalog_default_logic_pointer:
        default_logic_pointer = catalog_default_logic_pointer

    last_known_good_state = _normalize_last_known_good(local_state.get(LAST_KNOWN_GOOD_ARTIFACT_NAME))
    last_known_good_key = _normalize_logic_key(last_known_good_state.get("logic_key")) if last_known_good_state else None
    if last_known_good_key and last_known_good_key not in available_logic_keys:
        available_logic_keys = [*available_logic_keys, last_known_good_key]

    resolution = resolve_runtime_logic_selection(
        selected_logic_override=selected_logic_override,
        default_logic_pointer=default_logic_pointer,
        last_known_good=last_known_good_key,
        available_logic_keys=available_logic_keys,
        safe_fallback_key=_SAFE_FALLBACK_KEY,
    )

    selected_manifest = available_logic_lookup.get(resolution["selected_logic_key"] or "") if resolution["selected_logic_key"] else None
    last_known_good_artifact_uri = None
    if last_known_good_state:
        last_known_good_artifact_uri = _normalize_state_value(last_known_good_state.get("artifact_uri"))

    selected_logic_id = None
    selected_logic_version = None
    artifact_uri = None
    if selected_manifest:
        selected_logic_id = _normalize_state_value(selected_manifest.get("logic_id"))
        selected_logic_version = _normalize_state_value(selected_manifest.get("logic_version"))
        artifact_uri = _normalize_state_value(selected_manifest.get("logic_artifact_uri"))
    elif resolution["selected_source"] == LAST_KNOWN_GOOD_ARTIFACT_NAME and last_known_good_state:
        selected_logic_id = _normalize_state_value(last_known_good_state.get("logic_id"))
        selected_logic_version = _normalize_state_value(last_known_good_state.get("logic_version"))
        artifact_uri = last_known_good_artifact_uri

    return {
        "schema_version": LOGIC_SELECTION_SCHEMA_VERSION,
        "snapshot_created_at": snapshot_created_at,
        "selected_logic_override": selected_logic_override,
        "default_logic_pointer": default_logic_pointer,
        "last_known_good": last_known_good_state,
        "last_known_good_artifact_uri": last_known_good_artifact_uri,
        "safe_fallback_key": _SAFE_FALLBACK_KEY,
        "available_logic_manifest": available_logic_manifest,
        "available_logic_keys": available_logic_keys,
        "resolution": resolution,
        "selected_logic_key": resolution["selected_logic_key"],
        "selected_logic_id": selected_logic_id,
        "selected_logic_version": selected_logic_version,
        "artifact_uri": artifact_uri,
        "selected_source": resolution["selected_source"],
        "resolved_source": resolution["selected_source"],
        "selected_pointer_name": resolution["selected_pointer_name"],
        "matched_available": resolution["matched_available"],
        "notes": resolution["notes"],
        "catalog_default_logic_pointer": catalog_default_logic_pointer,
        "catalog": publish_catalog,
        "resolution_order": [
            SELECTED_LOGIC_OVERRIDE_NAME,
            DEFAULT_LOGIC_POINTER_NAME,
            LAST_KNOWN_GOOD_ARTIFACT_NAME,
            SAFE_FALLBACK_SOURCE,
        ],
    }

