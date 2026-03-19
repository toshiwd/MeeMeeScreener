from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from app.backend.infra.files.config_repo import ConfigRepository, PUBLISH_REGISTRY_SCHEMA_VERSION
from external_analysis.results.publish_registry import load_publish_registry_state as load_external_publish_registry_state


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _logic_key(logic_id: str | None, logic_version: str | None) -> str | None:
    logic_id = _normalize_text(logic_id)
    logic_version = _normalize_text(logic_version)
    if logic_id and logic_version:
        return f"{logic_id}:{logic_version}"
    return logic_id


def _has_registry_content(state: dict[str, Any]) -> bool:
    return any(
        [
            _normalize_text(state.get("champion_logic_key")),
            _normalize_text(state.get("default_logic_pointer")),
            _normalize_text(state.get("previous_stable_champion_logic_key")),
            bool(state.get("challengers")),
            bool(state.get("challenger_logic_keys")),
            bool(state.get("champion")),
        ]
    )


def _normalize_challenger(entry: dict[str, Any], *, queue_order: int) -> dict[str, Any] | None:
    logic_key = _normalize_text(entry.get("logic_key")) or _logic_key(entry.get("logic_id"), entry.get("logic_version"))
    if not logic_key:
        return None
    try:
        normalized_queue_order = int(entry.get("queue_order") or queue_order)
    except (TypeError, ValueError):
        normalized_queue_order = queue_order
    return {
        "logic_id": _normalize_text(entry.get("logic_id")),
        "logic_version": _normalize_text(entry.get("logic_version")),
        "logic_key": logic_key,
        "logic_family": _normalize_text(entry.get("logic_family")),
        "artifact_uri": _normalize_text(entry.get("artifact_uri")),
        "artifact_checksum": _normalize_text(entry.get("artifact_checksum")),
        "queued_at": _normalize_text(entry.get("queued_at")) or _now_iso(),
        "promotion_state": _normalize_text(entry.get("promotion_state")) or "queued",
        "queue_order": normalized_queue_order,
        "validation_state": _normalize_text(entry.get("validation_state")) or "queued",
        "status": _normalize_text(entry.get("status")) or "challenger",
        "role": _normalize_text(entry.get("role")) or "challenger",
        "source_publish_id": _normalize_text(entry.get("source_publish_id")),
        "promotion_reason": _normalize_text(entry.get("promotion_reason")),
    }


def _normalize_champion(entry: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(entry, dict):
        return None
    logic_key = _normalize_text(entry.get("logic_key")) or _logic_key(entry.get("logic_id"), entry.get("logic_version"))
    if not logic_key:
        return None
    payload = {
        "logic_id": _normalize_text(entry.get("logic_id")),
        "logic_version": _normalize_text(entry.get("logic_version")),
        "logic_key": logic_key,
        "logic_family": _normalize_text(entry.get("logic_family")),
        "artifact_uri": _normalize_text(entry.get("artifact_uri")),
        "artifact_checksum": _normalize_text(entry.get("artifact_checksum")),
        "published_at": _normalize_text(entry.get("published_at")),
        "status": "champion",
        "role": "champion",
        "promotion_state": "champion",
        "queued_at": _normalize_text(entry.get("queued_at")),
        "queue_order": None,
        "validation_state": _normalize_text(entry.get("validation_state")) or "approved",
        "source_publish_id": _normalize_text(entry.get("source_publish_id")),
    }
    if entry.get("previous_stable_champion_logic_key"):
        payload["previous_stable_champion_logic_key"] = _normalize_text(entry.get("previous_stable_champion_logic_key"))
    return payload


def _canonicalize_local_mirror(raw_state: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    payload = dict(raw_state or {})
    legacy_detected = str(payload.get("schema_version") or "").strip() != PUBLISH_REGISTRY_SCHEMA_VERSION
    challengers_source = payload.get("challengers")
    if not isinstance(challengers_source, list):
        legacy_detected = True
        challengers_source = []
    if not challengers_source and isinstance(payload.get("challenger"), dict):
        legacy_detected = True
        challengers_source = [payload.get("challenger")]

    challengers: list[dict[str, Any]] = []
    for index, item in enumerate(challengers_source, start=1):
        if isinstance(item, dict) and int(item.get("queue_order") or 0) <= 0:
            legacy_detected = True
        challenger = _normalize_challenger(item if isinstance(item, dict) else {}, queue_order=index)
        if challenger:
            challengers.append(challenger)

    champion = _normalize_champion(payload.get("champion"))
    if champion is None and payload.get("champion_logic_key"):
        champion = {
            "logic_key": _normalize_text(payload.get("champion_logic_key")),
            "status": "champion",
            "role": "champion",
        }
        legacy_detected = True

    previous_stable = payload.get("previous_stable_champion")
    if not isinstance(previous_stable, dict) and payload.get("previous_stable_champion_logic_key"):
        previous_stable = {
            "logic_key": _normalize_text(payload.get("previous_stable_champion_logic_key")),
            "status": "previous_stable",
            "role": "previous_stable",
        }
        legacy_detected = True

    normalized = dict(payload)
    normalized["schema_version"] = str(payload.get("schema_version") or PUBLISH_REGISTRY_SCHEMA_VERSION)
    normalized["registry_name"] = "publish_registry"
    normalized["source_of_truth"] = "local_mirror"
    normalized["challengers"] = challengers
    normalized["challenger"] = challengers[0] if challengers else None
    normalized["challenger_logic_key"] = _normalize_text(challengers[0].get("logic_key")) if challengers else None
    normalized["challenger_logic_keys"] = [str(entry.get("logic_key")) for entry in challengers if entry.get("logic_key")]
    normalized["challengers_json"] = challengers
    normalized["champion"] = champion
    normalized["champion_logic_key"] = _normalize_text(champion.get("logic_key")) if champion else _normalize_text(payload.get("champion_logic_key"))
    normalized["default_logic_pointer"] = _normalize_text(payload.get("default_logic_pointer")) or normalized["champion_logic_key"]
    normalized["previous_stable_champion"] = previous_stable
    normalized["previous_stable_champion_logic_key"] = _normalize_text(payload.get("previous_stable_champion_logic_key")) or _normalize_text(previous_stable.get("logic_key") if isinstance(previous_stable, dict) else None)
    normalized["rollback_candidates"] = list(payload.get("rollback_candidates") or [])
    normalized["retired_logic_keys"] = list(payload.get("retired_logic_keys") or [])
    normalized["demoted_logic_keys"] = list(payload.get("demoted_logic_keys") or [])
    normalized["promotion_history"] = list(payload.get("promotion_history") or [])
    normalized["bootstrap_rule"] = _normalize_text(payload.get("bootstrap_rule")) or "empty_safe_state"
    normalized["registry_sync_state"] = _normalize_text(payload.get("registry_sync_state")) or "mirror_legacy"
    normalized["sync_state"] = _normalize_text(payload.get("sync_state")) or normalized["registry_sync_state"]
    normalized["sync_message"] = _normalize_text(payload.get("sync_message")) or "legacy_mirror_normalized"
    normalized["degraded"] = bool(payload.get("degraded")) if payload.get("degraded") is not None else False
    normalized["local_mirror_version"] = payload.get("registry_version")
    normalized["mirror_schema_version"] = normalized["schema_version"]
    normalized["mirror_normalized"] = True
    checksum_payload = {k: v for k, v in normalized.items() if k != "registry_checksum"}
    normalized["registry_checksum"] = hashlib.sha256(
        json.dumps(checksum_payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return normalized, legacy_detected


def inspect_publish_registry_sync(
    *,
    config_repo: ConfigRepository,
    db_path: str | None = None,
    external_loader=load_external_publish_registry_state,
) -> dict[str, Any]:
    external_state = external_loader(db_path=db_path)
    local_raw = config_repo.load_publish_registry_state()
    local_normalized, local_changed = _canonicalize_local_mirror(local_raw)

    external_has_content = _has_registry_content(external_state)
    local_has_content = _has_registry_content(local_raw)
    external_valid = external_state.get("source_of_truth") == "external_analysis" and external_has_content
    local_legacy = local_has_content and (
        str(local_raw.get("schema_version") or "").strip() != PUBLISH_REGISTRY_SCHEMA_VERSION
        or local_changed
    )
    local_checksum = _normalize_text(local_raw.get("registry_checksum"))
    normalized_checksum = _normalize_text(local_normalized.get("registry_checksum"))
    in_sync = external_valid and local_has_content and local_checksum == normalized_checksum and not local_legacy

    if external_valid:
        if not local_has_content:
            registry_sync_state = "mirror_stale"
        elif in_sync:
            registry_sync_state = "in_sync"
        elif local_legacy:
            registry_sync_state = "mirror_legacy"
        else:
            registry_sync_state = "mirror_stale"
        source_of_truth = "external_analysis"
        degraded = bool(external_state.get("degraded")) if registry_sync_state == "in_sync" else True
        last_sync_time = _normalize_text(external_state.get("last_sync_at")) or _normalize_text(external_state.get("updated_at"))
    elif local_has_content:
        registry_sync_state = "external_unreachable"
        source_of_truth = "local_mirror"
        degraded = True
        last_sync_time = _normalize_text(local_raw.get("last_sync_at")) or _normalize_text(local_raw.get("updated_at"))
    else:
        registry_sync_state = "empty"
        source_of_truth = "empty"
        degraded = True
        last_sync_time = None

    return {
        "ok": True,
        "source_of_truth": source_of_truth,
        "registry_sync_state": registry_sync_state,
        "degraded": degraded,
        "bootstrap_rule": _normalize_text(external_state.get("bootstrap_rule")) or _normalize_text(local_normalized.get("bootstrap_rule")),
        "external_registry_version": external_state.get("registry_version"),
        "local_mirror_version": local_raw.get("registry_version"),
        "mirror_schema_version": local_raw.get("schema_version") or local_normalized.get("mirror_schema_version"),
        "mirror_normalized": bool(local_has_content and not local_legacy),
        "last_sync_time": last_sync_time,
        "external_state": external_state,
        "local_mirror_raw": local_raw,
        "local_mirror_normalized": local_normalized,
        "external_has_content": external_has_content,
        "local_has_content": local_has_content,
    }


def normalize_publish_registry_mirror(
    *,
    config_repo: ConfigRepository,
    db_path: str | None = None,
    source: str,
    reason: str | None = None,
    actor: str | None = None,
) -> dict[str, Any]:
    sync = inspect_publish_registry_sync(config_repo=config_repo, db_path=db_path)
    if sync["source_of_truth"] != "external_analysis" or not sync["external_has_content"]:
        return {
            "ok": False,
            "reason": "external_unavailable_or_invalid",
            "sync": sync,
        }

    external_state = dict(sync["external_state"])
    local_raw = dict(sync["local_mirror_raw"])
    external_version = external_state.get("registry_version")
    local_version = local_raw.get("registry_version")
    mirror_state = dict(external_state)
    mirror_state["source_of_truth"] = "local_mirror"
    mirror_state["mirror_of"] = "external_analysis"
    mirror_state["registry_sync_state"] = "in_sync"
    mirror_state["sync_state"] = "in_sync"
    mirror_state["degraded"] = False
    mirror_state["sync_message"] = "mirror_resynced_from_external"
    mirror_state["mirror_normalized"] = True
    mirror_state["schema_version"] = PUBLISH_REGISTRY_SCHEMA_VERSION
    mirror_state["mirror_schema_version"] = PUBLISH_REGISTRY_SCHEMA_VERSION
    mirror_state["external_registry_version"] = external_version
    mirror_state["local_mirror_version"] = local_version
    mirror_state["normalized_at"] = _now_iso()
    saved_path = config_repo.save_publish_registry_state(mirror_state)
    config_repo.append_publish_promotion_audit_event(
        {
            "event_type": "publish_registry_mirror_resynced",
            "action": "mirror_resync",
            "previous_logic_key": None,
            "new_logic_key": mirror_state.get("champion_logic_key"),
            "changed_at": _now_iso(),
            "source": source,
            "reason": reason,
            "actor": actor,
            "registry_name": "publish_registry",
            "external_registry_version": external_version,
            "local_mirror_version": local_version,
            "mirror_schema_version": mirror_state.get("mirror_schema_version"),
        }
    )
    return {
        "ok": True,
        "saved_path": saved_path,
        "source_of_truth": "external_analysis",
        "registry_sync_state": "in_sync",
        "degraded": False,
        "bootstrap_rule": mirror_state.get("bootstrap_rule"),
        "external_registry_version": external_version,
        "local_mirror_version": local_version,
        "mirror_schema_version": mirror_state.get("mirror_schema_version"),
        "mirror_normalized": True,
        "last_sync_time": mirror_state.get("last_sync_at") or mirror_state.get("updated_at"),
    }
