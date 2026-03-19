from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from external_analysis.results.publish import load_published_logic_catalog
from external_analysis.results.result_schema import connect_result_db, ensure_result_schema
from shared.contracts.publish_registry import (
    PUBLISH_BOOTSTRAP_RULE_DEFAULT_POINTER,
    PUBLISH_BOOTSTRAP_RULE_EMPTY,
    PUBLISH_BOOTSTRAP_RULE_EXPLICIT_CHAMPION,
    PUBLISH_BOOTSTRAP_RULE_LAST_STABLE_PROMOTED,
)

PUBLISH_REGISTRY_SCHEMA_VERSION = "publish_registry_v2"
PUBLISH_REGISTRY_NAME = "publish_registry"
PUBLISH_REGISTRY_SOURCE_OF_TRUTH = "external_analysis"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _checksum_json(payload: dict[str, Any]) -> str:
    digest = hashlib.sha256()
    digest.update(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8"))
    return digest.hexdigest()


def _normalize_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _logic_key(logic_id: str | None, logic_version: str | None) -> str | None:
    logic_id = _normalize_text(logic_id)
    logic_version = _normalize_text(logic_version)
    if logic_id and logic_version:
        return f"{logic_id}:{logic_version}"
    return logic_id


def _manifest_to_logic_key(entry: dict[str, Any] | None) -> str | None:
    if not isinstance(entry, dict):
        return None
    logic_id = _normalize_text(entry.get("logic_id"))
    logic_version = _normalize_text(entry.get("logic_version"))
    return _logic_key(logic_id, logic_version) or _normalize_text(entry.get("logic_key"))


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


def _normalize_challenger(entry: dict[str, Any], *, default_queue_order: int) -> dict[str, Any] | None:
    logic_key = _normalize_text(entry.get("logic_key")) or _logic_key(entry.get("logic_id"), entry.get("logic_version"))
    if not logic_key:
        return None
    queue_order = entry.get("queue_order")
    try:
        queue_order_int = int(queue_order) if queue_order is not None else int(default_queue_order)
    except (TypeError, ValueError):
        queue_order_int = int(default_queue_order)
    payload = {
        "logic_id": _normalize_text(entry.get("logic_id")),
        "logic_version": _normalize_text(entry.get("logic_version")),
        "logic_key": logic_key,
        "logic_family": _normalize_text(entry.get("logic_family")),
        "artifact_uri": _normalize_text(entry.get("artifact_uri")),
        "artifact_checksum": _normalize_text(entry.get("artifact_checksum")),
        "queued_at": _normalize_text(entry.get("queued_at")) or _now_iso(),
        "promotion_state": _normalize_text(entry.get("promotion_state")) or "queued",
        "queue_order": queue_order_int,
        "validation_state": _normalize_text(entry.get("validation_state")) or "queued",
        "status": _normalize_text(entry.get("status")) or "challenger",
        "role": _normalize_text(entry.get("role")) or "challenger",
        "source_publish_id": _normalize_text(entry.get("source_publish_id")),
        "promotion_reason": _normalize_text(entry.get("promotion_reason")),
    }
    return payload


def _empty_state() -> dict[str, Any]:
    return {
        "schema_version": PUBLISH_REGISTRY_SCHEMA_VERSION,
        "registry_name": PUBLISH_REGISTRY_NAME,
        "source_of_truth": PUBLISH_REGISTRY_SOURCE_OF_TRUTH,
        "registry_version": 0,
        "source_revision": None,
        "updated_at": None,
        "last_sync_at": None,
        "bootstrap_rule": PUBLISH_BOOTSTRAP_RULE_EMPTY,
        "champion": None,
        "challengers": [],
        "challenger": None,
        "champion_logic_key": None,
        "challenger_logic_key": None,
        "challenger_logic_keys": [],
        "challengers_json": [],
        "default_logic_pointer": None,
        "previous_stable_champion": None,
        "previous_stable_champion_logic_key": None,
        "rollback_candidates": [],
        "retired_logic_keys": [],
        "demoted_logic_keys": [],
        "promotion_history": [],
        "registry_checksum": None,
        "degraded": True,
        "registry_sync_state": "empty",
        "sync_state": "empty",
        "sync_message": "no_registry_state",
    }


def _normalize_registry_state(state: dict[str, Any]) -> dict[str, Any]:
    payload = dict(state or {})
    champion = _normalize_champion(payload.get("champion"))

    challengers_source = payload.get("challengers")
    if not isinstance(challengers_source, list):
        challengers_source = []
        if isinstance(payload.get("challenger"), dict):
            challengers_source = [payload.get("challenger")]

    challengers: list[dict[str, Any]] = []
    used_keys: set[str] = set()
    for index, item in enumerate(challengers_source):
        challenger = _normalize_challenger(item if isinstance(item, dict) else {}, default_queue_order=index + 1)
        if not challenger:
            continue
        logic_key = str(challenger["logic_key"])
        if logic_key in used_keys:
            continue
        used_keys.add(logic_key)
        challengers.append(challenger)
    challengers.sort(key=lambda item: (int(item.get("queue_order") or 0), str(item.get("logic_key") or "")))

    challenger_logic_keys = [str(item["logic_key"]) for item in challengers if item.get("logic_key")]
    challenger = challengers[0] if challengers else None
    previous_stable = payload.get("previous_stable_champion")
    if not isinstance(previous_stable, dict) and payload.get("previous_stable_champion_logic_key"):
        previous_stable = {
            "logic_key": _normalize_text(payload.get("previous_stable_champion_logic_key")),
            "status": "previous_stable",
            "role": "previous_stable",
        }
    rollback_candidates = payload.get("rollback_candidates")
    if not isinstance(rollback_candidates, list):
        rollback_candidates = []

    retired_logic_keys = list(payload.get("retired_logic_keys") or [])
    demoted_logic_keys = list(payload.get("demoted_logic_keys") or [])
    promotion_history = list(payload.get("promotion_history") or [])
    bootstrap_rule = _normalize_text(payload.get("bootstrap_rule")) or PUBLISH_BOOTSTRAP_RULE_EMPTY
    default_logic_pointer = _normalize_text(payload.get("default_logic_pointer")) or _normalize_text(champion.get("logic_key") if champion else None)
    champion_logic_key = _normalize_text(champion.get("logic_key") if champion else payload.get("champion_logic_key"))
    challenger_logic_key = _normalize_text(challenger.get("logic_key") if challenger else payload.get("challenger_logic_key"))

    payload.update(
        {
            "schema_version": PUBLISH_REGISTRY_SCHEMA_VERSION,
            "registry_name": PUBLISH_REGISTRY_NAME,
            "source_of_truth": PUBLISH_REGISTRY_SOURCE_OF_TRUTH,
            "registry_name": PUBLISH_REGISTRY_NAME,
            "champion": champion,
            "challengers": challengers,
            "challenger": challenger,
            "champion_logic_key": champion_logic_key,
            "challenger_logic_key": challenger_logic_key,
            "challenger_logic_keys": challenger_logic_keys,
            "challengers_json": challengers,
            "default_logic_pointer": default_logic_pointer,
            "previous_stable_champion": previous_stable,
            "previous_stable_champion_logic_key": _normalize_text(payload.get("previous_stable_champion_logic_key"))
            or _normalize_text(previous_stable.get("logic_key") if isinstance(previous_stable, dict) else None),
            "rollback_candidates": rollback_candidates,
            "retired_logic_keys": retired_logic_keys,
            "demoted_logic_keys": demoted_logic_keys,
            "promotion_history": promotion_history,
            "bootstrap_rule": bootstrap_rule,
            "degraded": bool(payload.get("degraded")) if payload.get("degraded") is not None else champion is None,
            "registry_sync_state": _normalize_text(payload.get("registry_sync_state")) or ("synced" if champion else "empty"),
            "sync_state": _normalize_text(payload.get("sync_state")) or ("synced" if champion else "empty"),
            "sync_message": _normalize_text(payload.get("sync_message")) or ("synced_from_external_analysis" if champion else "no_registry_state"),
        }
    )
    payload["registry_checksum"] = _checksum_json({k: v for k, v in payload.items() if k != "registry_checksum"})
    return payload


def _bootstrap_from_catalog(*, db_path: str | None = None) -> dict[str, Any]:
    catalog = load_published_logic_catalog(db_path=db_path)
    manifests = list(catalog.get("available_logic_manifest") or [])
    bootstrap_manifest = next((entry for entry in manifests if bool(entry.get("bootstrap_champion"))), None)
    bootstrap_rule = PUBLISH_BOOTSTRAP_RULE_EXPLICIT_CHAMPION if bootstrap_manifest else None
    if bootstrap_manifest is None:
        default_pointer = _normalize_text(catalog.get("default_logic_pointer"))
        if default_pointer:
            bootstrap_manifest = next((entry for entry in manifests if _normalize_text(entry.get("logic_key")) == default_pointer), None)
            if bootstrap_manifest:
                bootstrap_rule = PUBLISH_BOOTSTRAP_RULE_DEFAULT_POINTER
    if bootstrap_manifest is None:
        return _empty_state()
    champion = {
        "logic_id": _normalize_text(bootstrap_manifest.get("logic_id")),
        "logic_version": _normalize_text(bootstrap_manifest.get("logic_version")),
        "logic_key": _normalize_text(bootstrap_manifest.get("logic_key")) or _manifest_to_logic_key(bootstrap_manifest),
        "logic_family": _normalize_text(bootstrap_manifest.get("logic_family")),
        "artifact_uri": _normalize_text(bootstrap_manifest.get("logic_artifact_uri") or bootstrap_manifest.get("artifact_uri")),
        "artifact_checksum": _normalize_text(bootstrap_manifest.get("logic_artifact_checksum") or bootstrap_manifest.get("checksum")),
        "published_at": _normalize_text(bootstrap_manifest.get("published_at")),
        "status": "champion",
        "role": "champion",
        "promotion_state": "champion",
        "validation_state": "bootstrap",
        "source_publish_id": _normalize_text(bootstrap_manifest.get("publish_id")),
    }
    return _normalize_registry_state(
        {
            "schema_version": PUBLISH_REGISTRY_SCHEMA_VERSION,
            "registry_name": PUBLISH_REGISTRY_NAME,
            "source_of_truth": PUBLISH_REGISTRY_SOURCE_OF_TRUTH,
            "registry_version": 0,
            "source_revision": _normalize_text(bootstrap_manifest.get("publish_id")) or _normalize_text(bootstrap_manifest.get("logic_key")),
            "updated_at": _normalize_text(bootstrap_manifest.get("published_at")) or _now_iso(),
            "last_sync_at": _normalize_text(bootstrap_manifest.get("published_at")) or _now_iso(),
            "bootstrap_rule": bootstrap_rule or PUBLISH_BOOTSTRAP_RULE_LAST_STABLE_PROMOTED,
            "champion": champion,
            "challengers": [],
            "challenger": None,
            "champion_logic_key": champion["logic_key"],
            "challenger_logic_key": None,
            "challenger_logic_keys": [],
            "challengers_json": [],
            "default_logic_pointer": champion["logic_key"],
            "previous_stable_champion": None,
            "previous_stable_champion_logic_key": None,
            "rollback_candidates": [],
            "retired_logic_keys": [],
            "demoted_logic_keys": [],
            "promotion_history": [],
            "degraded": False,
            "registry_sync_state": "bootstrap_from_catalog",
            "sync_state": "bootstrap_from_catalog",
            "sync_message": "bootstrapped_from_catalog",
        }
    )


def _row_to_state(row: tuple[Any, ...]) -> dict[str, Any]:
    try:
        state_json = json.loads(str(row[15] or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        state_json = {}
    state = dict(state_json) if isinstance(state_json, dict) else {}
    state.update(
        {
            "schema_version": row[0],
            "registry_name": PUBLISH_REGISTRY_NAME,
            "registry_version": int(row[1] or 0),
            "source_of_truth": str(row[2] or PUBLISH_REGISTRY_SOURCE_OF_TRUTH),
            "source_revision": row[3],
            "updated_at": row[4],
            "last_sync_at": row[5],
            "champion_logic_key": row[6],
            "challenger_logic_key": row[7],
            "challengers_json": json.loads(str(row[8] or "[]")),
            "default_logic_pointer": row[9],
            "previous_stable_champion_logic_key": row[10],
            "bootstrap_rule": row[11],
            "retired_logic_keys": json.loads(str(row[12] or "[]")),
            "demoted_logic_keys": json.loads(str(row[13] or "[]")),
            "registry_checksum": row[16],
            "degraded": bool(row[17]),
            "registry_sync_state": row[18],
            "sync_state": row[18],
            "sync_message": row[18],
        }
    )
    if not isinstance(state.get("champion"), dict) and state.get("champion_logic_key"):
        state["champion"] = {
            "logic_key": _normalize_text(state.get("champion_logic_key")),
            "status": "champion",
            "role": "champion",
        }
    if not isinstance(state.get("challengers"), list) and isinstance(state.get("challengers_json"), list):
        state["challengers"] = list(state.get("challengers_json") or [])
    if state.get("challenger") is None and isinstance(state.get("challengers"), list) and state.get("challengers"):
        first_challenger = state["challengers"][0]
        if isinstance(first_challenger, dict):
            state["challenger"] = first_challenger
    if not isinstance(state.get("previous_stable_champion"), dict) and state.get("previous_stable_champion_logic_key"):
        state["previous_stable_champion"] = {
            "logic_key": _normalize_text(state.get("previous_stable_champion_logic_key")),
            "status": "previous_stable",
            "role": "previous_stable",
        }
    return _normalize_registry_state(state)


def _row_values(state: dict[str, Any]) -> list[Any]:
    payload = _normalize_registry_state(state)
    payload["registry_checksum"] = _checksum_json({k: v for k, v in payload.items() if k != "registry_checksum"})
    payload["registry_version"] = int(payload.get("registry_version") or 0)
    return [
        payload["registry_name"],
        payload["schema_version"],
        payload["registry_version"],
        payload["source_of_truth"],
        payload.get("source_revision"),
        payload.get("updated_at") or _now_iso(),
        payload.get("last_sync_at") or _now_iso(),
        payload.get("champion_logic_key"),
        payload.get("challenger_logic_key"),
        json.dumps(payload.get("challengers_json") or [], ensure_ascii=False),
        payload.get("default_logic_pointer"),
        payload.get("previous_stable_champion_logic_key"),
        payload.get("bootstrap_rule"),
        json.dumps(payload.get("retired_logic_keys") or [], ensure_ascii=False),
        json.dumps(payload.get("demoted_logic_keys") or [], ensure_ascii=False),
        json.dumps(payload, ensure_ascii=False, sort_keys=True),
        payload.get("registry_checksum"),
        bool(payload.get("degraded")),
        payload.get("registry_sync_state") or payload.get("sync_state") or "synced",
        payload.get("sync_message"),
    ]


def load_publish_registry_state(*, db_path: str | None = None) -> dict[str, Any]:
    try:
        conn = connect_result_db(db_path=db_path, read_only=True)
    except Exception:
        return _empty_state()
    try:
        row = conn.execute(
            """
            SELECT
                schema_version,
                registry_version,
                source_of_truth,
                source_revision,
                CAST(updated_at AS VARCHAR),
                CAST(last_sync_at AS VARCHAR),
                champion_logic_key,
                challenger_logic_key,
                challengers_json,
                default_logic_pointer,
                previous_stable_champion_logic_key,
                bootstrap_rule,
                retired_logic_keys,
                demoted_logic_keys,
                registry_state_json,
                registry_checksum,
                degraded,
                sync_state,
                sync_message
            FROM publish_registry_state
            WHERE registry_name = ?
            """,
            [PUBLISH_REGISTRY_NAME],
        ).fetchone()
    except Exception:
        return _bootstrap_from_catalog(db_path=db_path)
    finally:
        conn.close()
    if row:
        return _row_to_state(row)
    return _bootstrap_from_catalog(db_path=db_path)


def save_publish_registry_state(
    *,
    db_path: str | None = None,
    state: dict[str, Any],
    sync_state: str = "synced",
    degraded: bool = False,
    sync_message: str | None = None,
    source_revision: str | None = None,
) -> dict[str, Any]:
    conn = connect_result_db(db_path=db_path, read_only=False)
    try:
        ensure_result_schema(conn)
        current = load_publish_registry_state(db_path=db_path)
        next_version = max(int(current.get("registry_version") or 0), int(state.get("registry_version") or 0)) + 1
        payload = dict(state or {})
        payload["schema_version"] = PUBLISH_REGISTRY_SCHEMA_VERSION
        payload["registry_name"] = PUBLISH_REGISTRY_NAME
        payload["source_of_truth"] = PUBLISH_REGISTRY_SOURCE_OF_TRUTH
        payload["registry_version"] = next_version
        payload["source_revision"] = source_revision or payload.get("source_revision") or f"rv:{next_version}"
        payload["updated_at"] = _now_iso()
        payload["last_sync_at"] = payload["updated_at"]
        payload["degraded"] = bool(degraded)
        payload["registry_sync_state"] = sync_state
        payload["sync_state"] = sync_state
        payload["sync_message"] = sync_message
        payload = _normalize_registry_state(payload)
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO publish_registry_state (
                    registry_name, schema_version, registry_version, source_of_truth, source_revision,
                    updated_at, last_sync_at, champion_logic_key, challenger_logic_key, challengers_json,
                    default_logic_pointer, previous_stable_champion_logic_key, bootstrap_rule,
                    retired_logic_keys, demoted_logic_keys, registry_state_json, registry_checksum, degraded,
                    sync_state, sync_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                _row_values(payload),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("CHECKPOINT")
        return payload
    finally:
        conn.close()


def append_publish_registry_audit_event(
    *,
    db_path: str | None = None,
    event: dict[str, Any],
    registry_version: int | None = None,
) -> str:
    conn = connect_result_db(db_path=db_path, read_only=False)
    try:
        ensure_result_schema(conn)
        payload = dict(event or {})
        event_id = _normalize_text(payload.get("event_id")) or f"{payload.get('action') or 'event'}:{_now_iso()}"
        details_json = {
            k: v
            for k, v in payload.items()
            if k
            not in {
                "event_id",
                "action",
                "previous_logic_key",
                "new_logic_key",
                "logic_id",
                "logic_version",
                "logic_family",
                "artifact_uri",
                "artifact_checksum",
                "source",
                "reason",
                "actor",
                "created_at",
            }
        }
        conn.execute(
            """
            INSERT OR REPLACE INTO publish_registry_audit (
                event_id, registry_name, action, previous_logic_key, new_logic_key, logic_id, logic_version,
                logic_family, artifact_uri, artifact_checksum, source, reason, actor, registry_version, created_at,
                details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                event_id,
                PUBLISH_REGISTRY_NAME,
                payload.get("action"),
                payload.get("previous_logic_key"),
                payload.get("new_logic_key"),
                payload.get("logic_id"),
                payload.get("logic_version"),
                payload.get("logic_family"),
                payload.get("artifact_uri"),
                payload.get("artifact_checksum"),
                payload.get("source"),
                payload.get("reason"),
                payload.get("actor"),
                registry_version,
                payload.get("created_at") or _now_iso(),
                json.dumps(details_json, ensure_ascii=False, sort_keys=True),
            ],
        )
        conn.execute("CHECKPOINT")
        return event_id
    finally:
        conn.close()
