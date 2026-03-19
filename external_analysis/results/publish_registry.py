from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from external_analysis.results.publish import load_published_logic_catalog
from external_analysis.results.result_schema import connect_result_db, ensure_result_schema

PUBLISH_REGISTRY_SCHEMA_VERSION = "publish_registry_v1"
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


def _empty_state() -> dict[str, Any]:
    return {
        "schema_version": PUBLISH_REGISTRY_SCHEMA_VERSION,
        "registry_name": PUBLISH_REGISTRY_NAME,
        "source_of_truth": PUBLISH_REGISTRY_SOURCE_OF_TRUTH,
        "registry_version": 0,
        "source_revision": None,
        "updated_at": None,
        "last_sync_at": None,
        "champion": None,
        "challenger": None,
        "champion_logic_key": None,
        "challenger_logic_key": None,
        "default_logic_pointer": None,
        "retired_logic_keys": [],
        "demoted_logic_keys": [],
        "promotion_history": [],
        "registry_checksum": None,
        "degraded": True,
        "registry_sync_state": "empty",
        "sync_state": "empty",
        "sync_message": "no_registry_state",
    }


def _row_to_state(row: tuple[Any, ...]) -> dict[str, Any]:
    try:
        state_json = json.loads(str(row[11] or "{}"))
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
            "default_logic_pointer": row[8],
            "retired_logic_keys": json.loads(str(row[9] or "[]")),
            "demoted_logic_keys": json.loads(str(row[10] or "[]")),
            "registry_checksum": row[12],
            "degraded": bool(row[13]),
            "registry_sync_state": row[14],
            "sync_state": row[14],
            "sync_message": row[15],
        }
    )
    if state.get("champion") is None and state.get("champion_logic_key"):
        state["champion"] = {"logic_key": state.get("champion_logic_key"), "status": "champion", "role": "champion"}
    if state.get("challenger") is None and state.get("challenger_logic_key"):
        state["challenger"] = {"logic_key": state.get("challenger_logic_key"), "status": "challenger", "role": "challenger"}
    return state


def _bootstrap_from_catalog(*, db_path: str | None = None) -> dict[str, Any]:
    catalog = load_published_logic_catalog(db_path=db_path)
    manifests = list(catalog.get("available_logic_manifest") or [])
    chosen: dict[str, Any] | None = manifests[-1] if manifests else None
    if chosen is None:
        return _empty_state()
    return {
        "schema_version": PUBLISH_REGISTRY_SCHEMA_VERSION,
        "registry_name": PUBLISH_REGISTRY_NAME,
        "source_of_truth": PUBLISH_REGISTRY_SOURCE_OF_TRUTH,
        "registry_version": 0,
        "source_revision": _normalize_text(chosen.get("publish_id")) or _normalize_text(chosen.get("logic_key")),
        "updated_at": _normalize_text(chosen.get("published_at")) or _now_iso(),
        "last_sync_at": _normalize_text(chosen.get("published_at")) or _now_iso(),
        "champion": {
            "logic_id": _normalize_text(chosen.get("logic_id")),
            "logic_version": _normalize_text(chosen.get("logic_version")),
            "logic_key": _normalize_text(chosen.get("logic_key")),
            "logic_family": _normalize_text(chosen.get("logic_family")),
            "artifact_uri": _normalize_text(chosen.get("logic_artifact_uri") or chosen.get("artifact_uri")),
            "artifact_checksum": _normalize_text(chosen.get("logic_artifact_checksum") or chosen.get("checksum")),
            "published_at": _normalize_text(chosen.get("published_at")),
            "status": "champion",
            "role": "champion",
            "source_publish_id": _normalize_text(chosen.get("publish_id")),
        },
        "challenger": None,
        "champion_logic_key": _normalize_text(chosen.get("logic_key")),
        "challenger_logic_key": None,
        "default_logic_pointer": _normalize_text(chosen.get("default_logic_pointer")) or _normalize_text(chosen.get("logic_key")),
        "retired_logic_keys": [],
        "demoted_logic_keys": [],
        "promotion_history": [],
        "degraded": False,
        "registry_sync_state": "bootstrap_from_catalog",
        "sync_state": "bootstrap_from_catalog",
        "sync_message": "bootstrapped_from_catalog",
    }


def _row_values(state: dict[str, Any]) -> list[Any]:
    payload = dict(state)
    payload["schema_version"] = PUBLISH_REGISTRY_SCHEMA_VERSION
    payload["registry_name"] = PUBLISH_REGISTRY_NAME
    payload["source_of_truth"] = PUBLISH_REGISTRY_SOURCE_OF_TRUTH
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
        payload.get("default_logic_pointer"),
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
                default_logic_pointer,
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
        payload["registry_checksum"] = _checksum_json({k: v for k, v in payload.items() if k != "registry_checksum"})
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO publish_registry_state (
                    registry_name, schema_version, registry_version, source_of_truth, source_revision,
                    updated_at, last_sync_at, champion_logic_key, challenger_logic_key, default_logic_pointer,
                    retired_logic_keys, demoted_logic_keys, registry_state_json, registry_checksum, degraded,
                    sync_state, sync_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
