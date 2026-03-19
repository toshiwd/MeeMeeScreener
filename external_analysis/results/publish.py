from __future__ import annotations

import json
from typing import Any

from external_analysis.results.manifest import now_utc
from external_analysis.results.result_schema import (
    ALLOWED_FRESHNESS_STATES,
    CONTRACT_VERSION,
    POINTER_NAME_LATEST_SUCCESSFUL,
    SCHEMA_VERSION,
    connect_result_db,
    ensure_result_schema,
)
from shared.contracts.logic_artifacts import PUBLISHED_LOGIC_MANIFEST_FIELDS


def _logic_identity_key(logic_manifest: dict[str, Any] | None) -> str | None:
    if not isinstance(logic_manifest, dict):
        return None
    logic_id = str(logic_manifest.get("logic_id") or "").strip()
    logic_version = str(logic_manifest.get("logic_version") or "").strip()
    if logic_id and logic_version:
        return f"{logic_id}:{logic_version}"
    if logic_id:
        return logic_id
    return None


def _artifact_locator(logic_manifest: dict[str, Any] | None) -> str | None:
    if not isinstance(logic_manifest, dict):
        return None
    artifact_uri = str(logic_manifest.get("artifact_uri") or "").strip()
    return artifact_uri or None


def _normalize_logic_manifest(logic_manifest: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(logic_manifest, dict):
        return None
    normalized = {key: logic_manifest.get(key) for key in PUBLISHED_LOGIC_MANIFEST_FIELDS}
    if normalized.get("artifact_uri") is None:
        normalized["artifact_uri"] = _artifact_locator(logic_manifest)
    return normalized


def publish_result(
    *,
    publish_id: str,
    as_of_date: str,
    freshness_state: str = "fresh",
    pointer_name: str = POINTER_NAME_LATEST_SUCCESSFUL,
    table_row_counts: dict[str, int] | None = None,
    degrade_ready: bool = True,
    default_logic_pointer: str | None = None,
    logic_artifact_uri: str | None = None,
    logic_artifact_checksum: str | None = None,
    logic_manifest: dict[str, Any] | None = None,
    db_path: str | None = None,
) -> dict[str, Any]:
    if freshness_state not in ALLOWED_FRESHNESS_STATES:
        raise ValueError(f"unsupported freshness_state: {freshness_state}")
    row_counts = table_row_counts or {}
    ts = now_utc()
    normalized_logic_manifest = _normalize_logic_manifest(logic_manifest)
    resolved_default_logic_pointer = str(default_logic_pointer or "").strip() or None
    if not resolved_default_logic_pointer and normalized_logic_manifest:
        resolved_default_logic_pointer = _logic_identity_key(normalized_logic_manifest)
    resolved_logic_artifact_uri = str(logic_artifact_uri or "").strip() or None
    if not resolved_logic_artifact_uri and normalized_logic_manifest:
        resolved_logic_artifact_uri = _artifact_locator(normalized_logic_manifest)
    resolved_logic_artifact_checksum = str(logic_artifact_checksum or "").strip() or None
    if not resolved_logic_artifact_checksum and normalized_logic_manifest:
        resolved_logic_artifact_checksum = str(normalized_logic_manifest.get("checksum") or "").strip() or None
    resolved_logic_id = str((normalized_logic_manifest or {}).get("logic_id") or "").strip() or None
    resolved_logic_version = str((normalized_logic_manifest or {}).get("logic_version") or "").strip() or None
    resolved_logic_family = str((normalized_logic_manifest or {}).get("logic_family") or "").strip() or None
    conn = connect_result_db(db_path=db_path, read_only=False)
    try:
        ensure_result_schema(conn)
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO publish_runs (
                    publish_id, as_of_date, contract_version, schema_version, status, created_at, published_at,
                    validation_summary, row_counts
                ) VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    publish_id,
                    as_of_date,
                    CONTRACT_VERSION,
                    SCHEMA_VERSION,
                    "published",
                    ts,
                    ts,
                    json.dumps({"valid": True}, ensure_ascii=False),
                    json.dumps(row_counts, ensure_ascii=False),
                ],
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO publish_manifest (
                    publish_id, as_of_date, schema_version, contract_version, status, published_at,
                    freshness_state, degrade_ready, table_row_counts, logic_id, logic_version, logic_family,
                    default_logic_pointer, logic_artifact_uri, logic_artifact_checksum, logic_manifest_json
                ) VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    publish_id,
                    as_of_date,
                    SCHEMA_VERSION,
                    CONTRACT_VERSION,
                    "published",
                    ts,
                    freshness_state,
                    bool(degrade_ready),
                    json.dumps(row_counts, ensure_ascii=False),
                    resolved_logic_id,
                    resolved_logic_version,
                    resolved_logic_family,
                    resolved_default_logic_pointer,
                    resolved_logic_artifact_uri,
                    resolved_logic_artifact_checksum,
                    json.dumps(normalized_logic_manifest, ensure_ascii=False) if normalized_logic_manifest else None,
                ],
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO publish_pointer (
                    pointer_name, publish_id, as_of_date, published_at, schema_version, contract_version, freshness_state
                ) VALUES (?, ?, CAST(? AS DATE), ?, ?, ?, ?)
                """,
                [
                    pointer_name,
                    publish_id,
                    as_of_date,
                    ts,
                    SCHEMA_VERSION,
                    CONTRACT_VERSION,
                    freshness_state,
                ],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("CHECKPOINT")
        return {
            "ok": True,
            "publish_id": publish_id,
            "pointer_name": pointer_name,
            "as_of_date": as_of_date,
            "freshness_state": freshness_state,
            "default_logic_pointer": resolved_default_logic_pointer,
            "logic_artifact_uri": resolved_logic_artifact_uri,
            "logic_id": resolved_logic_id,
            "logic_version": resolved_logic_version,
            "logic_family": resolved_logic_family,
        }
    finally:
        conn.close()


def load_published_logic_catalog(*, db_path: str | None = None, limit: int = 32) -> dict[str, Any]:
    try:
        conn = connect_result_db(db_path=db_path, read_only=True)
    except Exception:
        return {
            "available_logic_manifest": [],
            "available_logic_keys": [],
            "default_logic_pointer": None,
        }
    try:
        try:
            conn.execute("SELECT 1 FROM publish_manifest LIMIT 1")
        except Exception:
            return {
                "available_logic_manifest": [],
                "available_logic_keys": [],
                "default_logic_pointer": None,
            }
        rows = conn.execute(
            """
            SELECT
                publish_id,
                CAST(as_of_date AS VARCHAR),
                schema_version,
                contract_version,
                status,
                published_at,
                freshness_state,
                degrade_ready,
                table_row_counts,
                logic_id,
                logic_version,
                logic_family,
                default_logic_pointer,
                logic_artifact_uri,
                logic_artifact_checksum,
                logic_manifest_json
            FROM publish_manifest
            WHERE status = 'published'
            ORDER BY published_at DESC
            LIMIT ?
            """,
            [int(limit)],
        ).fetchall()
    finally:
        conn.close()

    manifests: list[dict[str, Any]] = []
    for row in rows:
        manifest_json = row[15]
        manifest_payload: dict[str, Any] | None = None
        if manifest_json is not None:
            try:
                manifest_payload = json.loads(str(manifest_json))
            except Exception:
                manifest_payload = None
        logic_id = str(row[9] or "").strip() or None
        logic_version = str(row[10] or "").strip() or None
        logic_family = str(row[11] or "").strip() or None
        default_pointer = str(row[12] or "").strip() or None
        artifact_uri = str(row[13] or "").strip() or None
        checksum = str(row[14] or "").strip() or None
        manifest_key = (
            f"{logic_id}:{logic_version}"
            if logic_id and logic_version
            else logic_id
            or _logic_identity_key(manifest_payload)
        )
        manifests.append(
            {
                "publish_id": str(row[0]),
                "as_of_date": str(row[1]),
                "schema_version": str(row[2]),
                "contract_version": str(row[3]),
                "status": str(row[4]),
                "published_at": str(row[5]),
                "freshness_state": str(row[6]),
                "degrade_ready": bool(row[7]),
                "table_row_counts": row[8],
                "logic_id": logic_id,
                "logic_version": logic_version,
                "logic_family": logic_family,
                "default_logic_pointer": default_pointer,
                "logic_artifact_uri": artifact_uri,
                "logic_artifact_checksum": checksum,
                "logic_manifest": manifest_payload,
                "logic_key": manifest_key,
            }
        )

    default_logic_pointer = manifests[0]["default_logic_pointer"] if manifests else None
    if not default_logic_pointer and manifests:
        default_logic_pointer = manifests[0].get("logic_key")

    return {
        "available_logic_manifest": manifests,
        "available_logic_keys": [str(manifest["logic_key"]) for manifest in manifests if manifest.get("logic_key")],
        "default_logic_pointer": default_logic_pointer,
    }
