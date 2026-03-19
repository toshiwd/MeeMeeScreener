from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from external_analysis.ops.ops_schema import connect_ops_db, ensure_ops_schema
from external_analysis.results.result_schema import connect_result_db, ensure_result_schema
from shared.contracts.logic_artifacts import (
    PUBLISHED_LOGIC_ARTIFACT_FIELDS,
    PUBLISHED_LOGIC_MANIFEST_FIELDS,
    PUBLISHED_RANKING_SNAPSHOT_AUDIT_ROLE,
    VALIDATION_SUMMARY_FIELDS,
)
from shared.contracts.publish_candidates import (
    PUBLISH_CANDIDATE_STATUS_APPROVED,
    PUBLISH_CANDIDATE_STATUS_CANDIDATE,
    PUBLISH_CANDIDATE_STATUS_PROMOTED,
    PUBLISH_CANDIDATE_STATUS_REJECTED,
    PUBLISH_CANDIDATE_STATUS_RETIRED,
)

PUBLISH_CANDIDATE_BUNDLE_SCHEMA_VERSION = "publish_candidate_bundle_v1"
PUBLISH_CANDIDATE_VALIDATION_SCOPE = "publish_review"
PUBLISH_CANDIDATE_VALIDATION_DECISION_CANDIDATE = "candidate"
PUBLISH_CANDIDATE_VALIDATION_DECISION_APPROVED = "approved"
PUBLISH_CANDIDATE_VALIDATION_DECISION_REJECTED = "rejected"


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


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def _checksum_json(payload: Any) -> str:
    digest = hashlib.sha256()
    digest.update(_canonical_json(payload).encode("utf-8"))
    return digest.hexdigest()


def _resolve_artifact_path(locator: str | None) -> str | None:
    text = _normalize_text(locator)
    if not text:
        return None
    candidate = Path(text).expanduser()
    if candidate.exists():
        return str(candidate.resolve())
    return None


def _checksum_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _row_to_manifest(row: tuple[Any, ...]) -> dict[str, Any]:
    manifest_json = row[16]
    manifest_payload: dict[str, Any] | None = None
    if manifest_json is not None:
        try:
            parsed = json.loads(str(manifest_json))
            manifest_payload = parsed if isinstance(parsed, dict) else None
        except (TypeError, ValueError, json.JSONDecodeError):
            manifest_payload = None
    logic_key = _logic_key(row[9], row[10]) or _normalize_text((manifest_payload or {}).get("logic_key"))
    return {
        "publish_id": _normalize_text(row[0]),
        "as_of_date": _normalize_text(row[1]),
        "schema_version": _normalize_text(row[2]),
        "contract_version": _normalize_text(row[3]),
        "status": _normalize_text(row[4]),
        "published_at": _normalize_text(row[5]),
        "freshness_state": _normalize_text(row[6]),
        "degrade_ready": bool(row[7]),
        "table_row_counts": row[8],
        "logic_id": _normalize_text(row[9]),
        "logic_version": _normalize_text(row[10]),
        "logic_family": _normalize_text(row[11]),
        "logic_key": logic_key,
        "default_logic_pointer": _normalize_text(row[12]),
        "bootstrap_champion": bool(row[13]),
        "last_stable_promoted": bool((manifest_payload or {}).get("last_stable_promoted")),
        "artifact_uri": _normalize_text(row[14]),
        "checksum": _normalize_text(row[15]),
        "logic_manifest": manifest_payload if isinstance(manifest_payload, dict) else None,
    }


def _row_to_readiness(row: tuple[Any, ...]) -> dict[str, Any]:
    try:
        reason_codes = json.loads(str(row[11] or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        reason_codes = []
    try:
        summary = json.loads(str(row[12] or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        summary = {}
    return {
        "as_of_date": _normalize_text(row[0]),
        "champion_version": _normalize_text(row[1]),
        "challenger_version": _normalize_text(row[2]),
        "sample_count": int(row[3]),
        "expectancy_delta": row[4],
        "improved_expectancy": bool(row[5]),
        "mae_non_worse": bool(row[6]),
        "adverse_move_non_worse": bool(row[7]),
        "stable_window": bool(row[8]),
        "alignment_ok": bool(row[9]),
        "readiness_pass": bool(row[10]),
        "reason_codes": reason_codes if isinstance(reason_codes, list) else [],
        "summary": summary if isinstance(summary, dict) else {},
        "created_at": _normalize_text(row[13]),
    }


def _row_to_candidate_rows(rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for row in rows:
        payloads.append(
            {
                "publish_id": _normalize_text(row[0]),
                "as_of_date": _normalize_text(row[1]),
                "code": _normalize_text(row[2]),
                "side": _normalize_text(row[3]),
                "rank_position": int(row[4]),
                "candidate_score": row[5],
                "expected_horizon_days": row[6],
                "primary_reason_codes": row[7],
                "regime_tag": _normalize_text(row[8]),
                "freshness_state": _normalize_text(row[9]),
            }
        )
    return payloads


def _candidate_snapshot_from_rows(
    *,
    logic_id: str,
    logic_version: str,
    logic_family: str,
    rows: list[dict[str, Any]],
    as_of_date: str,
) -> dict[str, Any] | None:
    if not rows:
        return None
    return {
        "artifact_version": "published_ranking_snapshot_v1",
        "logic_id": logic_id,
        "logic_version": logic_version,
        "logic_family": logic_family,
        "as_of_date": as_of_date,
        "generated_at": _now_iso(),
        "universe_size": len({str(row.get("code") or "") for row in rows if row.get("code")}),
        "rows": rows,
        "audit_role": PUBLISHED_RANKING_SNAPSHOT_AUDIT_ROLE,
    }


def _build_logic_artifact(manifest: dict[str, Any]) -> dict[str, Any]:
    logic_manifest = manifest.get("logic_manifest") or {}
    if not isinstance(logic_manifest, dict):
        logic_manifest = {}
    required_inputs = logic_manifest.get("required_inputs")
    if not isinstance(required_inputs, list):
        required_inputs = []
    params = logic_manifest.get("params")
    if not isinstance(params, dict):
        params = {}
    thresholds = logic_manifest.get("thresholds")
    if not isinstance(thresholds, dict):
        thresholds = {}
    weights = logic_manifest.get("weights")
    if not isinstance(weights, dict):
        weights = {}
    output_spec = logic_manifest.get("output_spec")
    if not isinstance(output_spec, dict):
        output_spec = {}
    checksum = _normalize_text(manifest.get("checksum")) or _normalize_text(logic_manifest.get("checksum"))
    return {
        "artifact_version": _normalize_text(logic_manifest.get("artifact_version")) or "published_logic_artifact_v1",
        "logic_id": manifest["logic_id"],
        "logic_version": manifest["logic_version"],
        "logic_family": manifest["logic_family"],
        "feature_spec_version": _normalize_text(logic_manifest.get("feature_spec_version"))
        or _normalize_text(logic_manifest.get("input_schema_version"))
        or "unknown",
        "required_inputs": required_inputs,
        "scorer_type": _normalize_text(logic_manifest.get("scorer_type")) or "ranking",
        "params": params,
        "thresholds": thresholds,
        "weights": weights,
        "output_spec": output_spec,
        "checksum": checksum,
    }


def _build_logic_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    logic_manifest = manifest.get("logic_manifest") or {}
    if not isinstance(logic_manifest, dict):
        logic_manifest = {}
    artifact_uri = _normalize_text(manifest.get("artifact_uri"))
    checksum = _normalize_text(manifest.get("checksum"))
    if not artifact_uri or not checksum:
        raise ValueError("publish manifest is missing artifact locator or checksum")
    return {
        "logic_id": _normalize_text(manifest.get("logic_id")),
        "logic_version": _normalize_text(manifest.get("logic_version")),
        "logic_family": _normalize_text(manifest.get("logic_family")),
        "status": "candidate",
        "input_schema_version": _normalize_text(logic_manifest.get("input_schema_version")) or "unknown",
        "output_schema_version": _normalize_text(logic_manifest.get("output_schema_version")) or "unknown",
        "trained_at": _normalize_text(logic_manifest.get("trained_at")),
        "published_at": _normalize_text(manifest.get("published_at")) or _normalize_text(logic_manifest.get("published_at")),
        "artifact_uri": artifact_uri,
        "checksum": checksum,
        "bootstrap_champion": bool(manifest.get("bootstrap_champion")),
        "last_stable_promoted": bool(manifest.get("last_stable_promoted")),
    }


def _build_validation_summary(
    *,
    manifest: dict[str, Any],
    readiness: dict[str, Any],
    status: str,
) -> dict[str, Any]:
    return {
        "logic_id": _normalize_text(manifest.get("logic_id")),
        "logic_version": _normalize_text(manifest.get("logic_version")),
        "logic_family": _normalize_text(manifest.get("logic_family")),
        "evaluation_scope": PUBLISH_CANDIDATE_VALIDATION_SCOPE,
        "decision": status,
        "champion_logic_version": _normalize_text(readiness.get("champion_version")),
        "challenger_logic_version": _normalize_text(readiness.get("challenger_version")),
        "metrics": {
            "readiness_pass": bool(readiness.get("readiness_pass")),
            "sample_count": int(readiness.get("sample_count") or 0),
            "expectancy_delta": readiness.get("expectancy_delta"),
            "improved_expectancy": bool(readiness.get("improved_expectancy")),
            "mae_non_worse": bool(readiness.get("mae_non_worse")),
            "adverse_move_non_worse": bool(readiness.get("adverse_move_non_worse")),
            "stable_window": bool(readiness.get("stable_window")),
            "alignment_ok": bool(readiness.get("alignment_ok")),
        },
        "notes": [
            f"source_publish_id={_normalize_text(manifest.get('publish_id'))}",
        ],
        "created_at": _normalize_text(readiness.get("created_at")) or _now_iso(),
    }


def _validation_summary_complete(validation_summary: dict[str, Any] | None) -> tuple[bool, list[str]]:
    if not isinstance(validation_summary, dict):
        return False, ["validation_summary_missing"]
    reasons: list[str] = []
    for field in ("logic_id", "logic_version", "logic_family", "evaluation_scope", "decision", "metrics", "notes", "created_at"):
        if validation_summary.get(field) is None:
            reasons.append(f"missing_{field}")
    metrics = validation_summary.get("metrics")
    if not isinstance(metrics, dict):
        reasons.append("missing_metrics")
        metrics = {}
    for field in (
        "readiness_pass",
        "sample_count",
        "expectancy_delta",
        "improved_expectancy",
        "mae_non_worse",
        "adverse_move_non_worse",
        "stable_window",
        "alignment_ok",
    ):
        if field not in metrics:
            reasons.append(f"missing_metric_{field}")
    return len(reasons) == 0, reasons


def _validate_bundle(bundle: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    candidate_id = _normalize_text(bundle.get("candidate_id"))
    logic_key = _normalize_text(bundle.get("logic_key"))
    logic_id = _normalize_text(bundle.get("logic_id"))
    logic_version = _normalize_text(bundle.get("logic_version"))
    logic_family = _normalize_text(bundle.get("logic_family"))
    artifact = bundle.get("published_logic_artifact")
    manifest = bundle.get("published_logic_manifest")
    validation_summary = bundle.get("validation_summary")
    snapshot = bundle.get("published_ranking_snapshot")

    if not candidate_id:
        reasons.append("missing_candidate_id")
    if not logic_key:
        reasons.append("missing_logic_key")
    if candidate_id and logic_key and candidate_id != logic_key:
        reasons.append("candidate_id_mismatch")
    if not logic_id or not logic_version or not logic_family:
        reasons.append("missing_logic_identity")
    if not isinstance(artifact, dict):
        reasons.append("missing_logic_artifact")
        artifact = {}
    if not isinstance(manifest, dict):
        reasons.append("missing_logic_manifest")
        manifest = {}
    if not isinstance(validation_summary, dict):
        reasons.append("missing_validation_summary")
        validation_summary = {}
    else:
        summary_ok, summary_reasons = _validation_summary_complete(validation_summary)
        if not summary_ok:
            reasons.extend(summary_reasons)

    if artifact:
        for field in PUBLISHED_LOGIC_ARTIFACT_FIELDS:
            if field not in artifact:
                reasons.append(f"missing_artifact_field_{field}")
        if _normalize_text(artifact.get("logic_id")) != logic_id:
            reasons.append("artifact_logic_id_mismatch")
        if _normalize_text(artifact.get("logic_version")) != logic_version:
            reasons.append("artifact_logic_version_mismatch")
        if _normalize_text(artifact.get("logic_family")) != logic_family:
            reasons.append("artifact_logic_family_mismatch")

    if manifest:
        for field in PUBLISHED_LOGIC_MANIFEST_FIELDS:
            if field not in manifest:
                reasons.append(f"missing_manifest_field_{field}")
        if _normalize_text(manifest.get("logic_id")) != logic_id:
            reasons.append("manifest_logic_id_mismatch")
        if _normalize_text(manifest.get("logic_version")) != logic_version:
            reasons.append("manifest_logic_version_mismatch")
        if _normalize_text(manifest.get("logic_family")) != logic_family:
            reasons.append("manifest_logic_family_mismatch")
        artifact_uri = _normalize_text(manifest.get("artifact_uri"))
        checksum = _normalize_text(manifest.get("checksum"))
        if not artifact_uri:
            reasons.append("missing_artifact_uri")
        if not checksum:
            reasons.append("missing_artifact_checksum")
        artifact_path = _resolve_artifact_path(artifact_uri)
        if not artifact_path:
            reasons.append("artifact_missing")
        elif checksum and _checksum_file(artifact_path).lower() != checksum.lower():
            reasons.append("checksum_mismatch")

    if snapshot is not None and not isinstance(snapshot, dict):
        reasons.append("ranking_snapshot_invalid")
    if isinstance(snapshot, dict):
        for field in ("artifact_version", "logic_id", "logic_version", "logic_family", "as_of_date", "generated_at", "universe_size", "rows", "audit_role"):
            if field not in snapshot:
                reasons.append(f"missing_snapshot_field_{field}")

    return len(reasons) == 0, reasons


def _load_manifest_entry(
    conn,
    *,
    logic_key: str | None = None,
    publish_id: str | None = None,
) -> dict[str, Any] | None:
    if publish_id:
        row = conn.execute(
            """
            SELECT
                publish_id, CAST(as_of_date AS VARCHAR), schema_version, contract_version, status, published_at,
                freshness_state, degrade_ready, table_row_counts, logic_id, logic_version, logic_family,
                default_logic_pointer, bootstrap_champion, logic_artifact_uri, logic_artifact_checksum, logic_manifest_json
            FROM publish_manifest
            WHERE publish_id = ?
            """,
            [publish_id],
        ).fetchone()
        return _row_to_manifest(row) if row else None
    if logic_key:
        logic_id, logic_version = logic_key.split(":", 1) if ":" in logic_key else (logic_key, None)
        if logic_version:
            row = conn.execute(
                """
                SELECT
                    publish_id, CAST(as_of_date AS VARCHAR), schema_version, contract_version, status, published_at,
                    freshness_state, degrade_ready, table_row_counts, logic_id, logic_version, logic_family,
                    default_logic_pointer, bootstrap_champion, logic_artifact_uri, logic_artifact_checksum, logic_manifest_json
                FROM publish_manifest
                WHERE logic_id = ? AND logic_version = ?
                ORDER BY published_at DESC
                LIMIT 1
                """,
                [logic_id, logic_version],
            ).fetchone()
            return _row_to_manifest(row) if row else None
    return None


def _load_readiness_entry(
    *,
    publish_id: str,
    ops_db_path: str | None = None,
    db_path: str | None = None,
    result_conn: Any | None = None,
) -> dict[str, Any] | None:
    if ops_db_path:
        conn = connect_ops_db(ops_db_path)
        try:
            ensure_ops_schema(conn)
            row = conn.execute(
                """
                SELECT
                    CAST(as_of_date AS VARCHAR),
                    champion_version,
                    challenger_version,
                    sample_count,
                    expectancy_delta,
                    improved_expectancy,
                    mae_non_worse,
                    adverse_move_non_worse,
                    stable_window,
                    alignment_ok,
                    readiness_pass,
                    reason_codes,
                    summary_json,
                    created_at
                FROM external_state_eval_readiness
                WHERE publish_id = ?
                """,
                [publish_id],
            ).fetchone()
            if row:
                return _row_to_readiness(row)
        finally:
            conn.close()
    if result_conn is not None:
        try:
            row = result_conn.execute(
                """
                SELECT
                    CAST(as_of_date AS VARCHAR),
                    champion_version,
                    challenger_version,
                    sample_count,
                    expectancy_delta,
                    improved_expectancy,
                    mae_non_worse,
                    adverse_move_non_worse,
                    stable_window,
                    alignment_ok,
                    readiness_pass,
                    reason_codes,
                    summary_json,
                    created_at
                FROM external_state_eval_readiness
                WHERE publish_id = ?
                """,
                [publish_id],
            ).fetchone()
        except Exception:
            row = None
        if row:
            return _row_to_readiness(row)
    elif db_path:
        conn = connect_result_db(db_path=db_path, read_only=True)
        try:
            row = conn.execute(
                """
                SELECT
                    CAST(as_of_date AS VARCHAR),
                    champion_version,
                    challenger_version,
                    sample_count,
                    expectancy_delta,
                    improved_expectancy,
                    mae_non_worse,
                    adverse_move_non_worse,
                    stable_window,
                    alignment_ok,
                    readiness_pass,
                    reason_codes,
                    summary_json,
                    created_at
                FROM external_state_eval_readiness
                WHERE publish_id = ?
                """,
                [publish_id],
            ).fetchone()
        finally:
            conn.close()
        if row:
            return _row_to_readiness(row)
    return None


def _load_candidate_rows(conn, *, publish_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            publish_id, CAST(as_of_date AS VARCHAR), code, side, rank_position, candidate_score,
            expected_horizon_days, primary_reason_codes, regime_tag, freshness_state
        FROM candidate_daily
        WHERE publish_id = ?
        ORDER BY side, rank_position, code
        """,
        [publish_id],
    ).fetchall()
    return _row_to_candidate_rows(rows)


def _row_to_bundle(row: tuple[Any, ...]) -> dict[str, Any]:
    artifact = json.loads(str(row[15] or "{}"))
    manifest = json.loads(str(row[16] or "{}"))
    validation_summary = json.loads(str(row[17] or "{}"))
    ranking_snapshot = row[18]
    notes = json.loads(str(row[20] or "[]"))
    metadata = json.loads(str(row[21] or "{}"))
    return {
        "candidate_id": _normalize_text(row[0]),
        "logic_key": _normalize_text(row[1]),
        "logic_id": _normalize_text(row[2]),
        "logic_version": _normalize_text(row[3]),
        "logic_family": _normalize_text(row[4]),
        "source_publish_id": _normalize_text(row[5]),
        "bundle_schema_version": _normalize_text(row[6]) or PUBLISH_CANDIDATE_BUNDLE_SCHEMA_VERSION,
        "status": _normalize_text(row[7]) or PUBLISH_CANDIDATE_STATUS_CANDIDATE,
        "validation_state": _normalize_text(row[8]) or "unknown",
        "created_at": _normalize_text(row[9]),
        "updated_at": _normalize_text(row[10]),
        "approved_at": _normalize_text(row[11]),
        "rejected_at": _normalize_text(row[12]),
        "promoted_at": _normalize_text(row[13]),
        "retired_at": _normalize_text(row[14]),
        "published_logic_artifact": artifact if isinstance(artifact, dict) else {},
        "published_logic_manifest": manifest if isinstance(manifest, dict) else {},
        "validation_summary": validation_summary if isinstance(validation_summary, dict) else {},
        "published_ranking_snapshot": json.loads(str(ranking_snapshot)) if ranking_snapshot is not None else None,
        "bundle_checksum": _normalize_text(row[19]),
        "notes": notes if isinstance(notes, list) else [],
        "metadata": metadata if isinstance(metadata, dict) else {},
    }


def load_publish_candidate_bundle(
    *,
    db_path: str | None = None,
    logic_key: str | None = None,
    candidate_id: str | None = None,
    result_conn: Any | None = None,
) -> dict[str, Any] | None:
    resolved_key = _normalize_text(candidate_id) or _normalize_text(logic_key)
    if not resolved_key:
        return None
    if result_conn is not None:
        conn = result_conn
        row = conn.execute(
            """
            SELECT
                candidate_id, logic_key, logic_id, logic_version, logic_family, source_publish_id,
                bundle_schema_version, candidate_status, validation_state, created_at, updated_at,
                approved_at, rejected_at, promoted_at, retired_at, published_logic_artifact,
                published_logic_manifest, validation_summary, published_ranking_snapshot, bundle_checksum,
                notes, metadata
            FROM publish_candidate_bundle
            WHERE candidate_id = ? OR logic_key = ?
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 1
            """,
            [resolved_key, resolved_key],
        ).fetchone()
        return _row_to_bundle(row) if row else None
    conn = connect_result_db(db_path=db_path, read_only=True)
    try:
        row = conn.execute(
            """
            SELECT
                candidate_id, logic_key, logic_id, logic_version, logic_family, source_publish_id,
                bundle_schema_version, candidate_status, validation_state, created_at, updated_at,
                approved_at, rejected_at, promoted_at, retired_at, published_logic_artifact,
                published_logic_manifest, validation_summary, published_ranking_snapshot, bundle_checksum,
                notes, metadata
            FROM publish_candidate_bundle
            WHERE candidate_id = ? OR logic_key = ?
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 1
            """,
            [resolved_key, resolved_key],
        ).fetchone()
        return _row_to_bundle(row) if row else None
    finally:
        conn.close()


def list_publish_candidate_bundles(*, db_path: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
    conn = connect_result_db(db_path=db_path, read_only=True)
    try:
        query = """
            SELECT
                candidate_id, logic_key, logic_id, logic_version, logic_family, source_publish_id,
                bundle_schema_version, candidate_status, validation_state, created_at, updated_at,
                approved_at, rejected_at, promoted_at, retired_at, published_logic_artifact,
                published_logic_manifest, validation_summary, published_ranking_snapshot, bundle_checksum,
                notes, metadata
            FROM publish_candidate_bundle
        """
        params: list[Any] = []
        if status:
            query += " WHERE candidate_status = ?"
            params.append(status)
        query += " ORDER BY updated_at DESC, created_at DESC, logic_key ASC"
        rows = conn.execute(query, params).fetchall()
        return [_row_to_bundle(row) for row in rows]
    finally:
        conn.close()


def upsert_publish_candidate_bundle(
    *,
    db_path: str | None = None,
    bundle: dict[str, Any],
) -> dict[str, Any]:
    conn = connect_result_db(db_path=db_path, read_only=False)
    try:
        ensure_result_schema(conn)
        existing = load_publish_candidate_bundle(
            db_path=db_path,
            logic_key=_normalize_text(bundle.get("logic_key")),
            result_conn=conn,
        )
        existing_status = _normalize_text(existing.get("status")) if existing else None
        payload = dict(bundle)
        payload["candidate_id"] = _normalize_text(payload.get("candidate_id")) or _normalize_text(payload.get("logic_key"))
        payload["logic_key"] = _normalize_text(payload.get("logic_key")) or payload["candidate_id"]
        payload["bundle_schema_version"] = _normalize_text(payload.get("bundle_schema_version")) or PUBLISH_CANDIDATE_BUNDLE_SCHEMA_VERSION
        incoming_status = _normalize_text(payload.get("status")) or PUBLISH_CANDIDATE_STATUS_CANDIDATE
        payload["status"] = incoming_status
        if existing_status in {
            PUBLISH_CANDIDATE_STATUS_APPROVED,
            PUBLISH_CANDIDATE_STATUS_PROMOTED,
            PUBLISH_CANDIDATE_STATUS_REJECTED,
            PUBLISH_CANDIDATE_STATUS_RETIRED,
        } and incoming_status == PUBLISH_CANDIDATE_STATUS_CANDIDATE:
            payload["status"] = existing_status
        payload["validation_state"] = _normalize_text(payload.get("validation_state")) or "ok"
        payload["created_at"] = _normalize_text(payload.get("created_at")) or _now_iso()
        payload["updated_at"] = _now_iso()
        payload["approved_at"] = _normalize_text(payload.get("approved_at")) or (existing or {}).get("approved_at")
        payload["rejected_at"] = _normalize_text(payload.get("rejected_at")) or (existing or {}).get("rejected_at")
        payload["promoted_at"] = _normalize_text(payload.get("promoted_at")) or (existing or {}).get("promoted_at")
        payload["retired_at"] = _normalize_text(payload.get("retired_at")) or (existing or {}).get("retired_at")
        if existing_status == PUBLISH_CANDIDATE_STATUS_APPROVED:
            payload["validation_summary"] = (existing or {}).get("validation_summary") or payload.get("validation_summary")
            payload["approved_at"] = (existing or {}).get("approved_at") or payload.get("approved_at")
        elif existing_status == PUBLISH_CANDIDATE_STATUS_REJECTED:
            payload["validation_summary"] = (existing or {}).get("validation_summary") or payload.get("validation_summary")
            payload["rejected_at"] = (existing or {}).get("rejected_at") or payload.get("rejected_at")
        elif existing_status == PUBLISH_CANDIDATE_STATUS_PROMOTED:
            payload["validation_summary"] = (existing or {}).get("validation_summary") or payload.get("validation_summary")
            payload["promoted_at"] = (existing or {}).get("promoted_at") or payload.get("promoted_at")
        elif existing_status == PUBLISH_CANDIDATE_STATUS_RETIRED:
            payload["validation_summary"] = (existing or {}).get("validation_summary") or payload.get("validation_summary")
            payload["retired_at"] = (existing or {}).get("retired_at") or payload.get("retired_at")
        payload["notes"] = list(payload.get("notes") or (existing or {}).get("notes") or [])
        payload["metadata"] = dict(payload.get("metadata") or (existing or {}).get("metadata") or {})
        checksum_payload = {
            key: payload.get(key)
            for key in (
                "candidate_id",
                "logic_key",
                "logic_id",
                "logic_version",
                "logic_family",
                "source_publish_id",
                "bundle_schema_version",
                "status",
                "validation_state",
                "published_logic_artifact",
                "published_logic_manifest",
                "validation_summary",
                "published_ranking_snapshot",
                "notes",
                "metadata",
            )
        }
        payload["bundle_checksum"] = _checksum_json(checksum_payload)
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO publish_candidate_bundle (
                    candidate_id, logic_key, logic_id, logic_version, logic_family, source_publish_id,
                    bundle_schema_version, candidate_status, validation_state, created_at, updated_at,
                    approved_at, rejected_at, promoted_at, retired_at, published_logic_artifact,
                    published_logic_manifest, validation_summary, published_ranking_snapshot, bundle_checksum,
                    notes, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    payload["candidate_id"],
                    payload["logic_key"],
                    payload.get("logic_id"),
                    payload.get("logic_version"),
                    payload.get("logic_family"),
                    payload.get("source_publish_id"),
                    payload["bundle_schema_version"],
                    payload["status"],
                    payload["validation_state"],
                    payload["created_at"],
                    payload["updated_at"],
                    payload.get("approved_at"),
                    payload.get("rejected_at"),
                    payload.get("promoted_at"),
                    payload.get("retired_at"),
                    _canonical_json(payload["published_logic_artifact"]),
                    _canonical_json(payload["published_logic_manifest"]),
                    _canonical_json(payload["validation_summary"]),
                    _canonical_json(payload["published_ranking_snapshot"]) if payload.get("published_ranking_snapshot") is not None else None,
                    payload["bundle_checksum"],
                    _canonical_json(payload["notes"]),
                    _canonical_json(payload["metadata"]),
                ],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        conn.execute("CHECKPOINT")
        saved = load_publish_candidate_bundle(
            db_path=db_path,
            logic_key=payload["logic_key"],
            result_conn=conn,
        )
        return saved or payload
    finally:
        conn.close()


def build_publish_candidate_bundle(
    *,
    db_path: str | None = None,
    ops_db_path: str | None = None,
    logic_key: str | None = None,
    publish_id: str | None = None,
) -> dict[str, Any]:
    conn = connect_result_db(db_path=db_path, read_only=False)
    try:
        ensure_result_schema(conn)
        manifest = _load_manifest_entry(conn, logic_key=logic_key, publish_id=publish_id)
        if manifest is None:
            return {"ok": False, "reason": "publish_manifest_not_found", "logic_key": logic_key, "publish_id": publish_id}
        publish_id_value = _normalize_text(publish_id) or _normalize_text(manifest.get("publish_id"))
        readiness = _load_readiness_entry(
            publish_id=publish_id_value or "",
            ops_db_path=ops_db_path,
            db_path=db_path,
            result_conn=conn,
        )
        if readiness is None:
            return {"ok": False, "reason": "validation_summary_missing", "logic_key": manifest.get("logic_key"), "publish_id": publish_id_value}

        artifact = _build_logic_artifact(manifest)
        published_manifest = _build_logic_manifest(manifest)
        validation_summary = _build_validation_summary(manifest=manifest, readiness=readiness, status=PUBLISH_CANDIDATE_VALIDATION_DECISION_CANDIDATE)
        candidate_rows = _load_candidate_rows(conn, publish_id=publish_id_value or "")
        ranking_snapshot = _candidate_snapshot_from_rows(
            logic_id=_normalize_text(manifest.get("logic_id")) or "",
            logic_version=_normalize_text(manifest.get("logic_version")) or "",
            logic_family=_normalize_text(manifest.get("logic_family")) or "",
            rows=candidate_rows,
            as_of_date=_normalize_text(manifest.get("as_of_date")) or "",
        )
        bundle = {
            "candidate_id": _logic_key(manifest.get("logic_id"), manifest.get("logic_version")),
            "logic_key": _logic_key(manifest.get("logic_id"), manifest.get("logic_version")),
            "logic_id": _normalize_text(manifest.get("logic_id")),
            "logic_version": _normalize_text(manifest.get("logic_version")),
            "logic_family": _normalize_text(manifest.get("logic_family")),
            "created_at": _normalize_text(readiness.get("created_at")) or _now_iso(),
            "updated_at": _now_iso(),
            "status": PUBLISH_CANDIDATE_STATUS_CANDIDATE,
            "source_publish_id": publish_id_value,
            "bundle_schema_version": PUBLISH_CANDIDATE_BUNDLE_SCHEMA_VERSION,
            "published_logic_artifact": artifact,
            "published_logic_manifest": published_manifest,
            "validation_summary": validation_summary,
            "published_ranking_snapshot": ranking_snapshot,
            "notes": [f"source_publish_id={publish_id_value}"],
            "metadata": {
                "validation_source": "external_state_eval_readiness",
                "ops_db_path_present": bool(ops_db_path),
            },
        }
        ok, reasons = _validate_bundle(bundle)
        if not ok:
            return {"ok": False, "reason": "candidate_bundle_invalid", "validation_issues": reasons, "bundle": bundle}
        saved = upsert_publish_candidate_bundle(db_path=db_path, bundle=bundle)
        return {"ok": True, "bundle": saved}
    finally:
        conn.close()


def _append_candidate_audit_event(
    *,
    db_path: str | None = None,
    event: dict[str, Any],
) -> None:
    conn = connect_result_db(db_path=db_path, read_only=False)
    try:
        ensure_result_schema(conn)
        details_json = event.get("details_json")
        if not isinstance(details_json, dict):
            details_json = {}
        conn.execute(
            """
            INSERT OR REPLACE INTO publish_candidate_audit (
                event_id, candidate_id, logic_key, action, previous_status, new_status, source, reason,
                actor, queue_order_before, queue_order_after, changed_at, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                _normalize_text(event.get("event_id")) or f"{event.get('candidate_id')}:{_now_iso()}",
                _normalize_text(event.get("candidate_id")),
                _normalize_text(event.get("logic_key")),
                _normalize_text(event.get("action")),
                _normalize_text(event.get("previous_status")),
                _normalize_text(event.get("new_status")),
                _normalize_text(event.get("source")) or "external_analysis",
                _normalize_text(event.get("reason")),
                _normalize_text(event.get("actor")),
                event.get("queue_order_before"),
                event.get("queue_order_after"),
                _normalize_text(event.get("changed_at")) or _now_iso(),
                _canonical_json(details_json),
            ],
        )
    finally:
        conn.close()


def _update_candidate_status(
    *,
    db_path: str | None = None,
    logic_key: str,
    new_status: str,
    source: str,
    reason: str | None = None,
    actor: str | None = None,
) -> dict[str, Any]:
    current = load_publish_candidate_bundle(db_path=db_path, logic_key=logic_key)
    if current is None:
        return {"ok": False, "reason": "candidate_bundle_not_found", "logic_key": logic_key}
    previous_status = _normalize_text(current.get("status")) or PUBLISH_CANDIDATE_STATUS_CANDIDATE
    if previous_status == new_status:
        return {"ok": True, "changed": False, "bundle": current}
    if new_status == PUBLISH_CANDIDATE_STATUS_APPROVED and previous_status not in {
        PUBLISH_CANDIDATE_STATUS_CANDIDATE,
        PUBLISH_CANDIDATE_STATUS_APPROVED,
    }:
        return {"ok": False, "reason": "candidate_not_approvable", "logic_key": logic_key}
    if previous_status in {PUBLISH_CANDIDATE_STATUS_REJECTED, PUBLISH_CANDIDATE_STATUS_RETIRED} and new_status == PUBLISH_CANDIDATE_STATUS_APPROVED:
        return {"ok": False, "reason": "candidate_not_revivable", "logic_key": logic_key}

    validation_ok, validation_issues = _validate_bundle(current)
    if not validation_ok and new_status in {PUBLISH_CANDIDATE_STATUS_APPROVED, PUBLISH_CANDIDATE_STATUS_PROMOTED}:
        return {"ok": False, "reason": "candidate_bundle_invalid", "logic_key": logic_key, "validation_issues": validation_issues}
    if new_status == PUBLISH_CANDIDATE_STATUS_APPROVED and current.get("validation_summary", {}).get("decision") not in {
        PUBLISH_CANDIDATE_VALIDATION_DECISION_CANDIDATE,
        PUBLISH_CANDIDATE_VALIDATION_DECISION_APPROVED,
    }:
        return {"ok": False, "reason": "candidate_validation_invalid", "logic_key": logic_key}

    updated = dict(current)
    updated["status"] = new_status
    updated["updated_at"] = _now_iso()
    if new_status == PUBLISH_CANDIDATE_STATUS_APPROVED:
        updated["approved_at"] = _now_iso()
        validation_summary = dict(current.get("validation_summary") or {})
        validation_summary["decision"] = PUBLISH_CANDIDATE_VALIDATION_DECISION_APPROVED
        updated["validation_summary"] = validation_summary
        updated["validation_state"] = "approved"
    elif new_status == PUBLISH_CANDIDATE_STATUS_REJECTED:
        updated["rejected_at"] = _now_iso()
        validation_summary = dict(current.get("validation_summary") or {})
        validation_summary["decision"] = PUBLISH_CANDIDATE_VALIDATION_DECISION_REJECTED
        updated["validation_summary"] = validation_summary
        updated["validation_state"] = "rejected"
    elif new_status == PUBLISH_CANDIDATE_STATUS_PROMOTED:
        updated["promoted_at"] = _now_iso()
        updated["validation_state"] = "promoted"
    elif new_status == PUBLISH_CANDIDATE_STATUS_RETIRED:
        updated["retired_at"] = _now_iso()
        updated["validation_state"] = "retired"

    saved = upsert_publish_candidate_bundle(db_path=db_path, bundle=updated)
    _append_candidate_audit_event(
        db_path=db_path,
        event={
            "candidate_id": saved.get("candidate_id") or logic_key,
            "logic_key": logic_key,
            "action": new_status,
            "previous_status": previous_status,
            "new_status": new_status,
            "source": source,
            "reason": reason,
            "actor": actor,
            "queue_order_before": None,
            "queue_order_after": None,
            "changed_at": _now_iso(),
            "details_json": {"validation_state": saved.get("validation_state"), "validation_issues": validation_issues},
        },
    )
    return {"ok": True, "changed": True, "bundle": saved}


def approve_publish_candidate_bundle(
    *,
    db_path: str | None = None,
    logic_key: str,
    source: str,
    reason: str | None = None,
    actor: str | None = None,
) -> dict[str, Any]:
    return _update_candidate_status(db_path=db_path, logic_key=logic_key, new_status=PUBLISH_CANDIDATE_STATUS_APPROVED, source=source, reason=reason, actor=actor)


def reject_publish_candidate_bundle(
    *,
    db_path: str | None = None,
    logic_key: str,
    source: str,
    reason: str | None = None,
    actor: str | None = None,
) -> dict[str, Any]:
    return _update_candidate_status(db_path=db_path, logic_key=logic_key, new_status=PUBLISH_CANDIDATE_STATUS_REJECTED, source=source, reason=reason, actor=actor)


def promote_publish_candidate_bundle(
    *,
    db_path: str | None = None,
    logic_key: str,
    source: str,
    reason: str | None = None,
    actor: str | None = None,
) -> dict[str, Any]:
    return _update_candidate_status(db_path=db_path, logic_key=logic_key, new_status=PUBLISH_CANDIDATE_STATUS_PROMOTED, source=source, reason=reason, actor=actor)


def retire_publish_candidate_bundle(
    *,
    db_path: str | None = None,
    logic_key: str,
    source: str,
    reason: str | None = None,
    actor: str | None = None,
) -> dict[str, Any]:
    return _update_candidate_status(db_path=db_path, logic_key=logic_key, new_status=PUBLISH_CANDIDATE_STATUS_RETIRED, source=source, reason=reason, actor=actor)
