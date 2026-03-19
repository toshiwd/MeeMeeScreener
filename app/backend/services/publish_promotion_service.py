from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.backend.infra.files.config_repo import (
    ConfigRepository,
    PUBLISH_REGISTRY_SCHEMA_VERSION,
)
from external_analysis.ops.ops_schema import connect_ops_db, ensure_ops_schema
from external_analysis.results.publish import load_published_logic_catalog
from shared.contracts.publish_registry import (
    PUBLISH_PROMOTION_ACTION_DEMOTE,
    PUBLISH_PROMOTION_ACTION_PROMOTE,
    PUBLISH_PROMOTION_ACTION_ROLLBACK,
    PUBLISH_ROLE_CHAMPION,
)

_VALIDATION_OK = "ok"
_MIN_SAMPLE_COUNT = 20


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


def _catalog_lookup(*, db_path: str | None, logic_key: str) -> dict[str, Any] | None:
    catalog = load_published_logic_catalog(db_path=db_path)
    for entry in catalog.get("available_logic_manifest") or []:
        if _normalize_text(entry.get("logic_key")) == logic_key:
            return entry
    return None


def _resolve_artifact_path(artifact_uri: str | None) -> str | None:
    text = _normalize_text(artifact_uri)
    if not text:
        return None
    candidate = Path(text).expanduser()
    if candidate.exists():
        return str(candidate.resolve())
    return None


def _checksum_file(path: str) -> str:
    import hashlib

    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_review_for_publish_id(*, publish_id: str, ops_db_path: str | None = None) -> dict[str, Any] | None:
    conn = connect_ops_db(ops_db_path)
    try:
        ensure_ops_schema(conn)
        readiness_row = conn.execute(
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
                summary_json
            FROM external_state_eval_readiness
            WHERE publish_id = ?
            """,
            [publish_id],
        ).fetchone()
        decision_row = conn.execute(
            """
            SELECT decision_id, decision, note, actor, CAST(created_at AS VARCHAR), summary_json
            FROM external_promotion_decisions
            WHERE publish_id = ?
            ORDER BY created_at DESC, decision_id DESC
            LIMIT 1
            """,
            [publish_id],
        ).fetchone()
    finally:
        conn.close()

    if not readiness_row:
        return None

    try:
        reason_codes = json.loads(str(readiness_row[11] or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        reason_codes = []
    try:
        summary = json.loads(str(readiness_row[12] or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        summary = {}

    approval_decision = None
    if decision_row:
        try:
            decision_summary = json.loads(str(decision_row[5] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            decision_summary = {}
        approval_decision = {
            "decision_id": str(decision_row[0]),
            "decision": str(decision_row[1]),
            "note": None if decision_row[2] is None else str(decision_row[2]),
            "actor": None if decision_row[3] is None else str(decision_row[3]),
            "created_at": str(decision_row[4]),
            "summary": decision_summary,
        }

    return {
        "as_of_date": str(readiness_row[0]),
        "champion_version": str(readiness_row[1]),
        "challenger_version": str(readiness_row[2]),
        "sample_count": int(readiness_row[3]),
        "expectancy_delta": readiness_row[4],
        "improved_expectancy": bool(readiness_row[5]),
        "mae_non_worse": bool(readiness_row[6]),
        "adverse_move_non_worse": bool(readiness_row[7]),
        "stable_window": bool(readiness_row[8]),
        "alignment_ok": bool(readiness_row[9]),
        "readiness_pass": bool(readiness_row[10]),
        "reason_codes": reason_codes if isinstance(reason_codes, list) else [],
        "summary": summary,
        "approval_decision": approval_decision,
    }


def _promotion_gate(review: dict[str, Any] | None) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if not review:
        return False, ["promotion_review_missing"]

    if not review.get("readiness_pass"):
        reasons.append("readiness_pass_false")
    if int(review.get("sample_count") or 0) < _MIN_SAMPLE_COUNT:
        reasons.append("sample_count_too_small")
    expectancy_delta = review.get("expectancy_delta")
    if expectancy_delta is None or float(expectancy_delta) < 0:
        reasons.append("expectancy_delta_negative")
    if not review.get("improved_expectancy"):
        reasons.append("improved_expectancy_false")
    if not review.get("mae_non_worse"):
        reasons.append("mae_non_worse_false")
    if not review.get("adverse_move_non_worse"):
        reasons.append("adverse_move_non_worse_false")
    if not review.get("stable_window"):
        reasons.append("stable_window_false")
    if not review.get("alignment_ok"):
        reasons.append("alignment_ok_false")

    approval = review.get("approval_decision")
    if approval and str(approval.get("decision") or "").strip().lower() == "rejected":
        reasons.append("approval_rejected")

    return len(reasons) == 0, reasons


def _build_registry_entry(
    *,
    entry: dict[str, Any],
    role: str,
    review: dict[str, Any] | None,
    source_publish_id: str | None,
    promoted_at: str | None = None,
    promoted_by: str | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    logic_id = _normalize_text(entry.get("logic_id"))
    logic_version = _normalize_text(entry.get("logic_version"))
    logic_key = _logic_key(logic_id, logic_version)
    artifact_uri = _normalize_text(entry.get("logic_artifact_uri") or entry.get("artifact_uri"))
    checksum = _normalize_text(entry.get("logic_artifact_checksum") or entry.get("checksum"))
    return {
        "logic_id": logic_id,
        "logic_version": logic_version,
        "logic_key": logic_key,
        "logic_family": _normalize_text(entry.get("logic_family")),
        "artifact_uri": artifact_uri,
        "artifact_checksum": checksum,
        "published_at": _normalize_text(entry.get("published_at")),
        "status": role,
        "role": role,
        "source_publish_id": source_publish_id,
        "promotion_reason": reason,
        "promoted_at": promoted_at,
        "promoted_by": promoted_by,
        "review": review,
        "comparison_summary": review.get("summary") if review else None,
        "comparison_metrics": {
            "sample_count": None if review is None else review.get("sample_count"),
            "expectancy_delta": None if review is None else review.get("expectancy_delta"),
            "improved_expectancy": None if review is None else review.get("improved_expectancy"),
            "mae_non_worse": None if review is None else review.get("mae_non_worse"),
            "adverse_move_non_worse": None if review is None else review.get("adverse_move_non_worse"),
            "stable_window": None if review is None else review.get("stable_window"),
            "alignment_ok": None if review is None else review.get("alignment_ok"),
        },
    }


def _history_event(
    *,
    action: str,
    previous_logic_key: str | None,
    new_logic_key: str | None,
    source: str,
    reason: str | None,
    review: dict[str, Any] | None,
    target: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "event_type": f"publish_logic_{action}",
        "action": action,
        "previous_logic_key": previous_logic_key,
        "new_logic_key": new_logic_key,
        "changed_at": _now_iso(),
        "source": source,
        "reason": reason,
        "target_logic_id": None if target is None else target.get("logic_id"),
        "target_logic_version": None if target is None else target.get("logic_version"),
        "target_artifact_uri": None if target is None else target.get("artifact_uri"),
        "comparison_summary": None if review is None else review.get("summary"),
        "comparison_metrics": None
        if review is None
        else {
            "sample_count": review.get("sample_count"),
            "expectancy_delta": review.get("expectancy_delta"),
            "improved_expectancy": review.get("improved_expectancy"),
            "mae_non_worse": review.get("mae_non_worse"),
            "adverse_move_non_worse": review.get("adverse_move_non_worse"),
            "stable_window": review.get("stable_window"),
            "alignment_ok": review.get("alignment_ok"),
        },
    }


def _load_registry(config_repo: ConfigRepository) -> dict[str, Any]:
    state = config_repo.load_publish_registry_state()
    return state if isinstance(state, dict) else {}


def _save_registry(config_repo: ConfigRepository, state: dict[str, Any]) -> str:
    payload = dict(state or {})
    payload["schema_version"] = PUBLISH_REGISTRY_SCHEMA_VERSION
    payload["updated_at"] = _now_iso()
    return config_repo.save_publish_registry_state(payload)


def build_publish_promotion_snapshot(
    *,
    config_repo: ConfigRepository,
    db_path: str | None = None,
    ops_db_path: str | None = None,
) -> dict[str, Any]:
    registry = _load_registry(config_repo)
    catalog = load_published_logic_catalog(db_path=db_path)
    champion = registry.get("champion") if isinstance(registry.get("champion"), dict) else None
    challenger = registry.get("challenger") if isinstance(registry.get("challenger"), dict) else None
    candidate_logic_key = _normalize_text(challenger.get("logic_key")) if challenger else None
    champion_logic_key = _normalize_text(champion.get("logic_key")) if champion else None
    default_pointer = _normalize_text(registry.get("default_logic_pointer")) or catalog.get("default_logic_pointer")
    return {
        "schema_version": PUBLISH_REGISTRY_SCHEMA_VERSION,
        "default_logic_pointer": default_pointer,
        "champion_logic_key": champion_logic_key,
        "challenger_logic_key": candidate_logic_key,
        "champion": champion,
        "challenger": challenger,
        "previous_champion_logic_key": _normalize_text(registry.get("previous_champion_logic_key")),
        "retired_logic_keys": list(registry.get("retired_logic_keys") or []),
        "promotion_history": list(registry.get("promotion_history") or []),
        "registry": registry,
        "catalog": catalog,
        "ops_review": None if candidate_logic_key is None else _load_review_for_publish_id(
            publish_id=str(challenger.get("source_publish_id") or ""),
            ops_db_path=ops_db_path,
        ),
    }


def validate_promotion_target(
    *,
    config_repo: ConfigRepository,
    logic_key: str,
    db_path: str | None = None,
    ops_db_path: str | None = None,
) -> dict[str, Any]:
    normalized_key = _normalize_text(logic_key)
    if not normalized_key:
        return {"ok": False, "reason": "logic_key_required"}

    catalog_entry = _catalog_lookup(db_path=db_path, logic_key=normalized_key)
    if not catalog_entry:
        return {"ok": False, "reason": "logic_key_not_available", "logic_key": normalized_key}

    artifact_uri = _normalize_text(catalog_entry.get("logic_artifact_uri") or catalog_entry.get("artifact_uri"))
    artifact_path = _resolve_artifact_path(artifact_uri)
    checksum = _normalize_text(catalog_entry.get("logic_artifact_checksum") or catalog_entry.get("checksum"))
    if not artifact_path:
        return {"ok": False, "reason": "artifact_missing", "logic_key": normalized_key}
    if checksum and _checksum_file(artifact_path).lower() != checksum.lower():
        return {"ok": False, "reason": "checksum_mismatch", "logic_key": normalized_key}

    logic_id = _normalize_text(catalog_entry.get("logic_id"))
    logic_version = _normalize_text(catalog_entry.get("logic_version"))
    if _logic_key(logic_id, logic_version) != normalized_key:
        return {"ok": False, "reason": "manifest_mismatch", "logic_key": normalized_key}

    review = _load_review_for_publish_id(
        publish_id=str(catalog_entry.get("publish_id") or ""),
        ops_db_path=ops_db_path,
    )
    gate_pass, gate_reasons = _promotion_gate(review)
    validation_state = _VALIDATION_OK if gate_pass else "promotion_gate_blocked"
    return {
        "ok": gate_pass,
        "reason": None if gate_pass else gate_reasons[0],
        "logic_key": normalized_key,
        "logic_id": logic_id,
        "logic_version": logic_version,
        "artifact_uri": artifact_uri,
        "artifact_path": artifact_path,
        "artifact_checksum": checksum,
        "publish_id": catalog_entry.get("publish_id"),
        "review": review,
        "gate_pass": gate_pass,
        "gate_reasons": gate_reasons,
        "validation_state": validation_state,
        "manifest": catalog_entry,
    }


def validate_publish_logic_target(
    *,
    config_repo: ConfigRepository,
    logic_key: str,
    db_path: str | None = None,
) -> dict[str, Any]:
    normalized_key = _normalize_text(logic_key)
    if not normalized_key:
        return {"ok": False, "reason": "logic_key_required"}

    catalog_entry = _catalog_lookup(db_path=db_path, logic_key=normalized_key)
    if not catalog_entry:
        return {"ok": False, "reason": "logic_key_not_available", "logic_key": normalized_key}

    artifact_uri = _normalize_text(catalog_entry.get("logic_artifact_uri") or catalog_entry.get("artifact_uri"))
    artifact_path = _resolve_artifact_path(artifact_uri)
    checksum = _normalize_text(catalog_entry.get("logic_artifact_checksum") or catalog_entry.get("checksum"))
    if not artifact_path:
        return {"ok": False, "reason": "artifact_missing", "logic_key": normalized_key}
    if checksum and _checksum_file(artifact_path).lower() != checksum.lower():
        return {"ok": False, "reason": "checksum_mismatch", "logic_key": normalized_key}

    logic_id = _normalize_text(catalog_entry.get("logic_id"))
    logic_version = _normalize_text(catalog_entry.get("logic_version"))
    if _logic_key(logic_id, logic_version) != normalized_key:
        return {"ok": False, "reason": "manifest_mismatch", "logic_key": normalized_key}

    return {
        "ok": True,
        "reason": None,
        "logic_key": normalized_key,
        "logic_id": logic_id,
        "logic_version": logic_version,
        "artifact_uri": artifact_uri,
        "artifact_path": artifact_path,
        "artifact_checksum": checksum,
        "publish_id": catalog_entry.get("publish_id"),
        "manifest": catalog_entry,
    }


def promote_logic_key(
    *,
    config_repo: ConfigRepository,
    logic_key: str,
    source: str,
    reason: str | None = None,
    actor: str | None = None,
    db_path: str | None = None,
    ops_db_path: str | None = None,
) -> dict[str, Any]:
    validation = validate_promotion_target(
        config_repo=config_repo,
        logic_key=logic_key,
        db_path=db_path,
        ops_db_path=ops_db_path,
    )
    if not validation.get("ok"):
        return validation

    registry = _load_registry(config_repo)
    current_champion = registry.get("champion") if isinstance(registry.get("champion"), dict) else None
    current_champion_key = _normalize_text(current_champion.get("logic_key")) if current_champion else None
    target_entry = validation.get("manifest") or {}
    target_key = _normalize_text(validation.get("logic_key"))
    if current_champion_key == target_key:
        snapshot = build_publish_promotion_snapshot(config_repo=config_repo, db_path=db_path, ops_db_path=ops_db_path)
        return {
            "ok": True,
            "changed": False,
            "action": PUBLISH_PROMOTION_ACTION_PROMOTE,
            "validation": validation,
            "snapshot": snapshot,
        }

    promoted_at = _now_iso()
    champion_entry = _build_registry_entry(
        entry=target_entry,
        role=PUBLISH_ROLE_CHAMPION,
        review=validation.get("review"),
        source_publish_id=_normalize_text(validation.get("publish_id")),
        promoted_at=promoted_at,
        promoted_by=actor,
        reason=reason,
    )
    challenger_entry = current_champion
    previous_champion_key = current_champion_key
    retired_logic_keys = list(registry.get("retired_logic_keys") or [])
    if previous_champion_key and previous_champion_key not in retired_logic_keys:
        retired_logic_keys.append(previous_champion_key)

    registry["default_logic_pointer"] = target_key
    registry["champion"] = champion_entry
    registry["challenger"] = None
    registry["previous_champion_logic_key"] = previous_champion_key
    registry["retired_logic_keys"] = retired_logic_keys
    registry.setdefault("promotion_history", [])
    registry["promotion_history"] = [
        *list(registry.get("promotion_history") or [])[-19:],
        _history_event(
            action=PUBLISH_PROMOTION_ACTION_PROMOTE,
            previous_logic_key=previous_champion_key,
            new_logic_key=target_key,
            source=source,
            reason=reason,
            review=validation.get("review"),
            target=champion_entry,
        ),
    ]
    _save_registry(config_repo, registry)
    config_repo.append_publish_promotion_audit_event(
        {
            "event_type": "publish_logic_promoted",
            "action": PUBLISH_PROMOTION_ACTION_PROMOTE,
            "previous_logic_key": previous_champion_key,
            "new_logic_key": target_key,
            "changed_at": promoted_at,
            "source": source,
            "reason": reason,
            "actor": actor,
            "publish_id": validation.get("publish_id"),
            "artifact_uri": validation.get("artifact_uri"),
            "artifact_checksum": validation.get("artifact_checksum"),
            "gate_pass": validation.get("gate_pass"),
            "gate_reasons": validation.get("gate_reasons"),
            "comparison_summary": validation.get("review", {}).get("summary") if validation.get("review") else None,
            "comparison_metrics": None if not validation.get("review") else {
                "sample_count": validation["review"].get("sample_count"),
                "expectancy_delta": validation["review"].get("expectancy_delta"),
                "improved_expectancy": validation["review"].get("improved_expectancy"),
                "mae_non_worse": validation["review"].get("mae_non_worse"),
                "adverse_move_non_worse": validation["review"].get("adverse_move_non_worse"),
                "stable_window": validation["review"].get("stable_window"),
                "alignment_ok": validation["review"].get("alignment_ok"),
            },
        }
    )
    snapshot = build_publish_promotion_snapshot(config_repo=config_repo, db_path=db_path, ops_db_path=ops_db_path)
    return {
        "ok": True,
        "changed": True,
        "action": PUBLISH_PROMOTION_ACTION_PROMOTE,
        "validation": validation,
        "snapshot": snapshot,
        "previous_champion": challenger_entry,
        "champion": champion_entry,
    }


def demote_logic_key(
    *,
    config_repo: ConfigRepository,
    logic_key: str,
    source: str,
    reason: str | None = None,
    actor: str | None = None,
    db_path: str | None = None,
    ops_db_path: str | None = None,
) -> dict[str, Any]:
    normalized_key = _normalize_text(logic_key)
    if not normalized_key:
        return {"ok": False, "reason": "logic_key_required"}

    validation = validate_publish_logic_target(
        config_repo=config_repo,
        logic_key=normalized_key,
        db_path=db_path,
    )
    if not validation.get("ok"):
        return validation

    registry = _load_registry(config_repo)
    current_champion = registry.get("champion") if isinstance(registry.get("champion"), dict) else None
    current_champion_key = _normalize_text(current_champion.get("logic_key")) if current_champion else None
    if current_champion_key != normalized_key:
        retired_logic_keys = list(registry.get("retired_logic_keys") or [])
        if normalized_key not in retired_logic_keys:
            retired_logic_keys.append(normalized_key)
        registry["retired_logic_keys"] = retired_logic_keys
        registry["challenger"] = None if _normalize_text((registry.get("challenger") or {}).get("logic_key")) == normalized_key else registry.get("challenger")
        registry.setdefault("promotion_history", [])
        registry["promotion_history"] = [
            *list(registry.get("promotion_history") or [])[-19:],
            _history_event(
                action=PUBLISH_PROMOTION_ACTION_DEMOTE,
                previous_logic_key=current_champion_key,
                new_logic_key=normalized_key,
                source=source,
                reason=reason,
                review=validation.get("review"),
                target=validation.get("manifest"),
            ),
        ]
        _save_registry(config_repo, registry)
        config_repo.append_publish_promotion_audit_event(
            {
                "event_type": "publish_logic_demoted",
                "action": PUBLISH_PROMOTION_ACTION_DEMOTE,
                "previous_logic_key": current_champion_key,
                "new_logic_key": normalized_key,
                "changed_at": _now_iso(),
                "source": source,
                "reason": reason,
                "actor": actor,
                "publish_id": validation.get("publish_id"),
                "artifact_uri": validation.get("artifact_uri"),
                "artifact_checksum": validation.get("artifact_checksum"),
            }
        )
        snapshot = build_publish_promotion_snapshot(config_repo=config_repo, db_path=db_path, ops_db_path=ops_db_path)
        return {
            "ok": True,
            "changed": True,
            "action": PUBLISH_PROMOTION_ACTION_DEMOTE,
            "validation": validation,
            "snapshot": snapshot,
        }

    registry_previous_key = _normalize_text(registry.get("previous_champion_logic_key"))
    fallback_entry = _catalog_lookup(db_path=db_path, logic_key=registry_previous_key) if registry_previous_key else None
    if fallback_entry:
        rollback_target = _build_registry_entry(
            entry=fallback_entry,
            role=PUBLISH_ROLE_CHAMPION,
            review=_load_review_for_publish_id(
                publish_id=str(fallback_entry.get("publish_id") or ""),
                ops_db_path=ops_db_path,
            ),
            source_publish_id=_normalize_text(fallback_entry.get("publish_id")),
            promoted_at=_now_iso(),
            promoted_by=actor,
            reason=reason or "rollback_previous_champion",
        )
        registry["champion"] = rollback_target
        registry["default_logic_pointer"] = registry_previous_key
        registry["previous_champion_logic_key"] = normalized_key
    else:
        registry["champion"] = None
        registry["default_logic_pointer"] = None
        registry["previous_champion_logic_key"] = normalized_key
    retired_logic_keys = list(registry.get("retired_logic_keys") or [])
    if normalized_key not in retired_logic_keys:
        retired_logic_keys.append(normalized_key)
    registry["retired_logic_keys"] = retired_logic_keys
    registry["challenger"] = None
    registry.setdefault("promotion_history", [])
    registry["promotion_history"] = [
        *list(registry.get("promotion_history") or [])[-19:],
        _history_event(
            action=PUBLISH_PROMOTION_ACTION_ROLLBACK,
            previous_logic_key=normalized_key,
            new_logic_key=registry.get("default_logic_pointer"),
            source=source,
            reason=reason,
            review=_load_review_for_publish_id(
                publish_id=str(validation.get("publish_id") or ""),
                ops_db_path=ops_db_path,
            ),
            target=validation.get("manifest"),
        ),
    ]
    _save_registry(config_repo, registry)
    config_repo.append_publish_promotion_audit_event(
        {
            "event_type": "publish_logic_rollback",
            "action": PUBLISH_PROMOTION_ACTION_ROLLBACK,
            "previous_logic_key": normalized_key,
            "new_logic_key": registry.get("default_logic_pointer"),
            "changed_at": _now_iso(),
            "source": source,
            "reason": reason,
            "actor": actor,
            "publish_id": validation.get("publish_id"),
            "artifact_uri": validation.get("artifact_uri"),
            "artifact_checksum": validation.get("artifact_checksum"),
            "rollback_target_logic_key": registry_previous_key,
        }
    )
    snapshot = build_publish_promotion_snapshot(config_repo=config_repo, db_path=db_path, ops_db_path=ops_db_path)
    return {
        "ok": True,
        "changed": True,
        "action": PUBLISH_PROMOTION_ACTION_ROLLBACK,
        "validation": validation,
        "snapshot": snapshot,
    }
