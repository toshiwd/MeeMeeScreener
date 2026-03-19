from __future__ import annotations

import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.backend.infra.files.config_repo import (
    ConfigRepository,
    PUBLISH_REGISTRY_SCHEMA_VERSION,
)
from external_analysis.ops.ops_schema import connect_ops_db, ensure_ops_schema
from external_analysis.results.publish_registry import (
    append_publish_registry_audit_event as append_external_publish_registry_audit_event,
    load_publish_registry_state as load_external_publish_registry_state,
    save_publish_registry_state as save_external_publish_registry_state,
)
from external_analysis.results.publish import load_published_logic_catalog
from shared.contracts.publish_registry import (
    PUBLISH_ROLE_CHALLENGER,
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
        "promotion_state": role,
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


def _registry_challengers(registry: dict[str, Any]) -> list[dict[str, Any]]:
    challengers = registry.get("challengers")
    if isinstance(challengers, list):
        return [entry for entry in challengers if isinstance(entry, dict)]
    challenger = registry.get("challenger")
    if isinstance(challenger, dict):
        return [challenger]
    return []


def _registry_rollback_candidates(registry: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = registry.get("rollback_candidates")
    if isinstance(candidates, list):
        return [entry for entry in candidates if isinstance(entry, dict)]
    previous = registry.get("previous_stable_champion")
    if isinstance(previous, dict):
        return [previous]
    return []


def _set_challengers(registry: dict[str, Any], challengers: list[dict[str, Any]]) -> None:
    ordered = sorted(
        [entry for entry in challengers if isinstance(entry, dict)],
        key=lambda item: (int(item.get("queue_order") or 0), str(item.get("logic_key") or "")),
    )
    normalized: list[dict[str, Any]] = []
    for index, entry in enumerate(ordered, start=1):
        payload = dict(entry)
        try:
            queue_order = int(payload.get("queue_order") or 0)
        except (TypeError, ValueError):
            queue_order = 0
        if queue_order <= 0:
            payload["queue_order"] = index
        normalized.append(payload)
    registry["challengers"] = normalized
    registry["challenger"] = normalized[0] if normalized else None
    registry["challenger_logic_key"] = _normalize_text(normalized[0].get("logic_key")) if normalized else None
    registry["challenger_logic_keys"] = [str(entry.get("logic_key")) for entry in normalized if entry.get("logic_key")]
    registry["challengers_json"] = normalized


def _next_queue_order(registry: dict[str, Any]) -> int:
    challengers = _registry_challengers(registry)
    highest = 0
    for challenger in challengers:
        try:
            highest = max(highest, int(challenger.get("queue_order") or 0))
        except (TypeError, ValueError):
            continue
    if highest > 0:
        return highest + 1
    return len(challengers) + 1 if challengers else 1


def _queue_challenger_entry(
    *,
    validation: dict[str, Any],
    actor: str | None,
    reason: str | None,
    queue_order: int,
) -> dict[str, Any]:
    manifest = validation.get("manifest") or {}
    return {
        "logic_id": _normalize_text(validation.get("logic_id")),
        "logic_version": _normalize_text(validation.get("logic_version")),
        "logic_key": _normalize_text(validation.get("logic_key")),
        "logic_family": _normalize_text(manifest.get("logic_family")),
        "artifact_uri": _normalize_text(validation.get("artifact_uri")),
        "artifact_checksum": _normalize_text(validation.get("artifact_checksum")),
        "queued_at": _now_iso(),
        "promotion_state": "queued",
        "queue_order": queue_order,
        "validation_state": validation.get("validation_state") or "queued",
        "status": PUBLISH_ROLE_CHALLENGER,
        "role": PUBLISH_ROLE_CHALLENGER,
        "source_publish_id": _normalize_text(validation.get("publish_id")),
        "promotion_reason": reason,
        "actor": actor,
    }


def _push_rollback_candidate(registry: dict[str, Any], candidate: dict[str, Any] | None) -> None:
    if not isinstance(candidate, dict):
        return
    candidates = _registry_rollback_candidates(registry)
    candidate_key = _normalize_text(candidate.get("logic_key"))
    if candidate_key:
        candidates = [entry for entry in candidates if _normalize_text(entry.get("logic_key")) != candidate_key]
    candidates.append(candidate)
    registry["rollback_candidates"] = candidates[-5:]
    registry["previous_stable_champion"] = candidates[-1] if candidates else None
    registry["previous_stable_champion_logic_key"] = _normalize_text((candidates[-1] if candidates else {}).get("logic_key"))


def _remove_challenger_by_key(registry: dict[str, Any], logic_key: str) -> dict[str, Any] | None:
    challengers = _registry_challengers(registry)
    kept: list[dict[str, Any]] = []
    removed: dict[str, Any] | None = None
    for challenger in challengers:
        if _normalize_text(challenger.get("logic_key")) == logic_key and removed is None:
            removed = challenger
            continue
        kept.append(challenger)
    _set_challengers(registry, kept)
    return removed


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


def _has_mutation_details(registry: dict[str, Any]) -> bool:
    return any(
        [
            bool(registry.get("challengers")),
            bool(registry.get("challenger_logic_keys")),
            bool(registry.get("promotion_history")),
            bool(registry.get("retired_logic_keys")),
            bool(registry.get("demoted_logic_keys")),
            _normalize_text(registry.get("previous_stable_champion_logic_key")),
            bool(registry.get("rollback_candidates")),
        ]
    )


def _load_registry(config_repo: ConfigRepository, db_path: str | None = None) -> dict[str, Any]:
    external_registry = load_external_publish_registry_state(db_path=db_path)
    local_registry = config_repo.load_publish_registry_state()
    external_has_content = any(
        [
            _normalize_text(external_registry.get("champion_logic_key")),
            _normalize_text(external_registry.get("default_logic_pointer")),
            _normalize_text(external_registry.get("previous_stable_champion_logic_key")),
            bool(external_registry.get("challengers")),
            bool(external_registry.get("challenger_logic_keys")),
            bool(external_registry.get("champion")),
        ]
    )
    local_has_content = any(
        [
            _normalize_text(local_registry.get("champion_logic_key")),
            _normalize_text(local_registry.get("default_logic_pointer")),
            _normalize_text(local_registry.get("previous_stable_champion_logic_key")),
            bool(local_registry.get("challengers")),
            bool(local_registry.get("challenger_logic_keys")),
            bool(local_registry.get("champion")),
        ]
    )
    if external_registry.get("source_of_truth") == "external_analysis" and external_has_content:
        if local_has_content and _has_mutation_details(local_registry) and not _has_mutation_details(external_registry):
            if not _normalize_text(local_registry.get("bootstrap_rule")):
                local_registry["bootstrap_rule"] = _normalize_text(external_registry.get("bootstrap_rule"))
            return local_registry if isinstance(local_registry, dict) else {}
        return external_registry if isinstance(external_registry, dict) else {}
    if local_has_content:
        if not _normalize_text(local_registry.get("bootstrap_rule")):
            local_registry["bootstrap_rule"] = _normalize_text(external_registry.get("bootstrap_rule"))
        return local_registry if isinstance(local_registry, dict) else {}
    return external_registry if isinstance(external_registry, dict) else {}


def _save_registry(config_repo: ConfigRepository, state: dict[str, Any]) -> str:
    payload = dict(state or {})
    payload["schema_version"] = PUBLISH_REGISTRY_SCHEMA_VERSION
    payload["updated_at"] = _now_iso()
    return config_repo.save_publish_registry_state(payload)


def _persist_registry_state(
    *,
    config_repo: ConfigRepository,
    db_path: str | None,
    registry_state: dict[str, Any],
    audit_event: dict[str, Any],
    source_revision: str | None,
    degraded: bool = False,
    sync_state: str = "synced",
    sync_message: str | None = None,
) -> dict[str, Any]:
    external_state = save_external_publish_registry_state(
        db_path=db_path,
        state=registry_state,
        source_revision=source_revision,
        degraded=degraded,
        sync_state=sync_state,
        sync_message=sync_message,
    )
    append_external_publish_registry_audit_event(
        db_path=db_path,
        event={**audit_event, "registry_name": "publish_registry"},
        registry_version=external_state.get("registry_version"),
    )
    mirror_state = dict(external_state)
    mirror_state["source_of_truth"] = "local_mirror"
    mirror_state["mirror_of"] = "external_analysis"
    mirror_state["registry_sync_state"] = sync_state
    mirror_state["sync_state"] = sync_state
    mirror_state["degraded"] = bool(degraded)
    mirror_state["sync_message"] = sync_message
    mirror_state["last_sync_at"] = _now_iso()
    mirror_state["updated_at"] = mirror_state["last_sync_at"]
    mirror_state["source_revision"] = external_state.get("source_revision")
    mirror_payload = {k: v for k, v in mirror_state.items() if k != "registry_checksum"}
    mirror_state["registry_checksum"] = hashlib.sha256(
        json.dumps(mirror_payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    _save_registry(config_repo, mirror_state)
    config_repo.append_publish_promotion_audit_event(
        {
            **audit_event,
            "registry_name": "publish_registry",
            "mirror": True,
            "registry_version": external_state.get("registry_version"),
        }
    )
    return external_state


def build_publish_promotion_snapshot(
    *,
    config_repo: ConfigRepository,
    db_path: str | None = None,
    ops_db_path: str | None = None,
) -> dict[str, Any]:
    external_registry = load_external_publish_registry_state(db_path=db_path)
    local_registry = _load_registry(config_repo, db_path=db_path)
    external_has_content = any(
        [
            _normalize_text(external_registry.get("champion_logic_key")),
            _normalize_text(external_registry.get("default_logic_pointer")),
            _normalize_text(external_registry.get("previous_stable_champion_logic_key")),
            bool(external_registry.get("challengers")),
            bool(external_registry.get("challenger_logic_keys")),
            bool(external_registry.get("champion")),
        ]
    )
    if external_registry.get("source_of_truth") == "external_analysis" and external_has_content:
        registry = external_registry
        source_of_truth = "external_analysis"
        registry_sync_state = _normalize_text(external_registry.get("registry_sync_state")) or "synced"
        last_sync_time = _normalize_text(external_registry.get("last_sync_at")) or _normalize_text(external_registry.get("updated_at"))
        degraded = bool(external_registry.get("degraded"))
    elif local_registry:
        registry = local_registry
        source_of_truth = "local_mirror"
        registry_sync_state = "mirror_fallback"
        last_sync_time = _normalize_text(local_registry.get("last_sync_at")) or _normalize_text(local_registry.get("updated_at"))
        degraded = True
    else:
        registry = {}
        source_of_truth = "empty"
        registry_sync_state = "empty"
        last_sync_time = None
        degraded = True
    catalog = load_published_logic_catalog(db_path=db_path)
    champion = registry.get("champion") if isinstance(registry.get("champion"), dict) else None
    challengers = _registry_challengers(registry)
    challenger = challengers[0] if challengers else None
    challenger_logic_keys = [str(entry.get("logic_key")) for entry in challengers if entry.get("logic_key")]
    candidate_logic_key = _normalize_text(challenger.get("logic_key")) if challenger else None
    champion_logic_key = _normalize_text(champion.get("logic_key")) if champion else None
    default_pointer = _normalize_text(registry.get("default_logic_pointer")) or catalog.get("default_logic_pointer")
    return {
        "schema_version": PUBLISH_REGISTRY_SCHEMA_VERSION,
        "source_of_truth": source_of_truth,
        "registry_sync_state": registry_sync_state,
        "degraded": degraded,
        "last_sync_time": last_sync_time,
        "registry_version": registry.get("registry_version"),
        "source_revision": registry.get("source_revision"),
        "bootstrap_rule": _normalize_text(registry.get("bootstrap_rule")),
        "default_logic_pointer": default_pointer,
        "champion_logic_key": champion_logic_key,
        "challenger_logic_key": candidate_logic_key,
        "champion": champion,
        "challenger": challenger,
        "challengers": challengers,
        "challenger_logic_keys": challenger_logic_keys,
        "previous_champion_logic_key": _normalize_text(registry.get("previous_champion_logic_key")),
        "previous_stable_champion_logic_key": _normalize_text(registry.get("previous_stable_champion_logic_key")),
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

    registry = _load_registry(config_repo, db_path=db_path)
    current_champion = registry.get("champion") if isinstance(registry.get("champion"), dict) else None
    current_champion_key = _normalize_text(current_champion.get("logic_key")) if current_champion else None
    target_entry = validation.get("manifest") or {}
    target_key = _normalize_text(validation.get("logic_key"))
    challengers = _registry_challengers(registry)
    target_queue_order_before = None
    for challenger in challengers:
        if _normalize_text(challenger.get("logic_key")) == target_key:
            target_queue_order_before = challenger.get("queue_order")
            break
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
    target_challenger = None
    for challenger in challengers:
        if _normalize_text(challenger.get("logic_key")) == target_key:
            target_challenger = challenger
            break
    if target_challenger is None:
        target_challenger = _queue_challenger_entry(
            validation=validation,
            actor=actor,
            reason=reason,
            queue_order=_next_queue_order(registry),
        )
        challengers.append(target_challenger)
    challengers = [entry for entry in challengers if _normalize_text(entry.get("logic_key")) != target_key]
    champion_entry = _build_registry_entry(
        entry=target_entry,
        role=PUBLISH_ROLE_CHAMPION,
        review=validation.get("review"),
        source_publish_id=_normalize_text(validation.get("publish_id")),
        promoted_at=promoted_at,
        promoted_by=actor,
        reason=reason,
    )
    previous_champion_key = current_champion_key
    _push_rollback_candidate(registry, current_champion)
    registry["default_logic_pointer"] = target_key
    registry["champion"] = champion_entry
    _set_challengers(registry, challengers)
    registry["previous_stable_champion"] = current_champion
    registry["previous_stable_champion_logic_key"] = previous_champion_key
    registry["challenger"] = registry.get("challenger")
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
    registry_event = {
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
        "queue_order_before": target_queue_order_before,
        "queue_order_after": None,
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
    try:
        external_state = _persist_registry_state(
            config_repo=config_repo,
            db_path=db_path,
            registry_state=registry,
            audit_event=registry_event,
            source_revision=validation.get("publish_id") or target_key,
            sync_state="synced",
            degraded=False,
        )
    except Exception as exc:
        return {"ok": False, "reason": "external_registry_write_failed", "error": str(exc), "logic_key": target_key}
    snapshot = build_publish_promotion_snapshot(config_repo=config_repo, db_path=db_path, ops_db_path=ops_db_path)
    return {
        "ok": True,
        "changed": True,
        "action": PUBLISH_PROMOTION_ACTION_PROMOTE,
        "validation": validation,
        "snapshot": snapshot,
        "champion": champion_entry,
        "challenger": target_challenger,
        "previous_champion": current_champion,
        "external_state": external_state,
    }


def enqueue_challenger_logic_key(
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

    registry = _load_registry(config_repo, db_path=db_path)
    current_champion = registry.get("champion") if isinstance(registry.get("champion"), dict) else None
    if _normalize_text(current_champion.get("logic_key")) == normalized_key:
        snapshot = build_publish_promotion_snapshot(config_repo=config_repo, db_path=db_path, ops_db_path=ops_db_path)
        return {"ok": True, "changed": False, "action": "enqueue", "validation": validation, "snapshot": snapshot}

    challengers = _registry_challengers(registry)
    existing = next((entry for entry in challengers if _normalize_text(entry.get("logic_key")) == normalized_key), None)
    if existing:
        snapshot = build_publish_promotion_snapshot(config_repo=config_repo, db_path=db_path, ops_db_path=ops_db_path)
        return {
            "ok": True,
            "changed": False,
            "action": "enqueue",
            "validation": validation,
            "snapshot": snapshot,
            "challenger": existing,
        }

    queue_order_before = None
    queue_order_after = _next_queue_order(registry)
    challenger_entry = _queue_challenger_entry(
        validation=validation,
        actor=actor,
        reason=reason,
        queue_order=queue_order_after,
    )
    challengers.append(challenger_entry)
    _set_challengers(registry, challengers)
    registry.setdefault("promotion_history", [])
    registry["promotion_history"] = [
        *list(registry.get("promotion_history") or [])[-19:],
        _history_event(
            action="enqueue",
            previous_logic_key=None,
            new_logic_key=normalized_key,
            source=source,
            reason=reason,
            review=validation.get("review"),
            target=validation.get("manifest"),
        ),
    ]
    registry_event = {
        "event_type": "publish_logic_enqueued",
        "action": "enqueue",
        "previous_logic_key": None,
        "new_logic_key": normalized_key,
        "changed_at": _now_iso(),
        "source": source,
        "reason": reason,
        "actor": actor,
        "publish_id": validation.get("publish_id"),
        "artifact_uri": validation.get("artifact_uri"),
        "artifact_checksum": validation.get("artifact_checksum"),
        "queue_order_before": queue_order_before,
        "queue_order_after": queue_order_after,
        "validation_state": validation.get("validation_state"),
    }
    try:
        external_state = _persist_registry_state(
            config_repo=config_repo,
            db_path=db_path,
            registry_state=registry,
            audit_event=registry_event,
            source_revision=validation.get("publish_id") or normalized_key,
            sync_state="synced",
            degraded=False,
        )
    except Exception as exc:
        return {"ok": False, "reason": "external_registry_write_failed", "error": str(exc), "logic_key": normalized_key}
    snapshot = build_publish_promotion_snapshot(config_repo=config_repo, db_path=db_path, ops_db_path=ops_db_path)
    return {
        "ok": True,
        "changed": True,
        "action": "enqueue",
        "validation": validation,
        "snapshot": snapshot,
        "challenger": challenger_entry,
        "external_state": external_state,
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

    registry = _load_registry(config_repo, db_path=db_path)
    current_champion = registry.get("champion") if isinstance(registry.get("champion"), dict) else None
    current_champion_key = _normalize_text(current_champion.get("logic_key")) if current_champion else None
    challengers = _registry_challengers(registry)
    if current_champion_key != normalized_key:
        removed = _remove_challenger_by_key(registry, normalized_key)
        if not removed:
            return {"ok": False, "reason": "logic_key_not_active_challenger", "logic_key": normalized_key}
        retired_logic_keys = list(registry.get("retired_logic_keys") or [])
        if normalized_key not in retired_logic_keys:
            retired_logic_keys.append(normalized_key)
        registry["retired_logic_keys"] = retired_logic_keys
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
        demote_event = {
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
            "queue_order_before": removed.get("queue_order"),
            "queue_order_after": None,
        }
        try:
            external_state = _persist_registry_state(
                config_repo=config_repo,
                db_path=db_path,
                registry_state=registry,
                audit_event=demote_event,
                source_revision=validation.get("publish_id") or normalized_key,
                sync_state="synced",
                degraded=False,
            )
        except Exception as exc:
            return {"ok": False, "reason": "external_registry_write_failed", "error": str(exc), "logic_key": normalized_key}
        snapshot = build_publish_promotion_snapshot(config_repo=config_repo, db_path=db_path, ops_db_path=ops_db_path)
        return {
            "ok": True,
            "changed": True,
            "action": PUBLISH_PROMOTION_ACTION_DEMOTE,
            "validation": validation,
            "snapshot": snapshot,
            "external_state": external_state,
            "retired": removed,
        }

    rollback_candidates = _registry_rollback_candidates(registry)
    candidate = rollback_candidates[-1] if rollback_candidates else registry.get("previous_stable_champion")
    if not isinstance(candidate, dict):
        return {"ok": False, "reason": "rollback_candidate_missing", "logic_key": normalized_key}
    candidate_key = _normalize_text(candidate.get("logic_key"))
    if candidate_key and candidate_key == current_champion_key:
        return {"ok": True, "changed": False, "action": PUBLISH_PROMOTION_ACTION_DEMOTE, "validation": validation}

    retired_logic_keys = list(registry.get("retired_logic_keys") or [])
    if normalized_key not in retired_logic_keys:
        retired_logic_keys.append(normalized_key)
    demoted_logic_keys = list(registry.get("demoted_logic_keys") or [])
    if normalized_key not in demoted_logic_keys:
        demoted_logic_keys.append(normalized_key)
    registry["retired_logic_keys"] = retired_logic_keys
    registry["demoted_logic_keys"] = demoted_logic_keys
    registry["champion"] = _build_registry_entry(
        entry=candidate,
        role=PUBLISH_ROLE_CHAMPION,
        review=_load_review_for_publish_id(
            publish_id=str(candidate.get("source_publish_id") or candidate.get("publish_id") or ""),
            ops_db_path=ops_db_path,
        ),
        source_publish_id=_normalize_text(candidate.get("source_publish_id") or candidate.get("publish_id")),
        promoted_at=_now_iso(),
        promoted_by=actor,
        reason=reason or "demote_restore_previous_stable",
    )
    registry["default_logic_pointer"] = candidate_key
    registry["previous_stable_champion"] = current_champion
    registry["previous_stable_champion_logic_key"] = current_champion_key
    _push_rollback_candidate(registry, current_champion)
    _set_challengers(registry, challengers)
    registry.setdefault("promotion_history", [])
    registry["promotion_history"] = [
        *list(registry.get("promotion_history") or [])[-19:],
        _history_event(
            action=PUBLISH_PROMOTION_ACTION_DEMOTE,
            previous_logic_key=current_champion_key,
            new_logic_key=normalized_key,
            source=source,
            reason=reason,
            review=_load_review_for_publish_id(
                publish_id=str(validation.get("publish_id") or ""),
                ops_db_path=ops_db_path,
            ),
            target=validation.get("manifest"),
        ),
    ]
    rollback_event = {
        "event_type": "publish_logic_demoted",
        "action": PUBLISH_PROMOTION_ACTION_DEMOTE,
        "previous_logic_key": normalized_key,
        "new_logic_key": candidate_key,
        "changed_at": _now_iso(),
        "source": source,
        "reason": reason,
        "actor": actor,
        "publish_id": validation.get("publish_id"),
        "artifact_uri": validation.get("artifact_uri"),
        "artifact_checksum": validation.get("artifact_checksum"),
        "queue_order_before": None,
        "queue_order_after": None,
        "rollback_target_logic_key": candidate_key,
    }
    try:
        external_state = _persist_registry_state(
            config_repo=config_repo,
            db_path=db_path,
            registry_state=registry,
            audit_event=rollback_event,
            source_revision=validation.get("publish_id") or normalized_key,
            sync_state="synced",
            degraded=False,
        )
    except Exception as exc:
        return {"ok": False, "reason": "external_registry_write_failed", "error": str(exc), "logic_key": normalized_key}
    snapshot = build_publish_promotion_snapshot(config_repo=config_repo, db_path=db_path, ops_db_path=ops_db_path)
    return {
        "ok": True,
        "changed": True,
        "action": PUBLISH_PROMOTION_ACTION_ROLLBACK,
        "validation": validation,
        "snapshot": snapshot,
        "external_state": external_state,
    }


def retire_challenger_logic_key(
    *,
    config_repo: ConfigRepository,
    logic_key: str,
    source: str,
    reason: str | None = None,
    actor: str | None = None,
    db_path: str | None = None,
    ops_db_path: str | None = None,
) -> dict[str, Any]:
    registry = _load_registry(config_repo, db_path=db_path)
    current_champion = registry.get("champion") if isinstance(registry.get("champion"), dict) else None
    current_champion_key = _normalize_text(current_champion.get("logic_key")) if current_champion else None
    normalized_key = _normalize_text(logic_key)
    if current_champion_key == normalized_key:
        return {"ok": False, "reason": "not_active_challenger", "logic_key": normalized_key}
    return demote_logic_key(
        config_repo=config_repo,
        logic_key=normalized_key,
        source=source,
        reason=reason,
        actor=actor,
        db_path=db_path,
        ops_db_path=ops_db_path,
    )


def rollback_logic_key(
    *,
    config_repo: ConfigRepository,
    logic_key: str | None,
    source: str,
    reason: str | None = None,
    actor: str | None = None,
    db_path: str | None = None,
    ops_db_path: str | None = None,
) -> dict[str, Any]:
    registry = _load_registry(config_repo, db_path=db_path)
    current_champion = registry.get("champion") if isinstance(registry.get("champion"), dict) else None
    current_champion_key = _normalize_text(current_champion.get("logic_key")) if current_champion else None
    rollback_candidates = _registry_rollback_candidates(registry)
    target_key = _normalize_text(logic_key) or _normalize_text(registry.get("previous_stable_champion_logic_key"))
    candidate = None
    if target_key:
        candidate = next((entry for entry in rollback_candidates if _normalize_text(entry.get("logic_key")) == target_key), None)
        if candidate is None and _normalize_text(registry.get("previous_stable_champion_logic_key")) == target_key:
            candidate = registry.get("previous_stable_champion") if isinstance(registry.get("previous_stable_champion"), dict) else None
    elif rollback_candidates:
        candidate = rollback_candidates[-1]
        target_key = _normalize_text(candidate.get("logic_key"))
    if not isinstance(candidate, dict) or not target_key:
        return {"ok": False, "reason": "rollback_candidate_missing", "logic_key": logic_key}

    if current_champion_key == target_key:
        snapshot = build_publish_promotion_snapshot(config_repo=config_repo, db_path=db_path, ops_db_path=ops_db_path)
        return {"ok": True, "changed": False, "action": PUBLISH_PROMOTION_ACTION_ROLLBACK, "snapshot": snapshot}

    target_manifest = _catalog_lookup(db_path=db_path, logic_key=target_key) or candidate
    rollback_target = _build_registry_entry(
        entry=target_manifest,
        role=PUBLISH_ROLE_CHAMPION,
        review=_load_review_for_publish_id(
            publish_id=str(target_manifest.get("publish_id") or target_manifest.get("source_publish_id") or ""),
            ops_db_path=ops_db_path,
        ),
        source_publish_id=_normalize_text(target_manifest.get("publish_id") or target_manifest.get("source_publish_id")),
        promoted_at=_now_iso(),
        promoted_by=actor,
        reason=reason or "rollback_previous_stable",
    )
    registry["previous_stable_champion"] = current_champion
    registry["previous_stable_champion_logic_key"] = current_champion_key
    _push_rollback_candidate(registry, current_champion)
    registry["champion"] = rollback_target
    registry["default_logic_pointer"] = target_key
    demoted_logic_keys = list(registry.get("demoted_logic_keys") or [])
    if current_champion_key and current_champion_key not in demoted_logic_keys:
        demoted_logic_keys.append(current_champion_key)
    registry["demoted_logic_keys"] = demoted_logic_keys
    registry.setdefault("promotion_history", [])
    registry["promotion_history"] = [
        *list(registry.get("promotion_history") or [])[-19:],
        _history_event(
            action=PUBLISH_PROMOTION_ACTION_ROLLBACK,
            previous_logic_key=current_champion_key,
            new_logic_key=target_key,
            source=source,
            reason=reason,
            review=_load_review_for_publish_id(
                publish_id=str(target_manifest.get("publish_id") or ""),
                ops_db_path=ops_db_path,
            ),
            target=target_manifest,
        ),
    ]
    rollback_event = {
        "event_type": "publish_logic_rollback",
        "action": PUBLISH_PROMOTION_ACTION_ROLLBACK,
        "previous_logic_key": current_champion_key,
        "new_logic_key": target_key,
        "changed_at": _now_iso(),
        "source": source,
        "reason": reason,
        "actor": actor,
        "publish_id": target_manifest.get("publish_id"),
        "artifact_uri": target_manifest.get("artifact_uri"),
        "artifact_checksum": target_manifest.get("artifact_checksum"),
        "queue_order_before": None,
        "queue_order_after": None,
        "rollback_target_logic_key": target_key,
    }
    try:
        external_state = _persist_registry_state(
            config_repo=config_repo,
            db_path=db_path,
            registry_state=registry,
            audit_event=rollback_event,
            source_revision=_normalize_text(target_manifest.get("publish_id")) or target_key,
            sync_state="synced",
            degraded=False,
        )
    except Exception as exc:
        return {"ok": False, "reason": "external_registry_write_failed", "error": str(exc), "logic_key": target_key}
    snapshot = build_publish_promotion_snapshot(config_repo=config_repo, db_path=db_path, ops_db_path=ops_db_path)
    return {
        "ok": True,
        "changed": True,
        "action": PUBLISH_PROMOTION_ACTION_ROLLBACK,
        "validation": {
            "logic_key": target_key,
            "review": _load_review_for_publish_id(
                publish_id=str(target_manifest.get("publish_id") or target_manifest.get("source_publish_id") or ""),
                ops_db_path=ops_db_path,
            ),
        },
        "snapshot": snapshot,
        "external_state": external_state,
    }
