from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.backend.services.tradex_experiment_store import resolve_tradex_config_root
from app.backend.services.tradex_research_os_store import (
    JsonFileMissingError,
    JsonParseError,
    JsonReadError,
    JsonShapeError,
    read_json_object_strict,
)

TRADEX_SHADOW_INTEGRATION_SCHEMA_VERSION = "tradex_r2_shadow_integration_state_v7"
TRADEX_SHADOW_MONITORING_SCHEMA_VERSION = "tradex_r2_shadow_monitoring_contract_v7"
TRADEX_SHADOW_ROLLOUT_SCHEMA_VERSION = "tradex_r2_shadow_rollout_boundary_v7"
TRADEX_SHADOW_VERIFY_SCHEMA_VERSION = "tradex_r2_shadow_verify_v7"

TRADEX_SHADOW_INTEGRATION_STATE_FILE = "r2_shadow_integration_state_v7.json"
TRADEX_SHADOW_MONITORING_CONTRACT_FILE = "r2_shadow_monitoring_contract_v7.json"
TRADEX_SHADOW_ROLLOUT_BOUNDARY_FILE = "r2_shadow_rollout_boundary_v7.json"
TRADEX_SHADOW_VERIFY_FILE = "r2_shadow_verify_v7.json"


def _text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, str):
        text = value.strip()
        return text or fallback
    text = str(value).strip()
    return text or fallback


def _bool(value: Any, fallback: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        raw = value.strip().lower()
        if raw in {"1", "true", "yes", "on"}:
            return True
        if raw in {"0", "false", "no", "off"}:
            return False
    return fallback


def _list_of_text(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    out: list[str] = []
    for item in values:
        text = _text(item)
        if text:
            out.append(text)
    return out


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def tradex_shadow_config_root() -> Path:
    return resolve_tradex_config_root()


def tradex_shadow_integration_state_path() -> Path:
    return tradex_shadow_config_root() / TRADEX_SHADOW_INTEGRATION_STATE_FILE


def tradex_shadow_monitoring_contract_path() -> Path:
    return tradex_shadow_config_root() / TRADEX_SHADOW_MONITORING_CONTRACT_FILE


def tradex_shadow_rollout_boundary_path() -> Path:
    return tradex_shadow_config_root() / TRADEX_SHADOW_ROLLOUT_BOUNDARY_FILE


def tradex_shadow_verify_path() -> Path:
    return tradex_shadow_config_root() / TRADEX_SHADOW_VERIFY_FILE


def _load_json(path: Path, *, artifact_name: str) -> tuple[dict[str, Any] | None, list[str]]:
    try:
        payload = read_json_object_strict(path, artifact_name=artifact_name)
    except (JsonFileMissingError, JsonParseError, JsonReadError, JsonShapeError) as exc:
        return None, [str(exc)]
    return payload, []


def _ensure_schema(payload: dict[str, Any] | None, *, expected_schema_version: str, artifact_name: str) -> tuple[dict[str, Any] | None, list[str]]:
    if payload is None:
        return None, [f"{artifact_name}_missing"]
    issues: list[str] = []
    if _text(payload.get("schema_version")) != expected_schema_version:
        issues.append(f"{artifact_name}_schema_version_mismatch")
    return payload, issues


def _normalize_state(payload: dict[str, Any]) -> dict[str, Any]:
    fixed_contract = payload.get("fixed_contract") if isinstance(payload.get("fixed_contract"), dict) else {}
    return {
        "schema_version": _text(payload.get("schema_version"), fallback=TRADEX_SHADOW_INTEGRATION_SCHEMA_VERSION),
        "candidate_id": _text(payload.get("candidate_id")),
        "candidate_name": _text(payload.get("candidate_name")),
        "logic_key": _text(payload.get("logic_key")),
        "acceptance_state": _text(payload.get("acceptance_state")),
        "adoption_readiness": _text(payload.get("adoption_readiness")),
        "production_wiring_readiness": _bool(payload.get("production_wiring_readiness")),
        "compare_method": _text(payload.get("compare_method")),
        "outside_top20_locked": _bool(payload.get("outside_top20_locked")),
        "shadow_only": _bool(payload.get("shadow_only")),
        "publish_candidate_allowed": _bool(payload.get("publish_candidate_allowed")),
        "meeMee_reflect_allowed": _bool(payload.get("meeMee_reflect_allowed")),
        "production_path_allowed": _bool(payload.get("production_path_allowed")),
        "accepted_at": _text(payload.get("accepted_at")),
        "fixed_contract": {
            "logic_key": _text(fixed_contract.get("logic_key")),
            "artifact_name": _text(fixed_contract.get("artifact_name")),
            "artifact_path": _text(fixed_contract.get("artifact_path")),
            "contract_version": _text(fixed_contract.get("contract_version")),
        },
        "non_adopted_alternatives": _list_of_text(payload.get("non_adopted_alternatives")),
        "notes": _list_of_text(payload.get("notes")),
        "monitoring_contract_ref": _text(payload.get("monitoring_contract_ref")),
        "rollout_boundary_ref": _text(payload.get("rollout_boundary_ref")),
        "verify_ref": _text(payload.get("verify_ref")),
    }


def _normalize_monitoring_contract(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": _text(payload.get("schema_version"), fallback=TRADEX_SHADOW_MONITORING_SCHEMA_VERSION),
        "monitored_metrics": _list_of_text(payload.get("monitored_metrics")),
        "required_checks": _list_of_text(payload.get("required_checks")),
        "artifact_boundary": dict(payload.get("artifact_boundary") or {}),
        "fixed_conditions": dict(payload.get("fixed_conditions") or {}),
        "shadow_only": _bool(payload.get("shadow_only")),
        "outside_top20_locked": _bool(payload.get("outside_top20_locked")),
        "rollback_boundary": dict(payload.get("rollback_boundary") or {}),
    }


def _normalize_rollout_boundary(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": _text(payload.get("schema_version"), fallback=TRADEX_SHADOW_ROLLOUT_SCHEMA_VERSION),
        "accepted_for_shadow_integration_only": _text(payload.get("accepted_for_shadow_integration_only")),
        "adoption_readiness": _text(payload.get("adoption_readiness")),
        "production_wiring_readiness": _bool(payload.get("production_wiring_readiness")),
        "shadow_only": _bool(payload.get("shadow_only")),
        "publish_candidate_allowed": _bool(payload.get("publish_candidate_allowed")),
        "meeMee_reflect_allowed": _bool(payload.get("meeMee_reflect_allowed")),
        "production_path_allowed": _bool(payload.get("production_path_allowed")),
        "rollback": dict(payload.get("rollback") or {}),
        "allowed": _list_of_text(payload.get("allowed")),
        "not_allowed": _list_of_text(payload.get("not_allowed")),
    }


def _normalize_verify(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": _text(payload.get("schema_version"), fallback=TRADEX_SHADOW_VERIFY_SCHEMA_VERSION),
        "checks": _list_of_text(payload.get("checks")),
        "expected": dict(payload.get("expected") or {}),
        "observed": dict(payload.get("observed") or {}),
        "pass_expected": _bool(payload.get("pass_expected"), fallback=False),
    }


def load_tradex_shadow_integration_state(*, strict: bool = False) -> dict[str, Any]:
    state_payload, state_issues = _load_json(
        tradex_shadow_integration_state_path(),
        artifact_name="R2 shadow integration state",
    )
    monitoring_payload, monitoring_issues = _load_json(
        tradex_shadow_monitoring_contract_path(),
        artifact_name="R2 shadow monitoring contract",
    )
    rollout_payload, rollout_issues = _load_json(
        tradex_shadow_rollout_boundary_path(),
        artifact_name="R2 shadow rollout boundary",
    )
    verify_payload, verify_issues = _load_json(
        tradex_shadow_verify_path(),
        artifact_name="R2 shadow verify",
    )

    issues: list[str] = []
    if state_payload is None:
        issues.extend(state_issues)
    else:
        _, schema_issues = _ensure_schema(
            state_payload,
            expected_schema_version=TRADEX_SHADOW_INTEGRATION_SCHEMA_VERSION,
            artifact_name="r2_shadow_integration_state",
        )
        issues.extend(schema_issues)
    if monitoring_payload is None:
        issues.extend(monitoring_issues)
    else:
        _, schema_issues = _ensure_schema(
            monitoring_payload,
            expected_schema_version=TRADEX_SHADOW_MONITORING_SCHEMA_VERSION,
            artifact_name="r2_shadow_monitoring_contract",
        )
        issues.extend(schema_issues)
    if rollout_payload is None:
        issues.extend(rollout_issues)
    else:
        _, schema_issues = _ensure_schema(
            rollout_payload,
            expected_schema_version=TRADEX_SHADOW_ROLLOUT_SCHEMA_VERSION,
            artifact_name="r2_shadow_rollout_boundary",
        )
        issues.extend(schema_issues)
    if verify_payload is None:
        issues.extend(verify_issues)
    else:
        _, schema_issues = _ensure_schema(
            verify_payload,
            expected_schema_version=TRADEX_SHADOW_VERIFY_SCHEMA_VERSION,
            artifact_name="r2_shadow_verify",
        )
        issues.extend(schema_issues)

    state = _normalize_state(state_payload or {})
    monitoring_contract = _normalize_monitoring_contract(monitoring_payload or {})
    rollout_boundary = _normalize_rollout_boundary(rollout_payload or {})
    verify = _normalize_verify(verify_payload or {})
    available = not issues
    if strict and not available:
        raise ValueError(f"R2 shadow integration state invalid: {', '.join(issues)}")

    composite = {
        "schema_version": "tradex_r2_shadow_integration_boundary_v7",
        "loaded_at": datetime.now(timezone.utc).isoformat(),
        "config_root": str(tradex_shadow_config_root()),
        "available": available,
        "validation_issues": list(dict.fromkeys(issues)),
        "state": state,
        "monitoring_contract": monitoring_contract,
        "rollout_boundary": rollout_boundary,
        "verify": verify,
        "candidate_id": state.get("candidate_id"),
        "candidate_name": state.get("candidate_name"),
        "logic_key": state.get("logic_key"),
        "acceptance_state": state.get("acceptance_state"),
        "adoption_readiness": state.get("adoption_readiness"),
        "production_wiring_readiness": state.get("production_wiring_readiness"),
        "compare_method": state.get("compare_method"),
        "outside_top20_locked": state.get("outside_top20_locked"),
        "shadow_only": state.get("shadow_only"),
        "publish_candidate_allowed": state.get("publish_candidate_allowed"),
        "meeMee_reflect_allowed": state.get("meeMee_reflect_allowed"),
        "production_path_allowed": state.get("production_path_allowed"),
        "non_adopted_alternatives": state.get("non_adopted_alternatives"),
        "rollback_allowed": bool(_bool(rollout_boundary.get("rollback", {}).get("can_disable_shadow_only"), fallback=False)),
        "config_fingerprint": _stable_hash(
            {
                "state": state,
                "monitoring_contract": monitoring_contract,
                "rollout_boundary": rollout_boundary,
                "verify": verify,
            }
        ),
    }
    return composite
