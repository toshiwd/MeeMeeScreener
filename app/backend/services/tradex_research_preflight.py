from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Final

from app.backend.core.legacy_analysis_control import LEGACY_ANALYSIS_DISABLE_ENV, is_legacy_analysis_disabled
from app.backend.services import tradex_research_environment_readiness as environment_readiness_service
from app.backend.services import tradex_experiment_service as tradex_experiment_service
from app.backend.services import tradex_research_os_contracts as os_contracts
from app.backend.services import tradex_research_os_store as os_store


TRADEX_RESEARCH_PREFLIGHT_POLICY_SCHEMA_VERSION: Final[str] = "tradex_research_preflight_policy_v1"
TRADEX_RESEARCH_PREFLIGHT_POLICY_VERSION: Final[str] = "v1"
TRADEX_RESEARCH_PREFLIGHT_REPORT_STATUS_PASSED: Final[str] = "passed"
TRADEX_RESEARCH_PREFLIGHT_REPORT_STATUS_FAILED: Final[str] = "preflight_failed"


def _text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, str):
        text = value.strip()
        return text or fallback
    text = str(value).strip()
    return text or fallback


def _int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(float(value))
        except Exception:
            return None
    return None


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    return value


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def preflight_policy_path() -> Path:
    root = Path(__file__).resolve().parents[3]
    return root / "config" / "tradex" / "preflight_policy_v1.json"


def load_preflight_policy() -> dict[str, Any]:
    path = preflight_policy_path()
    payload = os_store.read_json_object_strict(path, artifact_name="preflight policy")
    if _text(payload.get("schema_version")) != TRADEX_RESEARCH_PREFLIGHT_POLICY_SCHEMA_VERSION:
        raise ValueError("preflight policy schema_version mismatch")
    if _text(payload.get("preflight_policy_version")) != TRADEX_RESEARCH_PREFLIGHT_POLICY_VERSION:
        raise ValueError("preflight policy version mismatch")
    if _text(payload.get("runner")) != "tradex_research_session":
        raise ValueError("preflight policy runner mismatch")
    if not isinstance(payload.get("check_order"), list) or not payload.get("check_order"):
        raise ValueError("preflight policy check_order must be a non-empty list")
    if not isinstance(payload.get("required_execution_fields"), list) or not payload.get("required_execution_fields"):
        raise ValueError("preflight policy required_execution_fields must be a non-empty list")
    if not isinstance(payload.get("failure_codes"), list) or not payload.get("failure_codes"):
        raise ValueError("preflight policy failure_codes must be a non-empty list")
    return payload


def _required_execution_fields(policy: dict[str, Any]) -> list[str]:
    return [_text(item) for item in policy.get("required_execution_fields") or [] if _text(item) and _text(item) != "target_method_family"]


def _min_window_count(policy: dict[str, Any]) -> int:
    value = policy.get("minimum_evaluation_window_count")
    return int(value) if isinstance(value, int) or isinstance(value, float) else 3


def _base_checked_inputs(
    *,
    hypothesis: dict[str, Any],
    execution: dict[str, Any],
    policy: dict[str, Any],
) -> dict[str, Any]:
    return {
        "policy": {
            "schema_version": _text(policy.get("schema_version")),
            "preflight_policy_version": _text(policy.get("preflight_policy_version")),
            "check_order": list(policy.get("check_order") or []),
            "minimum_evaluation_window_count": _min_window_count(policy),
        },
        "hypothesis_id": _text(hypothesis.get("hypothesis_id")),
        "target_method_family": _text(hypothesis.get("target_method_family")),
        "runner": _text(execution.get("runner")),
        "session_id": _text(execution.get("session_id")),
        "session_scope_id": _text(execution.get("session_scope_id")),
        "random_seed": int(execution.get("random_seed") or 0),
        "universe_size": int(execution.get("universe_size") or 0),
        "max_candidates_per_family": int(execution.get("max_candidates_per_family") or 0),
        "ret20_source_mode": _text(execution.get("ret20_source_mode")),
    }


def _provisional_experiment_id(
    *,
    hypothesis_id: str,
    repo_commit: str,
    runner_version: str,
    seed: int,
    started_at: str,
    checked_inputs: dict[str, Any],
    failure_code: str,
) -> str:
    payload = {
        "hypothesis_id": _text(hypothesis_id),
        "repo_commit": _text(repo_commit),
        "runner_version": _text(runner_version),
        "seed": int(seed),
        "started_at": _text(started_at),
        "checked_inputs": _json_ready(checked_inputs),
        "failure_code": _text(failure_code),
    }
    return f"exp_preflight_{_stable_hash(payload)[:20]}"


def _is_mapping_shape(payload: Any, *, required_fields: tuple[str, ...]) -> bool:
    if not isinstance(payload, dict):
        return False
    return all(field in payload for field in required_fields)


def _normalize_regime_tag(regime_id: str) -> str | None:
    raw = _text(regime_id).lower()
    if raw in tradex_experiment_service.TRADEX_EVAL_REGIME_UP_TAGS:
        return "up"
    if raw in tradex_experiment_service.TRADEX_EVAL_REGIME_DOWN_TAGS:
        return "down"
    if raw in tradex_experiment_service.TRADEX_EVAL_REGIME_FLAT_TAGS:
        return "flat"
    return None


def _normalize_regime_rows(regime_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    normalized_rows: list[dict[str, Any]] = []
    issues: list[str] = []
    normalization_applied: list[str] = []
    for index, row in enumerate(regime_rows):
        if not isinstance(row, dict):
            issues.append(f"regime_row_{index}_shape_unrecognized")
            continue
        dt = _int(row.get("dt"))
        regime_id = _text(row.get("regime_id"))
        if dt is None or not regime_id:
            issues.append(f"regime_row_{index}_missing_required_fields")
            continue
        provided_tag = _text(row.get("regime_tag"))
        derived_tag = _normalize_regime_tag(regime_id)
        if provided_tag and derived_tag and provided_tag != derived_tag:
            issues.append(f"regime_row_{index}_regime_tag_mismatch")
            continue
        if not provided_tag:
            if not derived_tag:
                issues.append(f"regime_row_{index}_regime_tag_unrecognized")
                continue
            provided_tag = derived_tag
            normalization_applied.append("regime_tag_derived_from_regime_id")
        normalized_row = dict(row)
        normalized_row["dt"] = dt
        normalized_row["regime_id"] = regime_id
        normalized_row["regime_tag"] = provided_tag
        normalized_rows.append(normalized_row)
    if issues and not normalized_rows:
        normalization_applied.append("regime_rows_shape_rejected")
    elif normalization_applied:
        normalization_applied.append("regime_rows_normalized")
    return normalized_rows, issues, normalization_applied


def _normalize_selected_windows(selected_windows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    normalized_windows: list[dict[str, Any]] = []
    issues: list[str] = []
    normalization_applied: list[str] = []
    for index, window in enumerate(selected_windows):
        if not isinstance(window, dict):
            issues.append(f"evaluation_window_{index}_shape_unrecognized")
            continue
        start_date = _text(window.get("start_date"))
        end_date = _text(window.get("end_date"))
        trading_day_count = _int(window.get("trading_day_count"))
        window_id = _text(window.get("evaluation_window_id"), fallback=_text(window.get("label")))
        regime_id = _text(window.get("regime_id"))
        regime_tag = _text(window.get("regime_tag"))
        if not window_id or not start_date or not end_date or trading_day_count is None:
            issues.append(f"evaluation_window_{index}_missing_required_fields")
            continue
        derived_tag = _normalize_regime_tag(regime_id)
        if regime_tag and derived_tag and regime_tag != derived_tag:
            issues.append(f"evaluation_window_{index}_regime_tag_mismatch")
            continue
        if not regime_tag:
            if derived_tag:
                regime_tag = derived_tag
                normalization_applied.append("evaluation_window_regime_tag_derived_from_regime_id")
            else:
                regime_tag = "flat"
                normalization_applied.append("evaluation_window_regime_tag_defaulted_flat")
        normalized_window = dict(window)
        normalized_window["evaluation_window_id"] = window_id
        normalized_window["regime_tag"] = regime_tag
        normalized_window["start_date"] = start_date
        normalized_window["end_date"] = end_date
        normalized_window["trading_day_count"] = int(trading_day_count)
        if regime_id:
            normalized_window["regime_id"] = regime_id
        normalized_windows.append(normalized_window)
    if issues and not normalized_windows:
        normalization_applied.append("evaluation_windows_shape_rejected")
    elif normalization_applied:
        normalization_applied.append("evaluation_windows_normalized")
    return normalized_windows, issues, normalization_applied


def _build_failure(
    *,
    failure_code: str,
    failure_detail: dict[str, Any],
    checked_inputs: dict[str, Any],
    normalization_applied: list[str],
    checked_at: str,
    provisional_experiment_id: str,
    hypothesis_id: str,
    runner: str,
    cause_class: str = "",
    cause_source: str = "",
    remediation_hint: str = "",
    readiness_checks: list[dict[str, Any]] | None = None,
    readiness_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "provisional_experiment_id": provisional_experiment_id,
        "experiment_id": provisional_experiment_id,
        "hypothesis_id": _text(hypothesis_id),
        "runner": _text(runner),
        "status": TRADEX_RESEARCH_PREFLIGHT_REPORT_STATUS_FAILED,
        "passed": False,
        "failure_code": _text(failure_code, fallback="preflight_failed"),
        "failure_detail": _json_ready(failure_detail),
        "checked_inputs": _json_ready(checked_inputs),
        "normalization_applied": [item for item in normalization_applied if _text(item)],
        "checked_at": _text(checked_at),
    }
    if _text(cause_class):
        payload["cause_class"] = _text(cause_class)
    if _text(cause_source):
        payload["cause_source"] = _text(cause_source)
    if _text(remediation_hint):
        payload["remediation_hint"] = _text(remediation_hint)
    if readiness_checks is not None:
        payload["readiness_checks"] = [dict(item) for item in readiness_checks if isinstance(item, dict)]
    if readiness_summary is not None:
        payload["readiness_summary"] = dict(readiness_summary)
    return payload


def _readiness_audit_view(readiness_report: dict[str, Any]) -> dict[str, Any]:
    summary = readiness_report.get("readiness_summary") if isinstance(readiness_report.get("readiness_summary"), dict) else {}
    return {
        "schema_version": _text(readiness_report.get("schema_version")),
        "environment_readiness_version": _text(readiness_report.get("environment_readiness_version")),
        "status": _text(readiness_report.get("status")),
        "ready": bool(readiness_report.get("ready")),
        "cause_class": _text(readiness_report.get("cause_class")),
        "cause_source": _text(readiness_report.get("cause_source")),
        "remediation_hint": _text(readiness_report.get("remediation_hint")),
        "readiness_checks": [dict(item) for item in readiness_report.get("readiness_checks") or [] if isinstance(item, dict)],
        "readiness_summary": dict(summary),
        "checked_at": _text(readiness_report.get("checked_at")),
    }


def _readiness_audit_kwargs(readiness_audit: dict[str, Any]) -> dict[str, Any]:
    return {
        "cause_class": _text(readiness_audit.get("cause_class")),
        "cause_source": _text(readiness_audit.get("cause_source")),
        "remediation_hint": _text(readiness_audit.get("remediation_hint")),
        "readiness_checks": [dict(item) for item in readiness_audit.get("readiness_checks") or [] if isinstance(item, dict)],
        "readiness_summary": dict(readiness_audit.get("readiness_summary") or {}),
    }


def evaluate_preflight(
    *,
    hypothesis: dict[str, Any],
    repo_commit: str,
    runner_version: str,
    started_at: str,
    policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    loaded_policy = policy or load_preflight_policy()
    checked_at = os_contracts.now_utc_iso()
    normalization_applied: list[str] = []
    if not isinstance(hypothesis, dict):
        hypothesis = {}
    execution = dict(hypothesis.get("execution") or {})
    checked_inputs = _base_checked_inputs(hypothesis=hypothesis, execution=execution, policy=loaded_policy)
    checked_inputs["legacy_analysis_disabled"] = is_legacy_analysis_disabled()
    checked_inputs["legacy_analysis_env"] = _text(os.getenv(LEGACY_ANALYSIS_DISABLE_ENV, "1"))
    checked_inputs["required_execution_fields"] = _required_execution_fields(loaded_policy)
    checked_inputs["minimum_window_count"] = _min_window_count(loaded_policy)
    checked_inputs["preflight_policy_version"] = _text(loaded_policy.get("preflight_policy_version"))

    try:
        os_contracts.validate_hypothesis(hypothesis)
        normalization_applied.append("hypothesis_validated")
    except Exception as exc:
        failure_code = "missing_required_inputs"
        return _build_failure(
            failure_code=failure_code,
            failure_detail={"reason": "hypothesis_validation_failed", "error": exc.__class__.__name__, "message": str(exc)},
            checked_inputs=checked_inputs,
            normalization_applied=normalization_applied,
            checked_at=checked_at,
            provisional_experiment_id=_provisional_experiment_id(
                hypothesis_id=_text(hypothesis.get("hypothesis_id")),
                repo_commit=repo_commit,
                runner_version=runner_version,
                seed=int(execution.get("random_seed") or 0),
                started_at=started_at,
                checked_inputs=checked_inputs,
                failure_code=failure_code,
            ),
            hypothesis_id=_text(hypothesis.get("hypothesis_id")),
            runner=_text(execution.get("runner")),
        )

    if _text(execution.get("runner")) != "tradex_research_session":
        failure_code = "missing_required_inputs"
        return _build_failure(
            failure_code=failure_code,
            failure_detail={"reason": "runner_must_be_tradex_research_session", "runner": _text(execution.get("runner"))},
            checked_inputs=checked_inputs,
            normalization_applied=normalization_applied,
            checked_at=checked_at,
            provisional_experiment_id=_provisional_experiment_id(
                hypothesis_id=_text(hypothesis.get("hypothesis_id")),
                repo_commit=repo_commit,
                runner_version=runner_version,
                seed=int(execution.get("random_seed") or 0),
                started_at=started_at,
                checked_inputs=checked_inputs,
                failure_code=failure_code,
            ),
            hypothesis_id=_text(hypothesis.get("hypothesis_id")),
            runner=_text(execution.get("runner")),
        )
    normalization_applied.append("runner_checked")

    missing_fields = [
        field
        for field in _required_execution_fields(loaded_policy)
        if not _text(execution.get(field))
    ]
    if not _text(hypothesis.get("target_method_family")):
        missing_fields.append("target_method_family")
    if missing_fields:
        failure_code = "missing_required_inputs"
        checked_inputs["missing_required_fields"] = missing_fields
        return _build_failure(
            failure_code=failure_code,
            failure_detail={"reason": "missing_required_execution_fields", "missing_fields": missing_fields},
            checked_inputs=checked_inputs,
            normalization_applied=normalization_applied,
            checked_at=checked_at,
            provisional_experiment_id=_provisional_experiment_id(
                hypothesis_id=_text(hypothesis.get("hypothesis_id")),
                repo_commit=repo_commit,
                runner_version=runner_version,
                seed=int(execution.get("random_seed") or 0),
                started_at=started_at,
                checked_inputs=checked_inputs,
                failure_code=failure_code,
            ),
            hypothesis_id=_text(hypothesis.get("hypothesis_id")),
            runner=_text(execution.get("runner")),
        )
    normalization_applied.append("required_inputs_checked")

    readiness_report = environment_readiness_service.evaluate_environment_readiness()
    readiness_audit = _readiness_audit_view(readiness_report)
    readiness_kwargs = _readiness_audit_kwargs(readiness_audit)
    checked_inputs["environment_readiness"] = readiness_audit

    if checked_inputs["legacy_analysis_disabled"]:
        failure_code = "legacy_analysis_disabled"
        return _build_failure(
            failure_code=failure_code,
            failure_detail={
                "reason": "legacy_analysis_disabled",
                "environment_variable": LEGACY_ANALYSIS_DISABLE_ENV,
                "environment_value": _text(os.getenv(LEGACY_ANALYSIS_DISABLE_ENV, "1")),
                "readiness": readiness_audit,
            },
            checked_inputs=checked_inputs,
            normalization_applied=normalization_applied,
            checked_at=checked_at,
            provisional_experiment_id=_provisional_experiment_id(
                hypothesis_id=_text(hypothesis.get("hypothesis_id")),
                repo_commit=repo_commit,
                runner_version=runner_version,
                seed=int(execution.get("random_seed") or 0),
                started_at=started_at,
                checked_inputs=checked_inputs,
                failure_code=failure_code,
            ),
            hypothesis_id=_text(hypothesis.get("hypothesis_id")),
            runner=_text(execution.get("runner")),
            **readiness_kwargs,
        )
    normalization_applied.append("legacy_analysis_checked")

    try:
        regime_rows, regime_issues = tradex_experiment_service._load_evaluation_regime_rows()
    except Exception as exc:
        failure_code = "evaluation_windows_unavailable"
        return _build_failure(
            failure_code=failure_code,
            failure_detail={"reason": "evaluation_regime_rows_probe_failed", "error": exc.__class__.__name__, "message": str(exc)},
            checked_inputs=checked_inputs,
            normalization_applied=normalization_applied,
            checked_at=checked_at,
            provisional_experiment_id=_provisional_experiment_id(
                hypothesis_id=_text(hypothesis.get("hypothesis_id")),
                repo_commit=repo_commit,
                runner_version=runner_version,
                seed=int(execution.get("random_seed") or 0),
                started_at=started_at,
                checked_inputs=checked_inputs,
                failure_code=failure_code,
            ),
            hypothesis_id=_text(hypothesis.get("hypothesis_id")),
            runner=_text(execution.get("runner")),
            **readiness_kwargs,
        )
    normalization_applied.append("evaluation_regime_rows_loaded")
    checked_inputs["regime_row_issues"] = list(regime_issues or [])
    checked_inputs["regime_row_count"] = len(regime_rows) if isinstance(regime_rows, list) else 0

    if any(_text(item).startswith("market_regime_daily_unavailable") for item in regime_issues or []):
        failure_code = "evaluation_windows_unavailable"
        return _build_failure(
            failure_code=failure_code,
            failure_detail={"reason": "evaluation_windows_unavailable", "issues": list(regime_issues or []), "readiness": readiness_audit},
            checked_inputs=checked_inputs,
            normalization_applied=normalization_applied,
            checked_at=checked_at,
            provisional_experiment_id=_provisional_experiment_id(
                hypothesis_id=_text(hypothesis.get("hypothesis_id")),
                repo_commit=repo_commit,
                runner_version=runner_version,
                seed=int(execution.get("random_seed") or 0),
                started_at=started_at,
                checked_inputs=checked_inputs,
                failure_code=failure_code,
            ),
            hypothesis_id=_text(hypothesis.get("hypothesis_id")),
            runner=_text(execution.get("runner")),
            **readiness_kwargs,
        )
    if any(_text(item).startswith("market_regime_daily_empty") for item in regime_issues or []):
        failure_code = "regime_rows_empty"
        return _build_failure(
            failure_code=failure_code,
            failure_detail={"reason": "regime_rows_empty", "issues": list(regime_issues or []), "readiness": readiness_audit},
            checked_inputs=checked_inputs,
            normalization_applied=normalization_applied,
            checked_at=checked_at,
            provisional_experiment_id=_provisional_experiment_id(
                hypothesis_id=_text(hypothesis.get("hypothesis_id")),
                repo_commit=repo_commit,
                runner_version=runner_version,
                seed=int(execution.get("random_seed") or 0),
                started_at=started_at,
                checked_inputs=checked_inputs,
                failure_code=failure_code,
            ),
            hypothesis_id=_text(hypothesis.get("hypothesis_id")),
            runner=_text(execution.get("runner")),
            **readiness_kwargs,
        )
    if not isinstance(regime_rows, list) or not regime_rows:
        failure_code = "regime_rows_empty"
        return _build_failure(
            failure_code=failure_code,
            failure_detail={"reason": "regime_rows_empty", "issues": list(regime_issues or []), "readiness": readiness_audit},
            checked_inputs=checked_inputs,
            normalization_applied=normalization_applied,
            checked_at=checked_at,
            provisional_experiment_id=_provisional_experiment_id(
                hypothesis_id=_text(hypothesis.get("hypothesis_id")),
                repo_commit=repo_commit,
                runner_version=runner_version,
                seed=int(execution.get("random_seed") or 0),
                started_at=started_at,
                checked_inputs=checked_inputs,
                failure_code=failure_code,
            ),
            hypothesis_id=_text(hypothesis.get("hypothesis_id")),
            runner=_text(execution.get("runner")),
            **readiness_kwargs,
        )
    normalized_regime_rows, regime_row_shape_issues, regime_row_normalization = _normalize_regime_rows(regime_rows)
    checked_inputs["regime_row_shape_issues"] = list(regime_row_shape_issues or [])
    checked_inputs["regime_row_normalization_applied"] = list(regime_row_normalization or [])
    normalization_applied.extend(regime_row_normalization or [])

    if regime_row_shape_issues or not normalized_regime_rows:
        failure_code = "artifact_shape_unrecognized"
        return _build_failure(
            failure_code=failure_code,
            failure_detail={"reason": "regime_row_shape_unrecognized", "issues": list(regime_row_shape_issues or []), "readiness": readiness_audit},
            checked_inputs=checked_inputs,
            normalization_applied=normalization_applied,
            checked_at=checked_at,
            provisional_experiment_id=_provisional_experiment_id(
                hypothesis_id=_text(hypothesis.get("hypothesis_id")),
                repo_commit=repo_commit,
                runner_version=runner_version,
                seed=int(execution.get("random_seed") or 0),
                started_at=started_at,
                checked_inputs=checked_inputs,
                failure_code=failure_code,
            ),
            hypothesis_id=_text(hypothesis.get("hypothesis_id")),
            runner=_text(execution.get("runner")),
            **readiness_kwargs,
        )

    normalization_applied.append("regime_rows_checked")

    try:
        selected_windows, window_issues = tradex_experiment_service._select_evaluation_windows(normalized_regime_rows)
    except Exception as exc:
        failure_code = "evaluation_windows_unavailable"
        return _build_failure(
            failure_code=failure_code,
            failure_detail={"reason": "evaluation_window_selection_failed", "error": exc.__class__.__name__, "message": str(exc), "readiness": readiness_audit},
            checked_inputs=checked_inputs,
            normalization_applied=normalization_applied,
            checked_at=checked_at,
            provisional_experiment_id=_provisional_experiment_id(
                hypothesis_id=_text(hypothesis.get("hypothesis_id")),
                repo_commit=repo_commit,
                runner_version=runner_version,
                seed=int(execution.get("random_seed") or 0),
                started_at=started_at,
                checked_inputs=checked_inputs,
                failure_code=failure_code,
            ),
            hypothesis_id=_text(hypothesis.get("hypothesis_id")),
            runner=_text(execution.get("runner")),
            **readiness_kwargs,
        )
    normalized_selected_windows, window_shape_issues, window_shape_normalization = _normalize_selected_windows(selected_windows)
    checked_inputs["evaluation_window_issues"] = list(window_issues or [])
    checked_inputs["evaluation_window_shape_issues"] = list(window_shape_issues or [])
    checked_inputs["evaluation_window_shape_normalization_applied"] = list(window_shape_normalization or [])
    normalization_applied.extend(window_shape_normalization or [])
    normalization_applied.append("evaluation_windows_selected")
    checked_inputs["evaluation_window_count"] = len(normalized_selected_windows) if isinstance(normalized_selected_windows, list) else 0

    if window_shape_issues or not isinstance(normalized_selected_windows, list) or len(normalized_selected_windows) < _min_window_count(loaded_policy):
        failure_code = "insufficient_evaluation_windows"
        return _build_failure(
            failure_code=failure_code,
            failure_detail={
                "reason": "insufficient_evaluation_windows",
                "issues": list(window_issues or []),
                "shape_issues": list(window_shape_issues or []),
                "selected_window_count": len(normalized_selected_windows) if isinstance(normalized_selected_windows, list) else 0,
                "minimum_window_count": _min_window_count(loaded_policy),
                "readiness": readiness_audit,
            },
            checked_inputs=checked_inputs,
            normalization_applied=normalization_applied,
            checked_at=checked_at,
            provisional_experiment_id=_provisional_experiment_id(
                hypothesis_id=_text(hypothesis.get("hypothesis_id")),
                repo_commit=repo_commit,
                runner_version=runner_version,
                seed=int(execution.get("random_seed") or 0),
                started_at=started_at,
                checked_inputs=checked_inputs,
                failure_code=failure_code,
            ),
            hypothesis_id=_text(hypothesis.get("hypothesis_id")),
            runner=_text(execution.get("runner")),
            **readiness_kwargs,
        )

    if window_shape_issues or any(
        not _is_mapping_shape(window, required_fields=("evaluation_window_id", "regime_tag", "start_date", "end_date", "trading_day_count"))
        for window in normalized_selected_windows
    ):
        failure_code = "artifact_shape_unrecognized"
        return _build_failure(
            failure_code=failure_code,
            failure_detail={"reason": "evaluation_window_shape_unrecognized", "issues": list(window_shape_issues or []), "readiness": readiness_audit},
            checked_inputs=checked_inputs,
            normalization_applied=normalization_applied,
            checked_at=checked_at,
            provisional_experiment_id=_provisional_experiment_id(
                hypothesis_id=_text(hypothesis.get("hypothesis_id")),
                repo_commit=repo_commit,
                runner_version=runner_version,
                seed=int(execution.get("random_seed") or 0),
                started_at=started_at,
                checked_inputs=checked_inputs,
                failure_code=failure_code,
            ),
            hypothesis_id=_text(hypothesis.get("hypothesis_id")),
            runner=_text(execution.get("runner")),
            **readiness_kwargs,
        )

    checked_inputs["selected_window_ids"] = [_text(window.get("evaluation_window_id")) for window in normalized_selected_windows]
    normalization_applied.append("artifact_shape_checked")

    preflight_fingerprint = _stable_hash(
        {
            "policy_version": _text(loaded_policy.get("preflight_policy_version")),
            "checked_inputs": _json_ready(checked_inputs),
            "normalization_applied": list(normalization_applied),
        }
    )
    provisional_experiment_id = _provisional_experiment_id(
        hypothesis_id=_text(hypothesis.get("hypothesis_id")),
        repo_commit=repo_commit,
        runner_version=runner_version,
        seed=int(execution.get("random_seed") or 0),
        started_at=started_at,
        checked_inputs={"preflight_fingerprint": preflight_fingerprint, "checked_inputs": checked_inputs},
        failure_code="",
    )
    return {
        "provisional_experiment_id": provisional_experiment_id,
        "experiment_id": provisional_experiment_id,
        "hypothesis_id": _text(hypothesis.get("hypothesis_id")),
        "runner": _text(execution.get("runner")),
        "status": TRADEX_RESEARCH_PREFLIGHT_REPORT_STATUS_PASSED,
        "passed": True,
        "failure_code": "",
        "failure_detail": {},
        "checked_inputs": _json_ready(checked_inputs),
        "normalization_applied": [item for item in normalization_applied if _text(item)],
        "checked_at": checked_at,
        "preflight_fingerprint": preflight_fingerprint,
        **readiness_kwargs,
    }
