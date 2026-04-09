from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Final

import duckdb

from app.backend.core.legacy_analysis_control import LEGACY_ANALYSIS_DISABLE_ENV, is_legacy_analysis_disabled
from app.backend.services import tradex_experiment_service as tradex_experiment_service
from app.backend.services import tradex_research_os_contracts as os_contracts
from app.backend.services import tradex_research_os_store as os_store
from app.core.config import config


TRADEX_RESEARCH_ENVIRONMENT_READINESS_SCHEMA_VERSION: Final[str] = "tradex_research_environment_readiness_v1"
TRADEX_RESEARCH_ENVIRONMENT_READINESS_VERSION: Final[str] = "v1"
TRADEX_RESEARCH_ENVIRONMENT_READINESS_STATUS_READY: Final[str] = "ready"
TRADEX_RESEARCH_ENVIRONMENT_READINESS_STATUS_NOT_READY: Final[str] = "not_ready"
TRADEX_RESEARCH_ENVIRONMENT_READINESS_CAUSE_CLASSES: Final[tuple[str, ...]] = (
    "environment_not_ready",
    "database_dependency_missing",
    "required_table_missing",
    "required_table_empty",
    "schema_mismatch",
    "genuine_data_unavailable",
)


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


def environment_readiness_policy_path() -> Path:
    root = Path(__file__).resolve().parents[3]
    return root / "config" / "tradex" / "environment_readiness_policy_v1.json"


def load_environment_readiness_policy() -> dict[str, Any]:
    path = environment_readiness_policy_path()
    payload = os_store.read_json_object_strict(path, artifact_name="environment readiness policy")
    if _text(payload.get("schema_version")) != TRADEX_RESEARCH_ENVIRONMENT_READINESS_SCHEMA_VERSION:
        raise ValueError("environment readiness policy schema_version mismatch")
    if _text(payload.get("environment_readiness_version")) != TRADEX_RESEARCH_ENVIRONMENT_READINESS_VERSION:
        raise ValueError("environment readiness policy version mismatch")
    if _text(payload.get("runner")) != "tradex_research_session":
        raise ValueError("environment readiness policy runner mismatch")
    if not isinstance(payload.get("check_order"), list) or not payload.get("check_order"):
        raise ValueError("environment readiness policy check_order must be a non-empty list")
    if not isinstance(payload.get("required_tables"), list) or not payload.get("required_tables"):
        raise ValueError("environment readiness policy required_tables must be a non-empty list")
    return payload


def _policy_table_spec(policy: dict[str, Any]) -> dict[str, Any]:
    tables = policy.get("required_tables") if isinstance(policy.get("required_tables"), list) else []
    for item in tables:
        if isinstance(item, dict) and _text(item.get("table_name")) == "market_regime_daily":
            return dict(item)
    raise ValueError("environment readiness policy missing market_regime_daily table spec")


def _check(
    *,
    check_id: str,
    passed: bool,
    actual: Any,
    expected: Any,
    cause_source: str,
    remediation_hint: str,
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "check_id": _text(check_id),
        "passed": bool(passed),
        "actual": _json_ready(actual),
        "expected": _json_ready(expected),
        "cause_source": _text(cause_source),
        "remediation_hint": _text(remediation_hint),
    }
    if detail:
        payload["detail"] = _json_ready(detail)
    return payload


def _open_db(db_path: Path) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(db_path), read_only=True)


def _table_names(conn: duckdb.DuckDBPyConnection) -> list[str]:
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name"
    ).fetchall()
    return sorted({_text(row[0]) for row in rows if _text(row[0])})


def _table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    columns: list[str] = []
    for row in rows:
        if len(row) < 2:
            continue
        column_name = _text(row[1])
        if column_name:
            columns.append(column_name)
    return columns


def _table_count(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    *,
    where_clause: str = "",
    params: list[Any] | None = None,
) -> int:
    sql = f"SELECT COUNT(*) FROM {table_name}"
    if where_clause:
        sql += f" WHERE {where_clause}"
    row = conn.execute(sql, params or []).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _regime_rows_for_readiness(
    conn: duckdb.DuckDBPyConnection,
    *,
    label_version: str,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT dt, regime_id, regime_score, label_version
        FROM market_regime_daily
        WHERE label_version = ?
        ORDER BY dt ASC
        """,
        [str(label_version)],
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        dt = _int(row[0])
        regime_id = _text(row[1])
        if dt is None or not regime_id:
            continue
        out.append(
            {
                "dt": dt,
                "date": tradex_experiment_service._format_ymd_int(dt),
                "regime_id": regime_id,
                "regime_tag": tradex_experiment_service._evaluation_regime_bucket(regime_id),
                "regime_score": float(row[2]) if row[2] is not None else None,
                "label_version": _text(row[3], fallback=str(label_version)),
            }
        )
    return out


def _ready_report(
    *,
    policy: dict[str, Any],
    db_path: Path,
    table_name: str,
    minimum_window_count: int,
    cause_class: str = "ready",
    cause_source: str = "environment_ready",
    remediation_hint: str = "",
    checks: list[dict[str, Any]],
    table_exists: bool,
    table_row_count: int,
    label_version_row_count: int,
    observed_columns: list[str],
    missing_columns: list[str],
    selected_window_count: int,
    selected_window_issues: list[str],
    checked_at: str,
) -> dict[str, Any]:
    ready = cause_class == "ready"
    payload = {
        "schema_version": TRADEX_RESEARCH_ENVIRONMENT_READINESS_SCHEMA_VERSION,
        "environment_readiness_version": TRADEX_RESEARCH_ENVIRONMENT_READINESS_VERSION,
        "runner": "tradex_research_session",
        "status": TRADEX_RESEARCH_ENVIRONMENT_READINESS_STATUS_READY if ready else TRADEX_RESEARCH_ENVIRONMENT_READINESS_STATUS_NOT_READY,
        "ready": ready,
        "cause_class": _text(cause_class, fallback="ready" if ready else "environment_not_ready"),
        "cause_source": _text(cause_source, fallback="environment_ready"),
        "remediation_hint": _text(remediation_hint),
        "readiness_checks": [dict(item) for item in checks if isinstance(item, dict)],
        "readiness_summary": {
            "database_path": str(db_path),
            "required_table": table_name,
            "minimum_window_count": int(minimum_window_count),
            "table_exists": bool(table_exists),
            "table_row_count": int(table_row_count),
            "label_version_row_count": int(label_version_row_count),
            "observed_columns": list(observed_columns),
            "missing_columns": list(missing_columns),
            "selected_window_count": int(selected_window_count),
            "selected_window_issues": list(selected_window_issues),
        },
        "checked_at": _text(checked_at),
    }
    payload["readiness_hash"] = _stable_hash(payload)
    return payload


def evaluate_environment_readiness(*, policy: dict[str, Any] | None = None) -> dict[str, Any]:
    loaded_policy = policy or load_environment_readiness_policy()
    checked_at = os_contracts.now_utc_iso()
    checks: list[dict[str, Any]] = []
    table_spec = _policy_table_spec(loaded_policy)
    table_name = _text(table_spec.get("table_name"), fallback="market_regime_daily")
    minimum_window_count = int(table_spec.get("minimum_window_count") or 3)
    expected_columns = [_text(item) for item in table_spec.get("expected_columns") or [] if _text(item)]
    expected_label_version = _text(table_spec.get("expected_label_version"), fallback=tradex_experiment_service.TRADEX_EVAL_REGIME_LABEL_VERSION)

    legacy_analysis_enabled = not is_legacy_analysis_disabled()
    checks.append(
        _check(
            check_id="legacy_analysis_enabled",
            passed=legacy_analysis_enabled,
            actual=not legacy_analysis_enabled,
            expected=False,
            cause_source="legacy_analysis",
            remediation_hint=f"set {LEGACY_ANALYSIS_DISABLE_ENV}=0 before running unshimmed TRADEX research",
        )
    )
    if not legacy_analysis_enabled:
        return _ready_report(
            policy=loaded_policy,
            db_path=Path(config.DB_PATH).expanduser().resolve(strict=False),
            table_name=table_name,
            minimum_window_count=minimum_window_count,
            cause_class="environment_not_ready",
            cause_source="legacy_analysis",
            remediation_hint=f"set {LEGACY_ANALYSIS_DISABLE_ENV}=0 before running unshimmed TRADEX research",
            checks=checks,
            table_exists=False,
            table_row_count=0,
            label_version_row_count=0,
            observed_columns=[],
            missing_columns=[],
            selected_window_count=0,
            selected_window_issues=[],
            checked_at=checked_at,
        )

    db_path = Path(config.DB_PATH).expanduser().resolve(strict=False)
    db_exists = db_path.exists()
    checks.append(
        _check(
            check_id="database_path_exists",
            passed=db_exists,
            actual=str(db_path),
            expected="existing duckdb file",
            cause_source="filesystem",
            remediation_hint="confirm the configured STOCKS_DB_PATH points to a prepared TRADEX DuckDB file",
        )
    )
    if not db_exists:
        return _ready_report(
            policy=loaded_policy,
            db_path=db_path,
            table_name=table_name,
            minimum_window_count=minimum_window_count,
            cause_class="database_dependency_missing",
            cause_source="filesystem",
            remediation_hint="confirm the configured STOCKS_DB_PATH points to a prepared TRADEX DuckDB file",
            checks=checks,
            table_exists=False,
            table_row_count=0,
            label_version_row_count=0,
            observed_columns=[],
            missing_columns=expected_columns,
            selected_window_count=0,
            selected_window_issues=[],
            checked_at=checked_at,
        )

    try:
        conn = _open_db(db_path)
    except Exception as exc:
        checks.append(
            _check(
                check_id="database_open",
                passed=False,
                actual={"error": exc.__class__.__name__, "message": str(exc)},
                expected="read-only duckdb connection",
                cause_source="database_connection",
                remediation_hint="ensure the TRADEX DuckDB file is present, readable, and not blocked by another incompatible open mode",
            )
        )
        return _ready_report(
            policy=loaded_policy,
            db_path=db_path,
            table_name=table_name,
            minimum_window_count=minimum_window_count,
            cause_class="database_dependency_missing",
            cause_source="database_connection",
            remediation_hint="ensure the TRADEX DuckDB file is present, readable, and not blocked by another incompatible open mode",
            checks=checks,
            table_exists=False,
            table_row_count=0,
            label_version_row_count=0,
            observed_columns=[],
            missing_columns=expected_columns,
            selected_window_count=0,
            selected_window_issues=[],
            checked_at=checked_at,
        )

    with conn:
        table_names = _table_names(conn)
        table_exists = table_name in table_names
        checks.append(
            _check(
                check_id="required_table_exists",
                passed=table_exists,
                actual=table_names,
                expected=[table_name],
                cause_source="table_presence",
                remediation_hint=f"run the regime-router foundation build so {table_name} exists before unshimmed TRADEX execution",
            )
        )
        if not table_exists:
            return _ready_report(
                policy=loaded_policy,
                db_path=db_path,
                table_name=table_name,
                minimum_window_count=minimum_window_count,
                cause_class="required_table_missing",
                cause_source="table_presence",
                remediation_hint=f"run the regime-router foundation build so {table_name} exists before unshimmed TRADEX execution",
                checks=checks,
                table_exists=False,
                table_row_count=0,
                label_version_row_count=0,
                observed_columns=[],
                missing_columns=expected_columns,
                selected_window_count=0,
                selected_window_issues=[],
                checked_at=checked_at,
            )

        observed_columns = _table_columns(conn, table_name)
        missing_columns = [column for column in expected_columns if column not in observed_columns]
        checks.append(
            _check(
                check_id="required_table_schema",
                passed=not missing_columns,
                actual=observed_columns,
                expected=expected_columns,
                cause_source="table_schema",
                remediation_hint=f"rebuild or migrate {table_name} so it exposes the expected regime columns",
                detail={"missing_columns": missing_columns},
            )
        )
        if missing_columns:
            return _ready_report(
                policy=loaded_policy,
                db_path=db_path,
                table_name=table_name,
                minimum_window_count=minimum_window_count,
                cause_class="schema_mismatch",
                cause_source="table_schema",
                remediation_hint=f"rebuild or migrate {table_name} so it exposes the expected regime columns",
                checks=checks,
                table_exists=True,
                table_row_count=0,
                label_version_row_count=0,
                observed_columns=observed_columns,
                missing_columns=missing_columns,
                selected_window_count=0,
                selected_window_issues=[],
                checked_at=checked_at,
            )

        table_row_count = _table_count(conn, table_name)
        checks.append(
            _check(
                check_id="required_table_row_count",
                passed=table_row_count > 0,
                actual=table_row_count,
                expected="> 0",
                cause_source="table_rows",
                remediation_hint=f"populate {table_name} before running unshimmed TRADEX execution",
            )
        )
        if table_row_count <= 0:
            return _ready_report(
                policy=loaded_policy,
                db_path=db_path,
                table_name=table_name,
                minimum_window_count=minimum_window_count,
                cause_class="required_table_empty",
                cause_source="table_rows",
                remediation_hint=f"populate {table_name} before running unshimmed TRADEX execution",
                checks=checks,
                table_exists=True,
                table_row_count=0,
                label_version_row_count=0,
                observed_columns=observed_columns,
                missing_columns=[],
                selected_window_count=0,
                selected_window_issues=[],
                checked_at=checked_at,
            )

        label_version_row_count = _table_count(
            conn,
            table_name,
            where_clause="label_version = ?",
            params=[expected_label_version],
        )
        checks.append(
            _check(
                check_id="required_label_version_row_count",
                passed=label_version_row_count > 0,
                actual=label_version_row_count,
                expected=f"> 0 for label_version={expected_label_version}",
                cause_source="label_version_filter",
                remediation_hint=f"rebuild {table_name} for label_version={expected_label_version} or align the readiness policy with the table's label_version",
            )
        )
        regime_rows = _regime_rows_for_readiness(conn, label_version=expected_label_version)
        selected_windows, window_issues = tradex_experiment_service._select_evaluation_windows(
            regime_rows,
            min_trading_days=minimum_window_count,
        )
        selected_window_count = len(selected_windows) if isinstance(selected_windows, list) else 0
        checks.append(
            _check(
                check_id="evaluation_window_probe",
                passed=selected_window_count >= minimum_window_count,
                actual={
                    "regime_row_count": len(regime_rows),
                    "selected_window_count": selected_window_count,
                    "window_issues": list(window_issues or []),
                },
                expected=f">= {minimum_window_count} windows",
                cause_source="evaluation_window_probe",
                remediation_hint="add enough regime coverage for up/down/flat windows before unshimmed TRADEX execution",
                detail={"minimum_window_count": minimum_window_count},
            )
        )
        if label_version_row_count <= 0:
            return _ready_report(
                policy=loaded_policy,
                db_path=db_path,
                table_name=table_name,
                minimum_window_count=minimum_window_count,
                cause_class="genuine_data_unavailable",
                cause_source="label_version_filter",
                remediation_hint=f"rebuild {table_name} for label_version={expected_label_version} or align the readiness policy with the table's label_version",
                checks=checks,
                table_exists=True,
                table_row_count=table_row_count,
                label_version_row_count=0,
                observed_columns=observed_columns,
                missing_columns=[],
                selected_window_count=selected_window_count,
                selected_window_issues=list(window_issues or []),
                checked_at=checked_at,
            )
        if selected_window_count < minimum_window_count:
            return _ready_report(
                policy=loaded_policy,
                db_path=db_path,
                table_name=table_name,
                minimum_window_count=minimum_window_count,
                cause_class="genuine_data_unavailable",
                cause_source="evaluation_window_probe",
                remediation_hint="add enough regime coverage for up/down/flat windows before unshimmed TRADEX execution",
                checks=checks,
                table_exists=True,
                table_row_count=table_row_count,
                label_version_row_count=label_version_row_count,
                observed_columns=observed_columns,
                missing_columns=[],
                selected_window_count=selected_window_count,
                selected_window_issues=list(window_issues or []),
                checked_at=checked_at,
            )

        return _ready_report(
            policy=loaded_policy,
            db_path=db_path,
            table_name=table_name,
            minimum_window_count=minimum_window_count,
            cause_class="ready",
            cause_source="environment_ready",
            remediation_hint="",
            checks=checks,
            table_exists=True,
            table_row_count=table_row_count,
            label_version_row_count=label_version_row_count,
            observed_columns=observed_columns,
            missing_columns=[],
            selected_window_count=selected_window_count,
            selected_window_issues=list(window_issues or []),
            checked_at=checked_at,
        )
