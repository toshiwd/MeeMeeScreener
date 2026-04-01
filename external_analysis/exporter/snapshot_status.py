from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import duckdb

from external_analysis.contracts.paths import resolve_export_db_path, resolve_source_db_path
from external_analysis.exporter.diff_export import _source_signature, run_diff_export
from external_analysis.exporter.export_schema import ensure_export_db
from external_analysis.exporter.source_reader import connect_source_db, normalize_market_date, source_table_exists

EXPORT_SNAPSHOT_SCHEMA_VERSION = "tradex_export_snapshot_status_v1"
EXPORT_SNAPSHOT_PROGRESS_SCHEMA_VERSION = "tradex_export_snapshot_progress_v1"

EXPORT_SNAPSHOT_STATUS_MISSING = "missing"
EXPORT_SNAPSHOT_STATUS_INCOMPLETE = "incomplete"
EXPORT_SNAPSHOT_STATUS_STALE = "stale"
EXPORT_SNAPSHOT_STATUS_MISMATCHED = "mismatched"
EXPORT_SNAPSHOT_STATUS_COMPLETE = "complete"
EXPORT_SNAPSHOT_STATUS_FAILED = "failed"

EXPORT_SNAPSHOT_REASON_SNAPSHOT_STATUS_MISSING = "snapshot_status_missing"
EXPORT_SNAPSHOT_REASON_META_MISSING = "meta_missing"
EXPORT_SNAPSHOT_REASON_REQUIRED_TABLE_MISSING = "required_table_missing"
EXPORT_SNAPSHOT_REASON_REQUIRED_COUNT_MISMATCH = "required_count_mismatch"
EXPORT_SNAPSHOT_REASON_MAX_TRADE_DATE_MISMATCH = "max_trade_date_mismatch"
EXPORT_SNAPSHOT_REASON_SOURCE_SIGNATURE_MISMATCH = "source_signature_mismatch"
EXPORT_SNAPSHOT_REASON_EXPORT_INCOMPLETE = "export_incomplete"
EXPORT_SNAPSHOT_REASON_COMPLETE_MATCH = "complete_match"

EXPORT_SNAPSHOT_REQUIRED_FIELDS: tuple[str, ...] = (
    "bars_count",
    "indicator_count",
    "pattern_count",
    "max_trade_date",
)

EXPORT_SNAPSHOT_REQUIRED_TABLES: tuple[str, ...] = (
    "bars_daily_export",
    "indicator_daily_export",
    "pattern_state_export",
)

SOURCE_ROW_COUNT_TABLES: tuple[str, ...] = (
    "daily_bars",
    "monthly_bars",
    "daily_ma",
    "feature_snapshot_daily",
    "positions_live",
    "position_rounds",
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)
    return path


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def _load_snapshot_progress(export_db_path: Path) -> dict[str, Any] | None:
    progress_path = resolve_snapshot_progress_path(export_db_path)
    if not progress_path.exists():
        return None
    return _read_json(progress_path)


def resolve_snapshot_status_path(export_db_path: str | Path) -> Path:
    export_path = Path(str(export_db_path)).expanduser().resolve()
    return export_path.with_name(f"{export_path.name}.snapshot_status.json")


def resolve_snapshot_progress_path(export_db_path: str | Path) -> Path:
    export_path = Path(str(export_db_path)).expanduser().resolve()
    return export_path.with_name(f"{export_path.name}.snapshot_progress.json")


def build_source_signature_payload(source_db_path: str | Path | None) -> dict[str, Any]:
    resolved_source_db_path = resolve_source_db_path(str(source_db_path) if source_db_path is not None else None)
    source_conn = connect_source_db(str(resolved_source_db_path))
    try:
        source_counts = {
            table_name: (
                int(source_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])
                if source_table_exists(source_conn, table_name)
                else 0
            )
            for table_name in SOURCE_ROW_COUNT_TABLES
        }
        max_trade_row = (
            source_conn.execute("SELECT MAX(date) FROM daily_bars").fetchone()
            if source_table_exists(source_conn, "daily_bars")
            else None
        )
        max_trade_date = normalize_market_date(max_trade_row[0]) if max_trade_row and max_trade_row[0] is not None else None
    finally:
        source_conn.close()
    expected_export_signature = {
        "bars_count": int(source_counts.get("daily_bars") or 0),
        "indicator_count": int(source_counts.get("feature_snapshot_daily") or 0),
        "pattern_count": int(source_counts.get("feature_snapshot_daily") or 0),
        "max_trade_date": int(max_trade_date or 0),
    }
    return {
        "source_db_path": str(resolved_source_db_path),
        "source_signature": _source_signature(source_counts, max_trade_date),
        "source_counts": source_counts,
        "expected_export_signature": expected_export_signature,
        "source_max_trade_date": int(max_trade_date or 0),
    }


def _load_latest_export_run_metadata(export_db_path: Path) -> dict[str, Any] | None:
    if not export_db_path.exists():
        return None
    conn = duckdb.connect(str(export_db_path), read_only=True)
    try:
        has_meta = bool(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = 'main' AND table_name = 'meta_export_runs'
                """
            ).fetchone()[0]
        )
        if not has_meta:
            return None
        row = conn.execute(
            """
            SELECT run_id, status, source_db_path, source_signature, source_max_trade_date
            FROM meta_export_runs
            ORDER BY started_at DESC, run_id DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    return {
        "run_id": str(row[0]),
        "status": str(row[1]),
        "source_db_path": str(row[2]),
        "source_signature": str(row[3]),
        "source_max_trade_date": int(row[4]) if row[4] is not None else None,
    }


def _collect_export_state(export_db_path: Path) -> dict[str, Any]:
    default_counts = {
        "bars_count": 0,
        "indicator_count": 0,
        "pattern_count": 0,
        "max_trade_date": 0,
    }
    default_presence = {table_name: False for table_name in (*EXPORT_SNAPSHOT_REQUIRED_TABLES, "meta_export_runs")}
    if not export_db_path.exists():
        return {
            "db_exists": False,
            "table_presence": default_presence,
            "export_counts": default_counts,
        }
    conn = duckdb.connect(str(export_db_path), read_only=True)
    try:
        tables = {
            str(row[0])
            for row in conn.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }
        table_presence = {
            "bars_daily_export": "bars_daily_export" in tables,
            "indicator_daily_export": "indicator_daily_export" in tables,
            "pattern_state_export": "pattern_state_export" in tables,
            "meta_export_runs": "meta_export_runs" in tables,
        }
        export_counts = dict(default_counts)
        if table_presence["bars_daily_export"]:
            row = conn.execute("SELECT COUNT(*), MAX(trade_date) FROM bars_daily_export").fetchone()
            export_counts["bars_count"] = int(row[0] or 0)
            export_counts["max_trade_date"] = int(row[1] or 0)
        if table_presence["indicator_daily_export"]:
            row = conn.execute("SELECT COUNT(*) FROM indicator_daily_export").fetchone()
            export_counts["indicator_count"] = int(row[0] or 0)
        if table_presence["pattern_state_export"]:
            row = conn.execute("SELECT COUNT(*) FROM pattern_state_export").fetchone()
            export_counts["pattern_count"] = int(row[0] or 0)
    finally:
        conn.close()
    return {
        "db_exists": True,
        "table_presence": table_presence,
        "export_counts": export_counts,
    }


def _base_snapshot_payload(*, source_payload: dict[str, Any], export_db_path: Path) -> dict[str, Any]:
    return {
        "schema_version": EXPORT_SNAPSHOT_SCHEMA_VERSION,
        "source_db_path": str(source_payload["source_db_path"]),
        "export_db_path": str(export_db_path),
        "source_signature": str(source_payload["source_signature"]),
        "source_counts": dict(source_payload["source_counts"]),
        "expected_export_signature": dict(source_payload["expected_export_signature"]),
        "required_fields": list(EXPORT_SNAPSHOT_REQUIRED_FIELDS),
    }


def _evaluate_snapshot_state(
    *,
    source_payload: dict[str, Any],
    export_state: dict[str, Any],
    latest_export_run: dict[str, Any] | None,
    snapshot_status: dict[str, Any] | None,
) -> tuple[str, str, dict[str, Any]]:
    export_signature = dict(export_state["export_counts"])
    table_presence = dict(export_state["table_presence"])
    snapshot_status_value = str((snapshot_status or {}).get("status") or "")
    snapshot_source_signature = str((snapshot_status or {}).get("source_signature") or "")

    if snapshot_status is None:
        return EXPORT_SNAPSHOT_STATUS_MISSING, EXPORT_SNAPSHOT_REASON_SNAPSHOT_STATUS_MISSING, export_signature

    missing_tables = [table_name for table_name in EXPORT_SNAPSHOT_REQUIRED_TABLES if not bool(table_presence.get(table_name))]
    if missing_tables:
        return EXPORT_SNAPSHOT_STATUS_INCOMPLETE, EXPORT_SNAPSHOT_REASON_REQUIRED_TABLE_MISSING, export_signature

    if latest_export_run is None:
        return EXPORT_SNAPSHOT_STATUS_INCOMPLETE, EXPORT_SNAPSHOT_REASON_META_MISSING, export_signature

    if snapshot_status_value != EXPORT_SNAPSHOT_STATUS_COMPLETE:
        return EXPORT_SNAPSHOT_STATUS_INCOMPLETE, EXPORT_SNAPSHOT_REASON_EXPORT_INCOMPLETE, export_signature

    expected_signature = source_payload["expected_export_signature"]
    if int(export_signature["max_trade_date"] or 0) != int(expected_signature["max_trade_date"] or 0):
        return EXPORT_SNAPSHOT_STATUS_STALE, EXPORT_SNAPSHOT_REASON_MAX_TRADE_DATE_MISMATCH, export_signature

    current_source_signature = str(source_payload["source_signature"])
    if (
        str(latest_export_run.get("source_signature") or "") != current_source_signature
        or snapshot_source_signature != current_source_signature
    ):
        return EXPORT_SNAPSHOT_STATUS_MISMATCHED, EXPORT_SNAPSHOT_REASON_SOURCE_SIGNATURE_MISMATCH, export_signature

    mismatched_fields = [
        field_name
        for field_name in EXPORT_SNAPSHOT_REQUIRED_FIELDS
        if int(export_signature.get(field_name) or 0) != int(expected_signature.get(field_name) or 0)
    ]
    if mismatched_fields:
        return EXPORT_SNAPSHOT_STATUS_INCOMPLETE, EXPORT_SNAPSHOT_REASON_REQUIRED_COUNT_MISMATCH, export_signature

    return EXPORT_SNAPSHOT_STATUS_COMPLETE, EXPORT_SNAPSHOT_REASON_COMPLETE_MATCH, export_signature


def _summarize_progress(snapshot_progress: dict[str, Any] | None) -> dict[str, Any]:
    if snapshot_progress is None:
        return {
            "progress_status": None,
            "progress_path": None,
            "last_completed_step": None,
            "incomplete_steps": [],
        }
    raw_steps = snapshot_progress.get("steps") or []
    if isinstance(raw_steps, dict):
        steps = []
        for step_name, step_payload in raw_steps.items():
            item = dict(step_payload) if isinstance(step_payload, dict) else {}
            item.setdefault("step_name", str(step_name))
            item.setdefault("step_kind", "export")
            steps.append(item)
    else:
        steps = list(raw_steps)
    completed_statuses = {"complete", "resumed", "skipped"}
    completed_steps = [step for step in steps if str(step.get("status") or "") in completed_statuses]
    incomplete_steps = [str(step.get("step_name") or "") for step in steps if str(step.get("status") or "") not in completed_statuses]
    last_completed_step = None
    if completed_steps:
        completed_steps.sort(key=lambda step: str(step.get("finished_at") or step.get("started_at") or ""))
        last_completed_step = str(completed_steps[-1].get("step_name") or "")
    return {
        "progress_status": snapshot_progress.get("status"),
        "progress_path": snapshot_progress.get("progress_path"),
        "last_completed_step": last_completed_step,
        "incomplete_steps": incomplete_steps,
    }


def _build_probe_payload(
    *,
    source_payload: dict[str, Any],
    export_db_path: Path,
    export_state: dict[str, Any],
    latest_export_run: dict[str, Any] | None,
    snapshot_status: dict[str, Any] | None,
    snapshot_progress: dict[str, Any] | None,
) -> dict[str, Any]:
    status, reason_code, export_signature = _evaluate_snapshot_state(
        source_payload=source_payload,
        export_state=export_state,
        latest_export_run=latest_export_run,
        snapshot_status=snapshot_status,
    )
    payload = {
        **_base_snapshot_payload(source_payload=source_payload, export_db_path=export_db_path),
        "status": status,
        "reason_code": reason_code,
        "reusable": status == EXPORT_SNAPSHOT_STATUS_COMPLETE,
        "snapshot_status_path": str(resolve_snapshot_status_path(export_db_path)),
        "snapshot_progress_path": str(resolve_snapshot_progress_path(export_db_path)),
        "export_signature": export_signature,
        "export_counts": dict(export_state["export_counts"]),
        "table_presence": dict(export_state["table_presence"]),
        "latest_export_run": latest_export_run,
        "snapshot_status": snapshot_status,
        "snapshot_progress": snapshot_progress,
    }
    payload.update(_summarize_progress(snapshot_progress))
    if reason_code == EXPORT_SNAPSHOT_REASON_REQUIRED_TABLE_MISSING:
        payload["missing_tables"] = [
            table_name for table_name in EXPORT_SNAPSHOT_REQUIRED_TABLES if not bool(export_state["table_presence"].get(table_name))
        ]
    return payload


def probe_export_snapshot_readiness(source_db_path: str | Path | None, export_db_path: str | Path | None) -> dict[str, Any]:
    resolved_export_db_path = resolve_export_db_path(str(export_db_path) if export_db_path is not None else None)
    source_payload = build_source_signature_payload(source_db_path)
    export_state = _collect_export_state(resolved_export_db_path)
    latest_export_run = _load_latest_export_run_metadata(resolved_export_db_path)
    snapshot_status_path = resolve_snapshot_status_path(resolved_export_db_path)
    snapshot_status = _read_json(snapshot_status_path) if snapshot_status_path.exists() else None
    snapshot_progress = _load_snapshot_progress(resolved_export_db_path)
    if snapshot_progress is not None:
        snapshot_progress = {
            **snapshot_progress,
            "progress_path": str(resolve_snapshot_progress_path(resolved_export_db_path)),
        }
    return _build_probe_payload(
        source_payload=source_payload,
        export_db_path=resolved_export_db_path,
        export_state=export_state,
        latest_export_run=latest_export_run,
        snapshot_status=snapshot_status,
        snapshot_progress=snapshot_progress,
    )


def build_export_snapshot(
    source_db_path: str | Path | None,
    export_db_path: str | Path | None,
    *,
    export_runner: Callable[[str, str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    resolved_export_db_path = resolve_export_db_path(str(export_db_path) if export_db_path is not None else None)
    source_payload = build_source_signature_payload(source_db_path)
    snapshot_status_path = resolve_snapshot_status_path(resolved_export_db_path)
    ensure_export_db(str(resolved_export_db_path))

    starting_payload = {
        **_base_snapshot_payload(source_payload=source_payload, export_db_path=resolved_export_db_path),
        "created_at": _utc_now_iso(),
        "status": EXPORT_SNAPSHOT_STATUS_INCOMPLETE,
        "reason_code": EXPORT_SNAPSHOT_REASON_EXPORT_INCOMPLETE,
        "export_run_id": None,
        "export_signature": {"bars_count": 0, "indicator_count": 0, "pattern_count": 0, "max_trade_date": 0},
        "export_counts": {"bars_count": 0, "indicator_count": 0, "pattern_count": 0, "max_trade_date": 0},
    }
    _write_json(snapshot_status_path, starting_payload)

    try:
        runner = export_runner or run_diff_export
        export_payload = runner(str(source_payload["source_db_path"]), str(resolved_export_db_path))
        export_state = _collect_export_state(resolved_export_db_path)
        latest_export_run = _load_latest_export_run_metadata(resolved_export_db_path)
        status, reason_code, export_signature = _evaluate_snapshot_state(
            source_payload=source_payload,
            export_state=export_state,
            latest_export_run=latest_export_run,
            snapshot_status={
                "status": EXPORT_SNAPSHOT_STATUS_COMPLETE,
                "source_signature": str(source_payload["source_signature"]),
            },
        )
        snapshot_payload = {
            **_base_snapshot_payload(source_payload=source_payload, export_db_path=resolved_export_db_path),
            "created_at": _utc_now_iso(),
            "status": status,
            "reason_code": reason_code,
            "export_run_id": export_payload.get("run_id"),
            "export_signature": export_signature,
            "export_counts": dict(export_state["export_counts"]),
        }
        _write_json(snapshot_status_path, snapshot_payload)
        return probe_export_snapshot_readiness(source_payload["source_db_path"], resolved_export_db_path)
    except Exception:
        failure_payload = {
            **_base_snapshot_payload(source_payload=source_payload, export_db_path=resolved_export_db_path),
            "created_at": _utc_now_iso(),
            "status": EXPORT_SNAPSHOT_STATUS_FAILED,
            "reason_code": EXPORT_SNAPSHOT_REASON_EXPORT_INCOMPLETE,
            "export_run_id": None,
            "export_signature": {"bars_count": 0, "indicator_count": 0, "pattern_count": 0, "max_trade_date": 0},
            "export_counts": {"bars_count": 0, "indicator_count": 0, "pattern_count": 0, "max_trade_date": 0},
        }
        _write_json(snapshot_status_path, failure_payload)
        raise
