from __future__ import annotations

import json
import shutil
import tempfile
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import patch
from uuid import uuid4

import duckdb

from app.backend.services.tradex_research_bridge_service import (
    get_internal_state_eval_action_queue,
    get_internal_state_eval_candle_combo_trend_summary,
    get_internal_state_eval_daily_summary,
    get_internal_state_eval_daily_summary_history,
    get_internal_state_eval_promotion_review,
    get_internal_state_eval_trend_summary,
)
from external_analysis.contracts.paths import (
    resolve_export_db_path,
    resolve_ops_db_path,
    resolve_result_db_path,
    resolve_source_db_path,
)
from external_analysis.exporter.snapshot_status import resolve_snapshot_progress_path
from external_analysis.ops.store import persist_review_artifact
from external_analysis.runtime.daily_research_prepare import probe_daily_research_prepared_environment
from external_analysis.runtime.nightly_pipeline import run_nightly_candidate_pipeline
from external_analysis.runtime.nightly_similarity_challenger_pipeline import run_nightly_similarity_challenger_pipeline
from external_analysis.runtime.nightly_similarity_pipeline import run_nightly_similarity_pipeline
from external_analysis.runtime.source_snapshot import create_source_snapshot
from shared.tradex_storage import tradex_scratch_path

DAILY_RESEARCH_LOOKBACK_DAYS = 5
DAILY_RESEARCH_STALE_RUNNING_HOURS = 1.0
DAILY_RESEARCH_PROGRESS_SCHEMA_VERSION = "tradex_daily_research_progress_v1"


def _normalized_trade_date_expr(column_name: str = "date") -> str:
    return f"""
        CASE
            WHEN {column_name} BETWEEN 19000101 AND 20991231 THEN {column_name}
            WHEN {column_name} >= 1000000000000 THEN CAST(strftime(to_timestamp({column_name} / 1000), '%Y%m%d') AS INTEGER)
            WHEN {column_name} >= 1000000000 THEN CAST(strftime(to_timestamp({column_name}), '%Y%m%d') AS INTEGER)
            ELSE NULL
        END
    """.strip()


def _latest_as_of_query() -> str:
    return """
        SELECT
            MAX({normalized_expr}) AS max_ymd
        FROM daily_bars
    """.format(normalized_expr=_normalized_trade_date_expr())


def resolve_latest_daily_research_as_of_date(*, source_db_path: str | None = None) -> str:
    conn = duckdb.connect(str(resolve_source_db_path(source_db_path)), read_only=True)
    try:
        row = conn.execute(_latest_as_of_query()).fetchone()
    finally:
        conn.close()
    if not row or row[0] is None:
        raise RuntimeError("latest_as_of_not_found")
    return str(int(row[0]))


def resolve_recent_daily_research_as_of_dates(
    *,
    source_db_path: str | None = None,
    limit: int = DAILY_RESEARCH_LOOKBACK_DAYS,
) -> list[str]:
    effective_limit = max(1, min(int(limit), 50))
    conn = duckdb.connect(str(resolve_source_db_path(source_db_path)), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT CAST(normalized_date AS VARCHAR)
            FROM (
                SELECT {normalized_expr} AS normalized_date
                FROM daily_bars
            )
            WHERE normalized_date IS NOT NULL
            ORDER BY normalized_date DESC
            LIMIT ?
            """.format(normalized_expr=_normalized_trade_date_expr()),
            [effective_limit],
        ).fetchall()
    finally:
        conn.close()
    return [str(row[0]) for row in rows if row and row[0] is not None]


def resolve_recent_daily_research_as_of_dates_from_export(
    *,
    export_db_path: str | None = None,
    limit: int = DAILY_RESEARCH_LOOKBACK_DAYS,
) -> list[str]:
    effective_limit = max(1, min(int(limit), 50))
    conn = duckdb.connect(str(resolve_export_db_path(export_db_path)), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT CAST(trade_date AS VARCHAR)
            FROM bars_daily_export
            WHERE trade_date IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            [effective_limit],
        ).fetchall()
    finally:
        conn.close()
    return [str(row[0]) for row in rows if row and row[0] is not None]


def resolve_latest_daily_research_as_of_date_from_export(*, export_db_path: str | None = None) -> str:
    conn = duckdb.connect(str(resolve_export_db_path(export_db_path)), read_only=True)
    try:
        row = conn.execute("SELECT MAX(trade_date) FROM bars_daily_export").fetchone()
    finally:
        conn.close()
    if not row or row[0] is None:
        raise RuntimeError("latest_as_of_not_found")
    return str(int(row[0]))


def _wal_path(db_path: Path) -> Path:
    return Path(f"{db_path}.wal")


@contextmanager
def _temporary_duckdb_snapshot_path(*, db_path: str | Path | None, label: str) -> Iterator[Path | None]:
    if db_path is None:
        yield None
        return
    resolved = Path(str(db_path)).expanduser().resolve()
    if not resolved.exists():
        yield None
        return
    temp_root = tradex_scratch_path("temp", "meemee_daily_research_snapshots").resolve()
    temp_root.mkdir(parents=True, exist_ok=True)
    temp_dir = (temp_root / f"{label}_{uuid4().hex}").resolve()
    temp_dir.mkdir(parents=True, exist_ok=True)
    try:
        target = temp_dir / resolved.name
        shutil.copy2(str(resolved), str(target))
        source_wal = _wal_path(resolved)
        if source_wal.exists():
            shutil.copy2(str(source_wal), str(_wal_path(target)))
        yield target
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def _load_json_value(value: Any) -> dict[str, Any]:
    try:
        return json.loads(str(value)) if value is not None else {}
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def _write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    temp_path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_output_path(path: str | None) -> Path | None:
    if not path:
        return None
    return Path(str(path)).expanduser().resolve()


def _write_progress(path: Path | None, payload: dict[str, Any]) -> None:
    if path is None:
        return
    materialized = dict(payload)
    materialized["updated_at"] = _utcnow_iso()
    _write_json_file(path, materialized)


def _observer_snapshot_path(*, ops_db_path: str | None = None) -> Path:
    resolved = resolve_ops_db_path(ops_db_path)
    return resolved.with_name(f"{resolved.stem}.observer.json")


def _load_ops_observer_snapshot_payload(*, ops_db_path: str | None = None) -> dict[str, Any] | None:
    snapshot_path = _observer_snapshot_path(ops_db_path=ops_db_path)
    if not snapshot_path.exists():
        return None
    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("ops_observer_snapshot_invalid")
    return payload


def _table_exists(conn: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
        LIMIT 1
        """,
        [str(table_name)],
    ).fetchone()
    return bool(row)


def _normalize_job_run_status(*, status: str | None, started_at: Any, created_at: Any, finished_at: Any) -> str:
    base_status = str(status or "").strip() or "unknown"
    if base_status != "running" or finished_at is not None:
        return base_status
    age_anchor = started_at or created_at
    if age_anchor is None:
        return base_status
    if isinstance(age_anchor, str):
        try:
            age_anchor = datetime.fromisoformat(age_anchor)
        except ValueError:
            return base_status
    if not isinstance(age_anchor, datetime):
        return base_status
    age_hours = max((datetime.now(timezone.utc).replace(tzinfo=None) - age_anchor).total_seconds(), 0.0) / 3600.0
    if age_hours >= DAILY_RESEARCH_STALE_RUNNING_HOURS:
        return "stale_running"
    return base_status


def _read_ops_query_rows(
    *,
    ops_db_path: str | None,
    table_name: str,
    query: str,
    params: list[Any],
) -> list[tuple[Any, ...]]:
    resolved = resolve_ops_db_path(ops_db_path)
    if not resolved.exists():
        return []
    conn = duckdb.connect(str(resolved), read_only=True)
    try:
        if not _table_exists(conn, table_name):
            return []
        return conn.execute(query, params).fetchall()
    finally:
        conn.close()


def _load_stale_running_job_observations(*, ops_db_path: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
    effective_limit = max(1, min(int(limit), 200))
    snapshot_rows: list[dict[str, Any]] | None = None
    try:
        rows = _read_ops_query_rows(
            ops_db_path=ops_db_path,
            table_name="external_job_runs",
            query="""
            SELECT
                job_id,
                job_type,
                status,
                CAST(as_of_date AS VARCHAR),
                publish_id,
                created_at,
                started_at,
                finished_at
            FROM external_job_runs
            WHERE status = 'running'
            ORDER BY COALESCE(started_at, created_at) DESC, job_id DESC
            LIMIT ?
            """,
            params=[effective_limit],
        )
    except (duckdb.Error, OSError, PermissionError) as exc:
        snapshot_payload = _load_ops_observer_snapshot_payload(ops_db_path=ops_db_path)
        if snapshot_payload is None:
            raise RuntimeError("ops_observer_snapshot_unavailable") from exc
        raw_snapshot_rows = snapshot_payload.get("external_job_runs") or []
        if not isinstance(raw_snapshot_rows, list):
            raise RuntimeError("ops_observer_snapshot_invalid")
        snapshot_rows = [row for row in raw_snapshot_rows if isinstance(row, dict)]
        rows = []
    items: list[dict[str, Any]] = []
    if snapshot_rows is not None:
        for row in snapshot_rows[:effective_limit]:
            normalized_status = _normalize_job_run_status(
                status=row.get("status"),
                started_at=row.get("started_at"),
                created_at=row.get("created_at"),
                finished_at=row.get("finished_at"),
            )
            if normalized_status != "stale_running":
                continue
            items.append(
                {
                    "job_id": str(row.get("job_id") or ""),
                    "job_type": str(row.get("job_type") or ""),
                    "status": normalized_status,
                    "as_of_date": row.get("as_of_date"),
                    "publish_id": row.get("publish_id"),
                    "created_at": None if row.get("created_at") is None else str(row.get("created_at")),
                    "started_at": None if row.get("started_at") is None else str(row.get("started_at")),
                    "finished_at": None if row.get("finished_at") is None else str(row.get("finished_at")),
                }
            )
        return items
    for row in rows:
        normalized_status = _normalize_job_run_status(
            status=row[2],
            started_at=row[6],
            created_at=row[5],
            finished_at=row[7],
        )
        if normalized_status != "stale_running":
            continue
        items.append(
            {
                "job_id": str(row[0]),
                "job_type": str(row[1]),
                "status": normalized_status,
                "as_of_date": row[3],
                "publish_id": row[4],
                "created_at": None if row[5] is None else str(row[5]),
                "started_at": None if row[6] is None else str(row[6]),
                "finished_at": None if row[7] is None else str(row[7]),
            }
        )
    return items


def _load_daily_research_history_rows(*, ops_db_path: str | None = None, limit: int = 10) -> list[dict[str, Any]]:
    effective_limit = max(1, min(int(limit), 50))
    snapshot_rows: list[dict[str, Any]] | None = None
    try:
        rows = _read_ops_query_rows(
            ops_db_path=ops_db_path,
            table_name="external_review_artifacts",
            query="""
            SELECT
                review_id,
                nightly_scope_id,
                CAST(latest_end_as_of_date AS VARCHAR),
                recent_failure_rate,
                recent_quarantine_count,
                summary_json,
                CAST(created_at AS VARCHAR)
            FROM external_review_artifacts
            WHERE review_kind = 'daily_research'
            ORDER BY created_at DESC, review_id DESC
            LIMIT ?
            """,
            params=[effective_limit],
        )
    except (duckdb.Error, OSError, PermissionError) as exc:
        snapshot_payload = _load_ops_observer_snapshot_payload(ops_db_path=ops_db_path)
        if snapshot_payload is None:
            raise RuntimeError("ops_observer_snapshot_unavailable") from exc
        raw_snapshot_rows = snapshot_payload.get("daily_research_reviews") or []
        if not isinstance(raw_snapshot_rows, list):
            raise RuntimeError("ops_observer_snapshot_invalid")
        snapshot_rows = [row for row in raw_snapshot_rows if isinstance(row, dict)]
        rows = []
    items: list[dict[str, Any]] = []
    if snapshot_rows is not None:
        for row in snapshot_rows[:effective_limit]:
            summary_payload = _load_json_value(row.get("summary_json"))
            items.append(
                {
                    "review_id": row.get("review_id"),
                    "publish_id": row.get("publish_id"),
                    "as_of_date": row.get("as_of_date"),
                    "recent_failure_rate": row.get("recent_failure_rate"),
                    "recent_quarantine_count": row.get("recent_quarantine_count"),
                    "created_at": row.get("created_at"),
                    "codex_next_step": ((summary_payload.get("report") or {}).get("codex_next_step")),
                    "codex_brief": ((summary_payload.get("report") or {}).get("codex_brief")),
                }
            )
        return items
    for row in rows:
        summary_payload = _load_json_value(row[5])
        items.append(
            {
                "review_id": row[0],
                "publish_id": row[1],
                "as_of_date": row[2],
                "recent_failure_rate": row[3],
                "recent_quarantine_count": row[4],
                "created_at": row[6],
                "codex_next_step": ((summary_payload.get("report") or {}).get("codex_next_step")),
                "codex_brief": ((summary_payload.get("report") or {}).get("codex_brief")),
            }
        )
    return items


@contextmanager
def _temporary_analysis_reader_paths(
    *,
    result_db_path: str | None,
    ops_db_path: str | None,
) -> Iterator[None]:
    with _temporary_duckdb_snapshot_path(
        db_path=result_db_path if result_db_path is not None else resolve_result_db_path(),
        label="daily_research_result_read",
    ) as result_snapshot_path:
        if result_snapshot_path is None:
            raise FileNotFoundError("result_db_not_found")
        with _temporary_duckdb_snapshot_path(
            db_path=ops_db_path if ops_db_path is not None else resolve_ops_db_path(),
            label="daily_research_ops_bridge_read",
        ) as ops_snapshot_path:
            result_path_patch = patch(
                "app.backend.services.analysis_bridge.reader.resolve_result_db_path",
                lambda _db_path=None: Path(str(result_snapshot_path)),
            )
            read_only_patch = patch(
                "app.backend.services.analysis_bridge.reader._connect_read_only",
                lambda: duckdb.connect(str(result_snapshot_path), read_only=True),
            )
            if ops_snapshot_path is not None:
                ops_path_patch = patch(
                    "app.backend.services.tradex_research_bridge_service.resolve_ops_db_path",
                    lambda _db_path=None: Path(str(ops_snapshot_path)),
                )
                ops_patch = patch(
                    "app.backend.services.tradex_research_bridge_service.connect_ops_db",
                    lambda _db_path=None: duckdb.connect(str(ops_snapshot_path), read_only=False),
                )
            else:
                ops_path_patch = None
                ops_patch = None
            with result_path_patch:
                with read_only_patch:
                    if ops_patch is None or ops_path_patch is None:
                        yield
                    else:
                        with ops_path_patch:
                            with ops_patch:
                                yield


def build_daily_research_report(
    *,
    source_db_path: str | None = None,
    result_db_path: str | None = None,
    ops_db_path: str | None = None,
    side: str | None = None,
) -> dict[str, Any]:
    with _temporary_analysis_reader_paths(
        result_db_path=result_db_path,
        ops_db_path=ops_db_path,
    ):
        daily_summary = get_internal_state_eval_daily_summary(side=side)
        daily_history = get_internal_state_eval_daily_summary_history(side=side, limit=7)
        action_queue = get_internal_state_eval_action_queue(side=side)
        promotion_review = get_internal_state_eval_promotion_review()
        trend_watch = get_internal_state_eval_trend_summary(side=side, lookback=14, limit=5)
        combo_trend_watch = get_internal_state_eval_candle_combo_trend_summary(side=side, lookback=14, limit=5)
    approval_decision = (promotion_review.get("review") or {}).get("approval_decision")
    history_rows = list(daily_history.get("rows") or [])
    pending_carryover = _collect_pending_carryover(
        rows=history_rows,
        current_publish_id=str((daily_summary.get("publish") or {}).get("publish_id") or ""),
    )
    history_comparison = _build_history_comparison(
        current_publish_id=str((daily_summary.get("publish") or {}).get("publish_id") or ""),
        current_daily_summary=daily_summary.get("daily_summary") or {},
        current_promotion_review=promotion_review.get("review") or {},
        history_rows=history_rows,
    )
    codex_next_step = _build_codex_next_step(
        action_queue=action_queue.get("actions") or [],
        approval_decision=approval_decision,
        pending_carryover=pending_carryover,
    )
    codex_brief = _build_codex_brief(
        pending_carryover=pending_carryover,
        history_comparison=history_comparison,
        action_queue=action_queue.get("actions") or [],
    )
    return {
        "publish": daily_summary.get("publish"),
        "as_of_date": daily_summary.get("as_of_date"),
        "freshness_state": daily_summary.get("freshness_state"),
        "daily_summary": daily_summary.get("daily_summary"),
        "daily_history": history_rows,
        "action_queue": action_queue.get("actions") or [],
        "promotion_review": promotion_review.get("review"),
        "approval_decision": approval_decision,
        "pending_carryover": pending_carryover,
        "history_comparison": history_comparison,
        "codex_next_step": codex_next_step,
        "codex_brief": codex_brief,
        "trend_watch": trend_watch.get("trends"),
        "combo_trend_watch": combo_trend_watch.get("trends"),
    }


def _collect_pending_carryover(
    *,
    rows: list[dict[str, Any]],
    current_publish_id: str,
) -> list[dict[str, Any]]:
    carryover: list[dict[str, Any]] = []
    for row in rows:
        publish_id = str(row.get("publish_id") or "")
        if publish_id and publish_id == current_publish_id:
            continue
        if not bool(row.get("promotion_ready")):
            continue
        if str(row.get("decision_status") or "").strip() == "recorded":
            continue
        carryover.append(
            {
                "publish_id": publish_id,
                "as_of_date": row.get("as_of_date"),
                "decision_status": row.get("decision_status"),
                "top_strategy_tag": row.get("top_strategy_tag"),
                "promotion_sample_count": row.get("promotion_sample_count"),
                "codex_command": row.get("codex_command"),
            }
        )
    return carryover[:5]


def _build_history_comparison(
    *,
    current_publish_id: str,
    current_daily_summary: dict[str, Any],
    current_promotion_review: dict[str, Any],
    history_rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    previous_row = next(
        (
            row
            for row in history_rows
            if str(row.get("publish_id") or "") and str(row.get("publish_id") or "") != current_publish_id
        ),
        None,
    )
    if previous_row is None:
        return None
    top_strategy = (current_daily_summary.get("top_strategy") or {}).get("strategy_tag")
    top_candle = (current_daily_summary.get("top_candle") or {}).get("strategy_tag")
    risk_watch = (current_daily_summary.get("risk_watch") or {}).get("strategy_tag")
    sample_watch = (current_daily_summary.get("sample_watch") or {}).get("strategy_tag")
    changes: list[dict[str, Any]] = []

    def _append_change(metric: str, current_value: Any, previous_value: Any) -> None:
        if current_value != previous_value:
            changes.append(
                {
                    "metric": metric,
                    "current": current_value,
                    "previous": previous_value,
                }
            )

    _append_change("top_strategy", top_strategy, previous_row.get("top_strategy_tag"))
    _append_change("top_candle", top_candle, previous_row.get("top_candle_tag"))
    _append_change("risk_watch", risk_watch, previous_row.get("risk_watch_tag"))
    _append_change("sample_watch", sample_watch, previous_row.get("sample_watch_tag"))
    _append_change(
        "promotion_ready",
        bool(current_promotion_review.get("readiness_pass")),
        bool(previous_row.get("promotion_ready")),
    )
    return {
        "previous_publish_id": previous_row.get("publish_id"),
        "previous_as_of_date": previous_row.get("as_of_date"),
        "changes": changes,
    }


def _build_codex_next_step(
    *,
    action_queue: list[dict[str, Any]],
    approval_decision: dict[str, Any] | None,
    pending_carryover: list[dict[str, Any]],
) -> dict[str, Any]:
    if pending_carryover:
        top = pending_carryover[0] or {}
        return {
            "kind": "pending_carryover",
            "title": "Resolve previous pending promotion decision",
            "note": f"carryover publish: {top.get('publish_id')}",
            "status": "carryover",
            "suggested_command": top.get("codex_command"),
        }
    if action_queue:
        first_action = action_queue[0] or {}
        kind = str(first_action.get("kind") or "").strip()
        if kind == "promotion_decision_pending":
            return {
                "kind": kind,
                "title": first_action.get("title"),
                "note": first_action.get("note"),
                "status": "pending",
                "suggested_command": 'python -m external_analysis promotion-decision-run --decision hold --note "needs_manual_review"',
            }
        return {
            "kind": kind or "review",
            "title": first_action.get("title"),
            "note": first_action.get("note"),
            "status": "ready",
            "suggested_command": None,
        }
    if approval_decision:
        return {
            "kind": "promotion_review",
            "title": "Promotion decision already recorded",
            "note": f"latest decision: {approval_decision.get('decision')}",
            "status": "recorded",
            "suggested_command": None,
        }
    return {
        "kind": "idle",
        "title": "No immediate Codex action",
        "note": "daily research completed without pending follow-up",
        "status": "idle",
        "suggested_command": None,
    }


def _build_codex_brief(
    *,
    pending_carryover: list[dict[str, Any]],
    history_comparison: dict[str, Any] | None,
    action_queue: list[dict[str, Any]],
) -> dict[str, Any]:
    pending_items = [
        {
            "kind": "pending_promotion",
            "publish_id": row.get("publish_id"),
            "as_of_date": row.get("as_of_date"),
            "tag": row.get("top_strategy_tag"),
            "command": row.get("codex_command"),
        }
        for row in pending_carryover[:3]
    ]
    changes = list((history_comparison or {}).get("changes") or [])
    improving_items: list[dict[str, Any]] = []
    risk_items: list[dict[str, Any]] = []
    for change in changes:
        metric = str(change.get("metric") or "")
        item = {
            "metric": metric,
            "previous": change.get("previous"),
            "current": change.get("current"),
        }
        if metric in {"top_strategy", "top_candle"}:
            improving_items.append(item)
        elif metric in {"risk_watch", "promotion_ready"}:
            risk_items.append(item)
    for action in action_queue[:5]:
        kind = str(action.get("kind") or "")
        item = {
            "kind": kind,
            "title": action.get("title"),
            "tag": action.get("strategy_tag"),
            "metric_label": action.get("metric_label"),
            "metric_value": action.get("metric_value"),
        }
        if kind in {"top_strategy", "improving_tag", "improving_combo"} and len(improving_items) < 3:
            improving_items.append(item)
        if kind in {"risk_watch", "persistent_risk", "weakening_tag", "weakening_combo"} and len(risk_items) < 3:
            risk_items.append(item)
    return {
        "pending": pending_items[:3],
        "improving": improving_items[:3],
        "risk": risk_items[:3],
    }


def format_daily_research_text_report(payload: dict[str, Any]) -> str:
    report = payload.get("report") or {}
    publish = report.get("publish") or {}
    daily_summary = report.get("daily_summary") or {}
    action_queue = list(report.get("action_queue") or [])
    promotion = report.get("promotion_review") or {}
    approval_decision = report.get("approval_decision") or {}
    codex_next_step = report.get("codex_next_step") or {}
    pending_carryover = list(report.get("pending_carryover") or [])
    history_comparison = report.get("history_comparison") or {}
    codex_brief = report.get("codex_brief") or {}
    lines = [
        f"Tradex Daily Research",
        f"as_of_date: {payload.get('as_of_date') or report.get('as_of_date') or '--'}",
        f"publish_id: {publish.get('publish_id') or '--'}",
        f"candidate_status: {str((payload.get('candidate') or {}).get('status') or '--')}",
        f"similarity_status: {str((payload.get('similarity') or {}).get('status') or '--')}",
        f"challenger_status: {str((payload.get('challenger') or {}).get('status') or '--')}",
        f"promotion_ready: {'yes' if promotion.get('readiness_pass') else 'no'}",
        f"promotion_expectancy_delta: {promotion.get('expectancy_delta') if promotion else '--'}",
        f"top_strategy: {((daily_summary.get('top_strategy') or {}).get('strategy_tag')) or '--'}",
        f"top_candle: {((daily_summary.get('top_candle') or {}).get('strategy_tag')) or '--'}",
        f"risk_watch: {((daily_summary.get('risk_watch') or {}).get('strategy_tag')) or '--'}",
        f"sample_watch: {((daily_summary.get('sample_watch') or {}).get('strategy_tag')) or '--'}",
        f"approval_decision: {approval_decision.get('decision') or 'pending'}",
        f"approval_actor: {approval_decision.get('actor') or '--'}",
        f"codex_next_step: {codex_next_step.get('title') or '--'}",
        f"codex_next_status: {codex_next_step.get('status') or '--'}",
        f"pending_carryover_count: {len(pending_carryover)}",
        f"history_compare_target: {history_comparison.get('previous_publish_id') or '--'}",
        f"codex_brief_pending: {len(list(codex_brief.get('pending') or []))}",
        f"codex_brief_improving: {len(list(codex_brief.get('improving') or []))}",
        f"codex_brief_risk: {len(list(codex_brief.get('risk') or []))}",
        "today_queue:",
    ]
    if not action_queue:
        lines.append("  - none")
    else:
        for item in action_queue[:5]:
            title = str(item.get("title") or "--")
            label = str(item.get("label") or "--")
            tag = str(item.get("strategy_tag") or "--")
            metric_label = str(item.get("metric_label") or "--")
            metric_value = item.get("metric_value")
            lines.append(f"  - [{label}] {title} | tag={tag} | {metric_label}={metric_value}")
    suggested_command = codex_next_step.get("suggested_command")
    if suggested_command:
        lines.append(f"codex_command: {suggested_command}")
    if pending_carryover:
        lines.append("pending_carryover:")
        for row in pending_carryover[:5]:
            lines.append(
                "  - "
                f"{row.get('publish_id') or '--'} | "
                f"date={row.get('as_of_date') or '--'} | "
                f"tag={row.get('top_strategy_tag') or '--'} | "
                f"status={row.get('decision_status') or '--'}"
            )
    changes = list(history_comparison.get("changes") or [])
    if changes:
        lines.append("history_changes:")
        for change in changes[:5]:
            lines.append(
                "  - "
                f"{change.get('metric')}: "
                f"{change.get('previous') or '--'} -> {change.get('current') or '--'}"
            )
    brief_pending = list(codex_brief.get("pending") or [])
    brief_improving = list(codex_brief.get("improving") or [])
    brief_risk = list(codex_brief.get("risk") or [])
    if brief_pending or brief_improving or brief_risk:
        lines.append("codex_brief:")
        for row in brief_pending:
            lines.append(
                "  - "
                f"[pending] {row.get('publish_id') or '--'} | "
                f"tag={row.get('tag') or '--'}"
            )
        for row in brief_improving:
            lines.append(
                "  - "
                f"[improving] {row.get('metric') or row.get('kind') or '--'} | "
                f"current={row.get('current') or row.get('tag') or '--'}"
            )
        for row in brief_risk:
            lines.append(
                "  - "
                f"[risk] {row.get('metric') or row.get('kind') or '--'} | "
                f"current={row.get('current') or row.get('tag') or '--'}"
            )
    return "\n".join(lines)


def _build_daily_research_review_row(payload: dict[str, Any]) -> dict[str, Any]:
    report = payload.get("report") or {}
    publish_id = str(payload.get("publish_id") or (report.get("publish") or {}).get("publish_id") or "")
    as_of_date = str(payload.get("as_of_date") or report.get("as_of_date") or "")
    promotion = report.get("promotion_review") or {}
    reason_codes = list(promotion.get("reason_codes") or [])
    quarantine_count = sum(
        1
        for section in ("candidate", "similarity", "challenger")
        if (payload.get(section) or {}).get("quarantine_reason")
    )
    normalized_as_of_date = _normalize_iso_date(as_of_date)
    return {
        "review_id": f"daily_research:{publish_id or as_of_date}:{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}",
        "review_kind": "daily_research",
        "latest_end_as_of_date": normalized_as_of_date,
        "replay_scope_id": None,
        "nightly_scope_id": publish_id or None,
        "combined_scope_id": publish_id or None,
        "combined_readiness_20": bool(promotion.get("readiness_pass")),
        "combined_readiness_40": bool(promotion.get("readiness_pass")),
        "combined_readiness_60": bool(promotion.get("readiness_pass")),
        "recent_run_limit": 7,
        "recent_failure_rate": 0.0 if payload.get("ok") else 1.0,
        "recent_quarantine_count": quarantine_count,
        "top_reason_codes_json": json.dumps(reason_codes, ensure_ascii=False, sort_keys=True),
        "replay_summary_json": json.dumps({}, ensure_ascii=False, sort_keys=True),
        "nightly_summary_json": json.dumps(
            {
                "candidate": payload.get("candidate"),
                "similarity": payload.get("similarity"),
                "challenger": payload.get("challenger"),
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        "combined_summary_json": json.dumps(
            {
                "codex_next_step": report.get("codex_next_step"),
                "codex_brief": report.get("codex_brief"),
                "history_comparison": report.get("history_comparison"),
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        "summary_json": json.dumps(payload, ensure_ascii=False, sort_keys=True),
        "created_at": datetime.now(timezone.utc),
    }


def _normalize_iso_date(value: str | None) -> str | None:
    text = str(value or "").strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    return text or None


def load_daily_research_history(*, ops_db_path: str | None = None, limit: int = 10) -> dict[str, Any]:
    items = _load_daily_research_history_rows(ops_db_path=ops_db_path, limit=limit)
    stale_running_jobs = _load_stale_running_job_observations(ops_db_path=ops_db_path, limit=20)
    return {
        "rows": items,
        "stale_running_jobs": stale_running_jobs,
    }


def format_daily_research_history_text_report(payload: dict[str, Any]) -> str:
    rows = list(payload.get("rows") or [])
    stale_running_jobs = list(payload.get("stale_running_jobs") or [])
    lines = ["Tradex Daily Research History"]
    if not rows:
        lines.append("  - none")
        if stale_running_jobs:
            lines.append("stale_running_jobs:")
            for item in stale_running_jobs[:5]:
                lines.append(
                    "  - "
                    f"{item.get('job_type') or '--'} | "
                    f"date={item.get('as_of_date') or '--'} | "
                    f"status={item.get('status') or '--'}"
                )
        return "\n".join(lines)
    for row in rows:
        next_step = row.get("codex_next_step") or {}
        brief = row.get("codex_brief") or {}
        lines.append(
            "  - "
            f"{row.get('publish_id') or '--'} | "
            f"date={row.get('as_of_date') or '--'} | "
            f"next={next_step.get('kind') or '--'} | "
            f"pending={len(list(brief.get('pending') or []))} | "
            f"improving={len(list(brief.get('improving') or []))} | "
            f"risk={len(list(brief.get('risk') or []))}"
        )
    if stale_running_jobs:
        lines.append("stale_running_jobs:")
        for item in stale_running_jobs[:5]:
            lines.append(
                "  - "
                f"{item.get('job_type') or '--'} | "
                f"date={item.get('as_of_date') or '--'} | "
                f"status={item.get('status') or '--'}"
            )
    return "\n".join(lines)


def build_daily_research_tag_report(
    *,
    strategy_tag: str,
    ops_db_path: str | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    target_tag = str(strategy_tag or "").strip()
    if not target_tag:
        raise ValueError("strategy_tag_required")
    history_payload = load_daily_research_history(ops_db_path=ops_db_path, limit=max(limit, 10))
    rows = list(history_payload.get("rows") or [])
    matches: list[dict[str, Any]] = []
    for row in rows:
        brief = row.get("codex_brief") or {}
        for bucket_name in ("pending", "improving", "risk"):
            for item in list(brief.get(bucket_name) or []):
                item_tag = str(item.get("tag") or item.get("current") or "").strip()
                if item_tag != target_tag:
                    continue
                matches.append(
                    {
                        "publish_id": row.get("publish_id"),
                        "as_of_date": row.get("as_of_date"),
                        "bucket": bucket_name,
                        "item": item,
                    }
                )
    return {
        "strategy_tag": target_tag,
        "rows": matches[: max(1, min(int(limit), 50))],
    }


def format_daily_research_tag_report_text_report(payload: dict[str, Any]) -> str:
    target_tag = str(payload.get("strategy_tag") or "--")
    rows = list(payload.get("rows") or [])
    lines = [
        "Tradex Daily Research Tag Report",
        f"strategy_tag: {target_tag}",
        f"matches: {len(rows)}",
    ]
    if not rows:
        lines.append("  - none")
        return "\n".join(lines)
    for row in rows:
        lines.append(
            "  - "
            f"{row.get('publish_id') or '--'} | "
            f"date={row.get('as_of_date') or '--'} | "
            f"bucket={row.get('bucket') or '--'}"
        )
    return "\n".join(lines)


def build_daily_research_watchlist(*, ops_db_path: str | None = None, limit: int = 10) -> dict[str, Any]:
    history_payload = load_daily_research_history(ops_db_path=ops_db_path, limit=limit)
    rows = list(history_payload.get("rows") or [])
    stale_running_jobs = list(history_payload.get("stale_running_jobs") or [])
    pending_by_publish: dict[str, dict[str, Any]] = {}
    improving_by_tag: dict[str, dict[str, Any]] = {}
    risk_by_tag: dict[str, dict[str, Any]] = {}
    for row in rows:
        brief = row.get("codex_brief") or {}
        for pending in list(brief.get("pending") or []):
            publish_id = str(pending.get("publish_id") or "")
            if not publish_id:
                continue
            slot = pending_by_publish.setdefault(
                publish_id,
                {
                    "publish_id": publish_id,
                    "first_seen_as_of_date": row.get("as_of_date"),
                    "latest_as_of_date": row.get("as_of_date"),
                    "count": 0,
                    "tag": pending.get("tag"),
                    "command": pending.get("command"),
                    "suggested_command": pending.get("command"),
                    "next_action_kind": "approve",
                    "priority_score": 0,
                    "priority_label": "normal",
                },
            )
            slot["count"] = int(slot["count"]) + 1
            slot["latest_as_of_date"] = row.get("as_of_date")
            slot["priority_score"] = int(slot["count"]) * 100
            slot["priority_label"] = "critical" if int(slot["count"]) >= 3 else ("high" if int(slot["count"]) >= 2 else "medium")
        for improving in list(brief.get("improving") or []):
            tag = str(improving.get("current") or improving.get("tag") or "").strip()
            if not tag:
                continue
            slot = improving_by_tag.setdefault(
                tag,
                {
                    "tag": tag,
                    "latest_as_of_date": row.get("as_of_date"),
                    "count": 0,
                    "source_metric": improving.get("metric") or improving.get("kind"),
                    "suggested_command": f'python -m external_analysis daily-research-tag-report --strategy-tag "{tag}" --limit 10',
                    "next_action_kind": "observe",
                    "priority_score": 0,
                    "priority_label": "normal",
                },
            )
            slot["count"] = int(slot["count"]) + 1
            slot["latest_as_of_date"] = row.get("as_of_date")
            slot["priority_score"] = int(slot["count"]) * 5
            slot["priority_label"] = "strong" if int(slot["count"]) >= 3 else ("watch" if int(slot["count"]) >= 2 else "emerging")
        for risk in list(brief.get("risk") or []):
            tag = str(risk.get("current") or risk.get("tag") or "")
            if not tag:
                continue
            slot = risk_by_tag.setdefault(
                tag,
                {
                    "tag": tag,
                    "latest_as_of_date": row.get("as_of_date"),
                    "count": 0,
                    "source_metric": risk.get("metric") or risk.get("kind"),
                    "suggested_command": f'python -m external_analysis daily-research-tag-report --strategy-tag "{tag}" --limit 10',
                    "next_action_kind": "avoid",
                    "priority_score": 0,
                    "priority_label": "normal",
                },
            )
            slot["count"] = int(slot["count"]) + 1
            slot["latest_as_of_date"] = row.get("as_of_date")
            slot["priority_score"] = int(slot["count"]) * 10
            slot["priority_label"] = "high" if int(slot["count"]) >= 3 else ("medium" if int(slot["count"]) >= 2 else "watch")
    pending = sorted(
        pending_by_publish.values(),
        key=lambda item: (-int(item["priority_score"]), -int(item["count"]), str(item["publish_id"])),
    )[:5]
    improving = sorted(
        improving_by_tag.values(),
        key=lambda item: (-int(item["priority_score"]), -int(item["count"]), str(item["tag"])),
    )[:5]
    persistent_risk = sorted(
        risk_by_tag.values(),
        key=lambda item: (-int(item["priority_score"]), -int(item["count"]), str(item["tag"])),
    )[:5]
    top_next_actions = sorted(
        [
            *[
                {
                    "kind": "pending_promotion",
                    "label": item.get("publish_id"),
                    "next_action_kind": item.get("next_action_kind"),
                    "priority_score": item.get("priority_score"),
                    "suggested_command": item.get("suggested_command"),
                }
                for item in pending
            ],
            *[
                {
                    "kind": "improving_tag",
                    "label": item.get("tag"),
                    "next_action_kind": item.get("next_action_kind"),
                    "priority_score": item.get("priority_score"),
                    "suggested_command": item.get("suggested_command"),
                }
                for item in improving
            ],
            *[
                {
                    "kind": "persistent_risk",
                    "label": item.get("tag"),
                    "next_action_kind": item.get("next_action_kind"),
                    "priority_score": item.get("priority_score"),
                    "suggested_command": item.get("suggested_command"),
                }
                for item in persistent_risk
            ],
        ],
        key=lambda item: (-int(item.get("priority_score") or 0), str(item.get("label") or "")),
    )[:3]
    return {
        "history_rows": len(rows),
        "stale_running_jobs": stale_running_jobs,
        "pending_promotions": pending,
        "improving_tags": improving,
        "persistent_risk_tags": persistent_risk,
        "top_next_actions": top_next_actions,
    }


def format_daily_research_watchlist_text_report(payload: dict[str, Any]) -> str:
    pending = list(payload.get("pending_promotions") or [])
    improving = list(payload.get("improving_tags") or [])
    risk = list(payload.get("persistent_risk_tags") or [])
    top_next_actions = list(payload.get("top_next_actions") or [])
    stale_running_jobs = list(payload.get("stale_running_jobs") or [])
    lines = [
        "Tradex Daily Research Watchlist",
        f"history_rows: {payload.get('history_rows') or 0}",
        f"pending_promotions: {len(pending)}",
        f"improving_tags: {len(improving)}",
        f"persistent_risk_tags: {len(risk)}",
        f"top_next_actions: {len(top_next_actions)}",
        f"stale_running_jobs: {len(stale_running_jobs)}",
    ]
    if top_next_actions:
        lines.append("top_next_actions:")
        for row in top_next_actions:
            lines.append(
                "  - "
                f"{row.get('kind') or '--'} | "
                f"{row.get('label') or '--'} | "
                f"action={row.get('next_action_kind') or '--'} | "
                f"priority={row.get('priority_score') or 0} | "
                f"command={row.get('suggested_command') or '--'}"
            )
    if pending:
        lines.append("pending:")
        for row in pending:
            lines.append(
                "  - "
                f"{row.get('publish_id') or '--'} | "
                f"count={row.get('count') or 0} | "
                f"tag={row.get('tag') or '--'} | "
                f"action={row.get('next_action_kind') or '--'} | "
                f"priority={row.get('priority_label') or '--'}:{row.get('priority_score') or 0} | "
                f"command={row.get('suggested_command') or '--'}"
            )
    if improving:
        lines.append("improving:")
        for row in improving:
            lines.append(
                "  - "
                f"{row.get('tag') or '--'} | "
                f"count={row.get('count') or 0} | "
                f"metric={row.get('source_metric') or '--'} | "
                f"action={row.get('next_action_kind') or '--'} | "
                f"priority={row.get('priority_label') or '--'}:{row.get('priority_score') or 0} | "
                f"command={row.get('suggested_command') or '--'}"
            )
    if risk:
        lines.append("risk:")
        for row in risk:
            lines.append(
                "  - "
                f"{row.get('tag') or '--'} | "
                f"count={row.get('count') or 0} | "
                f"metric={row.get('source_metric') or '--'} | "
                f"action={row.get('next_action_kind') or '--'} | "
                f"priority={row.get('priority_label') or '--'}:{row.get('priority_score') or 0} | "
                f"command={row.get('suggested_command') or '--'}"
            )
    if stale_running_jobs:
        lines.append("stale_running:")
        for item in stale_running_jobs[:5]:
            lines.append(
                "  - "
                f"{item.get('job_type') or '--'} | "
                f"date={item.get('as_of_date') or '--'} | "
                f"status={item.get('status') or '--'}"
            )
    if not pending and not improving and not risk:
        lines.append("  - none")
    return "\n".join(lines)


def build_daily_research_dispatch(*, ops_db_path: str | None = None, limit: int = 10, position: int = 1) -> dict[str, Any]:
    watchlist = build_daily_research_watchlist(ops_db_path=ops_db_path, limit=limit)
    top_next_actions = list(watchlist.get("top_next_actions") or [])
    selected_index = max(1, int(position)) - 1
    selected = top_next_actions[selected_index] if selected_index < len(top_next_actions) else None
    action_summary = _build_dispatch_action_summary(selected)
    return {
        "watchlist": watchlist,
        "selected_position": selected_index + 1,
        "selected_action": selected,
        "action_summary": action_summary,
    }


def _build_dispatch_action_summary(selected_action: dict[str, Any] | None) -> str:
    if not selected_action:
        return "No action available."
    action_kind = str(selected_action.get("next_action_kind") or "").strip()
    label = str(selected_action.get("label") or "--")
    if action_kind == "approve":
        return f"Approve review for {label} after checking the latest promotion evidence."
    if action_kind == "avoid":
        return f"Avoid or re-check risk around {label} before using it in decisions."
    if action_kind == "observe":
        return f"Observe {label} more closely and review its recent tag history."
    return f"Review {label}."


def format_daily_research_dispatch_text_report(payload: dict[str, Any]) -> str:
    selected = payload.get("selected_action") or {}
    lines = [
        "Tradex Daily Research Dispatch",
        f"selected_position: {payload.get('selected_position') or 1}",
        f"selected_kind: {selected.get('kind') or '--'}",
        f"selected_label: {selected.get('label') or '--'}",
        f"selected_action_kind: {selected.get('next_action_kind') or '--'}",
        f"selected_priority: {selected.get('priority_score') or 0}",
        f"selected_command: {selected.get('suggested_command') or '--'}",
        f"action_summary: {payload.get('action_summary') or '--'}",
    ]
    return "\n".join(lines)


def _load_daily_research_long_candidates(
    *,
    result_db_path: str | None = None,
    publish_id: str | None = None,
    as_of_date: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    resolved_publish_id = str(publish_id or "").strip()
    normalized_as_of_date = _normalize_iso_date(as_of_date)
    if not resolved_publish_id or not normalized_as_of_date:
        return []
    with _temporary_duckdb_snapshot_path(
        db_path=result_db_path if result_db_path is not None else resolve_result_db_path(),
        label="daily_research_candidates_read",
    ) as snapshot_path:
        if snapshot_path is None:
            return []
        conn = duckdb.connect(str(snapshot_path), read_only=True)
        try:
            if not _table_exists(conn, "candidate_daily"):
                return []
            rows = conn.execute(
                """
                SELECT
                    CAST(as_of_date AS VARCHAR),
                    publish_id,
                    code,
                    side,
                    rank_position,
                    candidate_score,
                    expected_horizon_days,
                    primary_reason_codes,
                    regime_tag,
                    freshness_state
                FROM candidate_daily
                WHERE publish_id = ?
                  AND CAST(as_of_date AS DATE) = CAST(? AS DATE)
                  AND side = 'long'
                ORDER BY rank_position ASC, code ASC
                LIMIT ?
                """,
                [resolved_publish_id, normalized_as_of_date, max(1, min(int(limit), 100))],
            ).fetchall()
        finally:
            conn.close()
    items: list[dict[str, Any]] = []
    for row in rows:
        reason_codes = []
        try:
            parsed = json.loads(str(row[7])) if row[7] is not None else []
            if isinstance(parsed, list):
                reason_codes = parsed
        except (TypeError, ValueError, json.JSONDecodeError):
            reason_codes = []
        items.append(
            {
                "as_of_date": row[0],
                "publish_id": row[1],
                "code": row[2],
                "side": row[3],
                "rank_position": int(row[4]),
                "candidate_score": None if row[5] is None else float(row[5]),
                "expected_horizon_days": None if row[6] is None else int(row[6]),
                "primary_reason_codes": reason_codes,
                "regime_tag": row[8],
                "freshness_state": row[9],
            }
        )
    return items


def _resolve_attempt_report_path(base_path: str | None, *, as_of_date: str, suffix: str) -> str | None:
    if not base_path:
        return None
    resolved = Path(str(base_path)).expanduser().resolve()
    stem = resolved.stem
    return str(resolved.with_name(f"{stem}_{as_of_date}{suffix}{resolved.suffix}"))


def _build_daily_research_loop_attempt_summary(
    *,
    as_of_date: str,
    payload: dict[str, Any],
    long_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    report = payload.get("report") or {}
    promotion_review = report.get("promotion_review") or {}
    daily_summary = report.get("daily_summary") or {}
    return {
        "as_of_date": as_of_date,
        "publish_id": payload.get("publish_id"),
        "promotion_ready": bool(promotion_review.get("readiness_pass")),
        "long_candidate_count": len(long_candidates),
        "candidate_status": ((payload.get("candidate") or {}).get("status")),
        "similarity_status": ((payload.get("similarity") or {}).get("status")),
        "challenger_status": ((payload.get("challenger") or {}).get("status")),
        "top_strategy_tag": ((daily_summary.get("top_strategy") or {}).get("strategy_tag")),
        "risk_watch_tag": ((daily_summary.get("risk_watch") or {}).get("strategy_tag")),
        "codex_next_step_kind": ((report.get("codex_next_step") or {}).get("kind")),
    }


def format_daily_research_loop_text_report(payload: dict[str, Any]) -> str:
    lines = [
        "Tradex Daily Research Loop",
        f"source_latest_as_of: {payload.get('source_latest_as_of') or '--'}",
        f"stop_reason: {payload.get('stop_reason') or '--'}",
        f"selected_as_of_date: {payload.get('selected_as_of_date') or '--'}",
        f"selected_publish_id: {payload.get('selected_publish_id') or '--'}",
        f"promotion_ready: {'yes' if payload.get('promotion_ready') else 'no'}",
        f"attempted_as_of_dates: {', '.join(list(payload.get('attempted_as_of_dates') or [])) or '--'}",
        f"long_candidates: {len(list(payload.get('long_candidates') or []))}",
    ]
    diagnostics = payload.get("diagnostics") or {}
    stale_running_jobs = list(diagnostics.get("stale_running_jobs") or [])
    attempts = list(diagnostics.get("attempts") or [])
    if attempts:
        lines.append("attempts:")
        for item in attempts:
            lines.append(
                "  - "
                f"{item.get('as_of_date') or '--'} | "
                f"publish={item.get('publish_id') or '--'} | "
                f"promotion_ready={'yes' if item.get('promotion_ready') else 'no'} | "
                f"long_candidates={item.get('long_candidate_count') or 0} | "
                f"candidate={item.get('candidate_status') or '--'} | "
                f"similarity={item.get('similarity_status') or '--'} | "
                f"challenger={item.get('challenger_status') or '--'}"
            )
    if stale_running_jobs:
        lines.append("stale_running_jobs:")
        for item in stale_running_jobs[:10]:
            lines.append(
                "  - "
                f"{item.get('job_type') or '--'} | "
                f"date={item.get('as_of_date') or '--'} | "
                f"status={item.get('status') or '--'}"
            )
    long_candidates = list(payload.get("long_candidates") or [])
    if long_candidates:
        lines.append("long_candidates:")
        for item in long_candidates[:20]:
            lines.append(
                "  - "
                f"rank={item.get('rank_position') or '--'} | "
                f"code={item.get('code') or '--'} | "
                f"score={item.get('candidate_score') if item.get('candidate_score') is not None else '--'} | "
                f"regime={item.get('regime_tag') or '--'}"
            )
    return "\n".join(lines)


def run_daily_research_loop(
    *,
    source_db_path: str | None = None,
    export_db_path: str | None = None,
    label_db_path: str | None = None,
    result_db_path: str | None = None,
    similarity_db_path: str | None = None,
    ops_db_path: str | None = None,
    freshness_state: str = "fresh",
    report_path: str | None = None,
    text_report_path: str | None = None,
    progress_path: str | None = None,
    snapshot_root: str | None = None,
    max_trading_days: int = DAILY_RESEARCH_LOOKBACK_DAYS,
) -> dict[str, Any]:
    resolved_progress_path = _resolve_output_path(progress_path)
    loop_progress = {
        "schema_version": DAILY_RESEARCH_PROGRESS_SCHEMA_VERSION,
        "mode": "loop",
        "status": "running",
        "current_phase": "starting",
        "started_at": _utcnow_iso(),
        "finished_at": None,
        "source_latest_as_of": None,
        "attempted_as_of_dates": [],
        "attempts": [],
        "current_as_of_date": None,
        "current_attempt_progress_path": None,
        "selected_as_of_date": None,
        "selected_publish_id": None,
        "stop_reason": None,
        "eta_seconds": None,
        "report_path": report_path,
        "text_report_path": text_report_path,
    }
    _write_progress(resolved_progress_path, loop_progress)
    effective_limit = max(1, min(int(max_trading_days), 50))
    try:
        prepared_probe = probe_daily_research_prepared_environment(
            source_db_path=source_db_path,
            export_db_path=export_db_path,
            label_db_path=label_db_path,
        )
        loop_progress["prepared_environment"] = prepared_probe
        if not bool(prepared_probe.get("prepared")):
            payload = {
                "ok": False,
                "source_latest_as_of": prepared_probe.get("latest_trade_date"),
                "attempted_as_of_dates": [],
                "selected_as_of_date": None,
                "selected_publish_id": None,
                "stop_reason": "prepare_required",
                "promotion_ready": False,
                "long_candidates": [],
                "diagnostics": {
                    "attempts": [],
                    "stale_running_jobs": _load_stale_running_job_observations(ops_db_path=ops_db_path, limit=20),
                    "prepared_environment": prepared_probe,
                },
                "source_snapshot": None,
            }
            if report_path:
                Path(str(report_path)).expanduser().resolve().write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
                    encoding="utf-8",
                )
            if text_report_path:
                Path(str(text_report_path)).expanduser().resolve().write_text(
                    format_daily_research_loop_text_report(payload),
                    encoding="utf-8",
                )
            loop_progress.update(
                {
                    "status": "complete",
                    "current_phase": "prepare_required",
                    "finished_at": _utcnow_iso(),
                    "source_latest_as_of": prepared_probe.get("latest_trade_date"),
                    "stop_reason": "prepare_required",
                    "promotion_ready": False,
                    "long_candidate_count": 0,
                    "eta_seconds": 0,
                }
            )
            _write_progress(resolved_progress_path, loop_progress)
            return payload

        candidate_dates = resolve_recent_daily_research_as_of_dates_from_export(export_db_path=export_db_path, limit=effective_limit)
        source_latest_as_of = candidate_dates[0] if candidate_dates else None
        attempted_dates: list[str] = []
        attempt_summaries: list[dict[str, Any]] = []
        selected_payload: dict[str, Any] | None = None
        selected_long_candidates: list[dict[str, Any]] = []
        stop_reason = "no_trading_dates_found"
        attempt_durations: list[float] = []

        loop_progress.update(
            {
                "current_phase": "attempt_selection",
                "source_latest_as_of": source_latest_as_of,
                "candidate_as_of_dates": candidate_dates,
                "source_snapshot": None,
            }
        )
        _write_progress(resolved_progress_path, loop_progress)

        for as_of_date in candidate_dates:
            attempted_dates.append(as_of_date)
            attempt_report_path = _resolve_attempt_report_path(report_path, as_of_date=as_of_date, suffix="_attempt")
            attempt_text_report_path = _resolve_attempt_report_path(text_report_path, as_of_date=as_of_date, suffix="_attempt")
            attempt_progress_path = _resolve_attempt_report_path(progress_path, as_of_date=as_of_date, suffix="_attempt")
            loop_progress.update(
                {
                    "current_phase": "attempt_running",
                    "attempted_as_of_dates": attempted_dates,
                    "current_as_of_date": as_of_date,
                    "current_attempt_progress_path": attempt_progress_path,
                }
            )
            _write_progress(resolved_progress_path, loop_progress)
            attempt_started_at = datetime.now(timezone.utc)
            attempt_payload = run_daily_research_cycle(
                source_db_path=source_db_path,
                export_db_path=export_db_path,
                label_db_path=label_db_path,
                result_db_path=result_db_path,
                similarity_db_path=similarity_db_path,
                ops_db_path=ops_db_path,
                as_of_date=as_of_date,
                publish_id=None,
                freshness_state=freshness_state,
                report_path=attempt_report_path,
                text_report_path=attempt_text_report_path,
                progress_path=attempt_progress_path,
                require_prepared_environment=True,
                snapshot_source=False,
            )
            attempt_duration_seconds = max((datetime.now(timezone.utc) - attempt_started_at).total_seconds(), 0.0)
            attempt_durations.append(attempt_duration_seconds)
            publish_id = str(attempt_payload.get("publish_id") or "").strip() or None
            long_candidates = _load_daily_research_long_candidates(
                result_db_path=result_db_path,
                publish_id=publish_id,
                as_of_date=as_of_date,
            )
            attempt_summary = _build_daily_research_loop_attempt_summary(
                as_of_date=as_of_date,
                payload=attempt_payload,
                long_candidates=long_candidates,
            )
            attempt_summaries.append(attempt_summary)
            loop_progress["attempts"] = attempt_summaries
            if attempt_durations:
                ordered = sorted(attempt_durations)
                mid = len(ordered) // 2
                median_seconds = ordered[mid] if len(ordered) % 2 == 1 else (ordered[mid - 1] + ordered[mid]) / 2.0
                loop_progress["eta_seconds"] = max(int(round(median_seconds * max(len(candidate_dates) - len(attempted_dates), 0))), 0)
            promotion_ready = bool(((attempt_payload.get("report") or {}).get("promotion_review") or {}).get("readiness_pass"))
            if promotion_ready and long_candidates:
                selected_payload = attempt_payload
                selected_long_candidates = long_candidates
                stop_reason = "promotion_ready_with_long_candidates"
                break
            stop_reason = "no_promotion_ready_in_window"
            loop_progress["stop_reason"] = stop_reason
            _write_progress(resolved_progress_path, loop_progress)

        stale_running_jobs = _load_stale_running_job_observations(ops_db_path=ops_db_path, limit=20)
        payload = {
            "ok": bool(selected_payload),
            "source_latest_as_of": source_latest_as_of,
            "attempted_as_of_dates": attempted_dates,
            "selected_as_of_date": None if selected_payload is None else str(selected_payload.get("as_of_date") or ""),
            "selected_publish_id": None if selected_payload is None else selected_payload.get("publish_id"),
            "stop_reason": stop_reason,
            "promotion_ready": bool(((selected_payload or {}).get("report") or {}).get("promotion_review", {}).get("readiness_pass")) if selected_payload else False,
            "long_candidates": selected_long_candidates,
            "diagnostics": {
                "attempts": attempt_summaries,
                "stale_running_jobs": stale_running_jobs,
            },
            "source_snapshot": None,
        }
        if report_path:
            Path(str(report_path)).expanduser().resolve().write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8",
            )
        if text_report_path:
            Path(str(text_report_path)).expanduser().resolve().write_text(
                format_daily_research_loop_text_report(payload),
                encoding="utf-8",
            )
        loop_progress.update(
            {
                "status": "complete",
                "current_phase": "completed",
                "finished_at": _utcnow_iso(),
                "attempted_as_of_dates": attempted_dates,
                "attempts": attempt_summaries,
                "current_as_of_date": None,
                "current_attempt_progress_path": None,
                "selected_as_of_date": payload["selected_as_of_date"],
                "selected_publish_id": payload["selected_publish_id"],
                "stop_reason": stop_reason,
                "promotion_ready": payload["promotion_ready"],
                "long_candidate_count": len(selected_long_candidates),
                "eta_seconds": 0,
            }
        )
        _write_progress(resolved_progress_path, loop_progress)
        return payload
    except Exception as exc:
        loop_progress.update(
            {
                "status": "failed",
                "current_phase": "failed",
                "finished_at": _utcnow_iso(),
                "error_class": exc.__class__.__name__,
                "error_message": str(exc),
            }
        )
        _write_progress(resolved_progress_path, loop_progress)
        raise


def run_daily_research_cycle(
    *,
    source_db_path: str | None = None,
    export_db_path: str | None = None,
    label_db_path: str | None = None,
    result_db_path: str | None = None,
    similarity_db_path: str | None = None,
    ops_db_path: str | None = None,
    as_of_date: str | None = None,
    publish_id: str | None = None,
    freshness_state: str = "fresh",
    report_path: str | None = None,
    text_report_path: str | None = None,
    progress_path: str | None = None,
    require_prepared_environment: bool = False,
    snapshot_source: bool = True,
    snapshot_root: str | None = None,
) -> dict[str, Any]:
    resolved_progress_path = _resolve_output_path(progress_path)
    export_progress_path = str(resolve_snapshot_progress_path(export_db_path)) if export_db_path else None
    cycle_progress = {
        "schema_version": DAILY_RESEARCH_PROGRESS_SCHEMA_VERSION,
        "mode": "cycle",
        "status": "running",
        "current_phase": "starting",
        "started_at": _utcnow_iso(),
        "finished_at": None,
        "as_of_date": None,
        "publish_id": publish_id,
        "report_path": report_path,
        "text_report_path": text_report_path,
        "export_progress_path": export_progress_path,
        "source_snapshot": None,
        "candidate": None,
        "similarity": None,
        "challenger": None,
    }
    _write_progress(resolved_progress_path, cycle_progress)
    try:
        snapshot_payload = None
        if snapshot_source and not require_prepared_environment:
            snapshot_payload = create_source_snapshot(
                source_db_path=source_db_path,
                snapshot_root=snapshot_root or (str(Path(str(export_db_path)).expanduser().resolve().parent / "source_snapshots") if export_db_path else None),
                label="daily_research",
            )
        effective_source_db_path = str((snapshot_payload or {}).get("snapshot_db_path") or source_db_path or "")
        resolved_as_of_date = (
            str(as_of_date).strip()
            if as_of_date is not None and str(as_of_date).strip()
            else (
                resolve_latest_daily_research_as_of_date_from_export(export_db_path=export_db_path)
                if require_prepared_environment
                else resolve_latest_daily_research_as_of_date(source_db_path=effective_source_db_path)
            )
        )
        cycle_progress.update(
            {
                "current_phase": "candidate",
                "as_of_date": resolved_as_of_date,
                "source_snapshot": snapshot_payload,
                "require_prepared_environment": bool(require_prepared_environment),
            }
        )
        _write_progress(resolved_progress_path, cycle_progress)
        candidate_payload = run_nightly_candidate_pipeline(
            source_db_path=effective_source_db_path,
            export_db_path=export_db_path,
            label_db_path=label_db_path,
            result_db_path=result_db_path,
            similarity_db_path=similarity_db_path,
            ops_db_path=ops_db_path,
            as_of_date=resolved_as_of_date,
            publish_id=publish_id,
            freshness_state=freshness_state,
            require_prepared_environment=require_prepared_environment,
            snapshot_source=False,
        )
        effective_publish_id = str((candidate_payload.get("baseline") or {}).get("publish_id") or publish_id or "")
        cycle_progress.update(
            {
                "current_phase": "similarity",
                "publish_id": effective_publish_id or publish_id,
                "candidate": {
                    "run_id": candidate_payload.get("run_id"),
                    "status": candidate_payload.get("status"),
                    "quarantine_reason": candidate_payload.get("quarantine_reason"),
                },
            }
        )
        _write_progress(resolved_progress_path, cycle_progress)

        similarity_payload = run_nightly_similarity_pipeline(
            export_db_path=export_db_path,
            label_db_path=label_db_path,
            result_db_path=result_db_path,
            similarity_db_path=similarity_db_path,
            ops_db_path=ops_db_path,
            as_of_date=resolved_as_of_date,
            publish_id=effective_publish_id or publish_id,
            freshness_state=freshness_state,
        )
        cycle_progress.update(
            {
                "current_phase": "challenger" if similarity_payload.get("ok", False) else "report",
                "similarity": {
                    "run_id": similarity_payload.get("run_id"),
                    "status": similarity_payload.get("status"),
                    "quarantine_reason": similarity_payload.get("quarantine_reason"),
                },
            }
        )
        _write_progress(resolved_progress_path, cycle_progress)

        challenger_payload: dict[str, Any]
        if similarity_payload.get("ok", False):
            challenger_payload = run_nightly_similarity_challenger_pipeline(
                export_db_path=export_db_path,
                label_db_path=label_db_path,
                result_db_path=result_db_path,
                similarity_db_path=similarity_db_path,
                ops_db_path=ops_db_path,
                as_of_date=resolved_as_of_date,
                publish_id=effective_publish_id or publish_id,
            )
        else:
            challenger_payload = {
                "ok": False,
                "status": "skipped",
                "reason": "similarity_failed",
            }
        cycle_progress.update(
            {
                "current_phase": "report",
                "challenger": {
                    "run_id": challenger_payload.get("run_id"),
                    "status": challenger_payload.get("status"),
                    "quarantine_reason": challenger_payload.get("quarantine_reason"),
                },
            }
        )
        _write_progress(resolved_progress_path, cycle_progress)

        report = build_daily_research_report(
            source_db_path=effective_source_db_path,
            result_db_path=result_db_path,
            ops_db_path=ops_db_path,
        )
        payload = {
            "ok": bool(candidate_payload.get("ok", False)) and bool(similarity_payload.get("ok", False)),
            "as_of_date": resolved_as_of_date,
            "publish_id": effective_publish_id or publish_id,
            "candidate": {
                "run_id": candidate_payload.get("run_id"),
                "status": candidate_payload.get("status"),
                "quarantine_reason": candidate_payload.get("quarantine_reason"),
            },
            "similarity": {
                "run_id": similarity_payload.get("run_id"),
                "status": similarity_payload.get("status"),
                "quarantine_reason": similarity_payload.get("quarantine_reason"),
            },
            "challenger": {
                "run_id": challenger_payload.get("run_id"),
                "status": challenger_payload.get("status"),
                "quarantine_reason": challenger_payload.get("quarantine_reason"),
            },
            "source_snapshot": snapshot_payload,
            "report": report,
        }
        cycle_progress["current_phase"] = "persist_review"
        _write_progress(resolved_progress_path, cycle_progress)
        persist_review_artifact(
            review_row=_build_daily_research_review_row(payload),
            ops_db_path=ops_db_path,
        )
        if report_path:
            Path(str(report_path)).expanduser().resolve().write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
                encoding="utf-8",
            )
        if text_report_path:
            Path(str(text_report_path)).expanduser().resolve().write_text(
                format_daily_research_text_report(payload),
                encoding="utf-8",
            )
        cycle_progress.update(
            {
                "status": "complete",
                "current_phase": "completed",
                "finished_at": _utcnow_iso(),
                "publish_id": payload.get("publish_id"),
                "promotion_ready": bool(((report.get("promotion_review") or {}).get("readiness_pass"))),
            }
        )
        _write_progress(resolved_progress_path, cycle_progress)
        return payload
    except Exception as exc:
        cycle_progress.update(
            {
                "status": "failed",
                "current_phase": "failed",
                "finished_at": _utcnow_iso(),
                "error_class": exc.__class__.__name__,
                "error_message": str(exc),
            }
        )
        _write_progress(resolved_progress_path, cycle_progress)
        raise
