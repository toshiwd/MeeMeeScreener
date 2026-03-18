from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from external_analysis.ops.ops_schema import connect_ops_db, ensure_ops_schema


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _normalize_date_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("-", "")
    if len(text) == 8 and text.isdigit():
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    return str(value)


def _retention_limit(name: str, default: int) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw is not None else int(default)
    except (TypeError, ValueError):
        value = int(default)
    return max(0, int(value))


def _trim_table_rows(
    conn,
    *,
    table_name: str,
    order_column: str,
    id_column: str,
    limit: int,
) -> None:
    if limit <= 0:
        conn.execute(f"DELETE FROM {table_name}")
        return
    conn.execute(
        f"""
        DELETE FROM {table_name}
        WHERE {id_column} IN (
            SELECT {id_column}
            FROM {table_name}
            ORDER BY {order_column} DESC, {id_column} DESC
            OFFSET ?
        )
        """,
        [int(limit)],
    )


def _apply_ops_retention(conn) -> None:
    _trim_table_rows(
        conn,
        table_name="external_job_runs",
        order_column="created_at",
        id_column="job_id",
        limit=_retention_limit("MEEMEE_EXTERNAL_JOB_RUNS_RETENTION", 200),
    )
    _trim_table_rows(
        conn,
        table_name="external_job_quarantine",
        order_column="created_at",
        id_column="quarantine_id",
        limit=_retention_limit("MEEMEE_EXTERNAL_JOB_QUARANTINE_RETENTION", 100),
    )
    _trim_table_rows(
        conn,
        table_name="external_work_items",
        order_column="created_at",
        id_column="work_id",
        limit=_retention_limit("MEEMEE_EXTERNAL_WORK_ITEMS_RETENTION", 500),
    )
    _trim_table_rows(
        conn,
        table_name="external_trade_teacher_profiles",
        order_column="created_at",
        id_column="profile_id",
        limit=_retention_limit("MEEMEE_EXTERNAL_TRADE_TEACHER_PROFILES_RETENTION", 2000),
    )
    _trim_table_rows(
        conn,
        table_name="external_state_eval_shadow_runs",
        order_column="created_at",
        id_column="shadow_id",
        limit=_retention_limit("MEEMEE_EXTERNAL_STATE_EVAL_SHADOW_RETENTION", 5000),
    )
    _trim_table_rows(
        conn,
        table_name="external_state_eval_failure_samples",
        order_column="created_at",
        id_column="sample_id",
        limit=_retention_limit("MEEMEE_EXTERNAL_STATE_EVAL_FAILURE_SAMPLES_RETENTION", 1000),
    )
    _trim_table_rows(
        conn,
        table_name="external_state_eval_tag_rollups",
        order_column="created_at",
        id_column="rollup_id",
        limit=_retention_limit("MEEMEE_EXTERNAL_STATE_EVAL_TAG_ROLLUPS_RETENTION", 4000),
    )
    _trim_table_rows(
        conn,
        table_name="external_state_eval_daily_summaries",
        order_column="created_at",
        id_column="summary_id",
        limit=_retention_limit("MEEMEE_EXTERNAL_STATE_EVAL_DAILY_SUMMARIES_RETENTION", 400),
    )
    _trim_table_rows(
        conn,
        table_name="external_replay_runs",
        order_column="created_at",
        id_column="replay_id",
        limit=_retention_limit("MEEMEE_EXTERNAL_REPLAY_RUNS_RETENTION", 20),
    )
    _trim_table_rows(
        conn,
        table_name="external_replay_summaries",
        order_column="created_at",
        id_column="summary_id",
        limit=_retention_limit("MEEMEE_EXTERNAL_REPLAY_SUMMARIES_RETENTION", 20),
    )
    _trim_table_rows(
        conn,
        table_name="external_promotion_decisions",
        order_column="created_at",
        id_column="decision_id",
        limit=_retention_limit("MEEMEE_EXTERNAL_PROMOTION_DECISIONS_RETENTION", 200),
    )
    _trim_table_rows(
        conn,
        table_name="external_review_artifacts",
        order_column="created_at",
        id_column="review_id",
        limit=_retention_limit("MEEMEE_EXTERNAL_REVIEW_ARTIFACTS_RETENTION", 20),
    )


def upsert_job_run(
    *,
    job_id: str,
    job_type: str,
    status: str,
    as_of_date: str | None = None,
    publish_id: str | None = None,
    attempt: int = 1,
    checkpoint_uri: str | None = None,
    error_class: str | None = None,
    details: dict[str, Any] | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    ops_db_path: str | None = None,
) -> None:
    conn = connect_ops_db(ops_db_path)
    try:
        ensure_ops_schema(conn)
        existing = conn.execute("SELECT created_at FROM external_job_runs WHERE job_id = ?", [job_id]).fetchone()
        created_at = existing[0] if existing else _utcnow()
        conn.execute(
            """
            INSERT OR REPLACE INTO external_job_runs (
                job_id, job_type, status, as_of_date, publish_id, attempt, created_at, started_at, finished_at,
                checkpoint_uri, error_class, details_json
            ) VALUES (?, ?, ?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                job_id,
                job_type,
                status,
                _normalize_date_text(as_of_date),
                publish_id,
                int(attempt),
                created_at,
                started_at,
                finished_at,
                checkpoint_uri,
                error_class,
                json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
            ],
        )
        _apply_ops_retention(conn)
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def insert_quarantine_record(
    *,
    quarantine_id: str,
    job_type: str,
    as_of_date: str | None,
    publish_id: str | None,
    attempt_count: int,
    reason: str,
    payload: dict[str, Any],
    ops_db_path: str | None = None,
) -> None:
    conn = connect_ops_db(ops_db_path)
    try:
        ensure_ops_schema(conn)
        conn.execute(
            """
            INSERT OR REPLACE INTO external_job_quarantine (
                quarantine_id, job_type, as_of_date, publish_id, attempt_count, reason, payload_json, created_at
            ) VALUES (?, ?, CAST(? AS DATE), ?, ?, ?, ?, ?)
            """,
            [
                quarantine_id,
                job_type,
                _normalize_date_text(as_of_date),
                publish_id,
                int(attempt_count),
                reason,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                _utcnow(),
            ],
        )
        _apply_ops_retention(conn)
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def upsert_work_item(
    *,
    work_id: str,
    work_type: str,
    scope_type: str,
    scope_id: str,
    status: str,
    depends_on: list[str] | None = None,
    payload: dict[str, Any] | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    error_class: str | None = None,
    ops_db_path: str | None = None,
) -> None:
    conn = connect_ops_db(ops_db_path)
    try:
        ensure_ops_schema(conn)
        existing = conn.execute("SELECT created_at FROM external_work_items WHERE work_id = ?", [work_id]).fetchone()
        created_at = existing[0] if existing else _utcnow()
        conn.execute(
            """
            INSERT OR REPLACE INTO external_work_items (
                work_id, work_type, scope_type, scope_id, status, depends_on_json, payload_json,
                created_at, started_at, finished_at, error_class
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                work_id,
                work_type,
                scope_type,
                scope_id,
                status,
                json.dumps(depends_on or [], ensure_ascii=False, sort_keys=True),
                json.dumps(payload or {}, ensure_ascii=False, sort_keys=True),
                created_at,
                started_at,
                finished_at,
                error_class,
            ],
        )
        _apply_ops_retention(conn)
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def load_work_item(*, work_id: str, ops_db_path: str | None = None) -> dict[str, Any] | None:
    conn = connect_ops_db(ops_db_path)
    try:
        ensure_ops_schema(conn)
        row = conn.execute(
            """
            SELECT work_id, work_type, scope_type, scope_id, status, depends_on_json, payload_json,
                   created_at, started_at, finished_at, error_class
            FROM external_work_items
            WHERE work_id = ?
            """,
            [work_id],
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    return {
        "work_id": str(row[0]),
        "work_type": str(row[1]),
        "scope_type": str(row[2]),
        "scope_id": str(row[3]),
        "status": str(row[4]),
        "depends_on": json.loads(row[5]) if isinstance(row[5], str) else (row[5] or []),
        "payload": json.loads(row[6]) if isinstance(row[6], str) else (row[6] or {}),
        "created_at": row[7],
        "started_at": row[8],
        "finished_at": row[9],
        "error_class": None if row[10] is None else str(row[10]),
    }


def upsert_replay_run(
    *,
    replay_id: str,
    job_type: str,
    status: str,
    start_as_of_date: str,
    end_as_of_date: str,
    max_days: int | None = None,
    universe_filter: str | None = None,
    universe_limit: int | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    last_completed_as_of_date: str | None = None,
    error_class: str | None = None,
    details: dict[str, Any] | None = None,
    ops_db_path: str | None = None,
) -> None:
    conn = connect_ops_db(ops_db_path)
    try:
        ensure_ops_schema(conn)
        existing = conn.execute("SELECT created_at FROM external_replay_runs WHERE replay_id = ?", [replay_id]).fetchone()
        created_at = existing[0] if existing else _utcnow()
        conn.execute(
            """
            INSERT OR REPLACE INTO external_replay_runs (
                replay_id, job_type, status, start_as_of_date, end_as_of_date, max_days, universe_filter, universe_limit,
                created_at, started_at, finished_at, last_completed_as_of_date, error_class, details_json
            ) VALUES (?, ?, ?, CAST(? AS DATE), CAST(? AS DATE), ?, ?, ?, ?, ?, ?, CAST(? AS DATE), ?, ?)
            """,
            [
                replay_id,
                job_type,
                status,
                _normalize_date_text(start_as_of_date),
                _normalize_date_text(end_as_of_date),
                None if max_days is None else int(max_days),
                universe_filter,
                None if universe_limit is None else int(universe_limit),
                created_at,
                started_at,
                finished_at,
                _normalize_date_text(last_completed_as_of_date),
                error_class,
                json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
            ],
        )
        _apply_ops_retention(conn)
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def upsert_replay_day(
    *,
    replay_id: str,
    as_of_date: str,
    status: str,
    attempt: int = 1,
    publish_id: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    error_class: str | None = None,
    details: dict[str, Any] | None = None,
    ops_db_path: str | None = None,
) -> None:
    conn = connect_ops_db(ops_db_path)
    try:
        ensure_ops_schema(conn)
        conn.execute(
            """
            INSERT OR REPLACE INTO external_replay_days (
                replay_id, as_of_date, status, attempt, publish_id, started_at, finished_at, error_class, details_json
            ) VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                replay_id,
                _normalize_date_text(as_of_date),
                status,
                int(attempt),
                publish_id,
                started_at,
                finished_at,
                error_class,
                json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
            ],
        )
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def persist_replay_summary(
    *,
    summary_id: str,
    replay_id: str,
    start_as_of_date: str,
    end_as_of_date: str,
    total_days: int,
    success_days: int,
    failed_days: int,
    skipped_days: int,
    summary: dict[str, Any],
    ops_db_path: str | None = None,
) -> None:
    conn = connect_ops_db(ops_db_path)
    try:
        ensure_ops_schema(conn)
        conn.execute(
            "DELETE FROM external_replay_summaries WHERE replay_id = ?",
            [replay_id],
        )
        conn.execute(
            """
            INSERT INTO external_replay_summaries (
                summary_id, replay_id, start_as_of_date, end_as_of_date, total_days, success_days, failed_days,
                skipped_days, summary_json, created_at
            ) VALUES (?, ?, CAST(? AS DATE), CAST(? AS DATE), ?, ?, ?, ?, ?, ?)
            """,
            [
                summary_id,
                replay_id,
                _normalize_date_text(start_as_of_date),
                _normalize_date_text(end_as_of_date),
                int(total_days),
                int(success_days),
                int(failed_days),
                int(skipped_days),
                json.dumps(summary, ensure_ascii=False, sort_keys=True),
                _utcnow(),
            ],
        )
        _apply_ops_retention(conn)
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def persist_replay_readiness(
    *,
    replay_id: str,
    readiness_rows: list[dict[str, Any]],
    ops_db_path: str | None = None,
) -> None:
    conn = connect_ops_db(ops_db_path)
    try:
        ensure_ops_schema(conn)
        conn.execute("DELETE FROM external_replay_readiness WHERE replay_id = ?", [replay_id])
        if readiness_rows:
            columns = list(readiness_rows[0].keys())
            conn.executemany(
                f"INSERT INTO external_replay_readiness ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})",
                [[row[column] for column in columns] for row in readiness_rows],
            )
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def persist_metric_daily_summaries(
    *,
    scope_type: str,
    scope_id: str,
    daily_rows: list[dict[str, Any]],
    ops_db_path: str | None = None,
) -> None:
    conn = connect_ops_db(ops_db_path)
    try:
        ensure_ops_schema(conn)
        conn.execute(
            "DELETE FROM external_metric_daily_summaries WHERE scope_type = ? AND scope_id = ?",
            [scope_type, scope_id],
        )
        if daily_rows:
            columns = list(daily_rows[0].keys())
            conn.executemany(
                f"INSERT INTO external_metric_daily_summaries ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})",
                [[row[column] for column in columns] for row in daily_rows],
            )
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def persist_comparison_rollups(
    *,
    scope_type: str,
    scope_id: str,
    rollup_rows: list[dict[str, Any]],
    ops_db_path: str | None = None,
) -> None:
    conn = connect_ops_db(ops_db_path)
    try:
        ensure_ops_schema(conn)
        conn.execute(
            "DELETE FROM external_comparison_rollups WHERE scope_type = ? AND scope_id = ?",
            [scope_type, scope_id],
        )
        if rollup_rows:
            columns = list(rollup_rows[0].keys())
            conn.executemany(
                f"INSERT INTO external_comparison_rollups ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})",
                [[row[column] for column in columns] for row in rollup_rows],
            )
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def persist_promotion_readiness_rows(
    *,
    scope_type: str,
    scope_id: str,
    readiness_rows: list[dict[str, Any]],
    ops_db_path: str | None = None,
) -> None:
    conn = connect_ops_db(ops_db_path)
    try:
        ensure_ops_schema(conn)
        conn.execute(
            "DELETE FROM external_promotion_readiness WHERE scope_type = ? AND scope_id = ?",
            [scope_type, scope_id],
        )
        if readiness_rows:
            columns = list(readiness_rows[0].keys())
            conn.executemany(
                f"INSERT INTO external_promotion_readiness ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})",
                [[row[column] for column in columns] for row in readiness_rows],
            )
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def persist_review_artifact(
    *,
    review_row: dict[str, Any],
    ops_db_path: str | None = None,
) -> None:
    conn = connect_ops_db(ops_db_path)
    try:
        ensure_ops_schema(conn)
        conn.execute("DELETE FROM external_review_artifacts WHERE review_id = ?", [review_row["review_id"]])
        columns = list(review_row.keys())
        conn.execute(
            f"INSERT INTO external_review_artifacts ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})",
            [review_row[column] for column in columns],
        )
        _apply_ops_retention(conn)
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def persist_promotion_decision(
    *,
    decision_row: dict[str, Any],
    ops_db_path: str | None = None,
) -> None:
    conn = connect_ops_db(ops_db_path)
    try:
        ensure_ops_schema(conn)
        columns = list(decision_row.keys())
        conn.execute(
            f"INSERT INTO external_promotion_decisions ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})",
            [decision_row[column] for column in columns],
        )
        _apply_ops_retention(conn)
        conn.execute("CHECKPOINT")
    finally:
        conn.close()
