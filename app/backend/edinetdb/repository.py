from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

from app.backend.edinetdb.schema import ensure_edinetdb_schema, utcnow_naive
from app.db.session import get_conn_for_path


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _to_text(value: Any, fallback: str = "unknown") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text or fallback


def _extract_list(payload: Any, keys: tuple[str, ...]) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return value
    for value in payload.values():
        if isinstance(value, list):
            return value
    return []


@dataclass
class TaskRow:
    task_key: str
    job_name: str
    phase: str
    edinet_code: str
    endpoint: str
    params: dict[str, Any]
    priority: int
    status: str
    tries: int
    http_status: int | None
    last_error: str | None


class EdinetdbRepository:
    def __init__(self, db_path: str | Path):
        self._db_path = str(Path(db_path).expanduser().resolve())

    def _connect_read(self):
        return get_conn_for_path(self._db_path, timeout_sec=2.5, read_only=True)

    def _connect_write(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(self._db_path)
        ensure_edinetdb_schema(conn)
        return conn

    def task_key_of(self, edinet_code: str, endpoint: str, params: dict[str, Any] | None = None) -> str:
        params = params or {}
        token = f"{edinet_code}|{endpoint}|{_json_dumps(params)}"
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def _enqueue_upsert_sql(self, force: bool) -> str:
        if force:
            return """
                INSERT INTO edinetdb_task_queue (
                    task_key, job_name, phase, edinet_code, endpoint, params_json,
                    priority, status, tries, http_status, last_error, retry_at, fetched_at, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, NULL, NULL, NULL, NULL, ?, ?)
                ON CONFLICT(task_key) DO UPDATE SET
                    job_name = excluded.job_name,
                    phase = excluded.phase,
                    edinet_code = excluded.edinet_code,
                    endpoint = excluded.endpoint,
                    params_json = excluded.params_json,
                    priority = excluded.priority,
                    status = 'pending',
                    tries = 0,
                    http_status = NULL,
                    last_error = NULL,
                    retry_at = NULL,
                    fetched_at = NULL,
                    updated_at = excluded.updated_at
            """
        return """
            INSERT INTO edinetdb_task_queue (
                task_key, job_name, phase, edinet_code, endpoint, params_json,
                priority, status, tries, http_status, last_error, retry_at, fetched_at, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, NULL, NULL, NULL, NULL, ?, ?)
            ON CONFLICT(task_key) DO UPDATE SET
                job_name = excluded.job_name,
                phase = excluded.phase,
                edinet_code = excluded.edinet_code,
                endpoint = excluded.endpoint,
                params_json = excluded.params_json,
                priority = CASE
                    WHEN edinetdb_task_queue.priority > excluded.priority THEN edinetdb_task_queue.priority
                    ELSE excluded.priority
                END,
                status = CASE
                    WHEN edinetdb_task_queue.status = 'ok' THEN 'ok'
                    ELSE 'pending'
                END,
                retry_at = CASE
                    WHEN edinetdb_task_queue.status = 'ok' THEN edinetdb_task_queue.retry_at
                    ELSE NULL
                END,
                last_error = CASE
                    WHEN edinetdb_task_queue.status = 'ok' THEN edinetdb_task_queue.last_error
                    ELSE NULL
                END,
                updated_at = excluded.updated_at
        """

    def enqueue_tasks_bulk(self, tasks: list[dict[str, Any]], *, force: bool = False) -> list[str]:
        if not tasks:
            return []
        now = utcnow_naive()
        rows: list[list[Any]] = []
        keys: list[str] = []
        for task in tasks:
            params = task.get("params") or {}
            task_key = self.task_key_of(str(task["edinet_code"]), str(task["endpoint"]), params)
            keys.append(task_key)
            rows.append(
                [
                    task_key,
                    str(task["job_name"]),
                    str(task.get("phase") or "general"),
                    str(task["edinet_code"]),
                    str(task["endpoint"]),
                    _json_dumps(params),
                    int(task.get("priority") or 0),
                    now,
                    now,
                ]
            )
        with self._connect_write() as conn:
            conn.executemany(self._enqueue_upsert_sql(force), rows)
        return keys

    def enqueue_task(
        self,
        *,
        job_name: str,
        edinet_code: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        priority: int = 0,
        phase: str = "general",
        force: bool = False,
    ) -> str:
        rows = self.enqueue_tasks_bulk(
            [
                {
                    "job_name": job_name,
                    "phase": phase,
                    "edinet_code": edinet_code,
                    "endpoint": endpoint,
                    "params": params or {},
                    "priority": int(priority),
                }
            ],
            force=force,
        )
        return rows[0]

    def next_runnable_task(self, job_name: str, *, phase: str | None = None, now: datetime | None = None) -> TaskRow | None:
        now = now or utcnow_naive()
        sql = """
            SELECT task_key, job_name, phase, edinet_code, endpoint, params_json, priority, status, tries, http_status, last_error
            FROM edinetdb_task_queue
            WHERE job_name = ?
              AND status IN ('pending', 'retry_wait')
              AND (retry_at IS NULL OR retry_at <= ?)
        """
        params: list[Any] = [job_name, now]
        if phase is not None:
            sql += " AND phase = ?"
            params.append(phase)
        sql += " ORDER BY priority DESC, COALESCE(retry_at, TIMESTAMP '1970-01-01') ASC, created_at ASC LIMIT 1"
        with self._connect_read() as conn:
            row = conn.execute(sql, params).fetchone()
        if not row:
            return None
        return TaskRow(
            task_key=row[0],
            job_name=row[1],
            phase=row[2] or "general",
            edinet_code=row[3],
            endpoint=row[4],
            params=json.loads(row[5] or "{}"),
            priority=int(row[6] or 0),
            status=row[7],
            tries=int(row[8] or 0),
            http_status=row[9],
            last_error=row[10],
        )

    def mark_ok(self, task_key: str, *, http_status: int = 200, fetched_at: datetime | None = None) -> None:
        fetched_at = fetched_at or utcnow_naive()
        with self._connect_write() as conn:
            conn.execute(
                """
                UPDATE edinetdb_task_queue
                SET status = 'ok',
                    tries = tries + 1,
                    http_status = ?,
                    fetched_at = ?,
                    retry_at = NULL,
                    last_error = NULL,
                    updated_at = ?
                WHERE task_key = ?
                """,
                [http_status, fetched_at, fetched_at, task_key],
            )

    def mark_failed(self, task_key: str, *, error: str, http_status: int | None = None) -> None:
        now = utcnow_naive()
        with self._connect_write() as conn:
            conn.execute(
                """
                UPDATE edinetdb_task_queue
                SET status = 'failed',
                    tries = tries + 1,
                    http_status = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE task_key = ?
                """,
                [http_status, str(error), now, task_key],
            )

    def mark_retry_wait(
        self,
        task_key: str,
        *,
        error: str,
        retry_at: datetime,
        http_status: int | None = None,
    ) -> None:
        now = utcnow_naive()
        with self._connect_write() as conn:
            conn.execute(
                """
                UPDATE edinetdb_task_queue
                SET status = 'retry_wait',
                    tries = tries + 1,
                    http_status = ?,
                    last_error = ?,
                    retry_at = ?,
                    updated_at = ?
                WHERE task_key = ?
                """,
                [http_status, str(error), retry_at, now, task_key],
            )

    def mark_skipped(self, task_key: str, *, reason: str) -> None:
        now = utcnow_naive()
        with self._connect_write() as conn:
            conn.execute(
                """
                UPDATE edinetdb_task_queue
                SET status = 'skipped',
                    last_error = ?,
                    updated_at = ?
                WHERE task_key = ?
                """,
                [reason, now, task_key],
            )

    def count_tasks(self, job_name: str, *, phase: str | None = None) -> dict[str, int]:
        sql = """
            SELECT status, COUNT(*)
            FROM edinetdb_task_queue
            WHERE job_name = ?
        """
        params: list[Any] = [job_name]
        if phase is not None:
            sql += " AND phase = ?"
            params.append(phase)
        sql += " GROUP BY status"
        out: dict[str, int] = {}
        with self._connect_read() as conn:
            rows = conn.execute(sql, params).fetchall()
        for status, cnt in rows:
            out[str(status)] = int(cnt)
        return out

    def pending_task_count(self, job_name: str) -> int:
        with self._connect_read() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM edinetdb_task_queue
                WHERE job_name = ?
                  AND status IN ('pending', 'retry_wait')
                """,
                [job_name],
            ).fetchone()
        return int((row[0] if row else 0) or 0)

    def save_company_map(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        now = utcnow_naive()
        payload: list[tuple[Any, ...]] = []
        for row in rows:
            sec_code = _to_text(row.get("sec_code"), fallback="")
            if not sec_code:
                continue
            payload.append(
                (
                    sec_code,
                    _to_text(row.get("edinet_code"), fallback=""),
                    row.get("name"),
                    row.get("industry"),
                    row.get("updated_at") or now,
                )
            )
        if not payload:
            return
        with self._connect_write() as conn:
            conn.executemany(
                """
                INSERT INTO edinetdb_company_map(sec_code, edinet_code, name, industry, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(sec_code) DO UPDATE SET
                    edinet_code = excluded.edinet_code,
                    name = excluded.name,
                    industry = excluded.industry,
                    updated_at = excluded.updated_at
                """,
                payload,
            )

    def company_map_count(self) -> int:
        with self._connect_read() as conn:
            row = conn.execute("SELECT COUNT(*) FROM edinetdb_company_map").fetchone()
        return int((row[0] if row else 0) or 0)

    def lookup_edinet_codes(self, sec_codes: list[str]) -> dict[str, str]:
        codes = sorted({str(code).strip() for code in sec_codes if str(code).strip()})
        if not codes:
            return {}
        placeholders = ",".join(["?"] * len(codes))
        with self._connect_read() as conn:
            rows = conn.execute(
                f"""
                SELECT sec_code, edinet_code
                FROM edinetdb_company_map
                WHERE sec_code IN ({placeholders})
                  AND edinet_code IS NOT NULL
                  AND edinet_code <> ''
                """,
                codes,
            ).fetchall()
        return {str(sec): str(ed) for sec, ed in rows}

    def upsert_unmapped_code(self, sec_code: str, reason: str) -> None:
        now = utcnow_naive()
        with self._connect_write() as conn:
            conn.execute(
                """
                INSERT INTO edinetdb_unmapped_codes(sec_code, reason, first_seen_at, last_seen_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(sec_code) DO UPDATE SET
                    reason = excluded.reason,
                    last_seen_at = excluded.last_seen_at
                """,
                [sec_code, reason, now, now],
            )

    def get_company_latest(self, edinet_code: str) -> dict[str, Any] | None:
        with self._connect_read() as conn:
            row = conn.execute(
                """
                SELECT edinet_code, latest_fiscal_year, latest_hash, fetched_at, last_checked_at
                FROM edinetdb_company_latest
                WHERE edinet_code = ?
                """,
                [edinet_code],
            ).fetchone()
        if not row:
            return None
        return {
            "edinet_code": row[0],
            "latest_fiscal_year": row[1],
            "latest_hash": row[2],
            "fetched_at": row[3],
            "last_checked_at": row[4],
        }

    def get_company_latest_bulk(self, edinet_codes: list[str]) -> dict[str, dict[str, Any]]:
        codes = sorted({str(code).strip() for code in edinet_codes if str(code).strip()})
        if not codes:
            return {}
        placeholders = ",".join(["?"] * len(codes))
        with self._connect_read() as conn:
            rows = conn.execute(
                f"""
                SELECT edinet_code, latest_fiscal_year, latest_hash, fetched_at, last_checked_at
                FROM edinetdb_company_latest
                WHERE edinet_code IN ({placeholders})
                """,
                codes,
            ).fetchall()
        out: dict[str, dict[str, Any]] = {}
        for row in rows:
            out[str(row[0])] = {
                "latest_fiscal_year": row[1],
                "latest_hash": row[2],
                "fetched_at": row[3],
                "last_checked_at": row[4],
            }
        return out

    def save_company_latest(
        self,
        edinet_code: str,
        *,
        latest_fiscal_year: str | None,
        latest_hash: str,
        fetched_at: datetime | None = None,
        last_checked_at: datetime | None = None,
    ) -> None:
        fetched_at = fetched_at or utcnow_naive()
        last_checked_at = last_checked_at or fetched_at
        with self._connect_write() as conn:
            conn.execute(
                """
                INSERT INTO edinetdb_company_latest(
                    edinet_code, latest_fiscal_year, latest_hash, fetched_at, last_checked_at
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(edinet_code) DO UPDATE SET
                    latest_fiscal_year = excluded.latest_fiscal_year,
                    latest_hash = excluded.latest_hash,
                    fetched_at = excluded.fetched_at,
                    last_checked_at = excluded.last_checked_at
                """,
                [edinet_code, latest_fiscal_year, latest_hash, fetched_at, last_checked_at],
            )

    def upsert_financials(self, edinet_code: str, payload: Any, *, fetched_at: datetime | None = None) -> int:
        fetched_at = fetched_at or utcnow_naive()
        records = _extract_list(payload, ("financials", "items", "data", "results"))
        if not records and isinstance(payload, dict):
            if any(key in payload for key in ("fiscal_year", "fiscalYear", "year")):
                records = [payload]
        rows: list[tuple[Any, ...]] = []
        for record in records:
            if not isinstance(record, dict):
                continue
            fiscal_year = _to_text(
                record.get("fiscal_year") or record.get("fiscalYear") or record.get("year"),
                fallback="unknown",
            )
            accounting_standard = _to_text(
                record.get("accounting_standard")
                or record.get("accountingStandard")
                or record.get("standard")
                or record.get("accounting"),
                fallback="unknown",
            )
            rows.append((edinet_code, fiscal_year, accounting_standard, _json_dumps(record), fetched_at))
        if not rows:
            return 0
        with self._connect_write() as conn:
            conn.executemany(
                """
                INSERT INTO edinetdb_financials(edinet_code, fiscal_year, accounting_standard, payload_json, fetched_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(edinet_code, fiscal_year, accounting_standard) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    fetched_at = excluded.fetched_at
                """,
                rows,
            )
        return len(rows)

    def upsert_ratios(self, edinet_code: str, payload: Any, *, fetched_at: datetime | None = None) -> int:
        fetched_at = fetched_at or utcnow_naive()
        records = _extract_list(payload, ("ratios", "items", "data", "results"))
        if not records and isinstance(payload, dict):
            if any(key in payload for key in ("fiscal_year", "fiscalYear", "year")):
                records = [payload]
        rows: list[tuple[Any, ...]] = []
        for record in records:
            if not isinstance(record, dict):
                continue
            fiscal_year = _to_text(
                record.get("fiscal_year") or record.get("fiscalYear") or record.get("year"),
                fallback="unknown",
            )
            accounting_standard = _to_text(
                record.get("accounting_standard")
                or record.get("accountingStandard")
                or record.get("standard")
                or record.get("accounting"),
                fallback="unknown",
            )
            rows.append((edinet_code, fiscal_year, accounting_standard, _json_dumps(record), fetched_at))
        if not rows:
            return 0
        with self._connect_write() as conn:
            conn.executemany(
                """
                INSERT INTO edinetdb_ratios(edinet_code, fiscal_year, accounting_standard, payload_json, fetched_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(edinet_code, fiscal_year, accounting_standard) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    fetched_at = excluded.fetched_at
                """,
                rows,
            )
        return len(rows)

    def upsert_text_blocks(
        self,
        edinet_code: str,
        payload: Any,
        *,
        fetched_at: datetime | None = None,
        fallback_year: str | None = None,
    ) -> int:
        fetched_at = fetched_at or utcnow_naive()
        fallback_year = _to_text(fallback_year, fallback="unknown") if fallback_year is not None else "unknown"
        records = _extract_list(payload, ("text_blocks", "blocks", "items", "data", "results"))
        rows: list[tuple[Any, ...]] = []
        if not records and isinstance(payload, dict):
            for key, value in payload.items():
                if isinstance(value, str):
                    rows.append((edinet_code, fallback_year, _to_text(key, "unknown"), value, fetched_at))
        else:
            for idx, record in enumerate(records):
                if not isinstance(record, dict):
                    continue
                fiscal_year = _to_text(
                    record.get("fiscal_year") or record.get("fiscalYear") or record.get("year") or fallback_year,
                    fallback="unknown",
                )
                block_name = _to_text(
                    record.get("block_name")
                    or record.get("blockName")
                    or record.get("name")
                    or record.get("title")
                    or f"block_{idx + 1}",
                    fallback=f"block_{idx + 1}",
                )
                text = (
                    record.get("text")
                    or record.get("content")
                    or record.get("value")
                    or _json_dumps(record)
                )
                rows.append((edinet_code, fiscal_year, block_name, str(text), fetched_at))
        if not rows:
            return 0
        with self._connect_write() as conn:
            conn.executemany(
                """
                INSERT INTO edinetdb_text_blocks(edinet_code, fiscal_year, block_name, text, fetched_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(edinet_code, fiscal_year, block_name) DO UPDATE SET
                    text = excluded.text,
                    fetched_at = excluded.fetched_at
                """,
                rows,
            )
        return len(rows)

    def upsert_analysis(self, edinet_code: str, payload: Any, *, fetched_at: datetime | None = None) -> int:
        fetched_at = fetched_at or utcnow_naive()
        rows: list[tuple[Any, ...]] = []
        if isinstance(payload, list):
            iterable = payload
        elif isinstance(payload, dict):
            iterable = _extract_list(payload, ("analysis", "items", "data", "results"))
            if not iterable:
                iterable = [payload]
        else:
            iterable = []
        for item in iterable:
            if not isinstance(item, dict):
                continue
            asof = _to_text(
                item.get("asof_date") or item.get("asOfDate") or item.get("asof") or item.get("date"),
                fallback=fetched_at.date().isoformat(),
            )
            rows.append((edinet_code, asof, _json_dumps(item), fetched_at))
        if not rows:
            return 0
        with self._connect_write() as conn:
            conn.executemany(
                """
                INSERT INTO edinetdb_analysis(edinet_code, asof_date, payload_json, fetched_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(edinet_code, asof_date) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    fetched_at = excluded.fetched_at
                """,
                rows,
            )
        return len(rows)

    def set_meta(self, key: str, value: Any) -> None:
        now = utcnow_naive()
        with self._connect_write() as conn:
            conn.execute(
                """
                INSERT INTO edinetdb_meta(key, value_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json = excluded.value_json,
                    updated_at = excluded.updated_at
                """,
                [key, _json_dumps(value), now],
            )

    def get_meta(self, key: str) -> Any | None:
        with self._connect_read() as conn:
            row = conn.execute("SELECT value_json FROM edinetdb_meta WHERE key = ?", [key]).fetchone()
        if not row or row[0] is None:
            return None
        try:
            return json.loads(row[0])
        except json.JSONDecodeError:
            return None

    def log_api_call(
        self,
        *,
        job_name: str,
        endpoint: str,
        edinet_code: str | None,
        http_status: int | None,
        error_type: str | None,
        called_at: datetime | None = None,
        jst_date: date | None = None,
    ) -> None:
        called_at = called_at or utcnow_naive()
        with self._connect_write() as conn:
            conn.execute(
                """
                INSERT INTO edinetdb_api_call_log(
                    id, called_at, jst_date, job_name, endpoint, edinet_code, http_status, error_type
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    uuid.uuid4().hex,
                    called_at,
                    jst_date,
                    job_name,
                    endpoint,
                    edinet_code,
                    http_status,
                    error_type,
                ],
            )

    def migrate_legacy_backfill_phase(self, job_name: str = "backfill_700") -> int:
        with self._connect_write() as conn:
            conn.execute(
                """
                UPDATE edinetdb_task_queue
                SET phase = CASE
                    WHEN endpoint IN ('companies_detail', 'companies_financials', 'companies_ratios') THEN 'backfill_core'
                    WHEN endpoint = 'companies_text-blocks' THEN 'backfill_text'
                    WHEN endpoint = 'companies_analysis' THEN 'backfill_analysis'
                    ELSE phase
                END,
                updated_at = ?
                WHERE job_name = ?
                  AND phase = 'backfill'
                """,
                [utcnow_naive(), job_name],
            )
            row = conn.execute("SELECT COUNT(*) FROM edinetdb_task_queue WHERE job_name = ? AND phase = 'backfill'", [job_name]).fetchone()
        remaining = int((row[0] if row else 0) or 0)
        return remaining
