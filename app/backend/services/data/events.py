from __future__ import annotations

import threading
import uuid
from datetime import datetime, timedelta

from app.backend.db import get_conn
from app.backend.events import fetch_earnings_snapshot, fetch_rights_snapshot, fetch_industry_snapshot, jst_now
from app.services.screener_engine import _invalidate_screener_cache

_events_refresh_lock = threading.Lock()
_events_refresh_timeout = timedelta(minutes=30)


def _ensure_industry_master_table(conn) -> None:
    # events refresh rewrites industry_master from JPX source, so we only need table existence.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS industry_master (
            code VARCHAR PRIMARY KEY,
            name VARCHAR,
            sector33_code VARCHAR,
            sector33_name VARCHAR,
            market_code VARCHAR
        )
        """
    )


def _ensure_events_meta_row(conn) -> None:
    row = conn.execute("SELECT id FROM events_meta LIMIT 1").fetchone()
    if row:
        return
    conn.execute(
        """
        INSERT INTO events_meta (
            id,
            is_refreshing
        ) VALUES (
            1,
            FALSE
        );
        """
    )


def _normalize_meta_datetime(value: object | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _format_event_date(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, str):
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return None
    return None


def _format_event_timestamp(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return None
    return None


def _load_events_meta(conn) -> dict:
    _ensure_events_meta_row(conn)
    row = conn.execute(
        """
        SELECT
            earnings_last_success_at,
            rights_last_success_at,
            last_error,
            last_attempt_at,
            is_refreshing,
            refresh_lock_job_id,
            refresh_lock_started_at
        FROM events_meta
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return {}
    return {
        "earnings_last_success_at": row[0],
        "rights_last_success_at": row[1],
        "last_error": row[2],
        "last_attempt_at": row[3],
        "is_refreshing": bool(row[4]) if row[4] is not None else False,
        "refresh_lock_job_id": row[5],
        "refresh_lock_started_at": row[6],
    }


def _is_events_lock_stale(started_at: object | None) -> bool:
    locked_at = _normalize_meta_datetime(started_at)
    if locked_at is None:
        return False
    return jst_now().replace(tzinfo=None) - locked_at >= _events_refresh_timeout


def _update_events_job(job_id: str, status: str, finished_at: datetime, error: str | None) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE events_refresh_jobs
            SET status = ?, finished_at = ?, error = ?
            WHERE job_id = ?
            """,
            [status, finished_at, error, job_id],
        )


def _table_primary_keys(conn, table_name: str) -> list[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    except Exception:
        return []
    keys: list[tuple[int, str]] = []
    for row in rows:
        # PRAGMA table_info: cid, name, type, notnull, dflt_value, pk
        order = int(row[5] or 0)
        if order > 0:
            keys.append((order, str(row[1])))
    keys.sort(key=lambda item: item[0])
    return [name for _, name in keys]


def _dedupe_for_primary_key(conn, table_name: str, rows: list[dict]) -> list[dict]:
    if not rows:
        return rows
    pk_cols = _table_primary_keys(conn, table_name)
    if not pk_cols:
        return rows
    deduped: dict[tuple[object, ...], dict] = {}
    for row in rows:
        key: list[object] = []
        valid = True
        for col in pk_cols:
            value = row.get(col)
            if isinstance(value, str):
                value = value.strip()
            if value in (None, ""):
                valid = False
                break
            key.append(value)
        if not valid:
            continue
        deduped[tuple(key)] = row
    return list(deduped.values())


def _run_events_refresh(job_id: str, reason: str | None) -> None:
    earnings_rows: list[dict] = []
    rights_rows: list[dict] = []
    industry_rows: list[dict] = []
    errors: list[str] = []
    finished_at = None
    error_text = None
    
    # 1. Earnings
    try:
        earnings_rows = fetch_earnings_snapshot()
        if not earnings_rows:
            errors.append("earnings:no_rows")
    except Exception as exc:
        errors.append(f"earnings:{exc}")
        
    # 2. Rights
    try:
        rights_rows = fetch_rights_snapshot()
        if not rights_rows:
            errors.append("rights:no_rows")
    except Exception as exc:
        errors.append(f"rights:{exc}")

    # 3. Industry Master (Sector Data)
    try:
        industry_rows = fetch_industry_snapshot()
        if not industry_rows:
            errors.append("industry:no_rows")
    except Exception as exc:
        # Industry fetch failure isn't critical enough to stop others, but log it
        errors.append(f"industry:{exc}")
        

    try:
        finished_at = jst_now().replace(tzinfo=None)
        error_text = "; ".join(errors) if errors else None

        with get_conn() as conn:
            _ensure_events_meta_row(conn)
            
            # Update Earnings
            if earnings_rows:
                conn.execute("DELETE FROM earnings_planned WHERE source = 'JPX'")
                earnings_rows = _dedupe_for_primary_key(conn, "earnings_planned", earnings_rows)
                conn.executemany(
                    """
                    INSERT INTO earnings_planned (
                        code,
                        planned_date,
                        kind,
                        company_name,
                        source,
                        fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            row.get("code"),
                            row.get("planned_date"),
                            row.get("kind"),
                            row.get("company_name"),
                            row.get("source"),
                            row.get("fetched_at"),
                        )
                        for row in earnings_rows
                    ],
                )
                conn.execute(
                    "UPDATE events_meta SET earnings_last_success_at = ? WHERE id = 1",
                    [finished_at],
                )
                
            # Update Rights
            if rights_rows:
                conn.execute("DELETE FROM ex_rights WHERE source = 'JPX'")
                rights_rows = _dedupe_for_primary_key(conn, "ex_rights", rights_rows)
                conn.executemany(
                    """
                    INSERT INTO ex_rights (
                        code,
                        ex_date,
                        record_date,
                        category,
                        last_rights_date,
                        source,
                        fetched_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            row.get("code"),
                            row.get("ex_date"),
                            row.get("record_date"),
                            row.get("category"),
                            row.get("last_rights_date"),
                            row.get("source"),
                            row.get("fetched_at"),
                        )
                        for row in rights_rows
                    ],
                )
                conn.execute(
                    "UPDATE events_meta SET rights_last_success_at = ? WHERE id = 1",
                    [finished_at],
                )

            # Update Industry Master
            if industry_rows:
                _ensure_industry_master_table(conn)
                
                # Replace industry_master
                conn.execute("DELETE FROM industry_master")
                deduped = {}
                for row in industry_rows:
                    raw_code = row.get("code")
                    if raw_code is None:
                        continue
                    code = str(raw_code).strip()
                    if not code:
                        continue
                    deduped[code] = {**row, "code": code}
                industry_insert_rows = _dedupe_for_primary_key(
                    conn,
                    "industry_master",
                    list(deduped.values()),
                )
                conn.executemany(
                    """
                    INSERT INTO industry_master (code, name, sector33_code, sector33_name, market_code)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            row.get("code"),
                            row.get("name"),
                            row.get("sector33_code"),
                            row.get("sector33_name"),
                            row.get("market_code")
                        )
                        for row in industry_insert_rows
                    ]
                )
                # We don't save industry_last_success_at in events_meta yet as schema change is risky/not strictly needed
                # User will see 'success' status for the job if it works.

            conn.execute(
                """
                UPDATE events_meta
                SET
                    last_error = ?,
                    last_attempt_at = ?,
                    is_refreshing = FALSE,
                    refresh_lock_job_id = NULL,
                    refresh_lock_started_at = NULL
                WHERE id = 1
                """,
                [error_text, finished_at],
            )
        _invalidate_screener_cache()
    except Exception as exc:
        finished_at = jst_now().replace(tzinfo=None)
        error_text = f"refresh_failed:{exc}"
        with get_conn() as conn:
            _ensure_events_meta_row(conn)
            conn.execute(
                """
                UPDATE events_meta
                SET
                    last_error = ?,
                    last_attempt_at = ?,
                    is_refreshing = FALSE,
                    refresh_lock_job_id = NULL,
                    refresh_lock_started_at = NULL
                WHERE id = 1
                """,
                [error_text, finished_at],
            )
        _invalidate_screener_cache()

    status = "success" if not error_text else "failed"
    _update_events_job(job_id, status, finished_at, error_text)



def _start_events_refresh(reason: str | None) -> str:
    with _events_refresh_lock:
        with get_conn() as conn:
            meta = _load_events_meta(conn)
            refreshing = bool(meta.get("is_refreshing"))
            lock_started_at = meta.get("refresh_lock_started_at")
            lock_job_id = meta.get("refresh_lock_job_id")
            if refreshing and lock_job_id and not _is_events_lock_stale(lock_started_at):
                return lock_job_id
            if refreshing:
                conn.execute(
                    """
                    UPDATE events_meta
                    SET
                        is_refreshing = FALSE,
                        refresh_lock_job_id = NULL,
                        refresh_lock_started_at = NULL
                    WHERE id = 1
                    """
                )
            job_id = uuid.uuid4().hex
            started_at = jst_now().replace(tzinfo=None)
            conn.execute(
                """
                UPDATE events_meta
                SET
                    is_refreshing = TRUE,
                    refresh_lock_job_id = ?,
                    refresh_lock_started_at = ?,
                    last_attempt_at = ?
                WHERE id = 1
                """,
                [job_id, started_at, started_at],
            )
            conn.execute(
                """
                INSERT INTO events_refresh_jobs (
                    job_id,
                    status,
                    reason,
                    started_at
                ) VALUES (?, ?, ?, ?)
                """,
                [job_id, "running", reason, started_at],
            )
    thread = threading.Thread(target=_run_events_refresh, args=(job_id, reason), daemon=True)
    thread.start()
    return job_id
