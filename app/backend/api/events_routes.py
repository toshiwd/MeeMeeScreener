from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.backend.db import get_conn
from app.backend.events import jst_now
from app.backend.services.events import (
    _format_event_date,
    _format_event_timestamp,
    _is_events_lock_stale,
    _load_events_meta,
    _start_events_refresh,
)

router = APIRouter()


@router.get("/api/events/meta")
def events_meta():
    with get_conn() as conn:
        meta = _load_events_meta(conn)
        if meta.get("is_refreshing"):
            lock_started_at = meta.get("refresh_lock_started_at")
            if not lock_started_at or _is_events_lock_stale(lock_started_at):
                finished_at = jst_now().replace(tzinfo=None)
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
                    ["refresh_timeout", finished_at],
                )
                job_id = meta.get("refresh_lock_job_id")
                if job_id:
                    conn.execute(
                        """
                        UPDATE events_refresh_jobs
                        SET status = ?, finished_at = ?, error = ?
                        WHERE job_id = ? AND status = 'running'
                        """,
                        ["failed", finished_at, "refresh_timeout", job_id],
                    )
                meta = _load_events_meta(conn)
        rights_max = conn.execute(
            """
            SELECT MAX(COALESCE(last_rights_date, ex_date)) AS rights_max_date
            FROM ex_rights
            """
        ).fetchone()[0]
    payload = {
        "earnings_last_success_at": _format_event_timestamp(meta.get("earnings_last_success_at")),
        "rights_last_success_at": _format_event_timestamp(meta.get("rights_last_success_at")),
        "is_refreshing": bool(meta.get("is_refreshing")),
        "refresh_job_id": meta.get("refresh_lock_job_id"),
        "last_error": meta.get("last_error"),
        "last_attempt_at": _format_event_timestamp(meta.get("last_attempt_at")),
        "data_coverage": {"rights_max_date": _format_event_date(rights_max)},
    }
    return payload


@router.post("/api/events/refresh")
def events_refresh(reason: str | None = None):
    job_id = _start_events_refresh(reason)
    if not job_id:
        return JSONResponse(content={"error": "refresh_lock_failed"}, status_code=409)
    return JSONResponse(content={"refresh_job_id": job_id})


@router.get("/api/events/refresh/{job_id}")
def events_refresh_status(job_id: str):
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT status, started_at, finished_at, error
            FROM events_refresh_jobs
            WHERE job_id = ?
            """,
            [job_id],
        ).fetchone()
    if not row:
        return JSONResponse(content={"error": "job_not_found"}, status_code=404)
    return JSONResponse(
        content={
            "status": row[0],
            "started_at": _format_event_timestamp(row[1]),
            "finished_at": _format_event_timestamp(row[2]),
            "error": row[3],
        }
    )
