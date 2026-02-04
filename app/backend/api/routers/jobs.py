from __future__ import annotations

import logging

import os

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.backend.core.jobs import cleanup_stale_jobs, job_manager
from app.backend.core.config import config
from app.db.session import get_conn

router = APIRouter()
logger = logging.getLogger(__name__)


def _count_active_jobs(job_type: str) -> int:
    with get_conn() as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM sys_jobs WHERE type = ? AND status IN ('queued', 'running')",
            [job_type],
        ).fetchone()[0]


def _submit_job(job_type: str):
    cleanup_stale_jobs()
    if _count_active_jobs(job_type) > 0:
        return JSONResponse(status_code=409, content={"error": "Job already running"})
    job_id = job_manager.submit(job_type, {})
    return {"ok": True, "job_id": job_id}


@router.post("/api/jobs/txt-update")
def submit_txt_update():
    try:
        return _submit_job("txt_update")
    except Exception as exc:
        logger.exception("Error submitting txt_update: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/jobs/force-sync")
def submit_force_sync():
    try:
        return _submit_job("force_sync")
    except Exception as exc:
        logger.exception("Error submitting force_sync: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/phase/rebuild")
def submit_phase_rebuild():
    try:
        return _submit_job("phase_rebuild")
    except Exception as exc:
        logger.exception("Error submitting phase_rebuild: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/txt_update/run")
def run_txt_update_legacy():
    cleanup_stale_jobs()
    if _count_active_jobs("txt_update") > 0:
        return JSONResponse(status_code=409, content={"ok": False, "error": "update_in_progress"})

    code_path = os.path.abspath(str(config.PAN_CODE_TXT_PATH))
    if not os.path.isfile(code_path):
        return JSONResponse(status_code=400, content={"ok": False, "error": "code_txt_missing"})

    vbs_path = os.path.abspath(str(config.PAN_EXPORT_VBS_PATH))
    if not os.path.isfile(vbs_path):
        return JSONResponse(
            status_code=500, content={"ok": False, "error": f"vbs_not_found:{vbs_path}"}
        )

    job_id = job_manager.submit("txt_update", {})
    if not job_id:
        return JSONResponse(status_code=409, content={"ok": False, "error": "update_in_progress"})

    return {"ok": True, "started": True, "job_id": job_id}


@router.get("/api/jobs/current")
def get_current_job():
    cleanup_stale_jobs()
    try:
        with get_conn() as conn:
            row = conn.execute(
                "SELECT id, type, status, created_at, started_at, progress, message "
                "FROM sys_jobs WHERE status = 'running' ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
            if not row:
                return JSONResponse(content=None)
            return {
                "id": row[0],
                "type": row[1],
                "status": row[2],
                "created_at": row[3],
                "started_at": row[4],
                "progress": row[5],
                "message": row[6],
            }
    except Exception as exc:
        logger.exception("Error current job: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/jobs/history")
def get_job_history(limit: int = 20):
    return job_manager.get_history(limit)


@router.get("/api/jobs/{job_id}")
def get_job_status(job_id: str):
    status = job_manager.get_status(job_id)
    if not status:
        return JSONResponse(status_code=404, content={"error": "Not Found"})
    return status


@router.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str):
    success = job_manager.cancel(job_id)
    return {"id": job_id, "cancel_requested": success}
