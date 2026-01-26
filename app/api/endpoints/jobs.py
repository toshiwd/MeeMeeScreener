from __future__ import annotations

import logging

from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()
logger = logging.getLogger(__name__)


def _load_main():
    # 遅延importで循環参照を避ける。
    from app.backend import main as main_module

    return main_module


@router.post("/api/jobs/txt-update")
def submit_txt_update():
    main_module = _load_main()
    try:
        main_module._cleanup_stale_jobs()
        with main_module.get_conn() as conn:
            cnt = conn.execute(
                "SELECT COUNT(*) FROM sys_jobs WHERE type = ? AND status IN ('queued', 'running')",
                ["txt_update"],
            ).fetchone()[0]
            if cnt > 0:
                return JSONResponse(status_code=409, content={"error": "Job already running"})
        job_id = main_module.job_manager.submit("txt_update", {})
        return {"ok": True, "job_id": job_id}
    except Exception as exc:
        logger.exception("Error submitting txt_update: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/jobs/force-sync")
def submit_force_sync():
    main_module = _load_main()
    try:
        main_module._cleanup_stale_jobs()
        with main_module.get_conn() as conn:
            cnt = conn.execute(
                "SELECT COUNT(*) FROM sys_jobs WHERE type = ? AND status IN ('queued', 'running')",
                ["force_sync"],
            ).fetchone()[0]
            if cnt > 0:
                return JSONResponse(status_code=409, content={"error": "Job already running"})
        job_id = main_module.job_manager.submit("force_sync", {})
        return {"ok": True, "job_id": job_id}
    except Exception as exc:
        logger.exception("Error submitting force_sync: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/jobs/current")
def get_current_job():
    main_module = _load_main()
    try:
        main_module._cleanup_stale_jobs()
        with main_module.get_conn() as conn:
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
    main_module = _load_main()
    return main_module.job_manager.get_history(limit)


@router.get("/api/jobs/{job_id}")
def get_job_status(job_id: str):
    main_module = _load_main()
    status = main_module.job_manager.get_status(job_id)
    if not status:
        return JSONResponse(status_code=404, content={"error": "Not Found"})
    return status


@router.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str):
    main_module = _load_main()
    success = main_module.job_manager.cancel(job_id)
    return {"id": job_id, "cancel_requested": success}
