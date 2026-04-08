from __future__ import annotations

import logging
import os
import threading
from datetime import datetime
from typing import Any

from app.backend.core.jobs import job_manager
from app.backend.edinetdb.config import load_config
from app.backend.edinetdb.jobs import run_backfill_700, run_daily_watch
from app.backend.edinetdb.repository import EdinetdbRepository
from app.backend.edinetdb.schema import ensure_edinetdb_schema_at_path
from app.backend.services.jpx_calendar import jst_now
from app.db.session import try_get_conn

logger = logging.getLogger(__name__)

EDINETDB_DAILY_WATCH_JOB_TYPE = "edinetdb_daily_watch"
EDINETDB_BACKFILL_700_JOB_TYPE = "edinetdb_backfill_700"
_ACTIVE_EDINET_JOB_TYPES = (
    EDINETDB_DAILY_WATCH_JOB_TYPE,
    EDINETDB_BACKFILL_700_JOB_TYPE,
)
_AUTO_START_DATE_META_KEY = "auto_start_last_jst_date"
_AUTO_START_MODE_META_KEY = "auto_start_last_mode"
_SCHEDULER_LOCK = threading.Lock()
_SCHEDULER_THREAD: threading.Thread | None = None
_SCHEDULER_STOP_EVENT = threading.Event()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.getenv(name)
    try:
        value = int(str(raw).strip()) if raw is not None else int(default)
    except (TypeError, ValueError):
        value = int(default)
    return max(int(minimum), int(value))


def _auto_start_enabled() -> bool:
    return _env_bool("MEEMEE_EDINET_AUTO_START_ENABLED", True)


def _auto_start_delay_sec() -> int:
    return _env_int("MEEMEE_EDINET_AUTO_START_DELAY_SEC", 45, minimum=0)


def _empty_db_backfill_enabled() -> bool:
    return _env_bool("MEEMEE_EDINET_EMPTY_DB_BACKFILL_ENABLED", True)


def _job_type_for_mode(mode: str) -> str:
    return (
        EDINETDB_BACKFILL_700_JOB_TYPE
        if mode == "backfill_700"
        else EDINETDB_DAILY_WATCH_JOB_TYPE
    )


def _mode_for_job_type(job_type: str | None) -> str | None:
    if job_type == EDINETDB_BACKFILL_700_JOB_TYPE:
        return "backfill_700"
    if job_type == EDINETDB_DAILY_WATCH_JOB_TYPE:
        return "daily_watch"
    return None


def _coerce_jst_date(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip().replace("-", "")
    if len(text) == 8 and text.isdigit():
        return int(text)
    return None


def _format_summary_message(mode: str, summary: dict[str, Any]) -> str:
    stop_reason = str(summary.get("stop_reason") or "completed")
    budget_used = int(summary.get("budget_used") or 0)
    budget_total = int(summary.get("budget_total") or 0)
    mapped_count = int(summary.get("mapped_count") or 0)
    pending_tasks = int(summary.get("pending_tasks") or 0)
    return (
        f"EDINET {mode} completed"
        f" (stop_reason={stop_reason}, mapped={mapped_count}, pending={pending_tasks}, budget={budget_used}/{budget_total})"
    )


def _run_wrapped_edinet_job(job_id: str, *, mode: str) -> None:
    job_type = _job_type_for_mode(mode)
    cfg = load_config()
    if not cfg.api_keys:
        job_manager._update_db(
            job_id,
            job_type,
            "success",
            progress=100,
            finished_at=datetime.now(),
            message="EDINET auto-start skipped: EDINETDB_API_KEY(S) is not set",
        )
        return

    job_manager._update_db(
        job_id,
        job_type,
        "running",
        progress=10,
        message=f"Running EDINET {mode}...",
    )
    summary = run_backfill_700(cfg) if mode == "backfill_700" else run_daily_watch(cfg)
    job_manager._update_db(
        job_id,
        job_type,
        "success",
        progress=100,
        finished_at=datetime.now(),
        message=_format_summary_message(mode, summary),
    )


def handle_edinetdb_backfill_700(job_id: str, payload: dict[str, Any]) -> None:
    _run_wrapped_edinet_job(job_id, mode="backfill_700")


def handle_edinetdb_daily_watch(job_id: str, payload: dict[str, Any]) -> None:
    _run_wrapped_edinet_job(job_id, mode="daily_watch")


def schedule_edinet_auto_start_if_needed(*, source: str) -> dict[str, Any]:
    if not _auto_start_enabled():
        return {"submitted": False, "reason": "disabled_by_env"}

    cfg = load_config()
    ensure_edinetdb_schema_at_path(cfg.db_path)
    repo = EdinetdbRepository(cfg.db_path)
    table_state = repo.get_seed_table_state()
    missing_tables = [str(name) for name in table_state.get("missing_tables") or [] if str(name).strip()]
    if missing_tables:
        return {
            "submitted": False,
            "reason": "missing_tables",
            "missing_tables": missing_tables,
        }

    if not cfg.api_keys:
        return {"submitted": False, "reason": "no_api_keys"}

    all_empty = bool(table_state.get("all_empty"))
    if all_empty and not _empty_db_backfill_enabled():
        return {"submitted": False, "reason": "empty_db_backfill_disabled"}

    mode = "backfill_700" if all_empty else "daily_watch"
    today_jst = int(jst_now().strftime("%Y%m%d"))
    last_date = _coerce_jst_date(repo.get_meta(_AUTO_START_DATE_META_KEY))
    last_mode = str(repo.get_meta(_AUTO_START_MODE_META_KEY) or "").strip() or None
    if last_date == today_jst and last_mode == mode:
        return {
            "submitted": False,
            "reason": "already_submitted_today",
            "mode": mode,
            "jst_date": today_jst,
        }

    job_id = job_manager.submit(
        _job_type_for_mode(mode),
        payload={"source": source, "mode": mode, "jst_date": today_jst},
        unique=True,
        message=f"Waiting in queue for EDINET {mode}...",
        progress=0,
    )
    if not job_id:
        return {
            "submitted": False,
            "reason": "already_active",
            "mode": mode,
            "jst_date": today_jst,
        }

    repo.set_meta(_AUTO_START_DATE_META_KEY, today_jst)
    repo.set_meta(_AUTO_START_MODE_META_KEY, mode)
    return {
        "submitted": True,
        "jobId": job_id,
        "mode": mode,
        "jst_date": today_jst,
        "table_counts": table_state.get("table_counts") or {},
    }


def _scheduler_loop() -> None:
    delay_sec = _auto_start_delay_sec()
    if delay_sec > 0 and _SCHEDULER_STOP_EVENT.wait(delay_sec):
        return
    try:
        result = schedule_edinet_auto_start_if_needed(source="startup_scheduler")
        logger.info("EDINET auto-start result: %s", result)
    except Exception as exc:
        logger.warning("EDINET auto-start scheduler loop error: %s", exc)


def start_edinet_auto_start_scheduler() -> None:
    if not _auto_start_enabled():
        logger.info("EDINET auto-start scheduler is disabled by env.")
        return
    global _SCHEDULER_THREAD
    with _SCHEDULER_LOCK:
        if _SCHEDULER_THREAD and _SCHEDULER_THREAD.is_alive():
            return
        _SCHEDULER_STOP_EVENT.clear()
        _SCHEDULER_THREAD = threading.Thread(
            target=_scheduler_loop,
            daemon=True,
            name="edinet-auto-start-scheduler",
        )
        _SCHEDULER_THREAD.start()
        logger.info("EDINET auto-start scheduler started.")


def stop_edinet_auto_start_scheduler(timeout_sec: float = 1.0) -> None:
    global _SCHEDULER_THREAD
    with _SCHEDULER_LOCK:
        thread = _SCHEDULER_THREAD
        if not thread:
            return
        _SCHEDULER_STOP_EVENT.set()
    if thread.is_alive():
        thread.join(timeout=max(0.0, float(timeout_sec)))
    with _SCHEDULER_LOCK:
        _SCHEDULER_THREAD = None


def get_active_edinet_bootstrap_state() -> dict[str, Any] | None:
    try:
        with try_get_conn(timeout_sec=0.4) as conn:
            if conn is None:
                return None
            row = conn.execute(
                """
                SELECT id, type, status, message
                FROM sys_jobs
                WHERE type IN (?, ?)
                  AND status IN ('queued', 'running', 'cancel_requested')
                ORDER BY COALESCE(started_at, created_at) DESC
                LIMIT 1
                """,
                list(_ACTIVE_EDINET_JOB_TYPES),
            ).fetchone()
    except Exception:
        return None

    if not row:
        return {
            "active": False,
            "mode": None,
            "jobId": None,
            "message": None,
        }
    return {
        "active": True,
        "mode": _mode_for_job_type(str(row[1]) if row[1] is not None else None),
        "jobId": str(row[0]) if row[0] is not None else None,
        "message": str(row[3]) if row[3] is not None else None,
    }
