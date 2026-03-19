from __future__ import annotations

import logging
import os
import threading
from datetime import datetime

from app.backend.core.jobs import job_manager
from app.backend.services.operator_mutation_lock import OperatorMutationBusyError, operator_mutation_scope
from app.backend.services.screener_snapshot_service import inspect_screener_snapshot, refresh_screener_snapshot

logger = logging.getLogger(__name__)
SCREENER_SNAPSHOT_JOB_TYPE = "screener_snapshot_refresh"
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
        value = int(raw) if raw is not None else int(default)
    except (TypeError, ValueError):
        value = int(default)
    return max(int(minimum), int(value))


def _snapshot_enabled() -> bool:
    return _env_bool("MEEMEE_SCREENER_SNAPSHOT_ENABLED", True)


def _snapshot_limit() -> int:
    return _env_int("MEEMEE_SCREENER_SNAPSHOT_LIMIT", 260, minimum=1)


def _snapshot_startup_delay_sec() -> int:
    return _env_int("MEEMEE_SCREENER_SNAPSHOT_STARTUP_DELAY_SEC", 8, minimum=0)


def schedule_screener_snapshot_refresh(*, source: str, force: bool = False) -> str | None:
    if not _snapshot_enabled():
        return None
    payload = {
        "source": source,
        "limit": _snapshot_limit(),
        "force": bool(force),
    }
    return job_manager.submit(
        SCREENER_SNAPSHOT_JOB_TYPE,
        payload=payload,
        unique=True,
        message="Waiting in queue...",
        progress=0,
    )


def handle_screener_snapshot_refresh(job_id: str, payload: dict) -> None:
    limit = max(1, int(payload.get("limit") or _snapshot_limit()))
    source = str(payload.get("source") or f"job:{job_id}")
    try:
        with operator_mutation_scope("screener_snapshot_refresh", timeout_sec=0.0):
            job_manager._update_db(
                job_id,
                SCREENER_SNAPSHOT_JOB_TYPE,
                "running",
                progress=10,
                message="Refreshing screener snapshot...",
            )
            snapshot = refresh_screener_snapshot(limit=limit, source=source)
            stale = bool(snapshot.get("stale"))
            build_failed = bool(snapshot.get("buildFailed"))
            if build_failed:
                job_manager._update_db(
                    job_id,
                    SCREENER_SNAPSHOT_JOB_TYPE,
                    "failed",
                    progress=100,
                    message="Screener snapshot refresh failed; stale snapshot preserved.",
                    error=str(snapshot.get("lastError") or "snapshot_build_failed"),
                    finished_at=datetime.now(),
                )
                return
            job_manager._update_db(
                job_id,
                SCREENER_SNAPSHOT_JOB_TYPE,
                "success",
                progress=100,
                message=(
                    f"Screener snapshot refreshed rows={snapshot.get('rowCount')} "
                    f"gen={snapshot.get('generation')} stale={stale}"
                ),
                finished_at=datetime.now(),
            )
    except OperatorMutationBusyError as exc:
        job_manager._update_db(
            job_id,
            SCREENER_SNAPSHOT_JOB_TYPE,
            "skipped",
            progress=100,
            message="Screener snapshot refresh skipped: operator mutation active.",
            error="operator_mutation_busy",
            finished_at=datetime.now(),
        )
        logger.info(
            "Screener snapshot refresh skipped due operator mutation active action=%s since=%s",
            exc.holder_action,
            exc.holder_since,
        )
        return


def _scheduler_loop() -> None:
    startup_delay = _snapshot_startup_delay_sec()
    if startup_delay > 0 and _SCHEDULER_STOP_EVENT.wait(startup_delay):
        return
    try:
        snapshot = inspect_screener_snapshot(limit=_snapshot_limit())
        if snapshot.get("exists") and snapshot.get("items"):
            logger.info(
                "Screener snapshot startup warmup skipped: existing snapshot rows=%s updated=%s",
                snapshot.get("rowCount"),
                snapshot.get("updatedAt"),
            )
            return
    except Exception as exc:
        logger.warning("Screener snapshot inspection failed during startup: %s", exc)
    try:
        schedule_screener_snapshot_refresh(source="startup_scheduler", force=False)
    except Exception as exc:
        logger.warning("Screener snapshot startup submission failed: %s", exc)


def start_screener_snapshot_scheduler() -> None:
    if not _snapshot_enabled():
        logger.info("Screener snapshot scheduler is disabled by env.")
        return
    global _SCHEDULER_THREAD
    with _SCHEDULER_LOCK:
        if _SCHEDULER_THREAD and _SCHEDULER_THREAD.is_alive():
            return
        _SCHEDULER_STOP_EVENT.clear()
        _SCHEDULER_THREAD = threading.Thread(
            target=_scheduler_loop,
            daemon=True,
            name="screener-snapshot-startup",
        )
        _SCHEDULER_THREAD.start()
        logger.info("Screener snapshot startup scheduler started.")


def stop_screener_snapshot_scheduler(timeout_sec: float = 1.0) -> None:
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
