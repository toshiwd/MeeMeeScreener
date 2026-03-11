from __future__ import annotations

import logging
import os
import threading

from app.backend.core.jobs import job_manager
from app.backend.services.analysis_backfill_service import inspect_analysis_backfill_coverage

logger = logging.getLogger(__name__)

ANALYSIS_PREWARM_JOB_TYPE = "analysis_backfill"
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


def _prewarm_enabled() -> bool:
    return _env_bool("MEEMEE_ANALYSIS_PREWARM_ENABLED", False)


def _prewarm_lookback_days() -> int:
    return _env_int("MEEMEE_ANALYSIS_PREWARM_LOOKBACK_DAYS", 130, minimum=20)


def _prewarm_max_missing_days() -> int | None:
    raw = os.getenv("MEEMEE_ANALYSIS_PREWARM_MAX_MISSING_DAYS")
    if raw is None or not str(raw).strip():
        return _prewarm_lookback_days()
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return _prewarm_lookback_days()


def _prewarm_include_sell() -> bool:
    return _env_bool("MEEMEE_ANALYSIS_PREWARM_INCLUDE_SELL", True)


def _prewarm_include_phase() -> bool:
    return _env_bool("MEEMEE_ANALYSIS_PREWARM_INCLUDE_PHASE", False)


def _prewarm_startup_delay_sec() -> int:
    return _env_int("MEEMEE_ANALYSIS_PREWARM_STARTUP_DELAY_SEC", 20, minimum=0)


def _prewarm_poll_sec() -> int:
    return _env_int("MEEMEE_ANALYSIS_PREWARM_POLL_SEC", 1800, minimum=60)


def _build_payload(*, source: str) -> dict[str, object]:
    return {
        "source": source,
        "lookback_days": _prewarm_lookback_days(),
        "max_missing_days": _prewarm_max_missing_days(),
        "include_sell": _prewarm_include_sell(),
        "include_phase": _prewarm_include_phase(),
    }


def _submit_if_needed(*, source: str) -> str | None:
    payload = _build_payload(source=source)
    coverage = inspect_analysis_backfill_coverage(
        lookback_days=int(payload["lookback_days"]),
        include_sell=bool(payload["include_sell"]),
        include_phase=bool(payload["include_phase"]),
    )
    if coverage.get("covered"):
        logger.debug(
            "Analysis prewarm skipped: coverage already complete (anchor_dt=%s lookback=%s)",
            coverage.get("anchor_dt"),
            coverage.get("lookback_days"),
        )
        return None

    job_id = job_manager.submit(
        ANALYSIS_PREWARM_JOB_TYPE,
        payload=payload,
        unique=True,
        message="Waiting in queue...",
        progress=0,
    )
    if job_id:
        logger.info(
            "Submitted analysis prewarm job id=%s source=%s anchor_dt=%s missing_ml=%s missing_sell=%s missing_phase=%s",
            job_id,
            source,
            coverage.get("anchor_dt"),
            len(coverage.get("missing_ml_dates") or []),
            len(coverage.get("missing_sell_dates") or []),
            len(coverage.get("missing_phase_dates") or []),
        )
    return job_id


def _scheduler_loop() -> None:
    startup_delay = _prewarm_startup_delay_sec()
    if startup_delay > 0 and _SCHEDULER_STOP_EVENT.wait(startup_delay):
        return

    while not _SCHEDULER_STOP_EVENT.is_set():
        try:
            _submit_if_needed(source="startup_scheduler")
        except Exception as exc:
            logger.warning("Analysis prewarm scheduler loop error: %s", exc)
        _SCHEDULER_STOP_EVENT.wait(_prewarm_poll_sec())


def schedule_analysis_prewarm_if_needed(*, source: str) -> str | None:
    if not _prewarm_enabled():
        return None
    return _submit_if_needed(source=source)


def start_analysis_prewarm_scheduler() -> None:
    if not _prewarm_enabled():
        logger.info("Analysis prewarm scheduler is disabled by env.")
        return
    global _SCHEDULER_THREAD
    with _SCHEDULER_LOCK:
        if _SCHEDULER_THREAD and _SCHEDULER_THREAD.is_alive():
            return
        _SCHEDULER_STOP_EVENT.clear()
        _SCHEDULER_THREAD = threading.Thread(
            target=_scheduler_loop,
            daemon=True,
            name="analysis-prewarm-scheduler",
        )
        _SCHEDULER_THREAD.start()
        logger.info("Analysis prewarm scheduler started.")


def stop_analysis_prewarm_scheduler(timeout_sec: float = 1.0) -> None:
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
