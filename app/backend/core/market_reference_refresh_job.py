from __future__ import annotations

import logging
import os
import threading
from datetime import datetime

from app.backend.core.jobs import job_manager
from app.backend.core.taisyaku_import_job import TAISYAKU_IMPORT_JOB_TYPE
from app.backend.core.tdnet_import_job import TDNET_IMPORT_JOB_TYPE
from app.backend.services.jpx_calendar import get_jpx_session_info, jst_now

logger = logging.getLogger(__name__)

_SCHEDULER_LOCK = threading.Lock()
_SCHEDULER_THREAD: threading.Thread | None = None
_SCHEDULER_STOP_EVENT = threading.Event()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw is not None else int(default)
    except (TypeError, ValueError):
        value = int(default)
    return max(minimum, value)


def _enabled() -> bool:
    return _env_bool("MEEMEE_MARKET_REFERENCE_REFRESH_ENABLED", True)


def _startup_delay_sec() -> int:
    return _env_int("MEEMEE_MARKET_REFERENCE_REFRESH_STARTUP_DELAY_SEC", 60, minimum=0)


def _poll_sec() -> int:
    return _env_int("MEEMEE_MARKET_REFERENCE_REFRESH_POLL_SEC", 300, minimum=30)


def _run_time_jst() -> tuple[int, int]:
    raw = str(os.getenv("MEEMEE_MARKET_REFERENCE_REFRESH_TIME_JST") or "18:30").strip()
    try:
        hour_text, minute_text = raw.split(":", 1)
        return max(0, min(23, int(hour_text))), max(0, min(59, int(minute_text)))
    except Exception:
        return 18, 30


def submit_market_reference_refresh(*, source: str) -> dict[str, str | None]:
    taisyaku_job_id = job_manager.submit(
        TAISYAKU_IMPORT_JOB_TYPE,
        payload={"source": source},
        unique=True,
        message="Waiting in queue for taisyaku import...",
    )
    tdnet_job_id = job_manager.submit(
        TDNET_IMPORT_JOB_TYPE,
        payload={"source": source, "limit": 500},
        unique=True,
        message="Waiting in queue for TDNET import...",
    )
    return {"taisyaku_job_id": taisyaku_job_id, "tdnet_job_id": tdnet_job_id}


def _scheduler_loop() -> None:
    if _SCHEDULER_STOP_EVENT.wait(_startup_delay_sec()):
        return
    submitted_dates: set[int] = set()
    while not _SCHEDULER_STOP_EVENT.is_set():
        try:
            now = jst_now()
            session = get_jpx_session_info(now)
            today_key = int(now.strftime("%Y%m%d"))
            due = session.is_trading_day and (now.hour, now.minute) >= _run_time_jst()
            if due and today_key not in submitted_dates:
                result = submit_market_reference_refresh(source="auto_scheduler")
                if any(result.values()):
                    submitted_dates.add(today_key)
                    logger.info("Submitted market reference refresh at %s: %s", now.isoformat(), result)
            submitted_dates = {date_key for date_key in submitted_dates if date_key >= today_key}
        except Exception as exc:
            logger.warning("Market reference refresh scheduler loop error: %s", exc)
        _SCHEDULER_STOP_EVENT.wait(_poll_sec())


def start_market_reference_refresh_scheduler() -> None:
    if not _enabled():
        logger.info("Market reference refresh scheduler is disabled by env.")
        return
    global _SCHEDULER_THREAD
    with _SCHEDULER_LOCK:
        if _SCHEDULER_THREAD and _SCHEDULER_THREAD.is_alive():
            return
        _SCHEDULER_STOP_EVENT.clear()
        _SCHEDULER_THREAD = threading.Thread(
            target=_scheduler_loop,
            daemon=True,
            name="market-reference-refresh-scheduler",
        )
        _SCHEDULER_THREAD.start()
        logger.info("Market reference refresh scheduler started.")


def stop_market_reference_refresh_scheduler(timeout_sec: float = 1.0) -> None:
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
