from __future__ import annotations

import logging
import os
import threading
from datetime import datetime

from app.backend.core.jobs import job_manager
from app.backend.services.jpx_calendar import get_jpx_session_info, jst_now
from app.backend.services.yahoo_daily_ingest import ingest_latest_provisional_daily_rows

logger = logging.getLogger(__name__)

YF_DAILY_INGEST_JOB_TYPE = "yf_daily_ingest"
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


def _daily_ingest_enabled() -> bool:
    return _env_bool("MEEMEE_YF_DAILY_INGEST_ENABLED", True)


def _daily_ingest_poll_sec() -> int:
    return _env_int("MEEMEE_YF_DAILY_INGEST_POLL_SEC", 300, minimum=30)


def _daily_ingest_startup_delay_sec() -> int:
    return _env_int("MEEMEE_YF_DAILY_INGEST_STARTUP_DELAY_SEC", 20, minimum=0)


def _daily_ingest_max_codes() -> int | None:
    value = _env_int("MEEMEE_YF_DAILY_INGEST_MAX_CODES", 0, minimum=0)
    return value if value > 0 else None


def _daily_ingest_time_jst(now: datetime | None = None) -> tuple[int, int]:
    raw = str(os.getenv("MEEMEE_YF_DAILY_INGEST_TIME_JST") or "").strip()
    if not raw:
        session = get_jpx_session_info(now or jst_now())
        raw = session.yahoo_persist_after_jst
    try:
        hour_text, minute_text = raw.split(":", 1)
        hour = max(0, min(23, int(hour_text)))
        minute = max(0, min(59, int(minute_text)))
        return hour, minute
    except Exception:
        return 12, 20


def _to_int(value: object | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def handle_yf_daily_ingest(job_id: str, payload: dict) -> None:
    dry_run = bool(payload.get("dry_run", False))
    asof_dt = _to_int(payload.get("asof_dt"))
    max_codes = _to_int(payload.get("max_codes"))
    if max_codes is not None and max_codes <= 0:
        max_codes = None
    if max_codes is None:
        max_codes = _daily_ingest_max_codes()

    if not _daily_ingest_enabled() and not dry_run:
        job_manager._update_db(
            job_id,
            YF_DAILY_INGEST_JOB_TYPE,
            "success",
            progress=100,
            finished_at=datetime.now(),
            message="Yahoo daily ingest skipped: disabled by env",
        )
        return

    job_manager._update_db(
        job_id,
        YF_DAILY_INGEST_JOB_TYPE,
        "running",
        progress=10,
        message="Fetching Yahoo provisional daily bars...",
    )

    report = ingest_latest_provisional_daily_rows(
        max_codes=max_codes,
        asof_dt=asof_dt,
        dry_run=dry_run,
    )
    inserted = int(report.get("inserted") or 0)
    target_codes = int(report.get("target_codes") or 0)
    coverage = report.get("coverage") if isinstance(report.get("coverage"), dict) else {}
    covered_codes = int(coverage.get("covered_codes") or 0) if coverage else 0
    target_date = coverage.get("target_date") if coverage else None
    message = (
        f"Yahoo daily ingest completed (inserted={inserted}, target_codes={target_codes}, "
        f"covered={covered_codes}, target_date={target_date})"
    )

    if inserted > 0 and not dry_run:
        try:
            from app.backend.services import rankings_cache

            rankings_cache.refresh_cache()
        except Exception as exc:
            logger.warning("Rankings cache refresh after Yahoo ingest failed: %s", exc)

    job_manager._update_db(
        job_id,
        YF_DAILY_INGEST_JOB_TYPE,
        "success",
        progress=100,
        finished_at=datetime.now(),
        message=message,
    )


def _scheduler_loop() -> None:
    startup_delay = _daily_ingest_startup_delay_sec()
    if startup_delay > 0:
        if _SCHEDULER_STOP_EVENT.wait(startup_delay):
            return

    submitted_dates: set[int] = set()
    while not _SCHEDULER_STOP_EVENT.is_set():
        try:
            now = jst_now()
            session = get_jpx_session_info(now)
            run_hour, run_minute = _daily_ingest_time_jst(now)
            today_key = int(now.strftime("%Y%m%d"))
            due = session.is_trading_day and (now.hour, now.minute) >= (run_hour, run_minute)
            if due and today_key not in submitted_dates:
                job_id = job_manager.submit(
                    YF_DAILY_INGEST_JOB_TYPE,
                    payload={"source": "auto_scheduler"},
                    unique=True,
                    message="Waiting in queue...",
                    progress=0,
                )
                if job_id:
                    submitted_dates.add(today_key)
                    logger.info(
                        "Submitted daily Yahoo ingest job id=%s at %s (JST)",
                        job_id,
                        now.isoformat(),
                    )
                else:
                    logger.debug("Daily Yahoo ingest submission skipped: already active.")
            old_dates = [date_key for date_key in submitted_dates if date_key < today_key]
            for date_key in old_dates:
                submitted_dates.discard(date_key)
        except Exception as exc:
            logger.warning("Yahoo daily ingest scheduler loop error: %s", exc)
        _SCHEDULER_STOP_EVENT.wait(_daily_ingest_poll_sec())


def start_yf_daily_ingest_scheduler() -> None:
    if not _daily_ingest_enabled():
        logger.info("Yahoo daily ingest scheduler is disabled by env.")
        return
    global _SCHEDULER_THREAD
    with _SCHEDULER_LOCK:
        if _SCHEDULER_THREAD and _SCHEDULER_THREAD.is_alive():
            return
        _SCHEDULER_STOP_EVENT.clear()
        _SCHEDULER_THREAD = threading.Thread(
            target=_scheduler_loop,
            daemon=True,
            name="yf-daily-ingest-scheduler",
        )
        _SCHEDULER_THREAD.start()
        logger.info("Yahoo daily ingest scheduler started.")


def stop_yf_daily_ingest_scheduler(timeout_sec: float = 1.0) -> None:
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
