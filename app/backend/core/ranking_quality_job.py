from __future__ import annotations

import logging
import os
import threading
from datetime import datetime

from app.backend.core.legacy_analysis_control import is_legacy_analysis_disabled
from app.backend.core.jobs import job_manager
from app.backend.services.ml.ranking_analysis_quality import compute_ranking_analysis_quality_snapshot
from app.utils.date_utils import jst_now

logger = logging.getLogger(__name__)

RANKING_ANALYSIS_QUALITY_JOB_TYPE = "ranking_analysis_quality_daily"
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


def _quality_enabled() -> bool:
    return _env_bool("MEEMEE_RANK_QUALITY_ENABLED", False)


def _quality_poll_sec() -> int:
    return _env_int("MEEMEE_RANK_QUALITY_POLL_SEC", 300, minimum=30)


def _quality_startup_delay_sec() -> int:
    return _env_int("MEEMEE_RANK_QUALITY_STARTUP_DELAY_SEC", 30, minimum=0)


def _quality_time_jst() -> tuple[int, int]:
    raw = str(os.getenv("MEEMEE_RANK_QUALITY_TIME_JST") or "18:40").strip()
    try:
        hour_text, minute_text = raw.split(":", 1)
        hour = max(0, min(23, int(hour_text)))
        minute = max(0, min(59, int(minute_text)))
        return hour, minute
    except Exception:
        return 18, 40


def _default_persist() -> bool:
    return _env_bool("MEEMEE_RANK_QUALITY_PERSIST", True)


def _to_int(value: object | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def handle_ranking_analysis_quality(job_id: str, payload: dict) -> None:
    if is_legacy_analysis_disabled():
        job_manager._update_db(
            job_id,
            RANKING_ANALYSIS_QUALITY_JOB_TYPE,
            "success",
            progress=100,
            finished_at=datetime.now(),
            message="Ranking-analysis quality skipped because legacy analysis is disabled.",
        )
        return
    as_of = _to_int(payload.get("as_of"))
    persist = bool(payload.get("persist", _default_persist()))
    job_manager._update_db(
        job_id,
        RANKING_ANALYSIS_QUALITY_JOB_TYPE,
        "running",
        progress=20,
        message="Computing ranking-analysis quality snapshot...",
    )

    snapshot = compute_ranking_analysis_quality_snapshot(as_of_ymd=as_of, persist=persist)
    kpi = snapshot.get("kpi_snapshot") if isinstance(snapshot, dict) else {}
    rolling = (kpi or {}).get("rolling6m") if isinstance(kpi, dict) else {}
    alignment = (kpi or {}).get("decisionAlignment") if isinstance(kpi, dict) else {}
    as_of_ymd = int(snapshot.get("as_of") or 0) if isinstance(snapshot, dict) else 0
    precision = kpi.get("precisionTop30_20d") if isinstance(kpi, dict) else None
    delta_pt = rolling.get("deltaPrecisionPt") if isinstance(rolling, dict) else None
    match_rate = alignment.get("matchRate") if isinstance(alignment, dict) else None
    samples = alignment.get("sampleSize") if isinstance(alignment, dict) else None
    message = (
        "Ranking-analysis quality completed "
        f"(as_of={as_of_ymd}, persist={persist}, precision={precision}, "
        f"rolling_delta_pt={delta_pt}, decision_match={match_rate}, decision_samples={samples})"
    )
    job_manager._update_db(
        job_id,
        RANKING_ANALYSIS_QUALITY_JOB_TYPE,
        "success",
        progress=100,
        finished_at=datetime.now(),
        message=message,
    )


def _scheduler_loop() -> None:
    startup_delay = _quality_startup_delay_sec()
    if startup_delay > 0 and _SCHEDULER_STOP_EVENT.wait(startup_delay):
        return

    submitted_dates: set[int] = set()
    while not _SCHEDULER_STOP_EVENT.is_set():
        try:
            now = jst_now()
            run_hour, run_minute = _quality_time_jst()
            today_key = int(now.strftime("%Y%m%d"))
            due = (now.hour, now.minute) >= (run_hour, run_minute)
            if due and today_key not in submitted_dates:
                job_id = job_manager.submit(
                    RANKING_ANALYSIS_QUALITY_JOB_TYPE,
                    payload={"source": "auto_scheduler", "persist": True},
                    unique=True,
                    message="Waiting in queue...",
                    progress=0,
                )
                if job_id:
                    submitted_dates.add(today_key)
                    logger.info(
                        "Submitted ranking quality job id=%s at %s (JST)",
                        job_id,
                        now.isoformat(),
                    )
                else:
                    logger.debug("Ranking quality job submission skipped: already active.")
            old_dates = [date_key for date_key in submitted_dates if date_key < today_key]
            for date_key in old_dates:
                submitted_dates.discard(date_key)
        except Exception as exc:
            logger.warning("Ranking quality scheduler loop error: %s", exc)
        _SCHEDULER_STOP_EVENT.wait(_quality_poll_sec())


def start_ranking_analysis_quality_scheduler() -> None:
    if is_legacy_analysis_disabled():
        logger.info("Ranking quality scheduler is disabled because legacy analysis is disabled.")
        return
    if not _quality_enabled():
        logger.info("Ranking quality scheduler is disabled by env.")
        return
    global _SCHEDULER_THREAD
    with _SCHEDULER_LOCK:
        if _SCHEDULER_THREAD and _SCHEDULER_THREAD.is_alive():
            return
        _SCHEDULER_STOP_EVENT.clear()
        _SCHEDULER_THREAD = threading.Thread(
            target=_scheduler_loop,
            daemon=True,
            name="ranking-quality-scheduler",
        )
        _SCHEDULER_THREAD.start()
        logger.info("Ranking quality scheduler started.")


def stop_ranking_analysis_quality_scheduler(timeout_sec: float = 1.0) -> None:
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
