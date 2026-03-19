from __future__ import annotations

import logging
import os
import threading
from datetime import datetime

from external_analysis.results.publish_candidates import (
    backfill_publish_candidate_bundles,
    load_publish_candidate_maintenance_state,
    save_publish_candidate_maintenance_state,
    sweep_publish_candidate_snapshots,
)

logger = logging.getLogger(__name__)

PUBLISH_CANDIDATE_MAINTENANCE_JOB_TYPE = "publish_candidate_maintenance"
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


def _maintenance_enabled() -> bool:
    return _env_bool("MEEMEE_PUBLISH_CANDIDATE_MAINTENANCE_ENABLED", False)


def _startup_delay_sec() -> int:
    return _env_int("MEEMEE_PUBLISH_CANDIDATE_MAINTENANCE_STARTUP_DELAY_SEC", 60, minimum=0)


def _poll_sec() -> int:
    return _env_int("MEEMEE_PUBLISH_CANDIDATE_MAINTENANCE_POLL_SEC", 86400, minimum=60)


def _backfill_limit() -> int | None:
    raw = os.getenv("MEEMEE_PUBLISH_CANDIDATE_MAINTENANCE_BACKFILL_LIMIT")
    if raw is None or not str(raw).strip():
        return None
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return None


def _dry_run() -> bool:
    return _env_bool("MEEMEE_PUBLISH_CANDIDATE_MAINTENANCE_DRY_RUN", False)


def run_publish_candidate_maintenance_cycle(
    *,
    result_db_path: str | None = None,
    dry_run: bool | None = None,
    limit: int | None = None,
    source: str = "maintenance_scheduler",
) -> dict[str, object]:
    effective_dry_run = _dry_run() if dry_run is None else bool(dry_run)
    backfill_result = backfill_publish_candidate_bundles(
        db_path=result_db_path,
        limit=limit if limit is not None else _backfill_limit(),
        dry_run=effective_dry_run,
    )
    sweep_result = sweep_publish_candidate_snapshots(
        db_path=result_db_path,
        dry_run=effective_dry_run,
    )
    maintenance_state = load_publish_candidate_maintenance_state(db_path=result_db_path)
    summary = {
        "ok": bool(backfill_result.get("ok")) and bool(sweep_result.get("ok")),
        "source": source,
        "dry_run": effective_dry_run,
        "started_at": datetime.now().isoformat(),
        "backfill": backfill_result,
        "snapshot_sweep": sweep_result,
        "candidate_backfill_last_run": maintenance_state.get("candidate_backfill_last_run"),
        "snapshot_sweep_last_run": maintenance_state.get("snapshot_sweep_last_run"),
        "non_promotable_legacy_count": int(maintenance_state.get("non_promotable_legacy_count") or 0),
        "maintenance_degraded": bool(maintenance_state.get("maintenance_degraded")),
    }
    save_publish_candidate_maintenance_state(
        db_path=result_db_path,
        state={
            **maintenance_state,
            "details_json": {
                **dict(maintenance_state.get("details_json") or {}),
                "last_cycle": {**summary},
            },
        },
    )
    logger.info(
        "Publish candidate maintenance cycle finished ok=%s dry_run=%s backfill_updated=%s sweep_deleted=%s",
        summary["ok"],
        summary["dry_run"],
        backfill_result.get("updated"),
        sweep_result.get("deleted"),
    )
    return summary


def _scheduler_loop() -> None:
    startup_delay = _startup_delay_sec()
    if startup_delay > 0 and _SCHEDULER_STOP_EVENT.wait(startup_delay):
        return

    while not _SCHEDULER_STOP_EVENT.is_set():
        try:
            run_publish_candidate_maintenance_cycle(source="startup_scheduler")
        except Exception as exc:
            logger.warning("Publish candidate maintenance scheduler loop error: %s", exc)
        _SCHEDULER_STOP_EVENT.wait(_poll_sec())


def start_publish_candidate_maintenance_scheduler() -> None:
    if not _maintenance_enabled():
        logger.info("Publish candidate maintenance scheduler is disabled by env.")
        return
    global _SCHEDULER_THREAD
    with _SCHEDULER_LOCK:
        if _SCHEDULER_THREAD and _SCHEDULER_THREAD.is_alive():
            return
        _SCHEDULER_STOP_EVENT.clear()
        _SCHEDULER_THREAD = threading.Thread(
            target=_scheduler_loop,
            daemon=True,
            name="publish-candidate-maintenance-scheduler",
        )
        _SCHEDULER_THREAD.start()
        logger.info("Publish candidate maintenance scheduler started.")


def stop_publish_candidate_maintenance_scheduler(timeout_sec: float = 1.0) -> None:
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
