from __future__ import annotations

from datetime import datetime

from app.backend.core.jobs import job_manager
from app.backend.services import ml_service


def _to_int(value: object) -> int | None:
    try:
        if value is None:
            return None
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def handle_ml_train(job_id: str, payload: dict) -> None:
    start_dt = _to_int(payload.get("start_dt"))
    end_dt = _to_int(payload.get("end_dt"))
    dry_run = bool(payload.get("dry_run", False))
    job_manager._update_db(
        job_id,
        "ml_train",
        "running",
        progress=10,
        message="Refreshing ML labels/features...",
    )
    result = ml_service.train_models(start_dt=start_dt, end_dt=end_dt, dry_run=dry_run)
    model_version = result.get("model_version")
    message = (
        f"ML train completed (dry_run={dry_run}, model_version={model_version})"
        if model_version
        else f"ML train completed (dry_run={dry_run})"
    )
    job_manager._update_db(
        job_id,
        "ml_train",
        "success",
        progress=100,
        message=message,
        finished_at=datetime.now(),
    )


def handle_ml_predict(job_id: str, payload: dict) -> None:
    dt = _to_int(payload.get("dt"))
    job_manager._update_db(
        job_id,
        "ml_predict",
        "running",
        progress=20,
        message="Predicting ML scores...",
    )
    result = ml_service.predict_for_dt(dt=dt)
    job_manager._update_db(
        job_id,
        "ml_predict",
        "success",
        progress=100,
        message=f"ML predict completed (dt={result.get('dt')}, rows={result.get('rows')})",
        finished_at=datetime.now(),
    )


def handle_ml_live_guard(job_id: str, payload: dict) -> None:
    job_manager._update_db(
        job_id,
        "ml_live_guard",
        "running",
        progress=20,
        message="Evaluating ML live guard...",
    )
    result = ml_service.enforce_live_guard()
    action = str(result.get("action") or "unknown")
    reason = str(result.get("reason") or "")
    rolled_back_to = result.get("rolled_back_to")
    suffix = f", rollback={rolled_back_to}" if rolled_back_to else ""
    job_manager._update_db(
        job_id,
        "ml_live_guard",
        "success",
        progress=100,
        message=f"ML live guard completed (action={action}, reason={reason}{suffix})",
        finished_at=datetime.now(),
    )
