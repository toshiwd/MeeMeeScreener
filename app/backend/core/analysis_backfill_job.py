from __future__ import annotations

from datetime import datetime

from app.backend.core.jobs import job_manager
from app.backend.services.analysis_backfill_service import backfill_missing_analysis_history


def _to_int(value: object, default: int | None = None, *, minimum: int | None = None) -> int | None:
    try:
        if value is None:
            result = default
        else:
            result = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        result = default
    if result is None:
        return None
    if minimum is not None:
        result = max(int(minimum), int(result))
    return int(result)


def _to_bool(value: object, default: bool) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def handle_analysis_backfill(job_id: str, payload: dict) -> None:
    lookback_days = _to_int(payload.get("lookback_days"), 130, minimum=1) or 130
    anchor_dt = _to_int(payload.get("anchor_dt"), None, minimum=1)
    start_dt = _to_int(payload.get("start_dt"), None, minimum=1)
    end_dt = _to_int(payload.get("end_dt"), None, minimum=1)
    max_missing_days = _to_int(payload.get("max_missing_days"), None, minimum=1)
    include_sell = _to_bool(payload.get("include_sell"), True)
    include_phase = _to_bool(payload.get("include_phase"), False)
    force_recompute = _to_bool(payload.get("force_recompute"), False)

    def _progress(progress: int, message: str) -> None:
        job_manager.update_status_cache_only(
            job_id=job_id,
            job_type="analysis_backfill",
            status="running",
            progress=max(0, min(99, int(progress))),
            message=message,
        )

    _progress(1, "不足期間バックフィルを開始...")
    result = backfill_missing_analysis_history(
        lookback_days=lookback_days,
        anchor_dt=anchor_dt,
        start_dt=start_dt,
        end_dt=end_dt,
        max_missing_days=max_missing_days,
        include_sell=include_sell,
        include_phase=include_phase,
        force_recompute=force_recompute,
        progress_cb=_progress,
    )

    errors = result.get("errors") or []
    status = "success" if not errors else "failed"
    message = (
        f"Backfill completed: missing_ml={result.get('missing_ml_total')} "
        f"target={len(result.get('target_dates') or [])} "
        f"selected={result.get('missing_ml_selected')} "
        f"predicted={len(result.get('predicted_dates') or [])} "
        f"sell={len(result.get('sell_refreshed_dates') or [])}"
    )
    if errors:
        message = f"{message} / errors={len(errors)}"
    job_manager._update_db(
        job_id,
        "analysis_backfill",
        status,
        progress=100,
        message=message,
        error=(" | ".join(str(e) for e in errors)[:800] if errors else None),
        finished_at=datetime.now(),
    )
