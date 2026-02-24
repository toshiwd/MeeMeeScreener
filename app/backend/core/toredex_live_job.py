from __future__ import annotations

from datetime import datetime

from app.backend.core.jobs import job_manager
from app.backend.services.toredex_runner import run_live


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "on"}


def handle_toredex_live(job_id: str, payload: dict) -> None:
    season_id = str(payload.get("season_id") or "").strip()
    if not season_id:
        raise RuntimeError("season_id is required")

    as_of = payload.get("asOf")
    as_of_text = str(as_of).strip() if as_of is not None else None
    dry_run = _to_bool(payload.get("dry_run"))

    job_manager._update_db(
        job_id,
        "toredex_live",
        "running",
        progress=10,
        message=f"TOREDEX run_live starting (season_id={season_id}, asOf={as_of_text or 'auto'})",
    )

    result = run_live(
        season_id=season_id,
        as_of=as_of_text,
        dry_run=dry_run,
    )
    status = str(result.get("status") or "success")
    message = (
        f"TOREDEX run_live completed (status={status}, asOf={result.get('asOf')}, "
        f"trades={result.get('trade_count')}, dry_run={dry_run})"
    )

    job_manager._update_db(
        job_id,
        "toredex_live",
        "success",
        progress=100,
        message=message,
        finished_at=datetime.now(),
    )
