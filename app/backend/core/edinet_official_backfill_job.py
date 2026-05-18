from __future__ import annotations

from datetime import datetime

from app.backend.core.jobs import job_manager
from app.backend.edinetdb.config import load_config
from app.backend.edinetdb.official_api import sync_official_documents_for_codes
from app.backend.edinetdb.repository import EdinetdbRepository
from app.backend.edinetdb.targets import normalize_sec_code
from app.core.config import config as app_config

EDINET_OFFICIAL_BACKFILL_JOB_TYPE = "edinet_official_backfill"
DEFAULT_LOOKBACK_DAYS = 45
MAX_LOOKBACK_DAYS = 180


def _to_int(value: object, default: int, *, minimum: int = 1, maximum: int | None = None) -> int:
    try:
        resolved = int(value) if value is not None else int(default)
    except (TypeError, ValueError):
        resolved = int(default)
    resolved = max(int(minimum), resolved)
    if maximum is not None:
        resolved = min(int(maximum), resolved)
    return resolved


def handle_edinet_official_backfill(job_id: str, payload: dict) -> None:
    code = normalize_sec_code(payload.get("code"))
    days = _to_int(payload.get("days"), DEFAULT_LOOKBACK_DAYS, minimum=1, maximum=MAX_LOOKBACK_DAYS)
    if not code:
        job_manager._update_db(
            job_id,
            EDINET_OFFICIAL_BACKFILL_JOB_TYPE,
            "skipped",
            progress=100,
            message="Official EDINET backfill skipped (reason=invalid_code)",
            finished_at=datetime.now(),
        )
        return

    job_manager._update_db(
        job_id,
        EDINET_OFFICIAL_BACKFILL_JOB_TYPE,
        "running",
        progress=15,
        message=f"Official EDINET backfill running (code={code}, days={days})",
    )
    try:
        repo = EdinetdbRepository(app_config.DB_PATH)
        repo.ensure_schema()
        cfg = load_config()
        summary = sync_official_documents_for_codes(
            repo=repo,
            cfg=cfg,
            job_name=f"official_backfill:{code}",
            sec_codes=[code],
            days=days,
        )
    except Exception as exc:
        job_manager._update_db(
            job_id,
            EDINET_OFFICIAL_BACKFILL_JOB_TYPE,
            "failed",
            progress=100,
            message=f"Official EDINET backfill failed (code={code}, days={days})",
            error=str(exc)[:800],
            finished_at=datetime.now(),
        )
        return

    if summary.get("skipped"):
        reason = str(summary.get("reason") or "skipped")
        job_manager._update_db(
            job_id,
            EDINET_OFFICIAL_BACKFILL_JOB_TYPE,
            "skipped",
            progress=100,
            message=f"Official EDINET backfill skipped (reason={reason}, code={code}, days={days})",
            finished_at=datetime.now(),
        )
        return

    message = (
        "Official EDINET backfill completed "
        f"(code={code}, days={summary.get('lookback_days')}, documents={summary.get('documents')}, "
        f"matched_dates={len(summary.get('matched_dates') or [])})"
    )
    job_manager._update_db(
        job_id,
        EDINET_OFFICIAL_BACKFILL_JOB_TYPE,
        "success",
        progress=100,
        message=message,
        finished_at=datetime.now(),
    )
