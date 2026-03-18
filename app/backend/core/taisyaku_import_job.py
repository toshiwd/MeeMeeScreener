from __future__ import annotations

from datetime import datetime

from app.backend.core.jobs import job_manager
from app.backend.services.data.taisyaku_import import import_taisyaku_csvs

TAISYAKU_IMPORT_JOB_TYPE = "taisyaku_import"


def handle_taisyaku_import(job_id: str, payload: dict) -> None:
    job_manager._update_db(
        job_id,
        TAISYAKU_IMPORT_JOB_TYPE,
        "running",
        progress=10,
        message="Fetching taisyaku.jp CSV snapshots...",
    )
    try:
        result = import_taisyaku_csvs()
    except Exception as exc:
        job_manager._update_db(
            job_id,
            TAISYAKU_IMPORT_JOB_TYPE,
            "failed",
            progress=100,
            message="Taisyaku import failed",
            error=str(exc)[:800],
            finished_at=datetime.now(),
        )
        return

    message = (
        "Taisyaku import completed "
        f"(balance={result.get('balanceSaved')}, fee={result.get('feeSaved')}, "
        f"restriction={result.get('restrictionSaved')})"
    )
    job_manager._update_db(
        job_id,
        TAISYAKU_IMPORT_JOB_TYPE,
        "success",
        progress=100,
        message=message,
        finished_at=datetime.now(),
    )

