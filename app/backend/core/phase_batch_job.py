from __future__ import annotations

import logging
from datetime import datetime

from app.backend.core.jobs import job_manager

try:
    from app.backend.db import get_conn
except ModuleNotFoundError:  # pragma: no cover - legacy tooling may import from app/backend on sys.path
    from db import get_conn  # type: ignore

try:
    from app.backend.jobs.phase_batch import run_batch
except ModuleNotFoundError:  # pragma: no cover
    from jobs.phase_batch import run_batch  # type: ignore

logger = logging.getLogger(__name__)


def handle_phase_rebuild(job_id: str, payload: dict) -> None:
    job_manager._update_db(
        job_id, "phase_rebuild", "running", message="Phase予測を再計算中...", progress=0
    )
    try:
        with get_conn() as conn:
            row = conn.execute("SELECT MAX(dt) FROM feature_snapshot_daily").fetchone()
        if not row or row[0] is None:
            raise RuntimeError("feature_snapshot_daily is empty")
        max_dt = int(row[0])
        run_batch(max_dt, max_dt, dry_run=False)
        job_manager._update_db(
            job_id,
            "phase_rebuild",
            "success",
            message=f"Phase予測を再計算しました (dt={max_dt})",
            progress=100,
            finished_at=datetime.now(),
        )
    except Exception as exc:
        logger.exception("Phase rebuild failed: %s", exc)
        job_manager._update_db(
            job_id,
            "phase_rebuild",
            "failed",
            error=str(exc),
            message=f"Phase予測の再計算に失敗しました: {exc}",
            finished_at=datetime.now(),
        )
