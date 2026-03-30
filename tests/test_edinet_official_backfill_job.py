from __future__ import annotations

import os
import tempfile

os.environ["MEEMEE_DATA_DIR"] = tempfile.mkdtemp(prefix="meemee_edinet_official_test_")

from fastapi.responses import JSONResponse

from app.backend.api.routers import jobs as jobs_router
from app.backend.core import edinet_official_backfill_job


def test_handle_edinet_official_backfill_marks_success(monkeypatch) -> None:
    updates: list[tuple[str, str, str, dict[str, object]]] = []

    class _DummyJobManager:
        def _update_db(self, job_id, job_type, status, **kwargs):
            updates.append((str(job_id), str(job_type), str(status), dict(kwargs)))

    class _DummyRepo:
        def __init__(self, db_path):
            self.db_path = db_path

        def ensure_schema(self):
            return None

    monkeypatch.setattr(edinet_official_backfill_job, "job_manager", _DummyJobManager())
    monkeypatch.setattr(edinet_official_backfill_job, "EdinetdbRepository", _DummyRepo)
    monkeypatch.setattr(edinet_official_backfill_job, "load_config", lambda: object())
    monkeypatch.setattr(
        edinet_official_backfill_job,
        "sync_official_documents_for_codes",
        lambda **kwargs: {
            "skipped": False,
            "lookback_days": 30,
            "documents": 2,
            "matched_dates": ["2026-03-30"],
        },
    )

    edinet_official_backfill_job.handle_edinet_official_backfill("job-1", {"code": "13010", "days": 30})

    assert [status for _, _, status, _ in updates] == ["running", "success"]
    assert "code=1301" in str(updates[-1][3]["message"])


def test_submit_edinet_official_backfill_normalizes_code(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object], bool, str]] = []

    class _DummyJobManager:
        def submit(self, job_type, payload, unique=False, *, message=""):
            calls.append((str(job_type), dict(payload), bool(unique), str(message)))
            return "job-123"

    monkeypatch.setattr(jobs_router, "job_manager", _DummyJobManager())

    response = jobs_router.submit_edinet_official_backfill(code="13010", days=45)

    assert response == {"ok": True, "job_id": "job-123"}
    assert calls == [
        (
            edinet_official_backfill_job.EDINET_OFFICIAL_BACKFILL_JOB_TYPE,
            {"code": "1301", "days": 45},
            True,
            "Official EDINET backfill queued (1301)",
        )
    ]


def test_submit_edinet_official_backfill_rejects_invalid_code() -> None:
    response = jobs_router.submit_edinet_official_backfill(code="abc", days=45)

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400


def test_submit_edinet_official_backfill_rejects_when_job_active(monkeypatch) -> None:
    monkeypatch.setattr(jobs_router, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(jobs_router, "_count_active_jobs", lambda job_type: (1, None))

    response = jobs_router.submit_edinet_official_backfill(code="13010", days=45)

    assert isinstance(response, JSONResponse)
    assert response.status_code == 409


def test_submit_edinet_official_backfill_returns_retryable_when_active_job_check_hits_db_lock(monkeypatch) -> None:
    monkeypatch.setattr(jobs_router, "cleanup_stale_jobs", lambda: None)
    monkeypatch.setattr(jobs_router, "_count_active_jobs", lambda job_type: (0, "db_lock_during_active_job_check"))

    response = jobs_router.submit_edinet_official_backfill(code="13010", days=45)

    assert isinstance(response, JSONResponse)
    assert response.status_code == 503
    assert response.body == (
        b'{"error":"db_unavailable","retryable":true,"message":"Database is temporarily unavailable",'
        b'"error_detail":"db_lock_during_active_job_check"}'
    )
    assert response.headers["Retry-After"] == "1"
