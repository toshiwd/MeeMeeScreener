import os
import sys
from unittest.mock import patch

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from app.backend.api.routers import jobs as jobs_router


class _NullConnContext:
    def __enter__(self):
        return None

    def __exit__(self, exc_type, exc, tb):
        return False


def test_get_job_status_returns_cached_status_when_db_is_temporarily_unavailable():
    cached = {
        "id": "job-1",
        "type": "txt_update",
        "status": "running",
        "created_at": "2026-03-11T12:00:00",
        "started_at": "2026-03-11T12:00:05",
        "finished_at": None,
        "progress": 95,
        "message": "Evaluating strategy walkforward gate...",
        "error": None,
    }

    with (
        patch.object(jobs_router, "try_get_conn", return_value=_NullConnContext()),
        patch.object(jobs_router.job_manager, "get_cached_status", return_value=cached),
    ):
        payload = jobs_router.get_job_status("job-1")

    assert payload == cached
