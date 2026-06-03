from __future__ import annotations

import os
import tempfile

os.environ["MEEMEE_DATA_DIR"] = tempfile.mkdtemp(prefix="meemee_tdnet_test_")

from app.backend.core import tdnet_import_job
from app.backend.services.data import tdnet_mcp_import


def test_tdnet_import_uses_yanoshin_when_fetch_command_is_missing(monkeypatch) -> None:
    monkeypatch.delenv("TDNET_MCP_FETCH_COMMAND", raising=False)
    monkeypatch.setattr(
        tdnet_mcp_import,
        "_fetch_yanoshin_items",
        lambda **kwargs: [
            {
                "disclosure_id": "yanoshin:1",
                "sec_code": "7203",
                "title": "test",
            }
        ],
    )

    result = tdnet_mcp_import.import_tdnet_from_mcp(code="7203", limit=25)

    assert result["provider"] == "yanoshin"
    assert result["saved"] == 1
    assert result["fetched"] == 1
    assert result["code"] == "7203"
    assert result["limit"] == 25
    assert result["command"] is None


def test_tdnet_import_job_marks_skipped_as_terminal(monkeypatch) -> None:
    updates: list[tuple[str, str, str, dict[str, object]]] = []

    class _DummyJobManager:
        def _update_db(self, job_id, job_type, status, **kwargs):
            updates.append((str(job_id), str(job_type), str(status), dict(kwargs)))

    monkeypatch.setattr(tdnet_import_job, "job_manager", _DummyJobManager())
    monkeypatch.setattr(
        tdnet_import_job,
        "import_tdnet_from_mcp",
        lambda **kwargs: {
            "status": "skipped",
            "reason": "TDNET_MCP_FETCH_COMMAND is not set",
            "summary": "tdnet_import=skipped(env_missing)",
        },
    )

    tdnet_import_job.handle_tdnet_import("job-1", {"code": "7203", "limit": 10})

    assert [status for _, _, status, _ in updates] == ["running", "skipped"]
    assert updates[-1][3]["message"] == "tdnet_import=skipped(env_missing) (TDNET_MCP_FETCH_COMMAND is not set)"
    assert updates[-1][3]["finished_at"] is not None
