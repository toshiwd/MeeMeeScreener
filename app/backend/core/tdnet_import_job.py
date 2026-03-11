from __future__ import annotations

from datetime import datetime

from app.backend.core.jobs import job_manager
from app.backend.services.tdnet_mcp_import import import_tdnet_from_mcp

TDNET_IMPORT_JOB_TYPE = "tdnet_import"


def _to_int(value: object, default: int, *, minimum: int = 1, maximum: int | None = None) -> int:
    try:
        resolved = int(value) if value is not None else int(default)
    except (TypeError, ValueError):
        resolved = int(default)
    resolved = max(int(minimum), resolved)
    if maximum is not None:
        resolved = min(int(maximum), resolved)
    return resolved


def handle_tdnet_import(job_id: str, payload: dict) -> None:
    code = str(payload.get("code") or "").strip() or None
    limit = _to_int(payload.get("limit"), 50, minimum=1, maximum=500)
    job_manager._update_db(
        job_id,
        TDNET_IMPORT_JOB_TYPE,
        "running",
        progress=15,
        message="Fetching TDNET disclosures from MCP...",
    )
    try:
        result = import_tdnet_from_mcp(code=code, limit=limit)
    except Exception as exc:
        job_manager._update_db(
            job_id,
            TDNET_IMPORT_JOB_TYPE,
            "failed",
            progress=100,
            message="TDNET import failed",
            error=str(exc)[:800],
            finished_at=datetime.now(),
        )
        return

    message = (
        f"TDNET import completed (saved={result.get('saved')}, fetched={result.get('fetched')}, "
        f"code={result.get('code') or 'all'})"
    )
    job_manager._update_db(
        job_id,
        TDNET_IMPORT_JOB_TYPE,
        "success",
        progress=100,
        message=message,
        finished_at=datetime.now(),
    )
