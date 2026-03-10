from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

from app.backend.tdnetdb.repository import TdnetdbRepository
from app.core.config import config


def _resolve_fetch_command(*, code: str | None, limit: int) -> str:
    template = str(os.getenv("TDNET_MCP_FETCH_COMMAND") or "").strip()
    if not template:
        raise RuntimeError("TDNET_MCP_FETCH_COMMAND is not set")
    return template.replace("{code}", (code or "").strip()).replace("{limit}", str(int(limit)))


def _load_items_from_stdout(stdout_text: str) -> list[dict[str, Any]]:
    text = str(stdout_text or "").strip()
    if not text:
        return []
    payload = json.loads(text)
    items = payload.get("items") if isinstance(payload, dict) else payload
    if not isinstance(items, list):
        raise RuntimeError("TDNET MCP output must be a list or {\"items\": [...]}")
    return [item for item in items if isinstance(item, dict)]


def import_tdnet_from_mcp(*, code: str | None = None, limit: int = 50, db_path: str | Path | None = None) -> dict[str, Any]:
    command = _resolve_fetch_command(code=code, limit=limit)
    completed = subprocess.run(
        command,
        shell=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=max(10, int(os.getenv("TDNET_MCP_TIMEOUT_SEC", "120"))),
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(f"TDNET MCP command failed ({completed.returncode}): {detail[:800]}")
    items = _load_items_from_stdout(completed.stdout)
    repo = TdnetdbRepository(db_path or config.DB_PATH)
    saved = repo.upsert_disclosures(items)
    return {
        "saved": int(saved),
        "fetched": int(len(items)),
        "code": (code or "").strip() or None,
        "limit": int(limit),
        "command": command,
    }
