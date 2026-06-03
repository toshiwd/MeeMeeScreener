from __future__ import annotations

import json
import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.backend.tdnetdb.repository import TdnetdbRepository
from app.core.config import config

logger = logging.getLogger(__name__)
YANOSHIN_TDNET_BASE_URL = "https://webapi.yanoshin.jp/webapi/tdnet/list"


def _resolve_fetch_command(*, code: str | None, limit: int) -> str | None:
    template = str(os.getenv("TDNET_MCP_FETCH_COMMAND") or "").strip()
    if not template:
        return None
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


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_yanoshin_item(item: dict[str, Any], *, fallback_code: str | None) -> dict[str, Any] | None:
    source = item.get("Tdnet") if isinstance(item.get("Tdnet"), dict) else item
    if not isinstance(source, dict):
        return None
    disclosure_id = _text(source.get("id"))
    if not disclosure_id:
        return None
    sec_code = _text(source.get("company_code") or source.get("sec_code")) or _text(fallback_code)
    if sec_code and len(sec_code) >= 4:
        sec_code = sec_code[:4]
    published_at = _text(source.get("pubdate") or source.get("published_at"))
    if published_at:
        try:
            published_at = datetime.strptime(published_at, "%Y-%m-%d %H:%M:%S").isoformat()
        except ValueError:
            published_at = published_at.replace(" ", "T")
    document_url = _text(source.get("document_url") or source.get("tdnet_url"))
    return {
        "disclosure_id": f"yanoshin:{disclosure_id}",
        "sec_code": sec_code,
        "company_name": _text(source.get("company_name")),
        "title": _text(source.get("title")),
        "category": _text(source.get("markets_string") or source.get("category")),
        "published_at": published_at,
        "tdnet_url": document_url,
        "pdf_url": document_url if document_url and document_url.lower().endswith(".pdf") else None,
        "xbrl_url": _text(source.get("url_xbrl") or source.get("xbrl_url")),
        "summary_text": _text(source.get("summary_text") or source.get("update_history")),
        "provider": "yanoshin",
        "markets": _text(source.get("markets_string") or source.get("markets")),
        "raw": source,
    }


def _fetch_yanoshin_items(*, code: str | None, limit: int) -> list[dict[str, Any]]:
    condition = _text(code) or "recent"
    safe_limit = max(1, min(500, int(limit)))
    url = f"{YANOSHIN_TDNET_BASE_URL}/{condition}.json?{urlencode({'limit': safe_limit})}"
    request = Request(url, headers={"User-Agent": "MeeMeeScreener/tdnet-yanoshin-fetcher"})
    timeout_sec = max(1, int(os.getenv("TDNET_YANOSHIN_TIMEOUT_SEC", "20")))
    with urlopen(request, timeout=timeout_sec) as response:
        payload = json.loads(response.read().decode("utf-8"))
    raw_items = payload.get("items") if isinstance(payload, dict) else payload
    if not isinstance(raw_items, list):
        raise RuntimeError("Yanoshin TDNET response must be a list or {items: [...]}")
    normalized = [
        _normalize_yanoshin_item(item, fallback_code=code)
        for item in raw_items
        if isinstance(item, dict)
    ]
    return [item for item in normalized if item is not None]


def import_tdnet_from_mcp(*, code: str | None = None, limit: int = 50, db_path: str | Path | None = None) -> dict[str, Any]:
    command = _resolve_fetch_command(code=code, limit=limit)
    if command:
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
        provider = "mcp_command"
    else:
        items = _fetch_yanoshin_items(code=code, limit=limit)
        provider = "yanoshin"
    repo = TdnetdbRepository(db_path or config.DB_PATH)
    saved = repo.upsert_disclosures(items)
    return {
        "saved": int(saved),
        "fetched": int(len(items)),
        "code": (code or "").strip() or None,
        "limit": int(limit),
        "command": command,
        "provider": provider,
    }
