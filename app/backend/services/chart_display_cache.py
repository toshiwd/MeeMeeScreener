from __future__ import annotations

import copy
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.config import DATA_DIR

_SCHEMA_VERSION = 1
_CACHE_DIR_ENV = "MEEMEE_CHART_DISPLAY_CACHE_DIR"


def _cache_root() -> Path:
    override = str(os.getenv(_CACHE_DIR_ENV) or "").strip()
    if override:
        return Path(override)
    return Path(DATA_DIR) / "cache" / "chart_display"


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def build_chart_display_cache_key(
    *,
    codes: list[str],
    requested_frames: list[str],
    limit: int,
    timeframe_limits: dict[str, int],
    include_provisional: bool,
    include_boxes: bool,
    asof_dt: int | None,
    forward_bars: dict[str, int],
    runtime_db_path: str | None,
) -> dict[str, Any]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "codes": list(codes),
        "requested_frames": list(requested_frames),
        "limit": int(limit),
        "timeframe_limits": {str(key): int(value) for key, value in sorted(timeframe_limits.items())},
        "include_provisional": bool(include_provisional),
        "include_boxes": bool(include_boxes),
        "asof_dt": int(asof_dt) if asof_dt is not None else None,
        "forward_bars": {str(key): int(value) for key, value in sorted(forward_bars.items())},
        "runtime_db_path": str(runtime_db_path or ""),
    }


def chart_display_cache_id(cache_key: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(cache_key).encode("utf-8")).hexdigest()


def _cache_path(cache_key: dict[str, Any]) -> Path:
    cache_id = chart_display_cache_id(cache_key)
    return _cache_root() / cache_id[:2] / f"{cache_id}.json"


def store_chart_display_cache(
    *,
    cache_key: dict[str, Any],
    payload: dict[str, Any],
) -> dict[str, Any]:
    root_path = _cache_path(cache_key)
    root_path.parent.mkdir(parents=True, exist_ok=True)
    stored_at = datetime.now(timezone.utc).isoformat()
    envelope = {
        "schema_version": _SCHEMA_VERSION,
        "stored_at": stored_at,
        "cache_key": cache_key,
        "payload": copy.deepcopy(payload),
    }
    fd, temp_name = tempfile.mkstemp(prefix=f".{root_path.name}.", suffix=".tmp", dir=str(root_path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(envelope, handle, ensure_ascii=False, separators=(",", ":"))
        os.replace(temp_name, root_path)
    finally:
        try:
            if os.path.exists(temp_name):
                os.remove(temp_name)
        except OSError:
            pass
    return {"cache_id": chart_display_cache_id(cache_key), "stored_at": stored_at, "path": str(root_path)}


def load_chart_display_cache(cache_key: dict[str, Any]) -> dict[str, Any] | None:
    path = _cache_path(cache_key)
    try:
        with path.open("r", encoding="utf-8") as handle:
            envelope = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(envelope, dict):
        return None
    if envelope.get("schema_version") != _SCHEMA_VERSION:
        return None
    if envelope.get("cache_key") != cache_key:
        return None
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        return None
    return {
        "cache_id": chart_display_cache_id(cache_key),
        "stored_at": envelope.get("stored_at"),
        "path": str(path),
        "payload": copy.deepcopy(payload),
    }
