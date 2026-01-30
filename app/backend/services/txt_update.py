from __future__ import annotations

import json
import os
from datetime import datetime

from app.core.config import _resolve_pan_out_txt_dir, find_code_txt_path, config
from app.db.session import try_get_conn

USE_CODE_TXT = os.getenv("USE_CODE_TXT", "0") == "1"


def _state_path() -> str:
    default_path = str(config.DATA_DIR / "update_state.json")
    return os.path.abspath(os.getenv("UPDATE_STATE_PATH") or default_path)


def _read_update_state() -> dict:
    path = _state_path()
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return {}


def _read_text_lines(path: str) -> list[str]:
    for encoding in ("utf-8", "cp932"):
        try:
            with open(path, "r", encoding=encoding, errors="ignore") as handle:
                return handle.read().splitlines()
        except OSError:
            continue
    return []


def _count_codes(path: str) -> int:
    count = 0
    for line in _read_text_lines(path):
        text = line.strip()
        if not text:
            continue
        if text.startswith("#") or text.startswith("'"):
            continue
        count += 1
    return count


def txt_update_status() -> dict[str, object | None]:
    snapshot: dict[str, object | None] = {
        "running": False,
        "phase": "idle",
        "message": "",
        "error": None,
        "job_id": None,
        "progress": 0,
        "created_at": None,
        "finished_at": None,
    }

    try:
        with try_get_conn(timeout_sec=0.05) as conn:
            if conn is not None:
                row = conn.execute(
                    """
                    SELECT id, status, message, error, progress, created_at, finished_at
                    FROM sys_jobs
                    WHERE type = 'txt_update'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ).fetchone()
                if row:
                    job_id, status, message, error, progress, created_at, finished_at = row
                    snapshot["job_id"] = job_id
                    snapshot["phase"] = status or "idle"
                    snapshot["running"] = status in ("queued", "running")
                    snapshot["message"] = message
                    snapshot["error"] = error
                    snapshot["progress"] = progress or 0
                    if created_at:
                        snapshot["created_at"] = created_at.isoformat()
                    if finished_at:
                        snapshot["finished_at"] = finished_at.isoformat()
    except Exception:
        pass

    state = _read_update_state()
    snapshot["last_txt_update_at"] = state.get("last_txt_update_at")
    snapshot["last_txt_update_date"] = state.get("last_txt_update_date")
    return snapshot


def get_txt_status() -> dict[str, object | None]:
    pan_out_txt_dir = _resolve_pan_out_txt_dir()
    if not os.path.isdir(pan_out_txt_dir):
        return {"txt_count": 0, "code_txt_missing": False, "last_updated": None}

    txt_files = [
        os.path.join(pan_out_txt_dir, name)
        for name in os.listdir(pan_out_txt_dir)
        if name.endswith(".txt") and name.lower() != "code.txt"
    ]
    code_txt_missing = False
    if USE_CODE_TXT:
        code_txt_missing = find_code_txt_path(pan_out_txt_dir) is None
    last_updated = None
    if txt_files:
        last_updated = max(os.path.getmtime(path) for path in txt_files)
        last_updated = datetime.utcfromtimestamp(last_updated).isoformat() + "Z"

    return {"txt_count": len(txt_files), "code_txt_missing": code_txt_missing, "last_updated": last_updated}
