from __future__ import annotations

import os
from datetime import datetime

from app.core.config import _resolve_pan_out_txt_dir, find_code_txt_path

USE_CODE_TXT = os.getenv("USE_CODE_TXT", "0") == "1"


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
