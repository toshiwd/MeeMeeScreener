from __future__ import annotations

import os

BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
STATIC_DIR = os.path.abspath(os.getenv("STATIC_DIR") or os.path.join(BACKEND_DIR, "static"))


def resolve_static_file(request_path: str) -> str | None:
    if not STATIC_DIR or not os.path.isdir(STATIC_DIR):
        return None
    safe_path = os.path.abspath(os.path.join(STATIC_DIR, request_path))
    try:
        if os.path.commonpath([STATIC_DIR, safe_path]) != STATIC_DIR:
            return None
    except ValueError:
        return None
    if os.path.isdir(safe_path):
        safe_path = os.path.join(safe_path, "index.html")
    return safe_path if os.path.isfile(safe_path) else None
