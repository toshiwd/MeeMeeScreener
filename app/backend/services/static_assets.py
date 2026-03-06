from __future__ import annotations

import os

BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
APP_DIR = os.path.abspath(os.path.join(BACKEND_DIR, ".."))
FRONTEND_DIST_DIR = os.path.abspath(os.path.join(APP_DIR, "frontend", "dist"))
BACKEND_STATIC_DIR = os.path.abspath(os.path.join(BACKEND_DIR, "static"))


def _candidate_static_dirs() -> list[str]:
    candidates: list[str] = []
    configured = os.getenv("STATIC_DIR")
    if configured:
        candidates.append(os.path.abspath(configured))
    # Prefer freshly built frontend assets, keep backend/static as fallback for packaged runs.
    candidates.extend([FRONTEND_DIST_DIR, BACKEND_STATIC_DIR])

    deduped: list[str] = []
    seen: set[str] = set()
    for path in candidates:
        norm = os.path.normcase(path)
        if norm in seen:
            continue
        seen.add(norm)
        deduped.append(path)
    return deduped


def resolve_static_file(request_path: str) -> str | None:
    for static_dir in _candidate_static_dirs():
        if not static_dir or not os.path.isdir(static_dir):
            continue
        safe_path = os.path.abspath(os.path.join(static_dir, request_path))
        try:
            if os.path.commonpath([static_dir, safe_path]) != static_dir:
                continue
        except ValueError:
            continue
        if os.path.isdir(safe_path):
            safe_path = os.path.join(safe_path, "index.html")
        if os.path.isfile(safe_path):
            return safe_path
    return None
