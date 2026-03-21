from __future__ import annotations

import os
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from mimetypes import guess_type

from app.backend.services.static_assets import resolve_static_file

router = APIRouter()

_FORCED_MEDIA_TYPES = {
    ".js": "application/javascript",
    ".mjs": "application/javascript",
    ".css": "text/css",
    ".html": "text/html",
}


def _file_response(path: str) -> FileResponse:
    ext = os.path.splitext(path)[1].lower()
    media_type = _FORCED_MEDIA_TYPES.get(ext)
    if not media_type:
        media_type, _ = guess_type(path)
    return FileResponse(path, media_type=media_type)


@router.get("/")
def serve_root():
    index_path = resolve_static_file("index.html")
    if not index_path:
        return JSONResponse(status_code=404, content={"error": "static_not_found"})
    return _file_response(index_path)


@router.get("/{full_path:path}")
def serve_spa(full_path: str):
    if full_path.startswith("api") or full_path.startswith("health"):
        raise HTTPException(status_code=404)
    resolved = resolve_static_file(full_path)
    if resolved:
        return _file_response(resolved)
    index_name = "tradex/index.html" if full_path == "tradex" or full_path.startswith("tradex/") else "index.html"
    index_path = resolve_static_file(index_name)
    if not index_path:
        return JSONResponse(status_code=404, content={"error": "static_not_found"})
    return _file_response(index_path)
