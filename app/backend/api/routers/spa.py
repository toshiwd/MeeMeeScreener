from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, JSONResponse

from app.backend.services.static_assets import resolve_static_file

router = APIRouter()


@router.get("/")
def serve_root():
    index_path = resolve_static_file("index.html")
    if not index_path:
        return JSONResponse(status_code=404, content={"error": "static_not_found"})
    return FileResponse(index_path)


@router.get("/{full_path:path}")
def serve_spa(full_path: str):
    if full_path.startswith("api") or full_path.startswith("health"):
        raise HTTPException(status_code=404)
    resolved = resolve_static_file(full_path)
    if resolved:
        return FileResponse(resolved)
    index_path = resolve_static_file("index.html")
    if not index_path:
        return JSONResponse(status_code=404, content={"error": "static_not_found"})
    return FileResponse(index_path)
