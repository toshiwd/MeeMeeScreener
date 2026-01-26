from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, JSONResponse

router = APIRouter()


@router.get("/")
def serve_root():
    from app.backend import main as main_module

    index_path = main_module._resolve_static_file("index.html")
    if not index_path:
        return JSONResponse(status_code=404, content={"error": "static_not_found"})
    return FileResponse(index_path)


@router.get("/{full_path:path}")
def serve_spa(full_path: str):
    from app.backend import main as main_module

    if full_path.startswith("api") or full_path.startswith("health"):
        raise HTTPException(status_code=404)
    resolved = main_module._resolve_static_file(full_path)
    if resolved:
        return FileResponse(resolved)
    index_path = main_module._resolve_static_file("index.html")
    if not index_path:
        return JSONResponse(status_code=404, content={"error": "static_not_found"})
    return FileResponse(index_path)

