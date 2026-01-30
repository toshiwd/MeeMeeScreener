from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
import traceback

from app.backend.api.dependencies import get_favorites_repo
from app.backend.services.watchlist import normalize_watch_code

router = APIRouter(prefix="/api", tags=["favorites"])


def _normalize_code(code: str) -> str:
    normalized = normalize_watch_code(code)
    if not normalized:
        raise HTTPException(status_code=400, detail="invalid_code")
    return normalized


@router.get("/favorites")
def list_favorites(repo=Depends(get_favorites_repo)):
    try:
        records = repo.get_all()
        items = [{"code": code} for code, _ in records]
        return {"items": items, "codes": [item["code"] for item in items]}
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"favorites_list_failed:{exc}") from exc


@router.post("/favorites/{code}")
def add_favorite(code: str, repo=Depends(get_favorites_repo)):
    normalized = _normalize_code(code)
    repo.add(normalized)
    return {"ok": True, "code": normalized}


@router.delete("/favorites/{code}")
def delete_favorite(code: str, repo=Depends(get_favorites_repo)):
    normalized = _normalize_code(code)
    repo.remove(normalized)
    return {"ok": True, "code": normalized}
