from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.backend.services import rankings_cache

router = APIRouter(prefix="/api", tags=["rankings"])


@router.get("/rankings")
def get_rankings(
    tf: str = Query("D"),
    which: str = Query("latest"),
    dir: str = Query("up"),
    limit: int = Query(50, ge=1, le=200),
):
    tf = tf.upper()
    if tf not in ("D", "W", "M"):
        raise HTTPException(status_code=400, detail="tf must be D/W/M")
    if which not in ("latest", "prev"):
        raise HTTPException(status_code=400, detail="which must be latest/prev")
    if dir not in ("up", "down"):
        raise HTTPException(status_code=400, detail="dir must be up/down")
    return rankings_cache.get_rankings(tf, which, dir, limit)
