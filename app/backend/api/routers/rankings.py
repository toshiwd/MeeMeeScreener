from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.backend.services import rankings_cache

router = APIRouter(prefix="/api", tags=["rankings"])


@router.get("/rankings")
def get_rankings(
    tf: str = Query("D"),
    which: str = Query("latest"),
    dir: str = Query("up"),
    mode: str = Query("hybrid"),
    risk_mode: str = Query("balanced"),
    limit: int = Query(50, ge=1, le=200),
):
    tf = tf.upper()
    mode = mode.lower()
    risk_mode = risk_mode.lower()
    if tf not in ("D", "W", "M"):
        raise HTTPException(status_code=400, detail="tf must be D/W/M")
    if which not in ("latest", "prev"):
        raise HTTPException(status_code=400, detail="which must be latest/prev")
    if dir not in ("up", "down"):
        raise HTTPException(status_code=400, detail="dir must be up/down")
    if mode not in ("rule", "ml", "hybrid", "turn"):
        raise HTTPException(status_code=400, detail="mode must be rule/ml/hybrid/turn")
    if risk_mode not in ("defensive", "balanced", "aggressive"):
        raise HTTPException(status_code=400, detail="risk_mode must be defensive/balanced/aggressive")
    return rankings_cache.get_rankings(tf, which, dir, limit, mode=mode, risk_mode=risk_mode)


@router.get("/rankings/trace/last-qualified")
def get_rankings_last_qualified_trace(
    tf: str = Query("D"),
    which: str = Query("latest"),
    dir: str = Query("up"),
    mode: str = Query("hybrid"),
    risk_mode: str = Query("balanced"),
    limit: int = Query(50, ge=1, le=200),
    lookback_days: int = Query(260, ge=20, le=1200),
    recent_hits: int = Query(10, ge=1, le=50),
    as_of: str | None = Query(None),
):
    tf = tf.upper()
    mode = mode.lower()
    risk_mode = risk_mode.lower()
    if tf not in ("D", "W", "M"):
        raise HTTPException(status_code=400, detail="tf must be D/W/M")
    if which not in ("latest", "prev"):
        raise HTTPException(status_code=400, detail="which must be latest/prev")
    if dir not in ("up", "down"):
        raise HTTPException(status_code=400, detail="dir must be up/down")
    if mode not in ("rule", "ml", "hybrid", "turn"):
        raise HTTPException(status_code=400, detail="mode must be rule/ml/hybrid/turn")
    if risk_mode not in ("defensive", "balanced", "aggressive"):
        raise HTTPException(status_code=400, detail="risk_mode must be defensive/balanced/aggressive")
    return rankings_cache.get_last_qualified_trace(
        tf,
        which,
        dir,
        limit,
        mode=mode,
        risk_mode=risk_mode,
        lookback_days=lookback_days,
        recent_hits=recent_hits,
        as_of=as_of,
    )
