from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.backend.services import rankings_cache

router = APIRouter(prefix="/api", tags=["rankings"])


@router.get("/rankings")
def get_rankings(
    tf: str = Query("D"),
    which: str = Query("latest"),
    dir: str = Query("up"),
    mode: str = Query("trade"),
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
    if mode not in ("rule", "ml", "hybrid", "turn", "trade"):
        raise HTTPException(status_code=400, detail="mode must be rule/ml/hybrid/turn/trade")
    if risk_mode not in ("defensive", "balanced", "aggressive"):
        raise HTTPException(status_code=400, detail="risk_mode must be defensive/balanced/aggressive")
    return rankings_cache.get_rankings(tf, which, dir, limit, mode=mode, risk_mode=risk_mode)


@router.get("/rankings/multi")
def get_rankings_multi(
    which: str = Query("latest"),
    dir: str = Query("up"),
    mode: str = Query("trade"),
    risk_mode: str = Query("balanced"),
    limit: int = Query(50, ge=1, le=200),
):
    mode = mode.lower()
    risk_mode = risk_mode.lower()
    if which not in ("latest", "prev"):
        raise HTTPException(status_code=400, detail="which must be latest/prev")
    if dir not in ("up", "down"):
        raise HTTPException(status_code=400, detail="dir must be up/down")
    if mode not in ("rule", "ml", "hybrid", "turn", "trade"):
        raise HTTPException(status_code=400, detail="mode must be rule/ml/hybrid/turn/trade")
    if risk_mode not in ("defensive", "balanced", "aggressive"):
        raise HTTPException(status_code=400, detail="risk_mode must be defensive/balanced/aggressive")

    items_by_tf: dict[str, list[dict]] = {"D": [], "W": [], "M": []}
    meta_by_tf: dict[str, dict[str, object]] = {}
    errors: list[str] = []
    freshness_state: str | None = None
    freshness_days: int | None = None
    snapshot_as_of: str | None = None
    current_candidate_available: bool | None = None
    for tf in ("D", "W", "M"):
        try:
            payload = rankings_cache.get_rankings(tf, which, dir, limit, mode=mode, risk_mode=risk_mode)
            items_by_tf[tf] = payload.get("items", []) if isinstance(payload, dict) else []
            if isinstance(payload, dict):
                meta_by_tf[tf] = {
                    "pred_dt": payload.get("pred_dt"),
                    "model_version": payload.get("model_version"),
                    "last_updated": payload.get("last_updated"),
                    "cache_generation": payload.get("cache_generation"),
                    "freshness_state": payload.get("freshness_state"),
                    "freshness_days": payload.get("freshness_days"),
                    "snapshot_as_of": payload.get("snapshot_as_of"),
                    "current_candidate_available": payload.get("current_candidate_available"),
                    "stale": payload.get("stale"),
                }
                payload_snapshot_as_of = str(payload.get("snapshot_as_of") or "").strip() or None
                if payload_snapshot_as_of:
                    snapshot_as_of = payload_snapshot_as_of if snapshot_as_of is None else max(snapshot_as_of, payload_snapshot_as_of)
                if freshness_state != "stale":
                    if str(payload.get("freshness_state") or "") == "stale" or bool(payload.get("stale")):
                        freshness_state = "stale"
                        current_candidate_available = False
                    elif str(payload.get("freshness_state") or "") == "fresh":
                        freshness_state = "fresh"
                        current_candidate_available = True
                if isinstance(payload.get("freshness_days"), int):
                    freshness_days = payload.get("freshness_days") if freshness_days is None else max(freshness_days, int(payload.get("freshness_days")))
        except Exception as exc:
            errors.append(f"{tf}:{exc}")
            items_by_tf[tf] = []
            freshness_state = "stale"
            current_candidate_available = False

    overall_freshness_state = freshness_state or ("stale" if errors else None)
    current_candidate_available = overall_freshness_state == "fresh"

    return {
        "which": which,
        "dir": dir,
        "mode": mode,
        "risk_mode": risk_mode,
        "limit": limit,
        "itemsByTf": items_by_tf,
        "metaByTf": meta_by_tf,
        "errors": errors,
        "freshness_state": overall_freshness_state,
        "freshness_days": freshness_days,
        "snapshot_as_of": snapshot_as_of,
        "current_candidate_available": current_candidate_available,
        "stale": overall_freshness_state != "fresh",
    }


@router.get("/rankings/trace/last-qualified")
def get_rankings_last_qualified_trace(
    tf: str = Query("D"),
    which: str = Query("latest"),
    dir: str = Query("up"),
    mode: str = Query("trade"),
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
    if mode not in ("rule", "ml", "hybrid", "turn", "trade"):
        raise HTTPException(status_code=400, detail="mode must be rule/ml/hybrid/turn/trade")
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


@router.get("/rankings/trade-summary")
def get_rankings_trade_summary(
    tf: str = Query("D"),
    which: str = Query("latest"),
    risk_mode: str = Query("balanced"),
    limit: int = Query(50, ge=1, le=200),
    top_n: int = Query(5, ge=1, le=20),
):
    tf = tf.upper()
    risk_mode = risk_mode.lower()
    if tf not in ("D", "W", "M"):
        raise HTTPException(status_code=400, detail="tf must be D/W/M")
    if which not in ("latest", "prev"):
        raise HTTPException(status_code=400, detail="which must be latest/prev")
    if risk_mode not in ("defensive", "balanced", "aggressive"):
        raise HTTPException(status_code=400, detail="risk_mode must be defensive/balanced/aggressive")
    return rankings_cache.get_trade_direction_summary(
        tf,
        which,
        limit,
        risk_mode=risk_mode,
        top_n=top_n,
    )


@router.get("/rankings/session")
def get_rankings_session(
    tf: str = Query("D"),
    which: str = Query("latest"),
    dir: str = Query("up"),
    mode: str = Query("trade"),
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
    if mode not in ("rule", "ml", "hybrid", "turn", "trade"):
        raise HTTPException(status_code=400, detail="mode must be rule/ml/hybrid/turn/trade")
    if risk_mode not in ("defensive", "balanced", "aggressive"):
        raise HTTPException(status_code=400, detail="risk_mode must be defensive/balanced/aggressive")
    return rankings_cache.get_rankings_session_bundle(tf, which, dir, limit, mode=mode, risk_mode=risk_mode)


@router.get("/rankings/trace/code-qualified")
def get_rankings_code_qualified_trace(
    code: str = Query(..., min_length=1),
    tf: str = Query("D"),
    which: str = Query("latest"),
    risk_mode: str = Query("balanced"),
    lookback_days: int = Query(120, ge=20, le=1200),
    recent_hits: int = Query(5, ge=1, le=50),
    as_of: str | None = Query(None),
    limit: int = Query(200, ge=1, le=200),
):
    tf = tf.upper()
    risk_mode = risk_mode.lower()
    if tf not in ("D", "W", "M"):
        raise HTTPException(status_code=400, detail="tf must be D/W/M")
    if which not in ("latest", "prev"):
        raise HTTPException(status_code=400, detail="which must be latest/prev")
    if risk_mode not in ("defensive", "balanced", "aggressive"):
        raise HTTPException(status_code=400, detail="risk_mode must be defensive/balanced/aggressive")
    return rankings_cache.get_trade_code_qualification_summary(
        tf,
        which,
        code,
        risk_mode=risk_mode,
        lookback_days=lookback_days,
        recent_hits=recent_hits,
        as_of=as_of,
        limit=limit,
    )


@router.get("/rankings/edinet/monitor")
def get_rankings_edinet_monitor(
    lookback_days: int = Query(365, ge=30, le=2000),
    dir: str = Query("all"),
    risk_mode: str = Query("all"),
    which: str = Query("latest"),
):
    dir = dir.lower()
    risk_mode = risk_mode.lower()
    which = which.lower()
    if dir not in ("all", "up", "down"):
        raise HTTPException(status_code=400, detail="dir must be all/up/down")
    if risk_mode not in ("all", "defensive", "balanced", "aggressive"):
        raise HTTPException(status_code=400, detail="risk_mode must be all/defensive/balanced/aggressive")
    if which not in ("latest", "prev"):
        raise HTTPException(status_code=400, detail="which must be latest/prev")
    return rankings_cache.get_edinet_monitor(
        lookback_days=lookback_days,
        direction=dir,
        risk_mode=risk_mode,
        which=which,
    )
