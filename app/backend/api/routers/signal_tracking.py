from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.backend.core.jobs import job_manager
from app.backend.core.signal_tracking_job import (
    RANKING_APPEARANCE_REBUILD_JOB_TYPE,
    SIGNAL_TRACKING_BASIS_BACKFILL_JOB_TYPE,
    SIGNAL_TRACKING_CAMPAIGN_REBUILD_JOB_TYPE,
    SIGNAL_TRACKING_DECISION_REBUILD_JOB_TYPE,
)
from app.backend.services import signal_tracking_service

router = APIRouter(prefix="/api/signal-tracking", tags=["signal-tracking"])


@router.get("/logic-versions")
def get_signal_logic_versions():
    return signal_tracking_service.list_logic_versions()


@router.post("/logic-versions/activate")
def post_activate_signal_logic_version(logic_version: str = Query(..., min_length=1)):
    return signal_tracking_service.activate_logic_version(logic_version)


@router.get("/campaigns")
def get_signal_tracking_campaigns(
    status: str = Query("active"),
    side: str = Query("buy"),
    logic_version: str = Query("latest"),
    q: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    as_of: int | None = Query(None),
):
    try:
        return signal_tracking_service.list_signal_campaigns(
            status=status,
            side=side,
            logic_version=logic_version,
            query=q,
            limit=limit,
            as_of=as_of,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/campaigns/{campaign_id}")
def get_signal_tracking_campaign_detail(
    campaign_id: str,
    as_of: int | None = Query(None),
):
    try:
        return signal_tracking_service.get_signal_campaign_detail(campaign_id, as_of=as_of)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="campaign not found") from exc


@router.get("/events")
def get_signal_tracking_events(
    status: str = Query("active"),
    side: str = Query("buy"),
    logic_version: str = Query("latest"),
    code: str | None = Query(None),
    q: str | None = Query(None),
    from_ymd: int | None = Query(None, alias="from"),
    to_ymd: int | None = Query(None, alias="to"),
    limit: int = Query(200, ge=1, le=500),
    as_of: int | None = Query(None),
):
    try:
        return signal_tracking_service.list_signal_events(
            status=status,
            side=side,
            logic_version=logic_version,
            code=code,
            query=q,
            from_ymd=from_ymd,
            to_ymd=to_ymd,
            limit=limit,
            as_of=as_of,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/events/{event_id}")
def get_signal_tracking_event_detail(
    event_id: str,
    as_of: int | None = Query(None),
):
    try:
        return signal_tracking_service.get_signal_event_detail(event_id, as_of=as_of)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="event not found") from exc


@router.get("/markers")
def get_signal_tracking_markers(
    code: str = Query(..., min_length=1),
    from_ymd: int | None = Query(None, alias="from"),
    to_ymd: int | None = Query(None, alias="to"),
    logic_version: str = Query("latest"),
    ranking_logic_version: str = Query("latest"),
):
    try:
        return signal_tracking_service.get_signal_markers(
            code=code,
            from_ymd=from_ymd,
            to_ymd=to_ymd,
            logic_version=logic_version,
            ranking_logic_version=ranking_logic_version,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/ranking-logic-versions")
def get_ranking_logic_versions():
    return signal_tracking_service.list_ranking_logic_versions()


@router.get("/status")
def get_signal_tracking_status():
    return signal_tracking_service.get_tracking_runtime_status()


@router.post("/ranking-logic-versions/activate")
def post_activate_ranking_logic_version(ranking_logic_version: str = Query(..., min_length=1)):
    return signal_tracking_service.activate_ranking_logic_version(ranking_logic_version)


@router.get("/summary")
def get_signal_tracking_summary(
    side: str = Query("buy"),
    logic_version: str = Query("latest"),
    as_of: int | None = Query(None),
):
    try:
        return signal_tracking_service.get_signal_tracking_summary(
            side=side,
            logic_version=logic_version,
            as_of=as_of,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/validation")
def get_signal_tracking_validation(
    side: str = Query("buy"),
    logic_version: str = Query("latest"),
    from_ymd: int | None = Query(None, alias="from"),
    to_ymd: int | None = Query(None, alias="to"),
    as_of: int | None = Query(None),
):
    try:
        return signal_tracking_service.get_signal_tracking_validation(
            side=side,
            logic_version=logic_version,
            from_ymd=from_ymd,
            to_ymd=to_ymd,
            as_of=as_of,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/analysis")
def get_signal_tracking_analysis(
    side: str = Query("buy"),
    logic_version: str = Query("latest"),
    from_ymd: int | None = Query(None, alias="from"),
    to_ymd: int | None = Query(None, alias="to"),
    as_of: int | None = Query(None),
):
    try:
        return signal_tracking_service.get_signal_tracking_analysis(
            side=side,
            logic_version=logic_version,
            from_ymd=from_ymd,
            to_ymd=to_ymd,
            as_of=as_of,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/compare")
def get_signal_tracking_comparison(
    side: str = Query("sell"),
    base_logic_version: str = Query(signal_tracking_service.DEFAULT_LOGIC_VERSION),
    target_logic_version: str = Query(signal_tracking_service.SELL_TIGHTENED_LOGIC_VERSION),
    primary_horizon: int | None = Query(None),
    from_ymd: int | None = Query(None, alias="from"),
    to_ymd: int | None = Query(None, alias="to"),
    as_of: int | None = Query(None),
):
    try:
        return signal_tracking_service.get_signal_tracking_comparison(
            side=side,
            base_logic_version=base_logic_version,
            target_logic_version=target_logic_version,
            primary_horizon=primary_horizon,
            from_ymd=from_ymd,
            to_ymd=to_ymd,
            as_of=as_of,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/leakage-audit")
def get_signal_tracking_leakage_audit(
    side: str = Query("buy"),
    logic_version: str = Query("latest"),
    from_ymd: int | None = Query(None, alias="from"),
    to_ymd: int | None = Query(None, alias="to"),
):
    try:
        return signal_tracking_service.get_signal_tracking_leakage_audit(
            side=side,
            logic_version=logic_version,
            from_ymd=from_ymd,
            to_ymd=to_ymd,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/refresh")
def post_signal_tracking_refresh(
    as_of: int | None = Query(None),
):
    return signal_tracking_service.refresh_signal_tracking(as_of=as_of)


@router.post("/basis/backfill")
def post_signal_tracking_basis_backfill(
    from_ymd: int | None = Query(None, alias="from"),
    to_ymd: int | None = Query(None, alias="to"),
    basis_version: str = Query(signal_tracking_service.DEFAULT_BASIS_VERSION),
    reset_scope: bool = Query(False),
):
    payload = {
        "from": from_ymd,
        "to": to_ymd,
        "basis_version": basis_version,
        "reset_scope": reset_scope,
    }
    job_id = job_manager.submit(SIGNAL_TRACKING_BASIS_BACKFILL_JOB_TYPE, payload)
    if not job_id:
        raise HTTPException(status_code=409, detail="failed to submit basis rebuild job")
    return {"ok": True, "job_id": job_id}


@router.post("/decisions/rebuild")
def post_signal_tracking_decisions_rebuild(
    from_ymd: int | None = Query(None, alias="from"),
    to_ymd: int | None = Query(None, alias="to"),
    logic_version: str = Query("latest"),
    side: str = Query("all"),
    basis_version: str | None = Query(None),
    reset_scope: bool = Query(False),
):
    payload = {
        "from": from_ymd,
        "to": to_ymd,
        "logic_version": logic_version,
        "side": side,
        "basis_version": basis_version,
        "reset_scope": reset_scope,
    }
    job_id = job_manager.submit(SIGNAL_TRACKING_DECISION_REBUILD_JOB_TYPE, payload)
    if not job_id:
        raise HTTPException(status_code=409, detail="failed to submit decision rebuild job")
    return {"ok": True, "job_id": job_id}


@router.post("/campaigns/rebuild")
def post_signal_tracking_campaigns_rebuild(
    logic_version: str = Query("latest"),
    side: str = Query("all"),
):
    payload = {
        "logic_version": logic_version,
        "side": side,
    }
    job_id = job_manager.submit(SIGNAL_TRACKING_CAMPAIGN_REBUILD_JOB_TYPE, payload)
    if not job_id:
        raise HTTPException(status_code=409, detail="failed to submit campaign rebuild job")
    return {"ok": True, "job_id": job_id}


@router.post("/ranking/rebuild")
def post_ranking_appearance_rebuild(
    from_ymd: int | None = Query(None, alias="from"),
    to_ymd: int | None = Query(None, alias="to"),
    ranking_logic_version: str = Query("latest"),
    signal_logic_version: str = Query("latest"),
    basis_version: str | None = Query(None),
    reset_scope: bool = Query(False),
):
    payload = {
        "from": from_ymd,
        "to": to_ymd,
        "ranking_logic_version": ranking_logic_version,
        "signal_logic_version": signal_logic_version,
        "basis_version": basis_version,
        "reset_scope": reset_scope,
    }
    job_id = job_manager.submit(RANKING_APPEARANCE_REBUILD_JOB_TYPE, payload)
    if not job_id:
        raise HTTPException(status_code=409, detail="failed to submit ranking rebuild job")
    return {"ok": True, "job_id": job_id}


@router.post("/backfill")
def post_signal_tracking_backfill(
    from_ymd: int | None = Query(None, alias="from"),
    to_ymd: int | None = Query(None, alias="to"),
    lookback_days: int = Query(signal_tracking_service.DEFAULT_BACKFILL_LOOKBACK_DAYS, ge=20, le=10000),
    logic_version: str = Query("latest"),
    side: str = Query("all"),
    basis_version: str = Query(signal_tracking_service.DEFAULT_BASIS_VERSION),
    reset_scope: bool = Query(False),
):
    try:
        return signal_tracking_service.backfill_signal_tracking(
            from_ymd=from_ymd,
            to_ymd=to_ymd,
            lookback_days=lookback_days,
            logic_version=logic_version,
            side=side,
            basis_version=basis_version,
            reset_scope=reset_scope,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/basis/provenance-backfill")
def post_signal_tracking_basis_provenance_backfill(
    from_ymd: int | None = Query(None, alias="from"),
    to_ymd: int | None = Query(None, alias="to"),
    basis_version: str = Query(signal_tracking_service.DEFAULT_BASIS_VERSION),
):
    try:
        return signal_tracking_service.backfill_signal_basis_provenance(
            from_ymd=from_ymd,
            to_ymd=to_ymd,
            basis_version=basis_version,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
