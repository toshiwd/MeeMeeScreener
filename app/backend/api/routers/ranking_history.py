from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.backend.services import signal_tracking_service

router = APIRouter(prefix="/api/ranking-history", tags=["ranking-history"])


@router.get("/logic-versions")
def get_ranking_history_logic_versions():
    return signal_tracking_service.list_ranking_logic_versions()


@router.get("/appearances")
def get_ranking_history_appearances(
    status: str = Query("active"),
    dir: str = Query("up"),
    ranking_logic_version: str = Query("latest"),
    code: str | None = Query(None),
    q: str | None = Query(None),
    rank_bucket: str | None = Query(None),
    from_ymd: int | None = Query(None, alias="from"),
    to_ymd: int | None = Query(None, alias="to"),
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
    sort: str = Query("recent"),
    outcome: str = Query("all"),
):
    try:
        return signal_tracking_service.list_ranking_appearances(
            status=status,
            direction=dir,
            ranking_logic_version=ranking_logic_version,
            code=code,
            query=q,
            rank_bucket=rank_bucket,
            from_ymd=from_ymd,
            to_ymd=to_ymd,
            limit=limit,
            offset=offset,
            sort=sort,
            outcome=outcome,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/appearances/{appearance_id}")
def get_ranking_history_appearance_detail(appearance_id: str):
    try:
        return signal_tracking_service.get_ranking_appearance_detail(appearance_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="appearance not found") from exc


@router.get("/summary")
def get_ranking_history_summary(
    dir: str = Query("up"),
    ranking_logic_version: str = Query("latest"),
):
    try:
        return signal_tracking_service.get_ranking_history_summary(
            direction=dir,
            ranking_logic_version=ranking_logic_version,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/analysis")
def get_ranking_history_analysis(
    ranking_logic_version: str = Query("latest"),
):
    try:
        return signal_tracking_service.get_ranking_history_analysis(
            ranking_logic_version=ranking_logic_version,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
