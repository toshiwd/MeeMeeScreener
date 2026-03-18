from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.backend.services.ml.ranking_analysis_quality import (
    compute_ranking_analysis_quality_snapshot,
    get_ranking_analysis_review,
)

router = APIRouter(prefix="/api/quality", tags=["quality"])


@router.get("/ranking-analysis")
def get_ranking_analysis_quality(
    as_of: int | None = Query(None),
    persist: bool = Query(True),
):
    if as_of is not None and not (19_000_101 <= int(as_of) <= 21_001_231):
        raise HTTPException(status_code=400, detail="as_of must be YYYYMMDD")
    return compute_ranking_analysis_quality_snapshot(as_of_ymd=as_of, persist=bool(persist))


@router.get("/ranking-analysis/review")
def get_ranking_analysis_quality_review(
    days: int = Query(7, ge=1, le=90),
    min_occurrence: int = Query(2, ge=1, le=30),
):
    return get_ranking_analysis_review(days=int(days), min_occurrence=int(min_occurrence))
