from __future__ import annotations

from typing import Dict, Iterable, List, Sequence

from fastapi import APIRouter, Depends, HTTPException

from app.backend.api.dependencies import get_stock_repo
from app.backend.infra.duckdb.stock_repo import StockRepository
from app.services.box_detector import detect_boxes


router = APIRouter(prefix="/api/ticker", tags=["ticker"])


def _normalize_rows(rows: Iterable[Sequence], *, fill_volume: bool) -> List[List[float]]:
    normalized: List[List[float]] = []
    for row in rows:
        if len(row) < 5:
            continue
        time_value, open_, high, low, close = row[:5]
        if time_value is None or open_ is None or high is None or low is None or close is None:
            continue
        volume = 0.0
        if len(row) >= 6 and row[5] is not None and fill_volume:
            try:
                volume = float(row[5])
            except (TypeError, ValueError):
                volume = 0.0
        normalized.append(
            [
                float(time_value),
                float(open_),
                float(high),
                float(low),
                float(close),
                volume,
            ]
        )
    return normalized


@router.get("/daily", response_model=None)
def get_daily_bars(
    code: str,
    limit: int = 400,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, List[List[float]]]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    rows = repo.get_daily_bars(code, limit)
    return {"data": _normalize_rows(rows, fill_volume=True), "errors": []}


@router.get("/monthly", response_model=None)
def get_monthly_bars(
    code: str,
    limit: int = 120,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, List[List[float]]]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    rows = repo.get_monthly_bars(code, limit)
    return {"data": _normalize_rows(rows, fill_volume=True), "errors": []}


@router.get("/boxes", response_model=None)
def get_boxes(
    code: str,
    limit: int = 120,
    repo: StockRepository = Depends(get_stock_repo),
) -> List[Dict]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    rows = repo.get_monthly_bars(code, limit)
    return detect_boxes(rows)
