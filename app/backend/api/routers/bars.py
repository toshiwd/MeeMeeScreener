from __future__ import annotations

from typing import Dict, List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.backend.api.dependencies import get_stock_repo
from app.backend.infra.duckdb.stock_repo import StockRepository
from app.services.box_detector import detect_boxes


router = APIRouter(prefix="/api", tags=["bars"])


class BatchBarsRequest(BaseModel):
    timeframe: str = Field(..., description="daily or monthly")
    codes: List[str] = Field(default_factory=list)
    limit: int = Field(..., ge=1, le=2000)


@router.post("/batch_bars")
def batch_bars(
    payload: BatchBarsRequest,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, Dict]:
    timeframe = payload.timeframe
    if timeframe not in {"daily", "monthly"}:
        raise HTTPException(status_code=400, detail="Unsupported timeframe")

    items: Dict[str, Dict] = {}
    for code in payload.codes:
        if not code:
            continue
        if timeframe == "daily":
            rows = repo.get_daily_bars(code, payload.limit)
            boxes: list[dict] = []
        else:
            rows = repo.get_monthly_bars(code, payload.limit)
            boxes = detect_boxes(rows)

        bars = [list(row) for row in rows]
        items[code] = {
            "bars": bars,
            "ma": {"ma7": [], "ma20": [], "ma60": []},
            "boxes": boxes,
        }

    return {"items": items}
