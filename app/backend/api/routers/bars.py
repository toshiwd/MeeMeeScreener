from __future__ import annotations

import os
from typing import Dict, List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.backend.api.dependencies import get_stock_repo
from app.backend.infra.duckdb.stock_repo import StockRepository
from app.services.box_detector import detect_boxes


router = APIRouter(prefix="/api", tags=["bars"])
_BATCH_BARS_V2 = os.getenv("BATCH_BARS_V2", "1").lower() in {"1", "true", "yes", "on"}


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
    valid_codes = [code.strip() for code in payload.codes if isinstance(code, str) and code.strip()]
    if not valid_codes:
        return {"items": items}

    if _BATCH_BARS_V2:
        if timeframe == "daily":
            rows_by_code = repo.get_daily_bars_batch(valid_codes, payload.limit)
            for code in valid_codes:
                rows = rows_by_code.get(code, [])
                items[code] = {
                    "bars": [list(row) for row in rows],
                    "ma": {"ma7": [], "ma20": [], "ma60": []},
                    "boxes": [],
                }
        else:
            rows_by_code = repo.get_monthly_bars_batch(valid_codes, payload.limit)
            for code in valid_codes:
                rows = rows_by_code.get(code, [])
                items[code] = {
                    "bars": [list(row) for row in rows],
                    "ma": {"ma7": [], "ma20": [], "ma60": []},
                    "boxes": detect_boxes(rows, range_basis="body", max_range_pct=0.2) if rows else [],
                }
        return {"items": items}

    for code in valid_codes:
        if timeframe == "daily":
            rows = repo.get_daily_bars(code, payload.limit)
            boxes: list[dict] = []
        else:
            rows = repo.get_monthly_bars(code, payload.limit)
            boxes = detect_boxes(rows, range_basis="body", max_range_pct=0.2)

        items[code] = {
            "bars": [list(row) for row in rows],
            "ma": {"ma7": [], "ma20": [], "ma60": []},
            "boxes": boxes,
        }

    return {"items": items}
