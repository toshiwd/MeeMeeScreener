from __future__ import annotations

from typing import Dict, Iterable, List, Sequence, Any

from datetime import datetime, timezone

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
    asof: str | int | None = None,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, List[List[float]]]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    asof_dt = _parse_dt(asof)
    rows = repo.get_daily_bars(code, limit, asof_dt)
    return {"data": _normalize_rows(rows, fill_volume=True), "errors": []}


@router.get("/monthly", response_model=None)
def get_monthly_bars(
    code: str,
    limit: int = 120,
    asof: str | int | None = None,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, List[List[float]]]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    asof_dt = _parse_dt(asof)
    rows = repo.get_monthly_bars(code, limit, asof_dt)
    return {"data": _normalize_rows(rows, fill_volume=True), "errors": []}


@router.get("/boxes", response_model=None)
def get_boxes(
    code: str,
    limit: int = 120,
    asof: str | int | None = None,
    repo: StockRepository = Depends(get_stock_repo),
) -> List[Dict]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    asof_dt = _parse_dt(asof)
    rows = repo.get_monthly_bars(code, limit, asof_dt)
    return detect_boxes(rows, range_basis="body", max_range_pct=0.2)


def _parse_dt(value: str | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        raw = str(value)
    else:
        raw = str(value).strip()
    if not raw:
        return None
    if raw.isdigit() and len(raw) == 8:
        parsed = datetime.strptime(raw, "%Y%m%d").replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            parsed = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            return int(parsed.timestamp())
        except ValueError:
            continue
    if raw.isdigit():
        value_int = int(raw)
        if value_int > 1_000_000_000_000:
            return int(value_int / 1000)
        return value_int
    return None


def _to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed else None


@router.get("/phase", response_model=None)
def get_phase_pred(
    code: str,
    asof: str | int | None = None,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, Any]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    asof_dt = _parse_dt(asof)
    row = repo.get_phase_pred(code, asof_dt)
    if not row:
        return {"item": None}
    return {
        "item": {
            "dt": row[0],
            "earlyScore": row[1],
            "lateScore": row[2],
            "bodyScore": row[3],
            "n": row[4],
            "reasonsTop3": row[5],
        }
    }


@router.get("/analysis", response_model=None)
def get_analysis_pred(
    code: str,
    asof: str | int | None = None,
    repo: StockRepository = Depends(get_stock_repo),
) -> Dict[str, Any]:
    if not code:
        raise HTTPException(status_code=400, detail="code is required")
    asof_dt = _parse_dt(asof)
    row = repo.get_ml_analysis_pred(code, asof_dt)
    if not row:
        return {"item": None}
    p_up = _to_float_or_none(row[1])
    p_down = (1.0 - p_up) if p_up is not None else None
    ev20 = _to_float_or_none(row[5])
    ev20_net_raw = _to_float_or_none(row[6])
    ev20_net = ev20_net_raw if ev20_net_raw is not None else (ev20 - 0.002 if ev20 is not None else None)
    model_version = row[7]
    return {
        "item": {
            "dt": row[0],
            "pUp": p_up,
            "pDown": p_down,
            "pTurnUp": _to_float_or_none(row[2]),
            "pTurnDown": _to_float_or_none(row[3]),
            "retPred20": _to_float_or_none(row[4]),
            "ev20": ev20,
            "ev20Net": ev20_net,
            "modelVersion": str(model_version) if model_version is not None else None,
        }
    }
