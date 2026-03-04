from __future__ import annotations

from fastapi import APIRouter, Query

from app.backend.services import toredex_simulation_service

router = APIRouter(prefix="/api/toredex", tags=["toredex"])


@router.get("/simulation/validate")
def get_validate_simulation(
    limit: int = Query(30, ge=1, le=200),
):
    return toredex_simulation_service.get_validate_simulation(
        principal_jpy=10_000_000,
        limit=int(limit),
    )
