from __future__ import annotations

from fastapi import APIRouter

from app.api.endpoints.events import router as events_router
from app.api.endpoints.health import router as health_router
from app.api.endpoints.jobs import router as jobs_router
from app.api.endpoints.practice import router as practice_router
from app.api.endpoints.screener import router as screener_router
from app.api.endpoints.spa import router as spa_router
from app.api.endpoints.trades import router as trades_router
from app.api.endpoints.txt_update import router as txt_update_router


def build_api_router() -> APIRouter:
    router = APIRouter()
    router.include_router(events_router)
    router.include_router(screener_router)
    router.include_router(trades_router)
    router.include_router(practice_router)

    router.include_router(health_router)
    router.include_router(txt_update_router)
    router.include_router(jobs_router)
    # Must be last so it doesn't shadow API routes.
    router.include_router(spa_router)
    return router
