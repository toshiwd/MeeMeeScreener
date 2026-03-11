from __future__ import annotations

from datetime import datetime
from threading import Lock, Thread
from typing import Any, Optional

from fastapi import APIRouter, HTTPException

from app.backend.similarity import SimilarityService


router = APIRouter(prefix="/api/search/similar", tags=["similar"])

_service: Optional[SimilarityService] = None
_refresh_lock = Lock()
_status: dict[str, Any] = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "error": None,
    "mode": None,
}


def _get_service() -> SimilarityService:
    global _service
    if _service is None:
        _service = SimilarityService()
    return _service


def _set_status(**updates: Any) -> None:
    _status.update(updates)


@router.get("")
def search_similar(
    ticker: str,
    asof: Optional[str] = None,
    k: int = 30,
    alpha: float = 0.7,
    match_tag: bool = False,
):
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker is required")
    if alpha < 0 or alpha > 1:
        raise HTTPException(status_code=400, detail="alpha must be between 0 and 1")
    if k <= 0:
        raise HTTPException(status_code=400, detail="k must be positive")

    service = _get_service()
    try:
        results = service.search(ticker=ticker, asof=asof, k=k, alpha=alpha, match_tag=match_tag)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"search failed: {exc}") from exc
    return [item.dict() for item in results]


@router.get("/status")
def similar_status():
    return {"status": _status}


def _run_refresh(incremental: bool) -> None:
    try:
        _get_service().refresh_data(incremental=incremental)
        _set_status(error=None)
    except Exception as exc:
        _set_status(error=str(exc))
    finally:
        _set_status(running=False, finished_at=datetime.utcnow().isoformat())
        _refresh_lock.release()


@router.post("/refresh")
def refresh_similar(mode: Optional[str] = None):
    if not _refresh_lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="refresh already running")

    incremental = mode == "incremental"
    _set_status(
        running=True,
        started_at=datetime.utcnow().isoformat(),
        finished_at=None,
        error=None,
        mode=mode or ("incremental" if incremental else "full"),
    )

    Thread(target=_run_refresh, args=(incremental,), daemon=True).start()
    return {"ok": True, "status": _status}
