from __future__ import annotations

from datetime import datetime
from typing import Any

from app.backend.core.jobs import job_manager
from app.backend.services import signal_tracking_service

SIGNAL_TRACKING_BASIS_BACKFILL_JOB_TYPE = "signal_tracking_basis_backfill"
SIGNAL_TRACKING_DECISION_REBUILD_JOB_TYPE = "signal_tracking_decision_rebuild"
SIGNAL_TRACKING_CAMPAIGN_REBUILD_JOB_TYPE = "signal_tracking_campaign_rebuild"
RANKING_APPEARANCE_REBUILD_JOB_TYPE = "ranking_appearance_rebuild"


def _handle_tracking_job(job_id: str, job_type: str, payload: dict[str, Any]) -> None:
    if job_type == SIGNAL_TRACKING_BASIS_BACKFILL_JOB_TYPE:
        job_manager._update_db(job_id, job_type, "running", message="Rebuilding signal basis...", progress=10)
        result = signal_tracking_service.backfill_signal_basis(
            from_ymd=payload.get("from"),
            to_ymd=payload.get("to"),
            basis_version=str(payload.get("basis_version") or signal_tracking_service.DEFAULT_BASIS_VERSION),
            reset_scope=bool(payload.get("reset_scope")),
        )
    elif job_type == SIGNAL_TRACKING_DECISION_REBUILD_JOB_TYPE:
        job_manager._update_db(job_id, job_type, "running", message="Rebuilding signal decisions...", progress=10)
        result = signal_tracking_service.rebuild_signal_decisions(
            from_ymd=payload.get("from"),
            to_ymd=payload.get("to"),
            logic_version=payload.get("logic_version"),
            side=str(payload.get("side") or "all"),
            basis_version=payload.get("basis_version"),
            reset_scope=bool(payload.get("reset_scope")),
        )
    elif job_type == RANKING_APPEARANCE_REBUILD_JOB_TYPE:
        job_manager._update_db(job_id, job_type, "running", message="Rebuilding ranking appearances...", progress=10)
        result = signal_tracking_service.rebuild_ranking_appearances(
            from_ymd=payload.get("from"),
            to_ymd=payload.get("to"),
            ranking_logic_version=payload.get("ranking_logic_version"),
            signal_logic_version=payload.get("signal_logic_version"),
            basis_version=payload.get("basis_version"),
            reset_scope=bool(payload.get("reset_scope")),
        )
    else:
        job_manager._update_db(job_id, job_type, "running", message="Rebuilding signal campaigns...", progress=10)
        result = signal_tracking_service.rebuild_signal_campaigns(
            logic_version=payload.get("logic_version"),
            side=str(payload.get("side") or "all"),
        )
    job_manager._update_db(
        job_id,
        job_type,
        "success",
        message=str(result),
        progress=100,
        finished_at=datetime.now(),
    )


def handle_signal_tracking_basis_backfill(job_id: str, payload: dict[str, Any]) -> None:
    try:
        _handle_tracking_job(job_id, SIGNAL_TRACKING_BASIS_BACKFILL_JOB_TYPE, payload)
    except Exception as exc:
        job_manager._update_db(
            job_id,
            SIGNAL_TRACKING_BASIS_BACKFILL_JOB_TYPE,
            "failed",
            error=str(exc),
            message="Signal basis rebuild failed",
            finished_at=datetime.now(),
        )
        raise


def handle_signal_tracking_decision_rebuild(job_id: str, payload: dict[str, Any]) -> None:
    try:
        _handle_tracking_job(job_id, SIGNAL_TRACKING_DECISION_REBUILD_JOB_TYPE, payload)
    except Exception as exc:
        job_manager._update_db(
            job_id,
            SIGNAL_TRACKING_DECISION_REBUILD_JOB_TYPE,
            "failed",
            error=str(exc),
            message="Signal decision rebuild failed",
            finished_at=datetime.now(),
        )
        raise


def handle_signal_tracking_campaign_rebuild(job_id: str, payload: dict[str, Any]) -> None:
    try:
        _handle_tracking_job(job_id, SIGNAL_TRACKING_CAMPAIGN_REBUILD_JOB_TYPE, payload)
    except Exception as exc:
        job_manager._update_db(
            job_id,
            SIGNAL_TRACKING_CAMPAIGN_REBUILD_JOB_TYPE,
            "failed",
            error=str(exc),
            message="Signal campaign rebuild failed",
            finished_at=datetime.now(),
        )
        raise


def handle_ranking_appearance_rebuild(job_id: str, payload: dict[str, Any]) -> None:
    try:
        _handle_tracking_job(job_id, RANKING_APPEARANCE_REBUILD_JOB_TYPE, payload)
    except Exception as exc:
        job_manager._update_db(
            job_id,
            RANKING_APPEARANCE_REBUILD_JOB_TYPE,
            "failed",
            error=str(exc),
            message="Ranking appearance rebuild failed",
            finished_at=datetime.now(),
        )
        raise
