from __future__ import annotations

import logging

import os

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.backend.core.jobs import cleanup_stale_jobs, job_manager
from app.backend.core.config import config
from app.backend.services import ml_service, strategy_backtest_service
from app.db.session import get_conn

router = APIRouter()
logger = logging.getLogger(__name__)


def _count_active_jobs(job_type: str) -> int:
    with get_conn() as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM sys_jobs WHERE type = ? AND status IN ('queued', 'running', 'cancel_requested')",
            [job_type],
        ).fetchone()[0]


def _submit_job(job_type: str, payload: dict | None = None):
    cleanup_stale_jobs()
    if _count_active_jobs(job_type) > 0:
        return JSONResponse(status_code=409, content={"error": "Job already running"})
    job_id = job_manager.submit(job_type, payload or {})
    return {"ok": True, "job_id": job_id}


@router.post("/api/jobs/txt-update")
def submit_txt_update(
    auto_ml_predict: bool = True,
    auto_ml_train: bool = False,
):
    try:
        return _submit_job(
            "txt_update",
            {
                "auto_ml_predict": bool(auto_ml_predict),
                "auto_ml_train": bool(auto_ml_train),
            },
        )
    except Exception as exc:
        logger.exception("Error submitting txt_update: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/jobs/force-sync")
def submit_force_sync():
    try:
        return _submit_job("force_sync")
    except Exception as exc:
        logger.exception("Error submitting force_sync: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/phase/rebuild")
def submit_phase_rebuild():
    try:
        return _submit_job("phase_rebuild")
    except Exception as exc:
        logger.exception("Error submitting phase_rebuild: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/jobs/ml/train")
def submit_ml_train(
    start_dt: int | None = None,
    end_dt: int | None = None,
    dry_run: bool = False,
):
    try:
        return _submit_job(
            "ml_train",
            {
                "start_dt": start_dt,
                "end_dt": end_dt,
                "dry_run": dry_run,
            },
        )
    except Exception as exc:
        logger.exception("Error submitting ml_train: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/jobs/ml/predict")
def submit_ml_predict(dt: int | None = None):
    try:
        return _submit_job("ml_predict", {"dt": dt})
    except Exception as exc:
        logger.exception("Error submitting ml_predict: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/jobs/ml/status")
def get_ml_job_status():
    try:
        return ml_service.get_ml_status()
    except Exception as exc:
        logger.exception("Error fetching ml status: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/jobs/strategy/backtest")
def submit_strategy_backtest(
    start_dt: int | None = None,
    end_dt: int | None = None,
    max_codes: int | None = 500,
    dry_run: bool = False,
    max_positions: int | None = None,
    initial_units: int | None = None,
    add1_units: int | None = None,
    add2_units: int | None = None,
    hedge_units: int | None = None,
    min_hedge_ratio: float | None = None,
    cost_bps: float | None = None,
    min_history_bars: int | None = None,
    prefer_net_short_ratio: float | None = None,
    event_lookback_days: int | None = None,
    event_lookahead_days: int | None = None,
    min_long_score: float | None = None,
    min_short_score: float | None = None,
    max_new_entries_per_day: int | None = None,
    max_new_entries_per_month: int | None = None,
    allowed_sides: str | None = None,
    require_decision_for_long: bool | None = None,
    require_ma_bull_stack_long: bool | None = None,
    max_dist_ma20_long: float | None = None,
    min_volume_ratio_long: float | None = None,
    max_atr_pct_long: float | None = None,
    min_ml_p_up_long: float | None = None,
    allowed_long_setups: str | None = None,
    allowed_short_setups: str | None = None,
    use_regime_filter: bool | None = None,
    regime_breadth_lookback_days: int | None = None,
    regime_long_min_breadth_above60: float | None = None,
    regime_short_max_breadth_above60: float | None = None,
):
    try:
        config_payload = {
            "max_positions": max_positions,
            "initial_units": initial_units,
            "add1_units": add1_units,
            "add2_units": add2_units,
            "hedge_units": hedge_units,
            "min_hedge_ratio": min_hedge_ratio,
            "cost_bps": cost_bps,
            "min_history_bars": min_history_bars,
            "prefer_net_short_ratio": prefer_net_short_ratio,
            "event_lookback_days": event_lookback_days,
            "event_lookahead_days": event_lookahead_days,
            "min_long_score": min_long_score,
            "min_short_score": min_short_score,
            "max_new_entries_per_day": max_new_entries_per_day,
            "max_new_entries_per_month": max_new_entries_per_month,
            "allowed_sides": allowed_sides,
            "require_decision_for_long": require_decision_for_long,
            "require_ma_bull_stack_long": require_ma_bull_stack_long,
            "max_dist_ma20_long": max_dist_ma20_long,
            "min_volume_ratio_long": min_volume_ratio_long,
            "max_atr_pct_long": max_atr_pct_long,
            "min_ml_p_up_long": min_ml_p_up_long,
            "allowed_long_setups": allowed_long_setups,
            "allowed_short_setups": allowed_short_setups,
            "use_regime_filter": use_regime_filter,
            "regime_breadth_lookback_days": regime_breadth_lookback_days,
            "regime_long_min_breadth_above60": regime_long_min_breadth_above60,
            "regime_short_max_breadth_above60": regime_short_max_breadth_above60,
        }
        return _submit_job(
            "strategy_backtest",
            {
                "start_dt": start_dt,
                "end_dt": end_dt,
                "max_codes": max_codes,
                "dry_run": dry_run,
                "config": {k: v for k, v in config_payload.items() if v is not None},
            },
        )
    except Exception as exc:
        logger.exception("Error submitting strategy_backtest: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/jobs/strategy/backtest/latest")
def get_strategy_backtest_latest():
    try:
        return strategy_backtest_service.get_latest_strategy_backtest()
    except Exception as exc:
        logger.exception("Error fetching strategy backtest status: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/txt_update/run")
def run_txt_update_legacy():
    cleanup_stale_jobs()
    if _count_active_jobs("txt_update") > 0:
        return JSONResponse(status_code=409, content={"ok": False, "error": "update_in_progress"})

    code_path = os.path.abspath(str(config.PAN_CODE_TXT_PATH))
    if not os.path.isfile(code_path):
        return JSONResponse(status_code=400, content={"ok": False, "error": "code_txt_missing"})

    vbs_path = os.path.abspath(str(config.PAN_EXPORT_VBS_PATH))
    if not os.path.isfile(vbs_path):
        return JSONResponse(
            status_code=500, content={"ok": False, "error": f"vbs_not_found:{vbs_path}"}
        )

    job_id = job_manager.submit("txt_update", {})
    if not job_id:
        return JSONResponse(status_code=409, content={"ok": False, "error": "update_in_progress"})

    return {"ok": True, "started": True, "job_id": job_id}


@router.get("/api/jobs/current")
def get_current_job():
    cleanup_stale_jobs()
    try:
        with get_conn() as conn:
            row = conn.execute(
                "SELECT id, type, status, created_at, started_at, progress, message "
                "FROM sys_jobs WHERE status IN ('running', 'cancel_requested') ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
            if not row:
                return JSONResponse(content=None)
            return {
                "id": row[0],
                "type": row[1],
                "status": row[2],
                "created_at": row[3],
                "started_at": row[4],
                "progress": row[5],
                "message": row[6],
            }
    except Exception as exc:
        logger.exception("Error current job: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/jobs/history")
def get_job_history(limit: int = 20):
    return job_manager.get_history(limit)


@router.get("/api/jobs/{job_id}")
def get_job_status(job_id: str):
    status = job_manager.get_status(job_id)
    if not status:
        return JSONResponse(status_code=404, content={"error": "Not Found"})
    return status


@router.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str):
    success = job_manager.cancel(job_id)
    return {"id": job_id, "cancel_requested": success}
