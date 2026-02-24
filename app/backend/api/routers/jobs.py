from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse

from app.backend.core.jobs import cleanup_stale_jobs, job_manager
from app.backend.core.config import config
from app.backend.services import ml_service, strategy_backtest_service
from app.db.session import get_conn

router = APIRouter()
logger = logging.getLogger(__name__)
TXT_UPDATE_JOB_TYPE = "txt_update"
ACTIVE_JOB_STATUSES = ("queued", "running", "cancel_requested")
TXT_UPDATE_SUCCESSOR_ENDPOINT = "/api/jobs/txt-update"
TXT_UPDATE_SUNSET_HTTP_DATE = "Tue, 30 Jun 2026 00:00:00 GMT"
TXT_UPDATE_DEPRECATION_DOC = "/docs/TXT_UPDATE_RUNBOOK.md"
TXT_UPDATE_DISABLE_LEGACY_ENV = "MEEMEE_DISABLE_LEGACY_TXT_UPDATE_ENDPOINTS"


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


def _txt_update_conflict_response(*, source: str, legacy_endpoint: str | None = None) -> JSONResponse:
    payload: dict[str, object] = {
        "ok": False,
        "started": False,
        "status": "conflict",
        "error": "update_in_progress",
        "message": "TXT update is already active",
        "type": TXT_UPDATE_JOB_TYPE,
        "active_statuses": list(ACTIVE_JOB_STATUSES),
        "source": source,
    }
    if legacy_endpoint:
        payload["deprecated_endpoint"] = legacy_endpoint
    return JSONResponse(status_code=409, content=payload)


def _txt_update_missing_code_response(*, source: str, legacy_endpoint: str | None = None) -> JSONResponse:
    payload: dict[str, object] = {
        "ok": False,
        "started": False,
        "status": "invalid_request",
        "error": "code_txt_missing",
        "message": "code.txt is missing",
        "type": TXT_UPDATE_JOB_TYPE,
        "source": source,
    }
    if legacy_endpoint:
        payload["deprecated_endpoint"] = legacy_endpoint
    return JSONResponse(status_code=400, content=payload)


def _txt_update_missing_vbs_response(
    *, source: str, vbs_path: str, legacy_endpoint: str | None = None
) -> JSONResponse:
    payload: dict[str, object] = {
        "ok": False,
        "started": False,
        "status": "invalid_request",
        "error": f"vbs_not_found:{vbs_path}",
        "message": "TXT update script is missing",
        "type": TXT_UPDATE_JOB_TYPE,
        "source": source,
    }
    if legacy_endpoint:
        payload["deprecated_endpoint"] = legacy_endpoint
    return JSONResponse(status_code=500, content=payload)


def _txt_update_submit_response(
    *,
    job_id: str,
    source: str,
    legacy_endpoint: str | None = None,
    payload: dict | None = None,
) -> dict[str, object]:
    response_payload: dict[str, object] = {
        "ok": True,
        "started": True,
        "status": "accepted",
        "message": "TXT update job started",
        "type": TXT_UPDATE_JOB_TYPE,
        "state": "queued",
        "job_id": job_id,
        "jobId": job_id,
        "source": source,
    }
    if payload:
        response_payload["request"] = payload
    if legacy_endpoint:
        response_payload["deprecated_endpoint"] = legacy_endpoint
    return response_payload


def _maybe_apply_legacy_headers(
    response_or_payload: JSONResponse | dict[str, object],
    *,
    legacy_endpoint: str | None,
) -> JSONResponse | dict[str, object]:
    if not legacy_endpoint:
        return response_or_payload

    response = (
        response_or_payload
        if isinstance(response_or_payload, JSONResponse)
        else JSONResponse(status_code=200, content=response_or_payload)
    )
    response.headers["Deprecation"] = "true"
    response.headers["Sunset"] = TXT_UPDATE_SUNSET_HTTP_DATE
    response.headers["Warning"] = (
        f'299 - "Deprecated API endpoint: {legacy_endpoint}. '
        f'Use {TXT_UPDATE_SUCCESSOR_ENDPOINT} instead."'
    )
    response.headers["Link"] = (
        f'<{TXT_UPDATE_SUCCESSOR_ENDPOINT}>; rel="successor-version", '
        f'<{TXT_UPDATE_DEPRECATION_DOC}>; rel="deprecation"'
    )
    return response


def _is_truthy_env(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _coerce_bool(value: object, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _legacy_endpoint_disabled() -> bool:
    return _is_truthy_env(os.getenv(TXT_UPDATE_DISABLE_LEGACY_ENV))


def _legacy_endpoint_sunset_reached(now: datetime | None = None) -> bool:
    try:
        sunset_dt = parsedate_to_datetime(TXT_UPDATE_SUNSET_HTTP_DATE)
    except (TypeError, ValueError):
        return False
    if sunset_dt.tzinfo is None:
        sunset_dt = sunset_dt.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current >= sunset_dt


def _legacy_endpoint_gone_response(*, source: str, legacy_endpoint: str) -> JSONResponse:
    payload: dict[str, object] = {
        "ok": False,
        "started": False,
        "status": "gone",
        "error": "legacy_endpoint_removed",
        "message": "Deprecated endpoint is disabled. Use /api/jobs/txt-update.",
        "type": TXT_UPDATE_JOB_TYPE,
        "source": source,
        "deprecated_endpoint": legacy_endpoint,
        "successor_endpoint": TXT_UPDATE_SUCCESSOR_ENDPOINT,
    }
    response = JSONResponse(status_code=410, content=payload)
    response.headers["Deprecation"] = "true"
    response.headers["Sunset"] = TXT_UPDATE_SUNSET_HTTP_DATE
    response.headers["Warning"] = (
        f'299 - "Deprecated API endpoint disabled: {legacy_endpoint}. '
        f'Use {TXT_UPDATE_SUCCESSOR_ENDPOINT} instead."'
    )
    response.headers["Link"] = (
        f'<{TXT_UPDATE_SUCCESSOR_ENDPOINT}>; rel="successor-version", '
        f'<{TXT_UPDATE_DEPRECATION_DOC}>; rel="deprecation"'
    )
    return response


def submit_txt_update_job(
    payload: dict | None = None,
    *,
    source: str,
    legacy_endpoint: str | None = None,
):
    if legacy_endpoint and (_legacy_endpoint_disabled() or _legacy_endpoint_sunset_reached()):
        return _legacy_endpoint_gone_response(source=source, legacy_endpoint=legacy_endpoint)

    code_path = os.path.abspath(str(config.PAN_CODE_TXT_PATH))
    if not os.path.isfile(code_path):
        return _maybe_apply_legacy_headers(
            _txt_update_missing_code_response(source=source, legacy_endpoint=legacy_endpoint),
            legacy_endpoint=legacy_endpoint,
        )

    vbs_path = os.path.abspath(str(config.PAN_EXPORT_VBS_PATH))
    if not os.path.isfile(vbs_path):
        return _maybe_apply_legacy_headers(
            _txt_update_missing_vbs_response(
                source=source,
                vbs_path=vbs_path,
                legacy_endpoint=legacy_endpoint,
            ),
            legacy_endpoint=legacy_endpoint,
        )

    cleanup_stale_jobs()
    if _count_active_jobs(TXT_UPDATE_JOB_TYPE) > 0:
        return _maybe_apply_legacy_headers(
            _txt_update_conflict_response(source=source, legacy_endpoint=legacy_endpoint),
            legacy_endpoint=legacy_endpoint,
        )

    job_id = job_manager.submit(TXT_UPDATE_JOB_TYPE, payload or {}, unique=True)
    if not job_id:
        return _maybe_apply_legacy_headers(
            _txt_update_conflict_response(source=source, legacy_endpoint=legacy_endpoint),
            legacy_endpoint=legacy_endpoint,
        )

    return _maybe_apply_legacy_headers(
        _txt_update_submit_response(
            job_id=job_id,
            source=source,
            legacy_endpoint=legacy_endpoint,
            payload=payload,
        ),
        legacy_endpoint=legacy_endpoint,
    )


@router.post("/api/jobs/txt-update")
def submit_txt_update(
    auto_ml_predict: bool = True,
    auto_ml_train: bool = True,
    auto_fill_missing_history: bool = True,
    backfill_lookback_days: int = 130,
    backfill_max_missing_days: int = 260,
):
    try:
        request_payload = {
            "auto_ml_predict": bool(auto_ml_predict),
            "auto_ml_train": bool(auto_ml_train),
            "auto_fill_missing_history": bool(auto_fill_missing_history),
            "backfill_lookback_days": int(backfill_lookback_days),
            "backfill_max_missing_days": int(backfill_max_missing_days),
        }
        return submit_txt_update_job(
            request_payload,
            source="/api/jobs/txt-update",
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


@router.post("/api/jobs/analysis/backfill-missing")
def submit_analysis_backfill(
    lookback_days: int = 130,
    max_missing_days: int | None = None,
    include_sell: bool = True,
    include_phase: bool = False,
    anchor_dt: int | None = None,
):
    try:
        return _submit_job(
            "analysis_backfill",
            {
                "lookback_days": int(lookback_days),
                "max_missing_days": int(max_missing_days) if max_missing_days is not None else None,
                "include_sell": bool(include_sell),
                "include_phase": bool(include_phase),
                "anchor_dt": int(anchor_dt) if anchor_dt is not None else None,
            },
        )
    except Exception as exc:
        logger.exception("Error submitting analysis_backfill: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/jobs/ml/live-guard")
def submit_ml_live_guard():
    try:
        return _submit_job("ml_live_guard")
    except Exception as exc:
        logger.exception("Error submitting ml_live_guard: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/jobs/ml/status")
def get_ml_job_status():
    try:
        return ml_service.get_ml_status()
    except Exception as exc:
        logger.exception("Error fetching ml status: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/jobs/ml/live-guard/latest")
def get_ml_live_guard_latest():
    try:
        return ml_service.get_latest_live_guard_status()
    except Exception as exc:
        logger.exception("Error fetching ml live guard status: %s", exc)
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


@router.post("/api/jobs/toredex/live")
def submit_toredex_live(
    season_id: str | None = None,
    asOf: str | None = None,
    dry_run: bool | None = None,
    payload: dict | None = Body(default=None),
):
    try:
        body = payload if isinstance(payload, dict) else {}
        season_text = str(
            season_id
            or body.get("season_id")
            or body.get("seasonId")
            or ""
        ).strip()
        if not season_text:
            return JSONResponse(status_code=400, content={"error": "season_id is required"})
        resolved_as_of = asOf if asOf is not None else body.get("asOf") or body.get("as_of")
        resolved_dry_run = (
            bool(dry_run)
            if dry_run is not None
            else _coerce_bool(body.get("dry_run", body.get("dryRun")), default=False)
        )
        return _submit_job(
            "toredex_live",
            {
                "season_id": season_text,
                "asOf": resolved_as_of,
                "dry_run": resolved_dry_run,
            },
        )
    except Exception as exc:
        logger.exception("Error submitting toredex_live: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/jobs/strategy/walkforward")
def submit_strategy_walkforward(
    start_dt: int | None = None,
    end_dt: int | None = None,
    max_codes: int | None = 500,
    dry_run: bool = False,
    train_months: int = 24,
    test_months: int = 3,
    step_months: int = 1,
    min_windows: int = 1,
    min_long_score: float | None = None,
    min_short_score: float | None = None,
    min_ml_p_up_long: float | None = None,
    max_new_entries_per_day: int | None = None,
    max_new_entries_per_month: int | None = None,
    allowed_sides: str | None = None,
    allowed_long_setups: str | None = None,
    allowed_short_setups: str | None = None,
    use_regime_filter: bool | None = None,
    regime_breadth_lookback_days: int | None = None,
    regime_long_min_breadth_above60: float | None = None,
    regime_short_max_breadth_above60: float | None = None,
):
    try:
        config_payload = {
            "min_long_score": min_long_score,
            "min_short_score": min_short_score,
            "min_ml_p_up_long": min_ml_p_up_long,
            "max_new_entries_per_day": max_new_entries_per_day,
            "max_new_entries_per_month": max_new_entries_per_month,
            "allowed_sides": allowed_sides,
            "allowed_long_setups": allowed_long_setups,
            "allowed_short_setups": allowed_short_setups,
            "use_regime_filter": use_regime_filter,
            "regime_breadth_lookback_days": regime_breadth_lookback_days,
            "regime_long_min_breadth_above60": regime_long_min_breadth_above60,
            "regime_short_max_breadth_above60": regime_short_max_breadth_above60,
        }
        return _submit_job(
            "strategy_walkforward",
            {
                "start_dt": start_dt,
                "end_dt": end_dt,
                "max_codes": max_codes,
                "dry_run": dry_run,
                "train_months": train_months,
                "test_months": test_months,
                "step_months": step_months,
                "min_windows": min_windows,
                "config": {k: v for k, v in config_payload.items() if v is not None},
            },
        )
    except Exception as exc:
        logger.exception("Error submitting strategy_walkforward: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.get("/api/jobs/strategy/walkforward/latest")
def get_strategy_walkforward_latest():
    try:
        return strategy_backtest_service.get_latest_strategy_walkforward()
    except Exception as exc:
        logger.exception("Error fetching strategy walkforward status: %s", exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})


@router.post("/api/txt_update/run")
def run_txt_update_legacy():
    return submit_txt_update_job(
        {},
        source="/api/txt_update/run",
        legacy_endpoint="/api/txt_update/run",
    )


@router.get("/api/jobs/current")
def get_current_job():
    try:
        with get_conn() as conn:
            row = conn.execute(
                "SELECT id, type, status, created_at, started_at, progress, message "
                "FROM sys_jobs WHERE status IN ('queued', 'running', 'cancel_requested') "
                "ORDER BY COALESCE(started_at, created_at) DESC LIMIT 1"
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
    status = job_manager.get_status(job_id)
    return {"id": job_id, "cancel_requested": success, "status": status["status"] if status else None}
