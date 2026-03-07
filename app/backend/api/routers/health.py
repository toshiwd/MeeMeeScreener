from __future__ import annotations

import os
from datetime import datetime, timezone

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.backend.services.system_status import (
    _collect_db_readiness,
    _collect_db_stats,
    _get_last_updated_timestamp,
    get_readiness_state,
)
from app.backend.services.txt_update import get_txt_status
from app.core.config import (
    APP_ENV,
    APP_VERSION,
    DATA_DIR,
    config,
    resolve_pan_out_txt_dir,
    resolve_trade_csv_paths,
)

router = APIRouter()
_HEALTH_LIGHT = os.getenv("HEALTH_LIGHT", "1").lower() in {"1", "true", "yes", "on"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _health_payload(
    *,
    ok: bool,
    status: str,
    ready: bool,
    phase: str,
    message: str,
    errors: list[str],
    retry_after_ms: int | None,
    extra: dict | None = None,
) -> dict:
    payload = {
        "ok": bool(ok),
        "status": status,
        "ready": bool(ready),
        "phase": phase,
        "message": message,
        "error_code": None if ok else "BACKEND_NOT_READY",
        "version": APP_VERSION,
        "env": APP_ENV,
        "time": _utc_now_iso(),
        "retryAfterMs": retry_after_ms,
        "errors": errors,
    }
    if extra:
        payload.update(extra)
    return payload


@router.get("/health")
def health_check():
    # Liveness endpoint for process-level checks.
    return _health_payload(
        ok=True,
        status="ok",
        ready=True,
        phase="alive",
        message="alive",
        errors=[],
        retry_after_ms=None,
        extra={"last_updated": _get_last_updated_timestamp()},
    )


@router.get("/api/health")
def health():
    if not _HEALTH_LIGHT:
        return health_deep()

    txt_status = get_txt_status()
    readiness = _collect_db_readiness()
    missing_tables = list(readiness.get("missing_tables") or [])
    errors = list(readiness.get("errors") or [])
    readiness_state = dict(readiness.get("readiness_state") or get_readiness_state() or {})
    transient_db_busy = (
        bool(readiness.get("db_retryable"))
        and not missing_tables
        and bool(errors)
        and bool(readiness_state.get("boot_ready") or readiness_state.get("db_ready"))
    )
    ready = (not missing_tables and not errors) or transient_db_busy

    detail_errors = list(errors)
    if missing_tables:
        detail_errors.append(f"missing_tables:{','.join(missing_tables)}")

    payload = _health_payload(
        ok=ready,
        status="degraded" if transient_db_busy else "ok" if ready else "starting",
        ready=ready,
        phase="ready" if ready else "starting",
        message="backend ready (database busy)" if transient_db_busy else "ready" if ready else "backend is starting",
        errors=detail_errors if (not ready or transient_db_busy) else [],
        retry_after_ms=None if ready else 1000,
        extra={
            "missing_tables": missing_tables,
            "db_retryable": bool(readiness.get("db_retryable")),
            "db_connect_stats": readiness.get("db_connect_stats"),
            "readiness_state": readiness_state,
            "txt_count": txt_status.get("txt_count"),
            "last_updated": txt_status.get("last_updated"),
            "code_txt_missing": txt_status.get("code_txt_missing"),
        },
    )
    if ready:
        return payload
    return JSONResponse(status_code=503, content=payload)


@router.get("/api/health/deep")
def health_deep():
    txt_status = get_txt_status()
    stats = _collect_db_stats()
    missing_tables = list(stats.get("missing_tables") or [])
    errors = list(stats.get("errors") or [])
    backend_ready = not missing_tables and not errors
    has_daily = (stats.get("daily_rows") or 0) > 0
    has_monthly = (stats.get("monthly_rows") or 0) > 0
    data_initialized = bool(has_daily or has_monthly)

    detail_errors = list(errors)
    if missing_tables:
        detail_errors.append(f"missing_tables:{','.join(missing_tables)}")

    payload = _health_payload(
        ok=backend_ready,
        status="ok" if data_initialized else "degraded",
        ready=backend_ready,
        phase="ready" if backend_ready else "starting",
        message=(
            "ready"
            if backend_ready and data_initialized
            else "data is not initialized yet"
            if backend_ready
            else "backend is starting"
        ),
        errors=[] if backend_ready else detail_errors,
        retry_after_ms=None if backend_ready else 1000,
        extra={
            "stats": stats,
            "data_initialized": data_initialized,
            "txt_count": txt_status.get("txt_count"),
            "code_count": stats.get("tickers"),
            "pan_out_txt_dir": resolve_pan_out_txt_dir(),
            "last_updated": txt_status.get("last_updated"),
            "code_txt_missing": txt_status.get("code_txt_missing"),
        },
    )
    if backend_ready:
        if not data_initialized:
            payload["error_code"] = "DATA_NOT_INITIALIZED"
        return payload
    return JSONResponse(status_code=503, content=payload)


@router.get("/api/diagnostics")
def diagnostics():
    db_path = str(config.DB_PATH)
    stats = _collect_db_stats()
    return {
        "ok": True,
        "version": APP_VERSION,
        "env": APP_ENV,
        "time": _utc_now_iso(),
        "data_dir": DATA_DIR,
        "pan_out_txt_dir": resolve_pan_out_txt_dir(),
        "db_path": db_path,
        "db_exists": os.path.isfile(db_path),
        "trade_csv_dir_env": os.getenv("TRADE_CSV_DIR"),
        "trade_csv_paths": [path for path in resolve_trade_csv_paths() if os.path.isfile(path)],
        "stats": stats,
        "db_retryable": bool(stats.get("db_retryable")),
        "db_connect_stats": stats.get("db_connect_stats"),
    }
