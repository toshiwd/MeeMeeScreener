from __future__ import annotations

import os
from datetime import datetime

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.backend.services.system_status import (
    _collect_db_readiness,
    _collect_db_stats,
    _get_last_updated_timestamp,
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


@router.get("/health")
def health_check():
    # Basic health check for launcher.
    return {"status": "ok", "last_updated": _get_last_updated_timestamp()}


@router.get("/api/health")
def health():
    if not _HEALTH_LIGHT:
        return health_deep()
    now = datetime.utcnow().isoformat()
    status = get_txt_status()
    readiness = _collect_db_readiness()
    missing_tables = readiness["missing_tables"]
    errors = readiness["errors"]
    ready = not missing_tables and not errors

    if not ready:
        detail_errors = list(errors)
        if missing_tables:
            detail_errors.append(f"missing_tables:{','.join(missing_tables)}")
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "status": "starting",
                "ready": False,
                "phase": "starting",
                "message": "バックエンド起動中",
                "error_code": "BACKEND_NOT_READY",
                "version": APP_VERSION,
                "env": APP_ENV,
                "time": now,
                "retryAfterMs": 1000,
                "missing_tables": missing_tables,
                "txt_count": status.get("txt_count"),
                "last_updated": status.get("last_updated"),
                "code_txt_missing": status.get("code_txt_missing"),
                "errors": detail_errors,
            },
        )

    return {
        "ok": True,
        "status": "ok",
        "ready": True,
        "phase": "ready",
        "message": "準備完了",
        "error_code": None,
        "version": APP_VERSION,
        "env": APP_ENV,
        "time": now,
        "txt_count": status.get("txt_count"),
        "last_updated": status.get("last_updated"),
        "code_txt_missing": status.get("code_txt_missing"),
        "errors": [],
    }


@router.get("/api/health/deep")
def health_deep():
    now = datetime.utcnow().isoformat()
    status = get_txt_status()
    stats = _collect_db_stats()
    has_daily = (stats["daily_rows"] or 0) > 0
    has_monthly = (stats["monthly_rows"] or 0) > 0
    data_initialized = has_daily or has_monthly
    is_backend_ready = (not stats["missing_tables"]) and stats["errors"] == []
    status_label = "ok" if data_initialized else "degraded"
    message = "準備完了" if data_initialized else "データ未初期化（空データで起動）"

    if not is_backend_ready:
        detail_errors = list(stats["errors"])
        if stats["missing_tables"]:
            detail_errors.append(f"missing_tables:{','.join(stats['missing_tables'])}")
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "status": "starting",
                "ready": False,
                "phase": "starting",
                "message": "バックエンド起動中",
                "error_code": "BACKEND_NOT_READY",
                "version": APP_VERSION,
                "env": APP_ENV,
                "time": now,
                "retryAfterMs": 1000,
                "stats": stats,
                "txt_count": status.get("txt_count"),
                "last_updated": status.get("last_updated"),
                "code_txt_missing": status.get("code_txt_missing"),
                "errors": detail_errors,
            },
        )

    return {
        "ok": True,
        "status": status_label,
        "ready": True,
        "phase": "ready",
        "message": message,
        "error_code": None if data_initialized else "DATA_NOT_INITIALIZED",
        "version": APP_VERSION,
        "env": APP_ENV,
        "time": now,
        "stats": stats,
        "data_initialized": data_initialized,
        "txt_count": status.get("txt_count"),
        "code_count": stats.get("tickers"),
        "pan_out_txt_dir": resolve_pan_out_txt_dir(),
        "last_updated": status.get("last_updated"),
        "code_txt_missing": status.get("code_txt_missing"),
        "errors": [],
    }


@router.get("/api/diagnostics")
def diagnostics():
    now = datetime.utcnow().isoformat()
    db_path = str(config.DB_PATH)
    stats = _collect_db_stats()
    return {
        "ok": True,
        "version": APP_VERSION,
        "env": APP_ENV,
        "time": now,
        "data_dir": DATA_DIR,
        "pan_out_txt_dir": resolve_pan_out_txt_dir(),
        "db_path": db_path,
        "db_exists": os.path.isfile(db_path),
        "trade_csv_dir_env": os.getenv("TRADE_CSV_DIR"),
        "trade_csv_paths": [path for path in resolve_trade_csv_paths() if os.path.isfile(path)],
        "stats": stats,
    }
