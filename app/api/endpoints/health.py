from __future__ import annotations

import os
from datetime import datetime

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.core.config import config, resolve_trade_csv_paths

router = APIRouter()


@router.get("/health")
def health_check():
    # Basic health check for launcher
    from app.backend import main as main_module

    return {"status": "ok", "last_updated": main_module._get_last_updated_timestamp()}


@router.get("/api/health")
def health():
    from app.backend import main as main_module

    now = datetime.utcnow().isoformat()
    status = main_module.get_txt_status()
    stats = main_module._collect_db_stats()
    has_daily = (stats["daily_rows"] or 0) > 0
    has_monthly = (stats["monthly_rows"] or 0) > 0
    is_data_ready = (
        not stats["missing_tables"]
        and stats["errors"] == []
        and (has_daily or has_monthly)
    )
    if not is_data_ready:
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "status": "starting",
                "ready": False,
                "phase": "starting",
                "message": "起動中",
                "error_code": "DATA_NOT_INITIALIZED",
                "version": main_module.APP_VERSION,
                "env": main_module.APP_ENV,
                "time": now,
                "retryAfterMs": 1000,
                "stats": stats,
                "txt_count": status.get("txt_count"),
                "last_updated": status.get("last_updated"),
                "code_txt_missing": status.get("code_txt_missing"),
                "errors": stats["errors"] + [f"missing_tables:{','.join(stats['missing_tables'])}"]
                if stats["missing_tables"]
                else stats["errors"],
            },
        )
    return {
        "ok": True,
        "status": "ok",
        "ready": True,
        "phase": "ready",
        "message": "準備完了",
        "version": main_module.APP_VERSION,
        "env": main_module.APP_ENV,
        "time": now,
        "stats": {"tickers": stats["tickers"], "daily_rows": stats["daily_rows"], "monthly_rows": stats["monthly_rows"]},
        "txt_count": status.get("txt_count"),
        "code_count": stats["tickers"],
        "pan_out_txt_dir": main_module._resolve_pan_out_txt_dir(),
        "last_updated": status.get("last_updated"),
        "code_txt_missing": status.get("code_txt_missing"),
        "errors": [],
    }


@router.get("/api/diagnostics")
def diagnostics():
    from app.backend import main as main_module

    now = datetime.utcnow().isoformat()
    db_path = str(config.DB_PATH)
    stats = main_module._collect_db_stats()
    return {
        "ok": True,
        "version": main_module.APP_VERSION,
        "env": main_module.APP_ENV,
        "time": now,
        "data_dir": main_module.DATA_DIR,
        "pan_out_txt_dir": main_module._resolve_pan_out_txt_dir(),
        "db_path": db_path,
        "db_exists": os.path.isfile(db_path),
        "trade_csv_dir_env": os.getenv("TRADE_CSV_DIR"),
        "trade_csv_paths": [path for path in resolve_trade_csv_paths() if os.path.isfile(path)],
        "stats": stats,
    }
