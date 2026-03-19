import logging
import os
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from app.backend.api.dependencies import get_config_repo
from app.backend.core import config as backend_config
from app.backend.core.config import write_data_dir_override
from app.backend.api.routers.jobs import submit_txt_update_job
from app.backend.infra.files.config_repo import ConfigRepository
from app.backend.services import strategy_backtest_service
from app.backend.services.runtime_selection_service import (
    build_runtime_selection_snapshot,
    clear_selected_logic_override,
    set_selected_logic_override,
    validate_selected_logic_override,
)
from app.backend.infra.files.config_repo import LOGIC_SELECTION_SCHEMA_VERSION

router = APIRouter(prefix="/api/system", tags=["system"])
logger = logging.getLogger(__name__)


class DataDirPayload(BaseModel):
    dataDir: str


class RuntimeSelectionOverridePayload(BaseModel):
    selectedLogicOverride: str | None = None
    reason: str | None = None


class RuntimeSelectionOverrideClearPayload(BaseModel):
    reason: str | None = None


@router.post("/update_data")
def trigger_update_data():
    return submit_txt_update_job(
        {},
        source="/api/system/update_data",
        legacy_endpoint="/api/system/update_data",
    )


@router.get("/data-dir")
def get_data_dir():
    current = backend_config.config.DATA_DIR
    return {
        "dataDir": str(current),
        "source": "env" if os.getenv("MEEMEE_DATA_DIR") else "config"
    }


@router.post("/data-dir")
def set_data_dir(payload: DataDirPayload):
    target = Path(payload.dataDir).expanduser().resolve()
    if not target:
        raise HTTPException(status_code=400, detail="dataDir is required")
    config_path = write_data_dir_override(target)
    os.environ["MEEMEE_DATA_DIR"] = str(target)
    return {
        "dataDir": str(target),
        "configPath": str(config_path),
        "restartRequired": True,
        "message": "Data directory override saved; restart the app for changes to fully apply."
    }


@router.get("/runtime-selection")
def get_runtime_selection(
    request: Request,
    config: ConfigRepository = Depends(get_config_repo),
):
    snapshot = build_runtime_selection_snapshot(
        config_repo=config,
        db_path=os.getenv("MEEMEE_RESULT_DB_PATH"),
    )
    request.app.state.runtime_selection_snapshot = snapshot
    return snapshot


@router.post("/runtime-selection/override")
def set_runtime_selection_override(
    payload: RuntimeSelectionOverridePayload,
    request: Request,
    config: ConfigRepository = Depends(get_config_repo),
):
    selected = str(payload.selectedLogicOverride or "").strip() or None
    validation = validate_selected_logic_override(
        config_repo=config,
        selected_logic_override=selected,
    )
    if not validation.get("ok"):
        raise HTTPException(
            status_code=400,
            detail={
                "ok": False,
                "reason": validation.get("reason"),
                "logic_key": validation.get("logic_key"),
            },
        )
    result = set_selected_logic_override(
        config_repo=config,
        selected_logic_override=str(validation["logic_key"]),
        source="api.system.runtime-selection.override",
        reason=payload.reason,
        db_path=os.getenv("MEEMEE_RESULT_DB_PATH"),
    )
    snapshot = result.get("snapshot") or build_runtime_selection_snapshot(
        config_repo=config,
        db_path=os.getenv("MEEMEE_RESULT_DB_PATH"),
    )
    request.app.state.runtime_selection_snapshot = snapshot
    return {
        "ok": True,
        "schema_version": LOGIC_SELECTION_SCHEMA_VERSION,
        "selected_logic_override": result.get("validation", {}).get("logic_key"),
        "validation": result.get("validation"),
        "snapshot": snapshot,
    }


@router.post("/runtime-selection/override/clear")
def clear_runtime_selection_override(
    payload: RuntimeSelectionOverrideClearPayload,
    request: Request,
    config: ConfigRepository = Depends(get_config_repo),
):
    result = clear_selected_logic_override(
        config_repo=config,
        source="api.system.runtime-selection.override.clear",
        reason=payload.reason,
        db_path=os.getenv("MEEMEE_RESULT_DB_PATH"),
    )
    snapshot = result.get("snapshot") or build_runtime_selection_snapshot(
        config_repo=config,
        db_path=os.getenv("MEEMEE_RESULT_DB_PATH"),
    )
    request.app.state.runtime_selection_snapshot = snapshot
    return {
        "ok": True,
        "schema_version": LOGIC_SELECTION_SCHEMA_VERSION,
        "snapshot": snapshot,
    }

@router.get("/status")
def get_system_status(config: ConfigRepository = Depends(get_config_repo)):
    state = config.load_update_state()
    walkforward_run = {
        "at": state.get("last_walkforward_run_at"),
        "month_key": state.get("last_walkforward_run_month_key"),
        "run_id": state.get("last_walkforward_run_run_id"),
        "summary": state.get("last_walkforward_run_summary"),
        "error": state.get("last_walkforward_run_error"),
        "error_at": state.get("last_walkforward_run_error_at"),
        "skipped_reason": state.get("last_walkforward_run_skipped_reason"),
        "skipped_at": state.get("last_walkforward_run_skipped_at"),
    }
    walkforward_gate = {
        "at": state.get("last_walkforward_gate_at"),
        "month_key": state.get("last_walkforward_gate_month_key"),
        "gate_id": state.get("last_walkforward_gate_gate_id"),
        "status": state.get("last_walkforward_gate_status"),
        "passed": state.get("last_walkforward_gate_passed"),
        "source_run_id": state.get("last_walkforward_gate_source_run_id"),
        "source_finished_at": state.get("last_walkforward_gate_source_finished_at"),
        "thresholds": state.get("last_walkforward_gate_thresholds"),
        "error": state.get("last_walkforward_gate_error"),
        "error_at": state.get("last_walkforward_gate_error_at"),
        "skipped_reason": state.get("last_walkforward_gate_skipped_reason"),
        "skipped_at": state.get("last_walkforward_gate_skipped_at"),
    }
    db_walkforward: dict | None = None
    db_walkforward_gate: dict | None = None
    db_status_error: str | None = None
    try:
        db_walkforward = strategy_backtest_service.get_latest_strategy_walkforward()
        db_walkforward_gate = strategy_backtest_service.get_latest_strategy_walkforward_gate()
    except Exception as exc:
        logger.exception("Failed to fetch latest walkforward status from DB: %s", exc)
        db_status_error = str(exc)
    return {
        "last_update": state.get("last_txt_update_at"),
        "version": "2.0.0-clean-arch",
        "pipeline": {
            "status": state.get("last_pipeline_status"),
            "stage": state.get("last_pipeline_stage"),
            "stage_status": state.get("last_pipeline_stage_status"),
            "stage_at": state.get("last_pipeline_stage_at"),
            "message": state.get("last_pipeline_message"),
            "started_at": state.get("last_pipeline_started_at"),
            "finished_at": state.get("last_pipeline_finished_at"),
        },
        "walkforward_run": walkforward_run,
        "walkforward_gate": walkforward_gate,
        "walkforward_db": {
            "status_error": db_status_error,
            "walkforward": db_walkforward,
            "walkforward_gate": db_walkforward_gate,
        },
    }
