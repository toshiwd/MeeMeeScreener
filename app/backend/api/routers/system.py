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
from app.backend.services.publish_promotion_service import (
    build_publish_promotion_snapshot,
    approve_publish_candidate_bundle,
    enqueue_challenger_logic_key,
    demote_logic_key,
    reject_publish_candidate_bundle,
    promote_logic_key,
    retire_challenger_logic_key,
    rollback_logic_key,
)
from external_analysis.results.publish_candidates import (
    backfill_publish_candidate_bundles,
    list_publish_candidate_bundles,
    load_publish_candidate_bundle,
    sweep_publish_candidate_snapshots,
)
from app.backend.services.publish_registry_sync_service import normalize_publish_registry_mirror
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


class PublishPromotionPayload(BaseModel):
    logicKey: str | None = None
    reason: str | None = None
    actor: str | None = None


class PublishRollbackPayload(BaseModel):
    logicKey: str | None = None
    reason: str | None = None
    actor: str | None = None


class PublishChallengerPayload(BaseModel):
    logicKey: str | None = None
    reason: str | None = None
    actor: str | None = None


class PublishCandidateActionPayload(BaseModel):
    reason: str | None = None
    actor: str | None = None


class PublishMirrorRepairPayload(BaseModel):
    reason: str | None = None
    actor: str | None = None


class PublishMaintenancePayload(BaseModel):
    dryRun: bool = False
    limit: int | None = None
    keepApprovedDays: int | None = None
    keepRejectedDays: int | None = None
    keepRetiredDays: int | None = None
    reason: str | None = None
    actor: str | None = None


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
    result_db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
    snapshot = build_runtime_selection_snapshot(
        config_repo=config,
        db_path=result_db_path,
    )
    request.app.state.runtime_selection_snapshot = snapshot
    return snapshot


@router.post("/runtime-selection/override")
def set_runtime_selection_override(
    payload: RuntimeSelectionOverridePayload,
    request: Request,
    config: ConfigRepository = Depends(get_config_repo),
):
    result_db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
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
        db_path=result_db_path,
    )
    snapshot = result.get("snapshot") or build_runtime_selection_snapshot(
        config_repo=config,
        db_path=result_db_path,
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
    result_db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
    result = clear_selected_logic_override(
        config_repo=config,
        source="api.system.runtime-selection.override.clear",
        reason=payload.reason,
        db_path=result_db_path,
    )
    snapshot = result.get("snapshot") or build_runtime_selection_snapshot(
        config_repo=config,
        db_path=result_db_path,
    )
    request.app.state.runtime_selection_snapshot = snapshot
    return {
        "ok": True,
        "schema_version": LOGIC_SELECTION_SCHEMA_VERSION,
        "snapshot": snapshot,
    }


@router.get("/publish/state")
def get_publish_state(
    config: ConfigRepository = Depends(get_config_repo),
):
    result_db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
    ops_db_path = os.getenv("MEEMEE_OPS_DB_PATH")
    return build_publish_promotion_snapshot(
        config_repo=config,
        db_path=result_db_path,
        ops_db_path=ops_db_path,
    )


@router.get("/publish/queue")
def get_publish_queue(
    config: ConfigRepository = Depends(get_config_repo),
):
    result_db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
    ops_db_path = os.getenv("MEEMEE_OPS_DB_PATH")
    snapshot = build_publish_promotion_snapshot(
        config_repo=config,
        db_path=result_db_path,
        ops_db_path=ops_db_path,
    )
    return {
        "ok": True,
        "champion": snapshot.get("champion"),
        "challengers": snapshot.get("challengers") or [],
        "challenger_logic_keys": snapshot.get("challenger_logic_keys") or [],
        "bootstrap_rule": snapshot.get("bootstrap_rule"),
        "source_of_truth": snapshot.get("source_of_truth"),
        "degraded": snapshot.get("degraded"),
        "registry_sync_state": snapshot.get("registry_sync_state"),
        "last_sync_time": snapshot.get("last_sync_time"),
        "default_logic_pointer": snapshot.get("default_logic_pointer"),
    }


@router.get("/publish/candidates")
def get_publish_candidates(
    config: ConfigRepository = Depends(get_config_repo),
):
    result_db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
    candidates = list_publish_candidate_bundles(db_path=result_db_path)
    return {
        "ok": True,
        "items": candidates,
        "count": len(candidates),
    }


@router.get("/publish/candidates/{logic_key}")
def get_publish_candidate(
    logic_key: str,
    config: ConfigRepository = Depends(get_config_repo),
):
    result_db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
    candidate = load_publish_candidate_bundle(db_path=result_db_path, logic_key=str(logic_key).strip())
    if not candidate:
        raise HTTPException(status_code=404, detail={"ok": False, "reason": "candidate_bundle_not_found", "logic_key": logic_key})
    return {
        "ok": True,
        "candidate": candidate,
    }


@router.post("/publish/candidates/{logic_key}/approve")
def approve_publish_candidate(
    logic_key: str,
    payload: PublishCandidateActionPayload,
    request: Request,
    config: ConfigRepository = Depends(get_config_repo),
):
    result_db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
    result = approve_publish_candidate_bundle(
        db_path=result_db_path,
        logic_key=str(logic_key).strip(),
        source="api.system.publish.candidates.approve",
        reason=payload.reason,
        actor=payload.actor,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail={"ok": False, "reason": result.get("reason"), "logic_key": logic_key, "validation_issues": result.get("validation_issues")})
    request.app.state.runtime_selection_snapshot = build_runtime_selection_snapshot(
        config_repo=config,
        db_path=result_db_path,
    )
    return result


@router.post("/publish/candidates/{logic_key}/reject")
def reject_publish_candidate(
    logic_key: str,
    payload: PublishCandidateActionPayload,
    request: Request,
    config: ConfigRepository = Depends(get_config_repo),
):
    result_db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
    result = reject_publish_candidate_bundle(
        db_path=result_db_path,
        logic_key=str(logic_key).strip(),
        source="api.system.publish.candidates.reject",
        reason=payload.reason,
        actor=payload.actor,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail={"ok": False, "reason": result.get("reason"), "logic_key": logic_key, "validation_issues": result.get("validation_issues")})
    request.app.state.runtime_selection_snapshot = build_runtime_selection_snapshot(
        config_repo=config,
        db_path=result_db_path,
    )
    return result


@router.post("/publish/mirror/normalize")
def normalize_publish_mirror(
    payload: PublishMirrorRepairPayload,
    config: ConfigRepository = Depends(get_config_repo),
):
    result_db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
    return normalize_publish_registry_mirror(
        config_repo=config,
        db_path=result_db_path,
        source="api.system.publish.mirror.normalize",
        reason=payload.reason,
        actor=payload.actor,
    )


@router.post("/publish/mirror/resync")
def resync_publish_mirror(
    payload: PublishMirrorRepairPayload,
    config: ConfigRepository = Depends(get_config_repo),
):
    result_db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
    return normalize_publish_registry_mirror(
        config_repo=config,
        db_path=result_db_path,
        source="api.system.publish.mirror.resync",
        reason=payload.reason,
        actor=payload.actor,
    )


@router.post("/publish/maintenance/backfill")
def run_publish_candidate_backfill(
    payload: PublishMaintenancePayload,
    config: ConfigRepository = Depends(get_config_repo),
):
    result_db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
    ops_db_path = os.getenv("MEEMEE_OPS_DB_PATH")
    return backfill_publish_candidate_bundles(
        db_path=result_db_path,
        ops_db_path=ops_db_path,
        limit=payload.limit,
        dry_run=bool(payload.dryRun),
    )


@router.post("/publish/maintenance/snapshot-sweep")
def run_publish_candidate_snapshot_sweep(
    payload: PublishMaintenancePayload,
    config: ConfigRepository = Depends(get_config_repo),
):
    result_db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
    return sweep_publish_candidate_snapshots(
        db_path=result_db_path,
        keep_approved_days=payload.keepApprovedDays or 90,
        keep_rejected_days=payload.keepRejectedDays or 14,
        keep_retired_days=payload.keepRetiredDays or 14,
        dry_run=bool(payload.dryRun),
    )


@router.post("/publish/challenger/enqueue")
def enqueue_publish_challenger(
    payload: PublishChallengerPayload,
    request: Request,
    config: ConfigRepository = Depends(get_config_repo),
):
    result_db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
    ops_db_path = os.getenv("MEEMEE_OPS_DB_PATH")
    logic_key = str(payload.logicKey or "").strip() or None
    if not logic_key:
        raise HTTPException(status_code=400, detail={"ok": False, "reason": "logic_key_required"})
    result = enqueue_challenger_logic_key(
        config_repo=config,
        logic_key=logic_key,
        source="api.system.publish.challenger.enqueue",
        reason=payload.reason,
        actor=payload.actor,
        db_path=result_db_path,
        ops_db_path=ops_db_path,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail={"ok": False, "reason": result.get("reason"), "logic_key": logic_key})
    request.app.state.runtime_selection_snapshot = build_runtime_selection_snapshot(
        config_repo=config,
        db_path=result_db_path,
    )
    return result


@router.post("/publish/challenger/retire")
def retire_publish_challenger(
    payload: PublishChallengerPayload,
    request: Request,
    config: ConfigRepository = Depends(get_config_repo),
):
    result_db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
    ops_db_path = os.getenv("MEEMEE_OPS_DB_PATH")
    logic_key = str(payload.logicKey or "").strip() or None
    if not logic_key:
        raise HTTPException(status_code=400, detail={"ok": False, "reason": "logic_key_required"})
    result = retire_challenger_logic_key(
        config_repo=config,
        logic_key=logic_key,
        source="api.system.publish.challenger.retire",
        reason=payload.reason,
        actor=payload.actor,
        db_path=result_db_path,
        ops_db_path=ops_db_path,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail={"ok": False, "reason": result.get("reason"), "logic_key": logic_key})
    request.app.state.runtime_selection_snapshot = build_runtime_selection_snapshot(
        config_repo=config,
        db_path=result_db_path,
    )
    return result


@router.post("/publish/promote")
def promote_publish_logic(
    payload: PublishPromotionPayload,
    request: Request,
    config: ConfigRepository = Depends(get_config_repo),
):
    result_db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
    ops_db_path = os.getenv("MEEMEE_OPS_DB_PATH")
    logic_key = str(payload.logicKey or "").strip() or None
    if not logic_key:
        raise HTTPException(status_code=400, detail={"ok": False, "reason": "logic_key_required"})
    result = promote_logic_key(
        config_repo=config,
        logic_key=logic_key,
        source="api.system.publish.promote",
        reason=payload.reason,
        actor=payload.actor,
        db_path=result_db_path,
        ops_db_path=ops_db_path,
    )
    if not result.get("ok"):
        raise HTTPException(
            status_code=400,
            detail={
                "ok": False,
                "reason": result.get("reason"),
                "logic_key": logic_key,
                "gate_reasons": result.get("gate_reasons"),
            },
    )
    request.app.state.runtime_selection_snapshot = build_runtime_selection_snapshot(
        config_repo=config,
        db_path=result_db_path,
    )
    return result


@router.post("/publish/demote")
def demote_publish_logic(
    payload: PublishPromotionPayload,
    request: Request,
    config: ConfigRepository = Depends(get_config_repo),
):
    result_db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
    ops_db_path = os.getenv("MEEMEE_OPS_DB_PATH")
    logic_key = str(payload.logicKey or "").strip() or None
    if not logic_key:
        raise HTTPException(status_code=400, detail={"ok": False, "reason": "logic_key_required"})
    result = demote_logic_key(
        config_repo=config,
        logic_key=logic_key,
        source="api.system.publish.demote",
        reason=payload.reason,
        actor=payload.actor,
        db_path=result_db_path,
        ops_db_path=ops_db_path,
    )
    if not result.get("ok"):
        raise HTTPException(
            status_code=400,
            detail={
                "ok": False,
                "reason": result.get("reason"),
                "logic_key": logic_key,
            },
    )
    request.app.state.runtime_selection_snapshot = build_runtime_selection_snapshot(
        config_repo=config,
        db_path=result_db_path,
    )
    return result


@router.post("/publish/rollback")
def rollback_publish_logic(
    payload: PublishRollbackPayload,
    request: Request,
    config: ConfigRepository = Depends(get_config_repo),
):
    result_db_path = os.getenv("MEEMEE_RESULT_DB_PATH")
    ops_db_path = os.getenv("MEEMEE_OPS_DB_PATH")
    snapshot = build_publish_promotion_snapshot(
        config_repo=config,
        db_path=result_db_path,
        ops_db_path=ops_db_path,
    )
    logic_key = str(payload.logicKey or "").strip() or snapshot.get("previous_stable_champion_logic_key") or snapshot.get("champion_logic_key")
    if not logic_key:
        raise HTTPException(status_code=400, detail={"ok": False, "reason": "logic_key_required"})
    result = rollback_logic_key(
        config_repo=config,
        logic_key=str(logic_key),
        source="api.system.publish.rollback",
        reason=payload.reason,
        actor=payload.actor,
        db_path=result_db_path,
        ops_db_path=ops_db_path,
    )
    if not result.get("ok"):
        raise HTTPException(
            status_code=400,
            detail={
                "ok": False,
                "reason": result.get("reason"),
                "logic_key": logic_key,
            },
    )
    request.app.state.runtime_selection_snapshot = build_runtime_selection_snapshot(
        config_repo=config,
        db_path=result_db_path,
    )
    return result

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
