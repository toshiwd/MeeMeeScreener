import sys
import os
import threading
import time
import subprocess
import logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

_LOGGED_RESOLVED_PATHS = False
_PROCESS_LOCK_STATE_LOCK = threading.Lock()
_PROCESS_LOCK_OWNED = False
_PROCESS_LOCK_REFCOUNT = 0
_PROCESS_LOCK_PATH: str | None = None
logger = logging.getLogger(__name__)


def _rankings_warmup_delay_sec() -> float:
    raw = os.getenv("MEEMEE_RANKINGS_WARMUP_DELAY_SEC", "0")
    try:
        return max(0.0, float(raw))
    except Exception:
        return 0.0


def _refresh_rankings_cache_async() -> None:
    try:
        delay_sec = _rankings_warmup_delay_sec()
        if delay_sec > 0:
            time.sleep(delay_sec)
        from app.backend.services import rankings_cache
        rankings_cache.refresh_cache()
        rankings_cache.warm_default_result_cache()
        print("[main] Rankings cache refreshed and warmed.")
    except Exception as exc:
        print(f"[main] Rankings cache refresh failed: {exc}")


def _process_lock_enabled() -> bool:
    raw = os.getenv("MEEMEE_PROCESS_LOCK_ENABLED", "1")
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _is_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            cmd = f'tasklist /fi "PID eq {pid}" /fo csv /nh'
            out = subprocess.check_output(cmd, shell=True).decode("cp932", errors="ignore")
            return str(pid) in out and "No tasks are running" not in out
        except Exception:
            return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _get_process_commandline(pid: int) -> str:
    if pid <= 0 or os.name != "nt":
        return ""
    script = (
        f"$p = Get-CimInstance Win32_Process -Filter \"ProcessId = {int(pid)}\"; "
        "if ($p) { $p.CommandLine }"
    )
    try:
        out = subprocess.check_output(
            ["powershell", "-NoProfile", "-Command", script],
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="cp932",
            errors="ignore",
        )
        return out.strip()
    except Exception:
        return ""


def _is_backend_process_pid(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name != "nt":
        return _is_pid_running(pid)
    cmdline = _get_process_commandline(pid).lower()
    if not cmdline:
        return False
    return (
        "--backend" in cmdline
        or "meemee_backend_only" in cmdline
        or ("uvicorn" in cmdline and ("app.main:app" in cmdline or " main:app" in cmdline))
    )


def _acquire_process_lock() -> tuple[str | None, bool]:
    from app.core.config import config as core_config

    global _PROCESS_LOCK_OWNED, _PROCESS_LOCK_REFCOUNT, _PROCESS_LOCK_PATH
    if not _process_lock_enabled():
        return None, False

    lock_path = str(core_config.LOCK_FILE_PATH)
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    current_pid = os.getpid()

    with _PROCESS_LOCK_STATE_LOCK:
        if _PROCESS_LOCK_OWNED:
            _PROCESS_LOCK_REFCOUNT += 1
            return _PROCESS_LOCK_PATH, True

        if os.path.exists(lock_path):
            stale = True
            try:
                with open(lock_path, "r", encoding="utf-8") as handle:
                    text = handle.read().strip()
                existing_pid = int(text) if text.isdigit() else -1
                if (
                    existing_pid > 0
                    and existing_pid != current_pid
                    and _is_pid_running(existing_pid)
                    and _is_backend_process_pid(existing_pid)
                ):
                    raise RuntimeError(f"another backend process is running (PID={existing_pid})")
                if existing_pid == current_pid:
                    stale = False
            except RuntimeError:
                raise
            except Exception:
                stale = True
            if stale:
                try:
                    os.remove(lock_path)
                except Exception:
                    pass

        if not os.path.exists(lock_path):
            with open(lock_path, "w", encoding="utf-8") as handle:
                handle.write(str(current_pid))
                handle.flush()
                os.fsync(handle.fileno())

        _PROCESS_LOCK_OWNED = True
        _PROCESS_LOCK_REFCOUNT = 1
        _PROCESS_LOCK_PATH = lock_path
        return lock_path, True


def _release_process_lock(lock_path: str | None, acquired: bool) -> None:
    global _PROCESS_LOCK_OWNED, _PROCESS_LOCK_REFCOUNT, _PROCESS_LOCK_PATH
    if not acquired:
        return
    with _PROCESS_LOCK_STATE_LOCK:
        if not _PROCESS_LOCK_OWNED:
            return
        _PROCESS_LOCK_REFCOUNT = max(0, int(_PROCESS_LOCK_REFCOUNT) - 1)
        if _PROCESS_LOCK_REFCOUNT > 0:
            return
        _PROCESS_LOCK_OWNED = False
        _PROCESS_LOCK_PATH = None
        try:
            if lock_path and os.path.exists(lock_path):
                os.remove(lock_path)
        except Exception:
            pass

# Add project root to sys.path
sys.path.insert(0, os.getcwd())

from app.backend.api.dependencies import get_config_repo, init_resources
from app.backend.api.routers import (
    analysis_bridge,
    bars,
    grid,
    system,
    ticker,
    trades,
    practice,
    health,
    jobs,
    events,
    spa,
    similar,
    favorites,
    market,
    memo,
    quality,
    toredex,
)
from app.backend.services.operator_mutation_lock import (
    get_operator_mutation_observability,
    get_operator_mutation_state,
    is_operator_mutation_active,
    record_operator_mutation_observation,
)
from app.backend.api import watchlist_routes
from app.backend.api.routers import rankings
from app.backend.core.force_sync_job import handle_force_sync
from app.backend.core.jobs import cleanup_stale_jobs, job_manager
from app.backend.core.ml_job import handle_ml_live_guard, handle_ml_predict, handle_ml_train
from app.backend.core.analysis_backfill_job import handle_analysis_backfill
from app.backend.core.analysis_prewarm_job import (
    start_analysis_prewarm_scheduler,
    stop_analysis_prewarm_scheduler,
)
from app.backend.core.screener_snapshot_job import (
    SCREENER_SNAPSHOT_JOB_TYPE,
    handle_screener_snapshot_refresh,
    start_screener_snapshot_scheduler,
    stop_screener_snapshot_scheduler,
)
from app.backend.core.phase_batch_job import handle_phase_rebuild
from app.backend.core.strategy_backtest_job import (
    handle_strategy_backtest,
    handle_strategy_walkforward,
    handle_strategy_walkforward_gate,
)
from app.backend.core.yahoo_daily_ingest_job import (
    YF_DAILY_INGEST_JOB_TYPE,
)
from app.backend.core.yahoo_daily_ingest_runtime import (
    handle_yf_daily_ingest,
    start_yf_daily_ingest_scheduler,
    stop_yf_daily_ingest_scheduler,
)
from app.backend.core.ranking_quality_job import (
    RANKING_ANALYSIS_QUALITY_JOB_TYPE,
    handle_ranking_analysis_quality,
    start_ranking_analysis_quality_scheduler,
    stop_ranking_analysis_quality_scheduler,
)
from app.backend.core.external_analysis_publish_job import (
    EXTERNAL_ANALYSIS_PUBLISH_JOB_TYPE,
    handle_external_analysis_publish_latest,
)
from app.backend.core.publish_candidate_maintenance_job import (
    start_publish_candidate_maintenance_scheduler,
    stop_publish_candidate_maintenance_scheduler,
)
from app.backend.core.taisyaku_import_job import TAISYAKU_IMPORT_JOB_TYPE, handle_taisyaku_import
from app.backend.core.tdnet_import_job import TDNET_IMPORT_JOB_TYPE, handle_tdnet_import
from app.backend.core.toredex_live_job import handle_toredex_live
from app.backend.core.toredex_self_improve_job import handle_toredex_self_improve
from app.backend.core.txt_followup_job import handle_txt_followup
from app.backend.core.txt_update_job import handle_txt_update
from app.backend.core.legacy_analysis_control import (
    is_legacy_analysis_disabled,
    legacy_analysis_disabled_log_value,
)
from app.backend.services.runtime_selection_service import build_runtime_selection_snapshot
from app.backend.services.publish_promotion_service import build_publish_promotion_snapshot
from app.db.session import get_connect_stats, is_transient_duckdb_error

job_manager.register_handler("force_sync", handle_force_sync)
job_manager.register_handler("txt_update", handle_txt_update)
job_manager.register_handler("txt_followup", handle_txt_followup)
if not is_legacy_analysis_disabled():
    job_manager.register_handler("phase_rebuild", handle_phase_rebuild)
    job_manager.register_handler("ml_train", handle_ml_train)
    job_manager.register_handler("ml_predict", handle_ml_predict)
    job_manager.register_handler("ml_live_guard", handle_ml_live_guard)
    job_manager.register_handler("analysis_backfill", handle_analysis_backfill)
else:
    logger.info(
        "Legacy analysis handlers not registered for Phase 1 (%s)",
        legacy_analysis_disabled_log_value(),
    )
job_manager.register_handler("strategy_backtest", handle_strategy_backtest)
job_manager.register_handler("strategy_walkforward", handle_strategy_walkforward)
job_manager.register_handler("strategy_walkforward_gate", handle_strategy_walkforward_gate)
job_manager.register_handler(EXTERNAL_ANALYSIS_PUBLISH_JOB_TYPE, handle_external_analysis_publish_latest)
job_manager.register_handler(YF_DAILY_INGEST_JOB_TYPE, handle_yf_daily_ingest)
if not is_legacy_analysis_disabled():
    job_manager.register_handler(RANKING_ANALYSIS_QUALITY_JOB_TYPE, handle_ranking_analysis_quality)
else:
    logger.info(
        "Ranking analysis quality handler not registered while legacy analysis is disabled (%s)",
        legacy_analysis_disabled_log_value(),
    )
job_manager.register_handler(TAISYAKU_IMPORT_JOB_TYPE, handle_taisyaku_import)
job_manager.register_handler(TDNET_IMPORT_JOB_TYPE, handle_tdnet_import)
job_manager.register_handler("toredex_live", handle_toredex_live)
job_manager.register_handler("toredex_self_improve", handle_toredex_self_improve)
job_manager.register_handler(SCREENER_SNAPSHOT_JOB_TYPE, handle_screener_snapshot_refresh)

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _LOGGED_RESOLVED_PATHS
    lock_path = None
    lock_acquired = False
    # Startup: Initialize Infra
    print("[Main] Initializing Resources...")
    # Determine data directory from env first; fall back to config if unset.
    data_dir = os.getenv("MEEMEE_DATA_DIR")
    if not data_dir:
        from app.backend.core.config import config
        data_dir = str(config.DATA_DIR)
        os.environ.setdefault("MEEMEE_DATA_DIR", data_dir)
    os.makedirs(data_dir, exist_ok=True)
    if not os.getenv("MEEMEE_RESOLVED_PATHS_LOGGED"):
        if not _LOGGED_RESOLVED_PATHS:
            _LOGGED_RESOLVED_PATHS = True
            exe_dir = os.path.dirname(sys.executable)
            app_env = os.getenv("APP_ENV", "")
            data_store = os.getenv("MEEMEE_DATA_STORE", "")
            db_path = os.getenv("STOCKS_DB_PATH", "")
            auto_update_enabled = os.getenv("MEEMEE_ENABLE_AUTO_UPDATE", "").lower() in ("1", "true", "yes", "on")
            print(
                "[main] Resolved paths:"
                f" exe_dir={exe_dir}"
                f" APP_ENV={app_env}"
                f" MEEMEE_DATA_DIR={data_dir}"
                f" MEEMEE_DATA_STORE={data_store}"
                f" STOCKS_DB_PATH={db_path}"
                f" auto_update_enabled={auto_update_enabled}"
            )
    
    lock_path, lock_acquired = _acquire_process_lock()
    try:
        logger.info("Backend startup mode (%s)", legacy_analysis_disabled_log_value())
        init_resources(data_dir)
        # Ensure jobs left by a previous backend process are finalized on boot.
        cleanup_stale_jobs()
        from app.backend.services.system_status import mark_backend_boot_ready

        mark_backend_boot_ready()
        try:
            app.state.runtime_selection_snapshot = build_runtime_selection_snapshot(
                config_repo=get_config_repo(),
                db_path=os.getenv("MEEMEE_RESULT_DB_PATH"),
            )
        except Exception as exc:
            logger.exception("Runtime selection bootstrap failed: %s", exc)
            app.state.runtime_selection_snapshot = {
                "schema_version": "logic_selection_v1",
                "snapshot_created_at": datetime.now(timezone.utc).isoformat(),
                "selected_logic_override": None,
                "default_logic_pointer": None,
                "logic_key": None,
                "selected_logic_key": None,
                "selected_logic_id": None,
                "selected_logic_version": None,
                "artifact_uri": None,
                "selected_manifest": None,
                "override_present": False,
                "last_known_good_present": False,
                "last_known_good": None,
                "last_known_good_artifact_uri": None,
                "safe_fallback_key": "builtin_safe_fallback",
                "available_logic_manifest": [],
                "available_logic_keys": [],
                "resolution": None,
                "selected_source": "unresolved",
                "resolved_source": "unresolved",
                "selected_pointer_name": None,
                "matched_available": False,
                "validation_state": "unresolved",
                "validation_issues": ["runtime_selection_bootstrap_failed"],
                "notes": ["runtime_selection_bootstrap_failed"],
                "catalog_default_logic_pointer": None,
                "catalog": {
                    "available_logic_manifest": [],
                    "available_logic_keys": [],
                    "default_logic_pointer": None,
                },
                "resolution_order": [
                    "selected_logic_override",
                    "default_logic_pointer",
                    "last_known_good",
                    "safe_fallback",
                ],
            }
        try:
            app.state.publish_promotion_snapshot = build_publish_promotion_snapshot(
                config_repo=get_config_repo(),
                db_path=os.getenv("MEEMEE_RESULT_DB_PATH"),
                ops_db_path=os.getenv("MEEMEE_OPS_DB_PATH"),
            )
        except Exception as exc:
            logger.exception("Publish promotion bootstrap failed: %s", exc)
            app.state.publish_promotion_snapshot = {
                "schema_version": "publish_registry_v2",
                "source_of_truth": "empty",
                "registry_sync_state": "empty",
                "degraded": True,
                "last_sync_time": None,
                "bootstrap_rule": "empty_safe_state",
                "champion_logic_key": None,
                "challenger_logic_keys": [],
                "challengers": [],
                "default_logic_pointer": None,
                "previous_stable_champion_logic_key": None,
                "retired_logic_keys": [],
                "promotion_history": [],
                "maintenance_state": {},
                "candidate_backfill_last_run": None,
                "snapshot_sweep_last_run": None,
                "non_promotable_legacy_count": 0,
                "maintenance_degraded": True,
                "external_registry_version": None,
                "local_mirror_version": None,
                "mirror_schema_version": None,
                "mirror_normalized": False,
            }
        print("[Main] Resources Initialized.")
        # Warm cache in background so /health is not blocked by heavy or locked DB work.
        threading.Thread(
            target=_refresh_rankings_cache_async,
            name="rankings-cache-warmup",
            daemon=True,
        ).start()
        start_screener_snapshot_scheduler()
        start_yf_daily_ingest_scheduler()
        if not is_legacy_analysis_disabled():
            start_ranking_analysis_quality_scheduler()
        else:
            logger.info(
                "Skipping ranking analysis quality scheduler at startup (%s)",
                legacy_analysis_disabled_log_value(),
            )
        if not is_legacy_analysis_disabled():
            start_analysis_prewarm_scheduler()
        else:
            logger.info(
                "Skipping legacy analysis prewarm scheduler at startup (%s)",
                legacy_analysis_disabled_log_value(),
            )
        start_publish_candidate_maintenance_scheduler()
        yield
    finally:
        # Shutdown
        stop_publish_candidate_maintenance_scheduler(timeout_sec=1.0)
        if not is_legacy_analysis_disabled():
            stop_analysis_prewarm_scheduler(timeout_sec=1.0)
            stop_ranking_analysis_quality_scheduler(timeout_sec=1.0)
        stop_screener_snapshot_scheduler(timeout_sec=1.0)
        stop_yf_daily_ingest_scheduler(timeout_sec=1.0)
        _release_process_lock(lock_path, lock_acquired)
        print("[Main] Shutting down.")

def create_app() -> FastAPI:
    app = FastAPI(title="MeeMee Screener (Clean Architecture)", lifespan=lifespan)

    # Middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_origin_regex="null|file://.*",
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def transient_duckdb_guard(request, call_next):
        try:
            return await call_next(request)
        except Exception as exc:
            if not is_transient_duckdb_error(exc):
                raise
            mutation_state = get_operator_mutation_state() if is_operator_mutation_active() else None
            request_path = str(getattr(getattr(request, "url", None), "path", "") or "")
            reason = "db_busy"
            if mutation_state and request_path.startswith("/api/system/"):
                if request_path.startswith("/api/system/publish/state") or request_path.startswith("/api/system/runtime-selection"):
                    reason = "publish_state_refresh_conflict"
                else:
                    reason = "operator_mutation_busy"
            record_operator_mutation_observation(reason)
            payload = {
                "error": "db_unavailable",
                "retryable": True,
                "reason": reason,
                "message": "Database is temporarily unavailable",
                "path": str(getattr(request, "url", "")),
                "db_connect_stats": get_connect_stats(),
            }
            if mutation_state:
                payload["operator_mutation_state"] = {
                    "active": mutation_state.active,
                    "active_action": mutation_state.active_action,
                    "active_since": mutation_state.active_since,
                }
            payload["operator_mutation_observability"] = get_operator_mutation_observability()
            return JSONResponse(status_code=503, content=payload, headers={"Retry-After": "1"})

    # Routes
    app.include_router(bars.router)
    app.include_router(analysis_bridge.router)
    app.include_router(ticker.router)
    app.include_router(grid.router)
    app.include_router(similar.router)
    app.include_router(system.router)
    app.include_router(trades.router)
    app.include_router(practice.router)
    app.include_router(health.router)
    app.include_router(jobs.router)
    app.include_router(events.router)
    app.include_router(favorites.router)
    app.include_router(market.router)
    app.include_router(memo.router)
    app.include_router(quality.router)
    app.include_router(toredex.router)
    app.include_router(rankings.router)
    app.include_router(watchlist_routes.router)
    app.include_router(spa.router)

    # Static Files (Frontend)
    # Ensure 'app/frontend/dist' exists or adjust path
    static_dir = os.path.join(os.getcwd(), "app", "frontend", "dist")
    if os.path.exists(static_dir):
        app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")

    return app

app = create_app()

if __name__ == "__main__":
    import uvicorn
    # Clean Arch Entrypoint
    uvicorn.run(app, host="127.0.0.1", port=8000)
