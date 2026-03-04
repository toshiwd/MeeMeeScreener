import sys
import os
import threading
import time
import subprocess
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


def _rankings_warmup_delay_sec() -> float:
    raw = os.getenv("MEEMEE_RANKINGS_WARMUP_DELAY_SEC", "5")
    try:
        return max(0.0, float(raw))
    except Exception:
        return 5.0


def _refresh_rankings_cache_async() -> None:
    try:
        delay_sec = _rankings_warmup_delay_sec()
        if delay_sec > 0:
            time.sleep(delay_sec)
        from app.backend.services import rankings_cache
        rankings_cache.refresh_cache()
        print("[main] Rankings cache refreshed.")
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
                if existing_pid > 0 and existing_pid != current_pid and _is_pid_running(existing_pid):
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

from app.backend.api.dependencies import init_resources
from app.backend.api.routers import (
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
from app.backend.api import watchlist_routes
from app.backend.api.routers import rankings
from app.backend.core.force_sync_job import handle_force_sync
from app.backend.core.jobs import cleanup_stale_jobs, job_manager
from app.backend.core.ml_job import handle_ml_live_guard, handle_ml_predict, handle_ml_train
from app.backend.core.analysis_backfill_job import handle_analysis_backfill
from app.backend.core.phase_batch_job import handle_phase_rebuild
from app.backend.core.strategy_backtest_job import (
    handle_strategy_backtest,
    handle_strategy_walkforward,
    handle_strategy_walkforward_gate,
)
from app.backend.core.yahoo_daily_ingest_job import (
    YF_DAILY_INGEST_JOB_TYPE,
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
from app.backend.core.toredex_live_job import handle_toredex_live
from app.backend.core.toredex_self_improve_job import handle_toredex_self_improve
from app.backend.core.txt_update_job import handle_txt_update
from app.db.session import get_connect_stats, is_transient_duckdb_error

job_manager.register_handler("force_sync", handle_force_sync)
job_manager.register_handler("txt_update", handle_txt_update)
job_manager.register_handler("phase_rebuild", handle_phase_rebuild)
job_manager.register_handler("ml_train", handle_ml_train)
job_manager.register_handler("ml_predict", handle_ml_predict)
job_manager.register_handler("ml_live_guard", handle_ml_live_guard)
job_manager.register_handler("analysis_backfill", handle_analysis_backfill)
job_manager.register_handler("strategy_backtest", handle_strategy_backtest)
job_manager.register_handler("strategy_walkforward", handle_strategy_walkforward)
job_manager.register_handler("strategy_walkforward_gate", handle_strategy_walkforward_gate)
job_manager.register_handler(YF_DAILY_INGEST_JOB_TYPE, handle_yf_daily_ingest)
job_manager.register_handler(RANKING_ANALYSIS_QUALITY_JOB_TYPE, handle_ranking_analysis_quality)
job_manager.register_handler("toredex_live", handle_toredex_live)
job_manager.register_handler("toredex_self_improve", handle_toredex_self_improve)

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
        init_resources(data_dir)
        # Ensure jobs left by a previous backend process are finalized on boot.
        cleanup_stale_jobs()
        print("[Main] Resources Initialized.")
        # Warm cache in background so /health is not blocked by heavy or locked DB work.
        threading.Thread(
            target=_refresh_rankings_cache_async,
            name="rankings-cache-warmup",
            daemon=True,
        ).start()
        start_yf_daily_ingest_scheduler()
        start_ranking_analysis_quality_scheduler()
        yield
    finally:
        # Shutdown
        stop_ranking_analysis_quality_scheduler(timeout_sec=1.0)
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
            payload = {
                "error": "db_unavailable",
                "retryable": True,
                "message": "Database is temporarily unavailable",
                "path": str(getattr(request, "url", "")),
                "db_connect_stats": get_connect_stats(),
            }
            return JSONResponse(status_code=503, content=payload, headers={"Retry-After": "1"})

    # Routes
    app.include_router(bars.router)
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
