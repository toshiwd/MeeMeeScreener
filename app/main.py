import sys
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

_LOGGED_RESOLVED_PATHS = False

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
)
from app.backend.api import watchlist_routes
from app.backend.api.routers import rankings

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _LOGGED_RESOLVED_PATHS
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
    
    init_resources(data_dir)
    print("[Main] Resources Initialized.")
    try:
        from app.backend.services import rankings_cache
        rankings_cache.refresh_cache()
        print("[main] Rankings cache refreshed.")
    except Exception as exc:
        print(f"[main] Rankings cache refresh failed: {exc}")
    yield
    # Shutdown
    print("[Main] Shutting down.")

def create_app() -> FastAPI:
    app = FastAPI(title="MeeMee Screener (Clean Architecture)", lifespan=lifespan)

    # Middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

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
