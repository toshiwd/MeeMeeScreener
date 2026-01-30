import sys
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

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
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize Infra
    print("[Main] Initializing Resources...")
    # Determine data directory (could be passed via env or args)
    # For now, use current directory or a specific data dir
    data_dir = os.path.join(os.getcwd(), "data")
    os.makedirs(data_dir, exist_ok=True)
    
    init_resources(data_dir)
    print("[Main] Resources Initialized.")
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
