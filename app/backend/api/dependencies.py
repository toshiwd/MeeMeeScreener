from app.backend.infra.duckdb.stock_repo import StockRepository
from app.backend.infra.duckdb.screener_repo import ScreenerRepository
from app.backend.infra.sqlite.favorites_repo import FavoritesRepository
from app.backend.infra.files.config_repo import ConfigRepository
from app.core.config import config as app_config
import logging
import os
from pathlib import Path
from threading import RLock

# Singleton instances or factories
# In a real app, these paths should come from env or config
# We use defaults for now

_stock_repo = None
_favorites_repo = None
_config_repo = None
_screener_repo = None

_repo_lock = RLock()
_logger = logging.getLogger(__name__)
_logged_paths = False


def _resolve_data_dir(data_dir: str | None = None) -> str:
    data_dir = data_dir or os.getenv("MEEMEE_DATA_DIR")
    if not data_dir:
        app_env = os.getenv("APP_ENV", "").lower()
        if app_env in ("prod", "production"):
            raise RuntimeError("MEEMEE_DATA_DIR is required in prod (fallback disabled)")
        data_dir = str(app_config.DATA_DIR)
    try:
        return str(Path(data_dir).expanduser().resolve(strict=False))
    except Exception:
        return data_dir


def _log_paths_once(data_dir: str, db_path: str, fav_path: str) -> None:
    global _logged_paths
    if _logged_paths:
        return
    _logged_paths = True
    _logger.info(
        "Resolved paths: data_dir=%s db=%s favorites=%s",
        data_dir,
        db_path,
        fav_path,
    )


def _init_repos(data_dir: str | None, allow_fallback: bool) -> None:
    global _stock_repo, _favorites_repo, _config_repo, _screener_repo
    if _stock_repo and _favorites_repo and _config_repo and _screener_repo:
        return
    with _repo_lock:
        if _stock_repo and _favorites_repo and _config_repo and _screener_repo:
            return
        if not allow_fallback and not data_dir:
            raise RuntimeError("data_dir is required for init_resources")
        resolved_data_dir = _resolve_data_dir(data_dir)
        if allow_fallback and not data_dir:
            _logger.warning("init_resources missing; using fallback data_dir=%s", resolved_data_dir)
        db_path = os.getenv("STOCKS_DB_PATH", os.path.join(resolved_data_dir, "stocks.duckdb"))
        fav_path = os.getenv("FAVORITES_DB_PATH", os.path.join(resolved_data_dir, "favorites.sqlite"))
        _log_paths_once(resolved_data_dir, db_path, fav_path)
        _stock_repo = _stock_repo or StockRepository(db_path)
        _screener_repo = _screener_repo or ScreenerRepository(db_path)
        _favorites_repo = _favorites_repo or FavoritesRepository(fav_path)
        _config_repo = _config_repo or ConfigRepository(resolved_data_dir)

def init_resources(data_dir: str):
    _init_repos(data_dir, allow_fallback=False)

def get_stock_repo() -> StockRepository:
    global _stock_repo
    if _stock_repo:
        return _stock_repo
    _init_repos(None, allow_fallback=True)
    if not _stock_repo:
        raise RuntimeError("StockRepo not initialized")
    return _stock_repo

def get_favorites_repo() -> FavoritesRepository:
    global _favorites_repo
    if _favorites_repo:
        return _favorites_repo
    _init_repos(None, allow_fallback=True)
    if not _favorites_repo:
        raise RuntimeError("FavoritesRepo not initialized")
    return _favorites_repo

def get_config_repo() -> ConfigRepository:
    global _config_repo
    if _config_repo:
        return _config_repo
    _init_repos(None, allow_fallback=True)
    if not _config_repo:
        raise RuntimeError("ConfigRepo not initialized")
    return _config_repo

def get_screener_repo() -> ScreenerRepository:
    global _screener_repo
    if _screener_repo:
        return _screener_repo
    _init_repos(None, allow_fallback=True)
    if not _screener_repo:
        raise RuntimeError("ScreenerRepo not initialized")
    return _screener_repo
