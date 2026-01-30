from app.backend.infra.duckdb.stock_repo import StockRepository
from app.backend.infra.duckdb.screener_repo import ScreenerRepository
from app.backend.infra.sqlite.favorites_repo import FavoritesRepository
from app.backend.infra.files.config_repo import ConfigRepository
import os

# Singleton instances or factories
# In a real app, these paths should come from env or config
# We use defaults for now

_stock_repo = None
_favorites_repo = None
_config_repo = None
_screener_repo = None

def init_resources(data_dir: str):
    global _stock_repo, _favorites_repo, _config_repo, _screener_repo
    
    db_path = os.getenv("STOCKS_DB_PATH", os.path.join(data_dir, "stocks.duckdb"))
    fav_path = os.getenv("FAVORITES_DB_PATH", os.path.join(data_dir, "favorites.sqlite"))
    
    _stock_repo = StockRepository(db_path)
    _screener_repo = ScreenerRepository(db_path)
    _favorites_repo = FavoritesRepository(fav_path)
    _config_repo = ConfigRepository(data_dir)

def get_stock_repo() -> StockRepository:
    if not _stock_repo:
        raise RuntimeError("StockRepo not initialized")
    return _stock_repo

def get_favorites_repo() -> FavoritesRepository:
    global _favorites_repo
    if _favorites_repo:
        return _favorites_repo
    # Lazy fallback to avoid 500 when init_resources was skipped.
    data_dir = os.getenv("MEEMEE_DATA_DIR", os.path.join(os.getcwd(), "data"))
    fav_path = os.getenv("FAVORITES_DB_PATH", os.path.join(data_dir, "favorites.sqlite"))
    _favorites_repo = FavoritesRepository(fav_path)
    return _favorites_repo

def get_config_repo() -> ConfigRepository:
    if not _config_repo:
        raise RuntimeError("ConfigRepo not initialized")
    return _config_repo

def get_screener_repo() -> ScreenerRepository:
    if not _screener_repo:
        raise RuntimeError("ScreenerRepo not initialized")
    return _screener_repo
