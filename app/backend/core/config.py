import os
import sys
import json
import logging
from pathlib import Path

# --- Constants & Defaults ---
APP_NAME = "MeeMeeScreener"
DEFAULT_DATA_DIR_NAME = "data"
CONFIG_FILENAME = "meemee.config.json"
PORTABLE_FLAG_FILENAME = "portable.flag"

# --- Logging Setup (Preliminary) ---
# Main logging setup should happen in main.py, but we might need simple logging here.
logger = logging.getLogger(__name__)

def _get_exe_dir() -> Path:
    """Returns the directory containing the executable or script."""
    if getattr(sys, 'frozen', False):
        return Path(sys.executable).parent
    # In dev mode, use the repo root context if possible, or the script dir
    # Here we assume this file is in app/backend/core/
    # Repo root is 3 levels up: app/backend/core -> app/backend -> app -> ROOT
    return Path(__file__).resolve().parent.parent.parent.parent

def _resolve_data_dir() -> Path:
    """
    Resolves the data directory based on priority:
    1. Environment Variable: MEEMEE_DATA_DIR
    2. Config File: meemee.config.json (next to exe)
    3. Portable Flag: portable.flag (next to exe) -> <exe_dir>/data
    4. Default: %LOCALAPPDATA%/MeeMeeScreener/data
    """
    exe_dir = _get_exe_dir()
    
    # 1. Environment Variable
    env_path = os.getenv("MEEMEE_DATA_DIR")
    if env_path:
        path = Path(env_path).resolve()
        logger.info(f"DataDir resolved via env var: {path}")
        return path

    # 2. Config File
    config_path = exe_dir / CONFIG_FILENAME
    if config_path.exists():
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if "dataDir" in data and data["dataDir"]:
                    path = Path(data["dataDir"]).resolve()
                    logger.info(f"DataDir resolved via config file: {path}")
                    return path
        except Exception as e:
            logger.warning(f"Failed to read {CONFIG_FILENAME}: {e}")

    # 3. Portable Flag
    portable_flag = exe_dir / PORTABLE_FLAG_FILENAME
    if portable_flag.exists():
        path = exe_dir / DEFAULT_DATA_DIR_NAME
        logger.info(f"DataDir resolved via portable flag: {path}")
        return path

    # 4. Default LocalAppData
    # Windows: %LOCALAPPDATA%
    local_app_data = os.getenv("LOCALAPPDATA")
    if not local_app_data:
        # Fallback for non-Windows or odd environment
        local_app_data = str(Path.home())
    
    path = Path(local_app_data) / APP_NAME / DEFAULT_DATA_DIR_NAME
    logger.info(f"DataDir resolved via default: {path}")
    return path

# --- Global Config Object ---
class AppConfig:
    def __init__(self):
        self.REPO_ROOT = _get_exe_dir()
        self.DATA_DIR = _resolve_data_dir()
        self.ensure_dirs()
        
    def ensure_dirs(self):
        """Ensure critical directories exist."""
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)
        (self.DATA_DIR / "txt").mkdir(exist_ok=True)
        (self.DATA_DIR / "logs").mkdir(exist_ok=True)

    @property
    def DB_PATH(self) -> Path:
        return self.DATA_DIR / "stocks.duckdb"

    @property
    def FAVORITES_DB_PATH(self) -> Path:
        return self.DATA_DIR / "favorites.sqlite"

    @property
    def PRACTICE_DB_PATH(self) -> Path:
        return self.DATA_DIR / "practice.sqlite"

    @property
    def LOG_FILE_PATH(self) -> Path:
        return self.DATA_DIR / "logs" / "app.log"
    
    @property
    def PAN_OUT_TXT_DIR(self) -> Path:
        return self.DATA_DIR / "txt"

    @property
    def LOCK_FILE_PATH(self) -> Path:
        return self.DATA_DIR / "app.lock"

    @property
    def PAN_CODE_TXT_PATH(self) -> Path:
        # Check env var for override, else default to tools/code.txt relative to REPO_ROOT
        env = os.getenv("PAN_CODE_TXT_PATH") 
        if env: return Path(env)
        return self.REPO_ROOT / "tools" / "code.txt"

    @property
    def PAN_EXPORT_VBS_PATH(self) -> Path:
        # Check env vars for override (launcher sets PAN_EXPORT_VBS_PATH)
        env = os.getenv("PAN_EXPORT_VBS_PATH") or os.getenv("UPDATE_VBS_PATH")
        if env:
            return Path(env)
        legacy = self.REPO_ROOT / "tools" / "PanRollingExport.vbs"
        preferred = self.REPO_ROOT / "tools" / "export_pan.vbs"
        return preferred if preferred.exists() else legacy

# Singleton Instance
config = AppConfig()
