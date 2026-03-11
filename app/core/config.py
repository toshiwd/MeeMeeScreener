import json
import logging
import os
import re
import sys
from pathlib import Path

# --- Constants & Defaults ---
APP_NAME = "MeeMeeScreener"
DEFAULT_DATA_DIR_NAME = "data"
CONFIG_FILENAME = "meemee.config.json"
PORTABLE_FLAG_FILENAME = "portable.flag"

logger = logging.getLogger(__name__)


def _get_exe_dir() -> Path:
    """Returns the directory containing the executable or repo root in dev."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent
    # In dev mode, this file is at app/core/config.py, so repo root is 2 levels up.
    return Path(__file__).resolve().parent.parent.parent


def _default_app_storage_name() -> str:
    if os.getenv("MEEMEE_SELFTEST", "").strip().lower() in {"1", "true", "yes", "on"}:
        return f"{APP_NAME}-selftest"
    if os.getenv("MEEMEE_DEV", "").strip().lower() in {"1", "true", "yes", "on"}:
        return f"{APP_NAME}-dev"
    if os.getenv("MEEMEE_DEV_MODE", "").strip().lower() in {"1", "true", "yes", "on"}:
        return f"{APP_NAME}-dev"
    app_env = (os.getenv("APP_ENV") or os.getenv("ENV") or "dev").strip().lower()
    if app_env in {"dev", "development", "test"}:
        return f"{APP_NAME}-dev"
    return APP_NAME


def _resolve_data_dir() -> Path:
    """
    Resolves the data directory based on priority:
    1. Environment Variable: MEEMEE_DATA_DIR
    2. Config File: meemee.config.json (next to exe)
    3. Portable Flag: portable.flag (next to exe) -> <exe_dir>/data
    4. Default: %LOCALAPPDATA%/<env-specific-app>/data
    """
    exe_dir = _get_exe_dir()

    env_path = os.getenv("MEEMEE_DATA_DIR")
    if env_path:
        path = Path(env_path).resolve()
        logger.info("DataDir resolved via env var: %s", path)
        return path

    config_path = exe_dir / CONFIG_FILENAME
    if config_path.exists():
        try:
            with open(config_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
                if data.get("dataDir"):
                    path = Path(str(data["dataDir"])).resolve()
                    logger.info("DataDir resolved via config file: %s", path)
                    return path
        except Exception as exc:
            logger.warning("Failed to read %s: %s", CONFIG_FILENAME, exc)

    portable_flag = exe_dir / PORTABLE_FLAG_FILENAME
    if portable_flag.exists():
        path = exe_dir / DEFAULT_DATA_DIR_NAME
        logger.info("DataDir resolved via portable flag: %s", path)
        return path

    local_app_data = os.getenv("LOCALAPPDATA") or str(Path.home())
    path = Path(local_app_data) / _default_app_storage_name() / DEFAULT_DATA_DIR_NAME
    logger.info("DataDir resolved via default: %s", path)
    return path


def _get_config_path() -> Path:
    return _get_exe_dir() / CONFIG_FILENAME


def write_data_dir_override(data_dir: Path | str) -> Path:
    target = Path(data_dir).expanduser().resolve()
    target.mkdir(parents=True, exist_ok=True)
    config_path = _get_config_path()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with open(config_path, "w", encoding="utf-8") as handle:
        json.dump({"dataDir": str(target)}, handle, ensure_ascii=False, indent=2)
    logger.info("Wrote MEEMEE_DATA_DIR override: %s -> %s", config_path, target)
    return config_path


class AppConfig:
    def __init__(self) -> None:
        self.REPO_ROOT = _get_exe_dir()
        self.DATA_DIR = _resolve_data_dir()
        self.ensure_dirs()

    def ensure_dirs(self) -> None:
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)
        (self.DATA_DIR / "txt").mkdir(exist_ok=True)
        (self.DATA_DIR / "logs").mkdir(exist_ok=True)

    @property
    def DB_PATH(self) -> Path:
        env = os.getenv("STOCKS_DB_PATH")
        if env:
            return Path(env)
        return self.DATA_DIR / "stocks.duckdb"

    @property
    def FAVORITES_DB_PATH(self) -> Path:
        env = os.getenv("FAVORITES_DB_PATH")
        if env:
            return Path(env)
        return self.DATA_DIR / "favorites.sqlite"

    @property
    def PRACTICE_DB_PATH(self) -> Path:
        env = os.getenv("PRACTICE_DB_PATH")
        if env:
            return Path(env)
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
        env = os.getenv("PAN_CODE_TXT_PATH")
        if env:
            return Path(env)
        return self.REPO_ROOT / "tools" / "code.txt"

    @property
    def PAN_EXPORT_VBS_PATH(self) -> Path:
        env = os.getenv("PAN_EXPORT_VBS_PATH") or os.getenv("UPDATE_VBS_PATH")
        if env:
            return Path(env)
        preferred_root = self.REPO_ROOT / "export_pan.vbs"
        if preferred_root.exists():
            return preferred_root
        preferred = self.REPO_ROOT / "tools" / "export_pan.vbs"
        if preferred.exists():
            return preferred
        legacy = self.REPO_ROOT / "tools" / "PanRollingExport.vbs"
        return legacy


config = AppConfig()


# --- Main.py-compatible constants/helpers (moved out of monolithic main) ---
REPO_ROOT = str(config.REPO_ROOT)
DATA_DIR = str(config.DATA_DIR)
DEFAULT_DB_PATH = str(config.DB_PATH)
FAVORITES_DB_PATH = str(config.FAVORITES_DB_PATH)
PRACTICE_DB_PATH = str(config.PRACTICE_DB_PATH)
SPLIT_SUSPECTS_PATH = str(config.DATA_DIR / "_split_suspects.csv")
DEFAULT_UPDATE_STATE_PATH = str(config.DATA_DIR / "update_state.json")
UPDATE_STATE_PATH = os.path.abspath(os.getenv("UPDATE_STATE_PATH") or DEFAULT_UPDATE_STATE_PATH)

APP_VERSION = os.getenv("APP_VERSION", "dev")
APP_ENV = os.getenv("APP_ENV") or os.getenv("ENV") or "dev"
DEBUG = os.getenv("DEBUG", "0") == "1"


def resolve_trade_csv_paths() -> list[str]:
    paths: list[str] = []
    preferred_names = (
        "rakuten_trade_history.csv",
        "sbi_trade_history.csv",
        "楽天証券取引履歴.csv",
        "SBI証券取引履歴.csv",
    )
    legacy_names = (
        "????????.csv",
        "SBI??????.csv",
        "????????????????.csv",
        "SBI????????????.csv",
    )
    def _scan_dir(base: Path) -> None:
        if not base or not base.is_dir():
            return
        # Common filenames (preferred).
        for filename in (*preferred_names, *legacy_names):
            candidate = base / filename
            if candidate.exists():
                paths.append(str(candidate))
        # Fallback: pick anything that looks like a trade history CSV.
        try:
            for entry in base.iterdir():
                if not entry.is_file():
                    continue
                if entry.suffix.lower() != ".csv":
                    continue
                name = entry.name.lower()
                if "????????" not in entry.name and "trade" not in name:
                    continue
                if any(key in entry.name for key in ("????", "??????", "SBI")) or any(key in name for key in ("rakuten", "sbi")):
                    paths.append(str(entry))
        except OSError:
            return

    # Legacy builds saved CSVs directly under DATA_DIR, newer launcher uses DATA_DIR/csv.
    _scan_dir(config.DATA_DIR)
    _scan_dir(config.DATA_DIR / "csv")

    trade_csv_dir = os.getenv("TRADE_CSV_DIR")
    if trade_csv_dir:
        try:
            _scan_dir(Path(trade_csv_dir))
        except Exception:
            pass

    env = os.getenv("TRADE_CSV_PATH")
    if env:
        parts = [p.strip() for p in re.split(r"[;,\n]+", env) if p.strip()]
        paths.extend([os.path.abspath(part) for part in parts])

    # Old repo-local defaults (fallback)
    for filename in (*preferred_names, *legacy_names):
        candidate = os.path.abspath(os.path.join(REPO_ROOT, "data", filename))
        if os.path.isfile(candidate):
            paths.append(candidate)

    unique_paths = list(set(paths))
    if not unique_paths and not paths:
        unique_paths.append(str(config.DATA_DIR / preferred_names[0]))
    return unique_paths


def resolve_trade_csv_dir() -> str:
    env = os.getenv("TRADE_CSV_DIR")
    if env:
        return os.path.abspath(env)
    csv_dir = config.DATA_DIR / "csv"
    return str(csv_dir if csv_dir.exists() else config.DATA_DIR)


def canonical_trade_csv_path(broker: str) -> str:
    base_dir = Path(resolve_trade_csv_dir())
    if broker == "sbi":
        return str(base_dir / "SBI証券取引履歴.csv")
    return str(base_dir / "楽天証券取引履歴.csv")




def resolve_pan_out_txt_dir() -> str:
    return os.path.abspath(os.getenv("PAN_OUT_TXT_DIR") or os.getenv("TXT_DATA_DIR") or str(config.PAN_OUT_TXT_DIR))


def resolve_pan_code_txt_path() -> str:
    return os.path.abspath(str(config.PAN_CODE_TXT_PATH))


def resolve_update_vbs_path() -> str:
    explicit = os.getenv("PAN_EXPORT_VBS_PATH") or os.getenv("UPDATE_VBS_PATH")
    if explicit:
        return os.path.abspath(explicit)
    return os.path.abspath(str(config.PAN_EXPORT_VBS_PATH))


def resolve_vbs_progress_paths() -> tuple[str, str]:
    out_dir = resolve_pan_out_txt_dir()
    return (os.path.join(out_dir, "vbs_progress.json"), os.path.join(out_dir, "_vbs_progress.json"))


# --- Backward-compatible aliases (from legacy main.py) ---
_resolve_pan_out_txt_dir = resolve_pan_out_txt_dir
_resolve_pan_code_txt_path = resolve_pan_code_txt_path
_resolve_update_vbs_path = resolve_update_vbs_path
_resolve_vbs_progress_paths = resolve_vbs_progress_paths
_canonical_trade_csv_path = canonical_trade_csv_path


def find_code_txt_path(data_dir: str) -> str | None:
    primary = _resolve_pan_code_txt_path()
    if os.path.exists(primary):
        return primary
    try:
        parent = os.path.abspath(os.path.join(data_dir, os.pardir))
        sibling = os.path.join(parent, "code.txt")
        if os.path.exists(sibling):
            return sibling
    except Exception:
        pass
    return None
