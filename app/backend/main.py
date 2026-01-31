from __future__ import annotations

import os
import sys
import traceback
from datetime import datetime
from pathlib import Path

# Tests (and some legacy tooling) put only `app/backend` on sys.path and import `main`.
# Make the repo root importable so `import app.*` works.
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
_backend_dir = Path(__file__).resolve().parent
if str(_backend_dir) not in sys.path:
    sys.path.insert(0, str(_backend_dir))

from app.main import app  # noqa: E402
from app.db.session import get_conn, try_get_conn  # noqa: E402
from app.backend.services.system_status import (
    _collect_db_stats,
    _get_last_updated_timestamp,
)
from app.backend.core.force_sync_job import handle_force_sync  # noqa: E402
from app.backend.core.jobs import cleanup_stale_jobs, job_manager  # noqa: E402
from app.backend.core.txt_update_job import (
    _load_update_state,
    _save_update_state,
    handle_txt_update,
    run_vbs_export,
)  # noqa: E402

# Register job handlers (idempotent).
job_manager.register_handler("force_sync", handle_force_sync)
job_manager.register_handler("txt_update", handle_txt_update)

STATIC_DIR = os.path.abspath(os.getenv("STATIC_DIR") or os.path.join(os.path.dirname(__file__), "static"))
_RESOLVED_PATHS_LOGGED = False


def _resolve_backend_log_path() -> Path:
    data_dir = os.getenv("MEEMEE_DATA_DIR")
    if data_dir:
        log_dir = Path(data_dir) / "logs"
    else:
        log_dir = _repo_root / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / "backend.log"


def _log_resolved_paths_once() -> None:
    global _RESOLVED_PATHS_LOGGED
    if _RESOLVED_PATHS_LOGGED:
        return
    _RESOLVED_PATHS_LOGGED = True
    exe_dir = os.path.dirname(sys.executable)
    cwd = os.getcwd()
    app_env = os.getenv("APP_ENV", "")
    data_dir = os.getenv("MEEMEE_DATA_DIR", "")
    data_store = os.getenv("MEEMEE_DATA_STORE", "")
    db_path = os.getenv("STOCKS_DB_PATH", "")
    auto_update_enabled = os.getenv("MEEMEE_ENABLE_AUTO_UPDATE", "").lower() in ("1", "true", "yes", "on")
    print(
        "[backend] Resolved paths:"
        f" exe_dir={exe_dir}"
        f" cwd={cwd}"
        f" APP_ENV={app_env}"
        f" MEEMEE_DATA_DIR={data_dir}"
        f" MEEMEE_DATA_STORE={data_store}"
        f" STOCKS_DB_PATH={db_path}"
        f" auto_update_enabled={auto_update_enabled}"
    )


def _install_excepthook() -> None:
    log_path = _resolve_backend_log_path()

    def _hook(exctype, value, tb):
        try:
            with open(log_path, "a", encoding="utf-8") as handle:
                handle.write("".join(traceback.format_exception(exctype, value, tb)))
        except Exception:
            pass
        sys.__excepthook__(exctype, value, tb)

    sys.excepthook = _hook


_install_excepthook()
_log_resolved_paths_once()

def _cleanup_stale_jobs() -> None:
    cleanup_stale_jobs()


def _run_txt_update_job(code_path: str, out_dir: str) -> None:
    now = datetime.now()
    print(f"[main] Running TXT update job: {code_path} -> {out_dir}")
    vbs_code, vbs_output = run_vbs_export(code_path, out_dir)
    summary_line = next((line for line in vbs_output if "SUMMARY:" in line), "Export completed")
    if vbs_code != 0:
        raise RuntimeError(f"VBS export failed ({vbs_code}): {summary_line}")

    state = _load_update_state()
    timestamp = datetime.now()
    state["last_txt_update_at"] = timestamp.isoformat()
    state["last_txt_update_date"] = timestamp.date().isoformat()
    _save_update_state(state)


def _resolve_static_file(request_path: str) -> str | None:
    if not STATIC_DIR or not os.path.isdir(STATIC_DIR):
        return None
    safe_path = os.path.abspath(os.path.join(STATIC_DIR, request_path))
    if os.path.commonpath([STATIC_DIR, safe_path]) != STATIC_DIR:
        return None
    if os.path.isdir(safe_path):
        safe_path = os.path.join(safe_path, "index.html")
    if os.path.isfile(safe_path):
        return safe_path
    return None
