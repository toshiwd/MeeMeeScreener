from __future__ import annotations

import json
import os
import queue
import re
import subprocess
import sys
import threading
import time
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
from app.core.config import (  # noqa: E402
    APP_ENV,
    APP_VERSION,
    DATA_DIR,
    DEBUG,
    UPDATE_STATE_PATH,
    SPLIT_SUSPECTS_PATH,
    find_code_txt_path,
    _resolve_pan_code_txt_path,
    _resolve_pan_out_txt_dir,
    _resolve_update_vbs_path,
    _resolve_vbs_progress_paths,
)
from app.db.session import get_conn, try_get_conn  # noqa: E402

from app.backend.core.force_sync_job import handle_force_sync  # noqa: E402
from app.backend.core.jobs import cleanup_stale_jobs, job_manager  # noqa: E402
from app.backend.core.txt_update_job import handle_txt_update  # noqa: E402

# Register job handlers (idempotent).
job_manager.register_handler("force_sync", handle_force_sync)
job_manager.register_handler("txt_update", handle_txt_update)

UPDATE_VBS_PATH = _resolve_update_vbs_path()
USE_CODE_TXT = os.getenv("USE_CODE_TXT", "0") == "1"
STATIC_DIR = os.path.abspath(os.getenv("STATIC_DIR") or os.path.join(os.path.dirname(__file__), "static"))

_update_txt_lock = threading.Lock()
_update_txt_status = {
    "running": False,
    "phase": "idle",
    "started_at": None,
    "finished_at": None,
    "processed": 0,
    "total": 0,
    "summary": {},
    "error": None,
    "stdout_tail": [],
    "status_message": None,
    "last_updated_at": None,
}


def _cleanup_stale_jobs() -> None:
    cleanup_stale_jobs()


def _read_text_lines(path: str) -> list[str]:
    for encoding in ("utf-8", "cp932"):
        try:
            with open(path, "r", encoding=encoding, errors="ignore") as handle:
                return handle.read().splitlines()
        except OSError:
            break
    return []


def _count_codes(path: str) -> int:
    count = 0
    for line in _read_text_lines(path):
        text = line.strip()
        if not text:
            continue
        if text.startswith("#") or text.startswith("'"):
            continue
        count += 1
    return count


def _write_vbs_progress(
    *,
    phase: str,
    job_id: str | None = None,
    current: str = "",
    started: int = 0,
    ok: int = 0,
    err: int = 0,
    split: int = 0,
    error: str = "",
) -> None:
    try:
        pan_out_txt_dir = _resolve_pan_out_txt_dir()
        progress_path, _legacy_path = _resolve_vbs_progress_paths()
        os.makedirs(pan_out_txt_dir, exist_ok=True)
        payload = {
            "phase": phase,
            "job_id": job_id or "",
            "current": current,
            "started": int(started),
            "processed": int(ok) + int(err) + int(split),
            "ok": int(ok),
            "err": int(err),
            "split": int(split),
            "error": error,
            "updatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        with open(progress_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
    except Exception:
        pass


def txt_update_status():
    # Minimal status surface for UI/tests: report progress file + latest sys_jobs row if available.
    snapshot = {"running": False, "phase": "idle"}
    progress = None
    try:
        progress_path, legacy_path = _resolve_vbs_progress_paths()
        for candidate in (progress_path, legacy_path):
            if candidate and os.path.isfile(candidate):
                with open(candidate, "r", encoding="utf-8", errors="ignore") as handle:
                    text = handle.read().strip()
                if text:
                    payload = json.loads(text)
                    if isinstance(payload, dict):
                        progress = payload
                        break
    except Exception:
        progress = None

    if isinstance(progress, dict):
        phase = str(progress.get("phase") or "").strip().lower()
        snapshot["phase"] = phase or snapshot["phase"]
        if phase in ("starting", "booting", "exporting", "ingesting", "queued", "running"):
            snapshot["running"] = True
        elif phase in ("error", "done"):
            snapshot["running"] = False

    try:
        with try_get_conn(timeout_sec=0.05) as conn:
            if conn is not None:
                row = conn.execute(
                    """
                    SELECT status
                    FROM sys_jobs
                    WHERE type = 'txt_update'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ).fetchone()
                if row and row[0] in ("queued", "running"):
                    snapshot["running"] = True
                    snapshot.setdefault("phase", "running")
    except Exception:
        pass
    return snapshot


def _set_update_status(**kwargs) -> None:
    with _update_txt_lock:
        _update_txt_status.update(kwargs)


def _get_update_status_snapshot() -> dict:
    with _update_txt_lock:
        return dict(_update_txt_status)


def _append_stdout_tail(line: str) -> None:
    with _update_txt_lock:
        tail = list(_update_txt_status.get("stdout_tail") or [])
        tail.append(line)
        if len(tail) > 20:
            tail = tail[-20:]
        _update_txt_status["stdout_tail"] = tail


def _parse_vbs_summary(output: str) -> dict:
    summary: dict[str, int] = {}
    for line in output.splitlines():
        if line.startswith("SUMMARY:"):
            for key, value in re.findall(r"(\w+)=(\d+)", line):
                summary[key] = int(value)
    return summary


def _run_streaming_command(cmd: list[str], timeout: int, on_line) -> tuple[int, str, bool]:
    # This is patched in tests; keep a simple, safe default implementation.
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="cp932" if os.name == "nt" else "utf-8",
            errors="replace",
        )
        output_lines: list[str] = []
        start = time.time()
        timed_out = False
        assert process.stdout is not None
        for line in process.stdout:
            on_line(line.rstrip("\\n"))
            output_lines.append(line.rstrip("\\n"))
            if time.time() - start > timeout:
                timed_out = True
                try:
                    process.kill()
                except Exception:
                    pass
                break
        code = process.wait(timeout=1) if not timed_out else 1
        return code, "\\n".join(output_lines), timed_out
    except Exception as exc:
        return 1, str(exc), False


def _run_ingest_command() -> tuple[int, str]:
    # This is patched in tests.
    return 0, ""


def _load_update_state() -> dict:
    if not os.path.isfile(UPDATE_STATE_PATH):
        return {}
    try:
        with open(UPDATE_STATE_PATH, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}


def _save_update_state(state: dict) -> None:
    try:
        with open(UPDATE_STATE_PATH, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, indent=2)
    except OSError:
        pass


def _run_txt_update_job(code_path: str, out_dir: str) -> None:
    processed = 0
    started_count = 0
    done_count = 0

    def on_line(line: str) -> None:
        nonlocal processed, started_count, done_count
        _append_stdout_tail(line)
        stripped = line.strip()
        if stripped.startswith("START:"):
            current_code = stripped.split(":", 1)[1].strip()
            if current_code:
                _set_update_status(status_message=f"Exporting {current_code}")
            started_count += 1
        is_ok = stripped.startswith("OK")
        is_err = stripped.startswith("ERROR")
        is_split = stripped.startswith("SPLIT")
        if is_ok or is_err or is_split:
            done_count += 1

        updated_processed = max(started_count, done_count)
        if updated_processed != processed:
            processed = updated_processed
            _set_update_status(processed=processed)

    try:
        _set_update_status(phase="exporting", status_message="Exporting...")
        _write_vbs_progress(phase="starting")
        sys_root = os.environ.get("SystemRoot") or "C:\\\\Windows"
        cscript = os.path.join(sys_root, "SysWOW64", "cscript.exe")
        if not os.path.isfile(cscript):
            cscript = os.path.join(sys_root, "System32", "cscript.exe")
        vbs_cmd = [cscript, "//nologo", UPDATE_VBS_PATH, code_path, out_dir]
        timeout_sec = 1800
        vbs_code, vbs_output, timed_out = _run_streaming_command(vbs_cmd, timeout=timeout_sec, on_line=on_line)
        summary = _parse_vbs_summary(vbs_output)
        _set_update_status(summary=summary)
        if summary:
            snapshot = _get_update_status_snapshot()
            summary_total = summary.get("total")
            summary_done = sum(value for key, value in summary.items() if key in ("ok", "err", "split"))
            if isinstance(summary_total, int) and (snapshot.get("total") is None or snapshot.get("total", 0) <= 0):
                _set_update_status(total=summary_total)
            if isinstance(summary_done, int) and summary_done > processed:
                processed = summary_done
                _set_update_status(processed=processed)
            if isinstance(summary_total, int) and processed > summary_total:
                processed = summary_total
                _set_update_status(processed=processed)
        if timed_out:
            _write_vbs_progress(phase="error", error=f"timeout:{timeout_sec}s")
            _set_update_status(running=False, phase="error", error="timeout", finished_at=datetime.now().isoformat(), timeout_sec=timeout_sec)
            return
        if vbs_code != 0:
            _write_vbs_progress(phase="error", error=f"vbs_failed:{vbs_code}")
            _set_update_status(running=False, phase="error", error=f"vbs_failed:{vbs_code}", finished_at=datetime.now().isoformat())
            return

        _set_update_status(phase="ingesting", status_message="Ingesting...")
        ingest_code, ingest_output = _run_ingest_command()
        for line in ingest_output.splitlines():
            _append_stdout_tail(line)
        if ingest_code != 0:
            _set_update_status(running=False, phase="error", error=f"ingest_failed:{ingest_code}", finished_at=datetime.now().isoformat(), summary=summary)
            return

        state = _load_update_state()
        state["last_txt_update_date"] = datetime.now().date().isoformat()
        state["last_txt_update_at"] = datetime.now().isoformat()
        _save_update_state(state)
        _set_update_status(
            running=False,
            phase="done",
            error=None,
            finished_at=datetime.now().isoformat(),
            summary=summary,
            last_updated_at=state.get("last_txt_update_at"),
            processed=processed,
        )
    except Exception as exc:
        _append_stdout_tail(str(exc))
        _set_update_status(running=False, phase="error", error=f"update_txt_failed:{exc}", finished_at=datetime.now().isoformat())


def get_txt_status() -> dict:
    pan_out_txt_dir = _resolve_pan_out_txt_dir()
    if not os.path.isdir(pan_out_txt_dir):
        return {"txt_count": 0, "code_txt_missing": False, "last_updated": None}

    txt_files = [
        os.path.join(pan_out_txt_dir, name)
        for name in os.listdir(pan_out_txt_dir)
        if name.endswith(".txt") and name.lower() != "code.txt"
    ]
    code_txt_missing = False
    if USE_CODE_TXT:
        code_txt_missing = find_code_txt_path(pan_out_txt_dir) is None
    last_updated = None
    if txt_files:
        last_updated = max(os.path.getmtime(path) for path in txt_files)
        last_updated = datetime.utcfromtimestamp(last_updated).isoformat() + "Z"

    return {"txt_count": len(txt_files), "code_txt_missing": code_txt_missing, "last_updated": last_updated}


def _list_tables(conn) -> set[str]:
    rows = conn.execute("SELECT table_name FROM duckdb_tables()").fetchall()
    return {row[0] for row in rows}


def _collect_db_stats() -> dict:
    stats = {
        "tickers": None,
        "daily_rows": None,
        "monthly_rows": None,
        "trade_events": None,
        "positions_live": None,
        "position_rounds": None,
        "missing_tables": [],
        "errors": [],
    }
    required_tables = [
        "tickers",
        "daily_bars",
        "monthly_bars",
        "daily_ma",
        "monthly_ma",
        "trade_events",
        "positions_live",
        "position_rounds",
        "initial_positions_seed",
    ]
    try:
        with get_conn() as conn:
            tables = _list_tables(conn)
            stats["missing_tables"] = [name for name in required_tables if name not in tables]
            if "tickers" in tables:
                stats["tickers"] = conn.execute("SELECT COUNT(*) FROM tickers").fetchone()[0]
            if "daily_bars" in tables:
                stats["daily_rows"] = conn.execute("SELECT COUNT(*) FROM daily_bars").fetchone()[0]
            if "monthly_bars" in tables:
                stats["monthly_rows"] = conn.execute("SELECT COUNT(*) FROM monthly_bars").fetchone()[0]
            if "trade_events" in tables:
                stats["trade_events"] = conn.execute("SELECT COUNT(*) FROM trade_events").fetchone()[0]
            if "positions_live" in tables:
                stats["positions_live"] = conn.execute("SELECT COUNT(*) FROM positions_live").fetchone()[0]
            if "position_rounds" in tables:
                stats["position_rounds"] = conn.execute("SELECT COUNT(*) FROM position_rounds").fetchone()[0]
    except Exception as exc:
        stats["errors"].append(str(exc))
    return stats


def _get_last_updated_timestamp():
    try:
        if os.path.isfile(UPDATE_STATE_PATH):
            with open(UPDATE_STATE_PATH, "r", encoding="utf-8") as handle:
                state = json.load(handle)
                return state.get("last_txt_update_at")
    except Exception:
        pass
    return datetime.now().isoformat()


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
