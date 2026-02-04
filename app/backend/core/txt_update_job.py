from __future__ import annotations

import io
import json
import logging
import os
import subprocess
import time
import traceback
from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime

from .config import config
from .jobs import job_manager

try:
    from app.backend import ingest_txt
except ImportError:
    try:
        import ingest_txt  # type: ignore
    except ImportError:
        ingest_txt = None

logger = logging.getLogger(__name__)


def _update_vbs_path() -> str:
    return os.path.abspath(str(config.PAN_EXPORT_VBS_PATH))


def _pan_out_txt_dir() -> str:
    return os.path.abspath(str(config.PAN_OUT_TXT_DIR))


def _pan_code_txt_path() -> str:
    return os.path.abspath(str(config.PAN_CODE_TXT_PATH))


def _update_state_path() -> str:
    default_path = str(config.DATA_DIR / "update_state.json")
    return os.path.abspath(os.getenv("UPDATE_STATE_PATH") or default_path)


def _load_update_state() -> dict:
    path = _update_state_path()
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)  # type: ignore
    except Exception:
        return {}


def _save_update_state(state: dict) -> None:
    path = _update_state_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _run_phase_batch_latest() -> int:
    try:
        from app.backend.db import get_conn
    except ModuleNotFoundError:  # pragma: no cover - legacy tooling may import from app/backend on sys.path
        from db import get_conn  # type: ignore
    try:
        from app.backend.jobs.phase_batch import run_batch
    except ModuleNotFoundError:  # pragma: no cover
        from jobs.phase_batch import run_batch  # type: ignore

    with get_conn() as conn:
        row = conn.execute("SELECT MAX(dt) FROM feature_snapshot_daily").fetchone()
    if not row or row[0] is None:
        raise RuntimeError("feature_snapshot_daily is empty")
    max_dt = int(row[0])
    run_batch(max_dt, max_dt, dry_run=False)
    return max_dt


def run_vbs_export(code_path: str, out_dir: str, timeout: int = 1800) -> tuple[int, list[str]]:
    sys_root = os.environ.get("SystemRoot") or "C:\\Windows"
    cscript = os.path.join(sys_root, "SysWOW64", "cscript.exe")
    if not os.path.isfile(cscript):
        cscript = os.path.join(sys_root, "System32", "cscript.exe")

    cmd = [cscript, "//nologo", _update_vbs_path(), str(code_path), str(out_dir)]
    logger.info("Running VBS export: %s", cmd)
    output_lines: list[str] = []

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="cp932",
            errors="replace",
            bufsize=1,
        )
    except Exception as exc:
        logger.exception("Failed to start VBS process")
        return -1, [f"Failed to start VBS: {exc}"]

    start_ts = time.time()
    try:
        if not process.stdout:
            raise RuntimeError("VBS stdout pipe is not available")
        while True:
            line = process.stdout.readline()
            if not line:
                break
            text = line.rstrip("\r\n")
            output_lines.append(text)
            print(f"[txt_update_job] {text}")
            if time.time() - start_ts > timeout:
                process.kill()
                raise subprocess.TimeoutExpired(cmd, timeout)
        return_code = process.wait()
        output_lines.append(f"[txt_update_job] VBS exit code {return_code}")
        return return_code, output_lines
    except subprocess.TimeoutExpired as exc:
        logger.error("VBS export timed out")
        process.kill()
        output_lines.append("Timeout expired")
        return -1, output_lines
    except Exception as exc:
        logger.exception("VBS export failed")
        process.kill()
        output_lines.append(str(exc))
        return -1, output_lines


def run_ingest(incremental: bool = True) -> tuple[str, str, dict]:
    print(f"[txt_update_job] run_ingest called incremental={incremental}")
    if not ingest_txt:
        error = "ingest_txt module not found"
        print(f"[txt_update_job] ERROR: {error}")
        return "", error, {}

    buffer = io.StringIO()
    stats: dict[str, int | str] = {}
    try:
        with redirect_stdout(buffer), redirect_stderr(buffer):
            ingest_txt.ingest(incremental=incremental)
        output = buffer.getvalue()
        for line in output.splitlines():
            if "Incremental Mode: Found" in line:
                parts = line.split()
                for idx, token in enumerate(parts):
                    if token == "Found" and idx + 1 < len(parts):
                        stats["changed"] = parts[idx + 1]
                    if token == "skipped" and idx + 1 < len(parts):
                        stats["skipped"] = parts[idx + 1].rstrip(".")
            if "Inserted" in line and "daily rows" in line:
                pieces = line.split()
                if len(pieces) >= 2:
                    stats["rows"] = pieces[1]
        print(f"[txt_update_job] run_ingest completed, stats={stats}")
        return output, "", stats
    except Exception as exc:
        print(f"[txt_update_job] run_ingest exception: {exc}")
        traceback.print_exc(file=buffer)
        return buffer.getvalue(), str(exc), {}


def handle_txt_update(job_id: str, payload: dict) -> None:
    job_manager._update_db(job_id, "txt_update", "running", message="Initializing update...", progress=0)
    code_path = _pan_code_txt_path()
    out_dir = _pan_out_txt_dir()

    if not os.path.isfile(code_path):
        error_msg = f"code.txt not found at {code_path}"
        print(f"[txt_update_job] ERROR: {error_msg}")
        job_manager._update_db(
            job_id, "txt_update", "failed", error=error_msg, message=error_msg, finished_at=datetime.now()
        )
        return

    os.makedirs(out_dir, exist_ok=True)
    job_manager._update_db(job_id, "txt_update", "running", message="Running Pan Rolling export...", progress=0)

    vbs_code, output_lines = run_vbs_export(code_path, out_dir)
    summary_line = next((line for line in output_lines if "SUMMARY:" in line), "Export completed")

    if vbs_code != 0:
        msg = output_lines[-1] if output_lines else "VBS failed"
        job_manager._update_db(
            job_id,
            "txt_update",
            "failed",
            message=f"{summary_line}: {msg}",
            error=f"VBS failed with code {vbs_code}",
            finished_at=datetime.now(),
        )
        return

    job_manager._update_db(
        job_id,
        "txt_update",
        "success",
        message=f"{summary_line}. Export completed.",
        progress=70,
    )

    # Run Ingest (Incremental)
    job_manager._update_db(job_id, "txt_update", "running", message="Ingesting (Incremental)...", progress=85)
    ingest_out, ingest_err, stats = run_ingest(incremental=True)
    if ingest_err:
        job_manager._update_db(
            job_id,
            "txt_update",
            "failed",
            error="Ingest Failed",
            message=f"Ingest Error: {ingest_err}",
            finished_at=datetime.now(),
        )
        return

    job_manager._update_db(job_id, "txt_update", "running", message="Phase予測を更新中...", progress=92)
    try:
        phase_dt = _run_phase_batch_latest()
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message=f"Phase予測を更新しました (dt={phase_dt})",
            progress=95,
        )
    except Exception as exc:
        job_manager._update_db(
            job_id,
            "txt_update",
            "failed",
            error="Phase update failed",
            message=f"Phase update failed: {exc}",
            finished_at=datetime.now(),
        )
        return

    _save_update_state(
        {
            "last_txt_update_at": datetime.now().isoformat(),
            "last_txt_update_date": datetime.now().date().isoformat(),
        }
    )
    try:
        from app.backend.services import rankings_cache
        rankings_cache.refresh_cache()
    except Exception as exc:
        print(f"[txt_update_job] ranking cache refresh failed: {exc}")
    job_manager._update_db(
        job_id,
        "txt_update",
        "success",
        message=f"{summary_line}. Ingest + Phase completed.",
        progress=100,
        finished_at=datetime.now(),
    )


def run_vbs_update(job_id: str, code_path: str, out_dir: str, *, timeout: int = 1800) -> tuple[int, list[str]]:
    """Legacy wrapper so callers can keep passing job_id first."""
    return run_vbs_export(code_path, out_dir, timeout=timeout)
