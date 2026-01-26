
import os
import sys
import time
import subprocess
import threading
import logging
import traceback
import io
import json
import re
from datetime import datetime
from contextlib import redirect_stdout, redirect_stderr

from .config import config
from .jobs import job_manager

# Try to import ingest_txt
try:
    from app.backend import ingest_txt
except ImportError:
    try:
        import ingest_txt
    except ImportError:
        ingest_txt = None

logger = logging.getLogger(__name__)
_CODE_RE = re.compile(r"^\d{4}[A-Z]?$")

def _update_vbs_path() -> str:
    # Resolve at call time so launcher can set env vars before/after import safely.
    return os.path.abspath(str(config.PAN_EXPORT_VBS_PATH))


def _pan_out_txt_dir() -> str:
    return os.path.abspath(str(config.PAN_OUT_TXT_DIR))


def _pan_code_txt_path() -> str:
    # config.PAN_CODE_TXT_PATH checks PAN_CODE_TXT_PATH env var internally.
    return os.path.abspath(str(config.PAN_CODE_TXT_PATH))


def _update_state_path() -> str:
    default_path = str(config.DATA_DIR / "update_state.json")
    # Keep consistent with app/backend/main.py which can override this via env var (launcher sets it).
    return os.path.abspath(os.getenv("UPDATE_STATE_PATH") or default_path)


def _progress_json_path() -> str:
    return os.path.join(_pan_out_txt_dir(), "vbs_progress.json")


def _write_progress_file(
    *,
    phase: str,
    job_id: str = "",
    current: str = "",
    started: int = 0,
    ok: int = 0,
    err: int = 0,
    split: int = 0,
    error: str = ""
) -> None:
    # Some environments don't stream stdout from cscript reliably. Persist a tiny JSON
    # progress file under the txt folder so the UI can still show progress/errors.
    try:
        pan_out_txt_dir = _pan_out_txt_dir()
        os.makedirs(pan_out_txt_dir, exist_ok=True)
        payload = {
            "phase": phase,
            "job_id": job_id,
            "current": current,
            "started": int(started),
            "processed": int(ok) + int(err) + int(split),
            "ok": int(ok),
            "err": int(err),
            "split": int(split),
            "error": error,
            "updatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        with open(_progress_json_path(), "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
    except Exception:
        pass

def _save_update_state(state: dict) -> None:
    try:
        update_state_path = _update_state_path()
        os.makedirs(os.path.dirname(update_state_path), exist_ok=True)
        with open(update_state_path, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, indent=2)
    except OSError:
        pass

def run_ingest(incremental: bool = True) -> tuple[str, str, dict]:
    """Runs ingest_txt.ingest() capturing stdout/stderr. Returns (stdout, stderr, stats)."""
    print(f"[txt_update_job] run_ingest called, incremental={incremental}")
    if not ingest_txt:
        print("[txt_update_job] ERROR: ingest_txt module not found")
        return "", "ingest_txt module not found", {}
        
    s_out = io.StringIO()
    stats = {}

    try:
        with redirect_stdout(s_out), redirect_stderr(s_out):
            ingest_txt.ingest(incremental=incremental)
        
        # Parse stdout for stats (simple scraping)
        output = s_out.getvalue()
        for line in output.splitlines():
            if "Incremental Mode: Found" in line:
                 # e.g. "Incremental Mode: Found 12 changed files, skipped 300."
                 parts = line.split()
                 for i, p in enumerate(parts):
                     if p == "Found": stats["changed"] = parts[i+1]
                     if p == "skipped": stats["skipped"] = parts[i+1].rstrip(".")
            if "Inserted" in line and "daily rows" in line:
                 # e.g. "Inserted 123 daily rows"
                 parts = line.split()
                 if len(parts) >= 2: stats["rows"] = parts[1]

        print(f"[txt_update_job] run_ingest completed, stats={stats}")
        return output, "", stats
    except Exception as e:
        print(f"[txt_update_job] run_ingest exception: {e}")
        traceback.print_exc(file=s_out)
        return s_out.getvalue(), str(e), {}

def run_vbs_update(
    job_id: str,
    code_path: str,
    out_dir: str,
    total_count: int | None = None
) -> tuple[int, str, dict]:
    """Runs the VBScript to export TXT files, streaming progress lines."""
    print(f"[txt_update_job] run_vbs_update called")
    sys_root = os.environ.get("SystemRoot") or "C:\\Windows"
    cscript = os.path.join(sys_root, "SysWOW64", "cscript.exe")
    if not os.path.isfile(cscript):
        cscript = os.path.join(sys_root, "System32", "cscript.exe")

    cmd = [cscript, "//nologo", _update_vbs_path(), str(code_path), str(out_dir)]

    print(f"[txt_update_job] VBS command: {cmd}")
    logger.info(f"Job {job_id}: Running VBS: {cmd}")

    processed = 0
    started_count = 0
    done_count = 0
    ok_count = 0
    err_count = 0
    split_count = 0
    summary: dict[str, int | None] = {
        "processed": None,
        "ok": None,
        "err": None,
        "split": None,
        "total": total_count
    }

    try:
        _write_progress_file(phase="starting", job_id=job_id, current="", started=0, ok=0, err=0, split=0, error="")
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            encoding="cp932",  # VBS usually outputs cp932 on JP Windows
            errors="replace"
        )
        output_lines: list[str] = []
        last_progress_ts = 0.0
        start_ts = time.time()

        if process.stdout is None:
            raise RuntimeError("VBS stdout pipe not available")

        for raw_line in process.stdout:
            line = raw_line.rstrip("\r\n")
            output_lines.append(line)

            normalized = line.strip()
            is_start = normalized.startswith("START:")

            # VBS also prints summary lines like "OK   : 679" and "SPLIT_SUSPECT: 0".
            # Only count per-code lines by validating the extracted code token.
            is_ok = False
            is_err = False
            is_split = False
            code_token = ""

            if normalized.startswith("OK"):
                after = normalized.split(":", 1)[1].strip() if ":" in normalized else ""
                code_token = after.split()[0] if after else ""
                is_ok = bool(code_token and _CODE_RE.match(code_token))
            elif normalized.startswith("ERROR:"):
                after = normalized.split(":", 1)[1].strip() if ":" in normalized else ""
                code_token = after.split()[0] if after else ""
                is_err = bool(code_token and _CODE_RE.match(code_token))
            elif normalized.startswith("SPLIT :"):
                after = normalized.split(":", 1)[1].strip() if ":" in normalized else ""
                code_token = after.split()[0] if after else ""
                is_split = bool(code_token and _CODE_RE.match(code_token))
            if is_start:
                started_count += 1
                current = normalized.split(":", 1)[1].strip() if ":" in normalized else ""
                _write_progress_file(
                    phase="exporting",
                    job_id=job_id,
                    current=current,
                    started=started_count,
                    ok=ok_count,
                    err=err_count,
                    split=split_count,
                    error=""
                )
            if is_ok or is_err or is_split:
                done_count += 1
                if is_ok:
                    ok_count += 1
                elif is_split:
                    split_count += 1
                else:
                    err_count += 1

            updated_processed = max(started_count, done_count)
            if updated_processed != processed:
                processed = updated_processed

            if is_start or is_ok or is_err or is_split:
                now_ts = time.time()
                if now_ts - last_progress_ts >= 0.6:
                    last_progress_ts = now_ts
                    job_manager._update_db(
                        job_id,
                        "txt_update",
                        "running",
                        message="Running Pan Rolling Export (VBS)...",
                        progress=processed
                    )
                    _write_progress_file(
                        phase="exporting",
                        job_id=job_id,
                        current="",
                        started=started_count,
                        ok=ok_count,
                        err=err_count,
                        split=split_count,
                        error=""
                    )

            if line.startswith("SUMMARY:"):
                summary["processed"] = processed
                summary["ok"] = ok_count
                summary["err"] = err_count
                summary["split"] = split_count

            if time.time() - start_ts > 1800:
                process.kill()
                raise subprocess.TimeoutExpired(cmd, 1800)

        return_code = process.wait()
        print(f"[txt_update_job] VBS returncode: {return_code}")
        _write_progress_file(
            phase=("done" if return_code == 0 else "error"),
            job_id=job_id,
            current="",
            started=started_count,
            ok=ok_count,
            err=err_count,
            split=split_count,
            error=("" if return_code == 0 else f"vbs_failed:{return_code}")
        )
        summary["processed"] = processed
        summary["ok"] = ok_count
        summary["err"] = err_count
        summary["split"] = split_count
        output = "\n".join(output_lines)
        return return_code, output, summary
    except subprocess.TimeoutExpired:
        logger.error(f"Job {job_id}: VBS timed out")
        print("[txt_update_job] VBS timed out")
        _write_progress_file(
            phase="error",
            job_id=job_id,
            current="",
            started=started_count,
            ok=ok_count,
            err=err_count,
            split=split_count,
            error="timeout"
        )
        summary["processed"] = processed
        summary["ok"] = ok_count
        summary["err"] = err_count
        summary["split"] = split_count
        return -1, "Timeout expired", summary
    except Exception as e:
        logger.error(f"Job {job_id}: VBS execution failed: {e}")
        print(f"[txt_update_job] VBS exception: {e}")
        _write_progress_file(
            phase="error",
            job_id=job_id,
            current="",
            started=started_count,
            ok=ok_count,
            err=err_count,
            split=split_count,
            error=str(e)
        )
        summary["processed"] = processed
        summary["ok"] = ok_count
        summary["err"] = err_count
        summary["split"] = split_count
        return -1, str(e), summary

def handle_txt_update(job_id: str, payload: dict):
    """
    Job Handler for 'txt_update'.
    Steps:
    1. Check code.txt existence
    2. Run VBS export
    3. Run Python Ingest
    """
    print(f"[txt_update_job] handle_txt_update called: job_id={job_id}")
    logger.info(f"Starting txt_update job {job_id}")
    
    # Update status to running (already done by manager, but we can set message)
    job_manager._update_db(job_id, "txt_update", "running", message="Initializing...")
    _write_progress_file(phase="starting", job_id=job_id, current="", started=0, ok=0, err=0, split=0, error="")

    code_txt_path = _pan_code_txt_path()
    pan_out_txt_dir = _pan_out_txt_dir()

    print(f"[txt_update_job] Checking code.txt at: {code_txt_path}")
    if not os.path.isfile(code_txt_path):
        error_msg = f"code.txt not found at {code_txt_path}"
        print(f"[txt_update_job] ERROR: {error_msg}")
        _write_progress_file(phase="error", job_id=job_id, current="", started=0, ok=0, err=0, split=0, error=error_msg)
        job_manager._update_db(job_id, "txt_update", "failed", error=error_msg, finished_at=datetime.now())
        return

    # Ensure output dir exists
    os.makedirs(pan_out_txt_dir, exist_ok=True)

    # 1. Run VBS
    print(f"[txt_update_job] Starting VBS export...")
    job_manager._update_db(job_id, "txt_update", "running", message="Running Pan Rolling Export (VBS)...", progress=0)
    total_count = None
    try:
        with open(code_txt_path, "r", encoding="utf-8") as handle:
            total_count = sum(1 for line in handle if line.strip() and not line.strip().startswith(("#", "'")))
    except OSError:
        total_count = None

    vbs_code, vbs_output, vbs_summary = run_vbs_update(
        job_id,
        code_txt_path,
        pan_out_txt_dir,
        total_count=total_count
    )

    if isinstance(vbs_summary, dict):
        vbs_summary["total"] = total_count

    # Log VBS output to a file or append to message (message might be too small)
    # We'll just look for "SUMMARY" line to update progress/message
    summary_line = "No summary"
    for line in vbs_output.splitlines():
        if "SUMMARY:" in line:
            summary_line = line.strip()
            break
            
    if vbs_code != 0:
        print(f"[txt_update_job] VBS failed with code {vbs_code}")
        logger.error(f"VBS failed with code {vbs_code}. Output: {vbs_output[:500]}")
        _write_progress_file(
            phase="error",
            job_id=job_id,
            current="",
            started=int(vbs_summary.get("processed") or 0) if isinstance(vbs_summary, dict) else 0,
            ok=int(vbs_summary.get("ok") or 0) if isinstance(vbs_summary, dict) else 0,
            err=int(vbs_summary.get("err") or 0) if isinstance(vbs_summary, dict) else 0,
            split=int(vbs_summary.get("split") or 0) if isinstance(vbs_summary, dict) else 0,
            error=f"vbs_failed:{vbs_code}"
        )
        job_manager._update_db(
            job_id, "txt_update", "failed", 
            error=f"VBS Failed ({vbs_code})", 
            message=f"VBS Error. Last output: {vbs_output[-200:]}",
            finished_at=datetime.now()
        )
        return

    print(f"[txt_update_job] VBS success: {summary_line}")
    logger.info(f"VBS Success. {summary_line}")
    job_manager._update_db(
        job_id,
        "txt_update",
        "running",
        message=f"Export Done. {summary_line}. Ingesting...",
        progress=total_count if total_count else 50
    )
    # VBS marks phase=done at export completion; overwrite to reflect the real next phase.
    _write_progress_file(
        phase="ingesting",
        job_id=job_id,
        current="",
        started=int(vbs_summary.get("processed") or 0) if isinstance(vbs_summary, dict) else 0,
        ok=int(vbs_summary.get("ok") or 0) if isinstance(vbs_summary, dict) else 0,
        err=int(vbs_summary.get("err") or 0) if isinstance(vbs_summary, dict) else 0,
        split=int(vbs_summary.get("split") or 0) if isinstance(vbs_summary, dict) else 0,
        error=""
    )

    # 2. Run Ingest
    print(f"[txt_update_job] Starting ingest...")
    ingest_out, ingest_err, stats = run_ingest(incremental=False)
    
    if ingest_err:
        print(f"[txt_update_job] Ingest failed: {ingest_err}")
        logger.error(f"Ingest failed: {ingest_err}")
        _write_progress_file(
            phase="error",
            job_id=job_id,
            current="",
            started=int(vbs_summary.get("processed") or 0) if isinstance(vbs_summary, dict) else 0,
            ok=int(vbs_summary.get("ok") or 0) if isinstance(vbs_summary, dict) else 0,
            err=int(vbs_summary.get("err") or 0) if isinstance(vbs_summary, dict) else 0,
            split=int(vbs_summary.get("split") or 0) if isinstance(vbs_summary, dict) else 0,
            error=f"ingest_failed:{ingest_err}"
        )
        job_manager._update_db(
            job_id, "txt_update", "failed", 
            error=f"Ingest Logic Failed", 
            message=f"Ingest Error: {ingest_err}",
            finished_at=datetime.now()
        )
        return

    # Construct summary from stats
    ingest_msg = "Ingest OK."
    if stats:
        ingest_msg = f"Ingest: {stats.get('changed', '?')} updated, {stats.get('skipped', '?')} skipped."
    elif "No changed files" in ingest_out:
        ingest_msg = "Ingest: No changes."
    ingest_summary = "Ingest completed"
    for line in ingest_out.splitlines():
        if "[STEP_END] ingest_total" in line:
            ingest_summary = line.strip()
    
    print(f"[txt_update_job] Ingest success: {ingest_msg}")
    logger.info(f"Ingest Success. {ingest_msg}")
    
    job_manager._update_db(
        job_id,
        "txt_update",
        "success",
        progress=total_count if total_count else 100,
        message=f"Update Complete. {summary_line}. {ingest_msg}",
        finished_at=datetime.now()
    )
    _save_update_state({
        "last_txt_update_at": datetime.now().isoformat(),
        "last_txt_update_date": datetime.now().date().isoformat()
    })
    _write_progress_file(
        phase="done",
        job_id=job_id,
        current="",
        started=int(vbs_summary.get("processed") or 0) if isinstance(vbs_summary, dict) else 0,
        ok=int(vbs_summary.get("ok") or 0) if isinstance(vbs_summary, dict) else 0,
        err=int(vbs_summary.get("err") or 0) if isinstance(vbs_summary, dict) else 0,
        split=int(vbs_summary.get("split") or 0) if isinstance(vbs_summary, dict) else 0,
        error=""
    )
    print(f"[txt_update_job] Job {job_id} completed successfully")
