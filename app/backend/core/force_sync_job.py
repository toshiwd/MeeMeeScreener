
import io
import logging
import os
import subprocess
import time
import traceback
from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime

from .config import config
from .jobs import job_manager
from .code_ops import normalize_code_txt

try:
    from app.backend import ingest_txt
except ImportError:
    try:
        import ingest_txt  # type: ignore
    except ImportError:
        ingest_txt = None

logger = logging.getLogger(__name__)

PAN_CODE_TXT_PATH = config.PAN_CODE_TXT_PATH
PAN_OUT_TXT_DIR = config.PAN_OUT_TXT_DIR


def _vbs_script_path() -> str:
    return os.path.abspath(str(config.PAN_EXPORT_VBS_PATH))


def _run_vbs_export(code_path: str, out_dir: str, timeout: int = 1800) -> tuple[int, list[str]]:
    sys_root = os.environ.get("SystemRoot") or "C:\\Windows"
    cscript = os.path.join(sys_root, "SysWOW64", "cscript.exe")
    if not os.path.isfile(cscript):
        cscript = os.path.join(sys_root, "System32", "cscript.exe")

    cmd = [cscript, "//nologo", _vbs_script_path(), str(code_path), str(out_dir)]
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
            print(f"[force_sync_job] {text}")
            if time.time() - start_ts > timeout:
                process.kill()
                raise subprocess.TimeoutExpired(cmd, timeout)
        return_code = process.wait()
        output_lines.append(f"[force_sync_job] VBS exit code {return_code}")
        return return_code, output_lines
    except subprocess.TimeoutExpired:
        logger.error("VBS export timed out")
        process.kill()
        output_lines.append("Timeout expired")
        return -1, output_lines
    except Exception:
        logger.exception("VBS export failed")
        process.kill()
        output_lines.append("VBS export failed")
        return -1, output_lines


def _run_ingest(incremental: bool = True) -> tuple[str, str, dict]:
    print(f"[force_sync_job] run_ingest called incremental={incremental}")
    if not ingest_txt:
        error = "ingest_txt module not found"
        print(f"[force_sync_job] ERROR: {error}")
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
        print(f"[force_sync_job] run_ingest completed, stats={stats}")
        return output, "", stats
    except Exception as exc:
        print(f"[force_sync_job] run_ingest exception: {exc}")
        traceback.print_exc(file=buffer)
        return buffer.getvalue(), str(exc), {}


def handle_force_sync(job_id: str, payload: dict):
    """
    Job Handler for 'force_sync'.
    Steps:
    1. Normalize code.txt (sort/dedup)
    2. Run VBS Export (overwrite)
    3. Run Ingest (Full Mode - wipe and reload)
    """
    logger.info(f"Starting force_sync job {job_id}")
    job_manager._update_db(job_id, "force_sync", "running", message="Normalizing code.txt...", progress=5)

    try:
        if os.path.exists(PAN_CODE_TXT_PATH):
            changed = normalize_code_txt(PAN_CODE_TXT_PATH)
            msg = "code.txt normalized (updated)" if changed else "code.txt validated (no changes)"
            logger.info(msg)
            job_manager._update_db(job_id, "force_sync", "running", message=msg, progress=10)
        else:
            logger.warning("code.txt not found, skipping normalization")

        # Run VBS
        job_manager._update_db(job_id, "force_sync", "running", message="Running VBS Export...", progress=20)
        vbs_code, vbs_output = _run_vbs_export(PAN_CODE_TXT_PATH, PAN_OUT_TXT_DIR)

        if vbs_code != 0:
            job_manager._update_db(
                job_id,
                "force_sync",
                "failed",
                error=f"VBS Failed: {vbs_code}",
                message=f"VBS Error: {vbs_output[-1] if vbs_output else 'unknown'}",
            )
            return

        # Run Ingest (Full)
        job_manager._update_db(job_id, "force_sync", "running", message="Ingesting (Full Mode)...", progress=60)
        ingest_out, ingest_err, stats = _run_ingest(incremental=False)

        if ingest_err:
            job_manager._update_db(
                job_id,
                "force_sync",
                "failed",
                error="Ingest Failed",
                message=f"Ingest Error: {ingest_err}",
            )
            return

        # CSV Sync
        from .csv_sync import sync_trade_csvs

        job_manager._update_db(job_id, "force_sync", "running", message="Syncing Trade CSVs...", progress=80)
        csv_res = sync_trade_csvs()

        # Build detailed CSV message
        csv_msg = f"Trades: {csv_res.get('imported')} rows."
        if csv_res.get("details"):
            details = csv_res.get("details")[:3]
            csv_msg += " [" + ", ".join(details) + "]"
            if len(csv_res.get("details")) > 3:
                csv_msg += "..."

        if csv_res.get("warnings"):
            csv_msg += " WARN: " + "; ".join(csv_res.get("warnings"))

        job_manager._update_db(
            job_id,
            "force_sync",
            "success",
            progress=100,
            message=f"Complete. {csv_msg}",
            finished_at=datetime.now(),
        )

    except Exception as e:
        logger.error(f"Force sync failed: {e}")
        job_manager._update_db(job_id, "force_sync", "failed", error=str(e), message="Internal Error", finished_at=datetime.now())
