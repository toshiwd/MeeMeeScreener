
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


def _is_transient_db_lock_error(text: str) -> bool:
    lowered = (text or "").lower()
    return (
        "cannot open file" in lowered
        or "already open" in lowered
        or "used by" in lowered
        or "file is already open" in lowered
    )


def _run_ingest_with_retry(
    *,
    incremental: bool,
    max_attempts: int,
    sleep_seconds: float,
) -> tuple[str, str, dict, int]:
    attempt = 0
    last_output = ""
    last_error = ""
    last_stats: dict = {}
    while attempt < max_attempts:
        attempt += 1
        out, err, stats = _run_ingest(incremental=incremental)
        last_output, last_error, last_stats = out, err, stats
        if not err:
            return out, "", stats, attempt
        if attempt >= max_attempts or not _is_transient_db_lock_error(err):
            break
        logger.warning(
            "force_sync ingest retry due to transient DB lock (attempt %s/%s): %s",
            attempt,
            max_attempts,
            err,
        )
        time.sleep(max(0.1, float(sleep_seconds)))
    return last_output, last_error, last_stats, attempt


def _run_post_pan_recalc() -> dict:
    summary: dict[str, object] = {
        "phase_dt": None,
        "scoring_rows": None,
        "cache_refreshed": False,
        "warnings": [],
    }
    warnings: list[str] = []

    try:
        try:
            from app.backend.db import get_conn
        except ModuleNotFoundError:  # pragma: no cover
            from db import get_conn  # type: ignore
        try:
            from app.backend.jobs.phase_batch import run_batch
        except ModuleNotFoundError:  # pragma: no cover
            from jobs.phase_batch import run_batch  # type: ignore

        with get_conn() as conn:
            row = conn.execute("SELECT MAX(dt) FROM feature_snapshot_daily").fetchone()
        if row and row[0] is not None:
            max_dt = int(row[0])
            run_batch(max_dt, max_dt, dry_run=False)
            summary["phase_dt"] = max_dt
    except Exception as exc:
        logger.exception("force_sync post-recalc phase failed: %s", exc)
        warnings.append(f"phase:{exc}")

    try:
        from app.backend.infra.duckdb.stock_repo import StockRepository
        from app.backend.jobs.scoring_job import ScoringJob

        score_repo = StockRepository()
        scoring_results = ScoringJob(score_repo).run()
        summary["scoring_rows"] = int(len(scoring_results))
    except Exception as exc:
        logger.exception("force_sync post-recalc scoring failed: %s", exc)
        warnings.append(f"scoring:{exc}")

    try:
        from app.backend.services import rankings_cache

        rankings_cache.refresh_cache()
        summary["cache_refreshed"] = True
    except Exception as exc:
        logger.exception("force_sync post-recalc cache refresh failed: %s", exc)
        warnings.append(f"cache:{exc}")

    summary["warnings"] = warnings
    return summary


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
        try:
            ingest_retry = max(
                1,
                int(payload.get("ingest_retry", os.getenv("MEEMEE_FORCE_SYNC_INGEST_RETRY", 3))),
            )
        except (TypeError, ValueError):
            ingest_retry = 3
        try:
            ingest_retry_sleep = max(
                0.1,
                float(payload.get("ingest_retry_sleep", os.getenv("MEEMEE_FORCE_SYNC_INGEST_RETRY_SLEEP", 1.5))),
            )
        except (TypeError, ValueError):
            ingest_retry_sleep = 1.5
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
        ingest_out, ingest_err, stats, ingest_attempts = _run_ingest_with_retry(
            incremental=False,
            max_attempts=ingest_retry,
            sleep_seconds=ingest_retry_sleep,
        )

        if ingest_err:
            job_manager._update_db(
                job_id,
                "force_sync",
                "failed",
                error="Ingest Failed",
                message=f"Ingest Error: {ingest_err} (attempts={ingest_attempts})",
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
            "running",
            message="Recalculating derived metrics...",
            progress=90,
        )
        recalc = _run_post_pan_recalc()
        phase_dt = recalc.get("phase_dt")
        scoring_rows = recalc.get("scoring_rows")
        cache_refreshed = bool(recalc.get("cache_refreshed"))
        recalc_msg = (
            f" Recalc: phase_dt={phase_dt if phase_dt is not None else 'n/a'}"
            f", scoring_rows={scoring_rows if scoring_rows is not None else 'n/a'}"
            f", cache={'ok' if cache_refreshed else 'skip'}."
        )
        recalc_warnings = recalc.get("warnings") if isinstance(recalc.get("warnings"), list) else []
        if recalc_warnings:
            recalc_msg += " WARN: " + "; ".join(str(item) for item in recalc_warnings[:3])

        job_manager._update_db(
            job_id,
            "force_sync",
            "success",
            progress=100,
            message=f"Complete. {csv_msg}{recalc_msg}",
            finished_at=datetime.now(),
        )

    except Exception as e:
        logger.error(f"Force sync failed: {e}")
        job_manager._update_db(job_id, "force_sync", "failed", error=str(e), message="Internal Error", finished_at=datetime.now())
