from __future__ import annotations

import io
import json
import logging
import os
import queue
import subprocess
import threading
import time
import traceback
from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime
from typing import Callable

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
    except Exception as exc:
        logger.warning("Failed to load update state (%s): %s", path, exc)
        return {}


def _save_update_state(state: dict) -> None:
    path = _update_state_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, indent=2)
    except Exception as exc:
        logger.warning("Failed to save update state (%s): %s", path, exc)


def _set_pipeline_stage(
    state: dict,
    stage: str,
    *,
    status: str = "running",
    message: str | None = None,
    save: bool = True,
) -> None:
    now_iso = datetime.now().isoformat()
    state["last_pipeline_stage"] = stage
    state["last_pipeline_stage_status"] = status
    state["last_pipeline_stage_at"] = now_iso
    if message is not None:
        state["last_pipeline_message"] = message
    if save:
        _save_update_state(state)


def _record_pipeline_failure(state: dict, *, stage: str, error: str, message: str | None = None) -> None:
    now_iso = datetime.now().isoformat()
    state["last_pipeline_status"] = "failed"
    state["last_pipeline_finished_at"] = now_iso
    state["last_failed_at"] = now_iso
    state["last_failed_stage"] = stage
    state["last_error"] = str(error)
    state["last_error_message"] = message or str(error)
    _set_pipeline_stage(state, stage, status="failed", message=message or str(error), save=False)
    _save_update_state(state)


def _record_pipeline_canceled(state: dict, *, stage: str, message: str) -> None:
    now_iso = datetime.now().isoformat()
    state["last_pipeline_status"] = "canceled"
    state["last_pipeline_finished_at"] = now_iso
    state["last_canceled_at"] = now_iso
    state["last_canceled_stage"] = stage
    state["last_error"] = "canceled"
    state["last_error_message"] = message
    _set_pipeline_stage(state, stage, status="canceled", message=message, save=False)
    _save_update_state(state)


def _record_pipeline_success(state: dict, *, stage: str, message: str) -> None:
    now_iso = datetime.now().isoformat()
    state["last_pipeline_status"] = "success"
    state["last_pipeline_finished_at"] = now_iso
    state.pop("last_error", None)
    state.pop("last_error_message", None)
    _set_pipeline_stage(state, stage, status="success", message=message, save=False)
    _save_update_state(state)


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


def run_vbs_export(
    code_path: str,
    out_dir: str,
    timeout: int = 1800,
    should_cancel: Callable[[], bool] | None = None,
) -> tuple[int, list[str]]:
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

    line_queue: "queue.Queue[str | None]" = queue.Queue()

    def _reader() -> None:
        if not process.stdout:
            line_queue.put(None)
            return
        try:
            for raw_line in process.stdout:
                line_queue.put(raw_line)
        finally:
            line_queue.put(None)

    reader_thread = threading.Thread(target=_reader, daemon=True, name="txt-update-vbs-reader")
    reader_thread.start()

    start_ts = time.time()
    try:
        while True:
            if should_cancel and should_cancel():
                if process.poll() is None:
                    process.kill()
                output_lines.append("Canceled by request")
                return -2, output_lines

            if time.time() - start_ts > timeout:
                if process.poll() is None:
                    process.kill()
                raise subprocess.TimeoutExpired(cmd, timeout)

            try:
                line = line_queue.get(timeout=0.2)
            except queue.Empty:
                if process.poll() is not None:
                    break
                continue

            if line is None:
                break

            text = line.rstrip("\r\n")
            output_lines.append(text)
            print(f"[txt_update_job] {text}")

        return_code = process.wait()
        output_lines.append(f"[txt_update_job] VBS exit code {return_code}")
        return return_code, output_lines
    except subprocess.TimeoutExpired:
        logger.error("VBS export timed out")
        process.kill()
        output_lines.append("Timeout expired")
        return -1, output_lines
    except Exception as exc:
        logger.exception("VBS export failed")
        process.kill()
        output_lines.append(str(exc))
        return -1, output_lines
    finally:
        if process.poll() is None:
            process.kill()
        if process.stdout:
            try:
                process.stdout.close()
            except Exception as exc:
                logger.debug("Failed to close VBS stdout pipe: %s", exc)


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


def _to_bool(value: object, default: bool) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _to_int(value: object, default: int, *, minimum: int = 1) -> int:
    try:
        if value is None:
            parsed = int(default)
        else:
            parsed = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        parsed = int(default)
    return max(int(minimum), int(parsed))


def _to_float(value: object, default: float, *, minimum: float = 0.0) -> float:
    try:
        parsed = float(default if value is None else value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        parsed = float(default)
    return max(float(minimum), float(parsed))


def _to_optional_int(value: object) -> int | None:
    try:
        if value is None:
            return None
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _latest_txt_export_at(out_dir: str) -> datetime | None:
    latest_ts: float | None = None
    try:
        with os.scandir(out_dir) as entries:
            for entry in entries:
                if not entry.is_file():
                    continue
                if not entry.name.lower().endswith(".txt"):
                    continue
                try:
                    mtime = float(entry.stat().st_mtime)
                except OSError:
                    continue
                if latest_ts is None or mtime > latest_ts:
                    latest_ts = mtime
    except FileNotFoundError:
        return None
    except OSError:
        return None
    if latest_ts is None:
        return None
    return datetime.fromtimestamp(latest_ts)


def _is_existing_txt_data_fresh(out_dir: str, *, max_age_hours: float) -> tuple[bool, str]:
    latest = _latest_txt_export_at(out_dir)
    if latest is None:
        return False, "no_txt_files"
    age_hours = max(0.0, (time.time() - latest.timestamp()) / 3600.0)
    status = f"latest_txt_age_hours={age_hours:.2f}"
    return age_hours <= float(max_age_hours), status


def _is_transient_db_lock_error(exc: Exception) -> bool:
    text = str(exc).lower()
    if not text:
        return False
    return (
        "cannot open file" in text
        or "already open" in text
        or "used by" in text
        or "アクセスできません" in str(exc)
    )


def _classify_ingest_error_text(error_text: str) -> str:
    if not error_text:
        return "none"
    if _is_transient_db_lock_error(RuntimeError(error_text)):
        return "db_lock"
    lowered = error_text.lower()
    if "module not found" in lowered:
        return "missing_module"
    if "permission" in lowered:
        return "permission"
    return "other"


def _run_phase_with_retry(*, max_attempts: int, sleep_seconds: float) -> int:
    attempt = 0
    while True:
        attempt += 1
        try:
            return _run_phase_batch_latest()
        except Exception as exc:
            if attempt >= max_attempts or not _is_transient_db_lock_error(exc):
                raise
            logger.warning(
                "Phase update retry due to transient DB lock (attempt %s/%s): %s",
                attempt,
                max_attempts,
                exc,
            )
            time.sleep(max(0.1, float(sleep_seconds)))


def _run_ingest_with_retry(
    *,
    incremental: bool,
    max_attempts: int,
    sleep_seconds: float,
) -> tuple[str, str, dict, int, str]:
    attempt = 0
    last_output = ""
    last_error = ""
    last_stats: dict = {}
    last_error_kind = "none"
    while attempt < max_attempts:
        attempt += 1
        out, err, stats = run_ingest(incremental=incremental)
        last_output, last_error, last_stats = out, err, stats
        last_error_kind = _classify_ingest_error_text(err)
        if not err:
            return out, "", stats, attempt, "none"
        if attempt >= max_attempts or last_error_kind != "db_lock":
            break
        logger.warning(
            "Ingest retry due to transient DB lock (attempt %s/%s): %s",
            attempt,
            max_attempts,
            err,
        )
        time.sleep(max(0.1, float(sleep_seconds)))
    return last_output, last_error, last_stats, attempt, last_error_kind


def _is_job_canceled(job_id: str) -> bool:
    return job_manager.is_cancel_requested(job_id)


def _mark_job_canceled(
    job_id: str,
    message: str = "Canceled",
    *,
    state: dict | None = None,
    stage: str = "cancel",
) -> None:
    if state is not None:
        _record_pipeline_canceled(state, stage=stage, message=message)
    job_manager._update_db(
        job_id,
        "txt_update",
        "canceled",
        message=message,
        error="canceled",
        finished_at=datetime.now(),
    )


def _exit_if_canceled(job_id: str, state: dict, *, stage: str, message: str) -> bool:
    if not _is_job_canceled(job_id):
        return False
    _mark_job_canceled(job_id, message, state=state, stage=stage)
    return True


def handle_txt_update(job_id: str, payload: dict) -> None:
    auto_ml_predict = _to_bool(payload.get("auto_ml_predict"), True)
    auto_ml_train = _to_bool(payload.get("auto_ml_train"), True)
    force_ml_train = _to_bool(payload.get("force_ml_train"), False)
    skip_ml_train_if_no_change = _to_bool(
        payload.get("skip_ml_train_if_no_change"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_SKIP_ML_TRAIN_IF_NO_CHANGE"), True),
    )
    auto_fill_missing_history = _to_bool(payload.get("auto_fill_missing_history"), False)
    pan_retry = _to_int(
        payload.get("pan_retry"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_PAN_RETRY"), 3, minimum=1),
        minimum=1,
    )
    pan_retry_sleep = _to_float(
        payload.get("pan_retry_sleep"),
        _to_float(os.getenv("MEEMEE_TXT_UPDATE_PAN_RETRY_SLEEP"), 2.0, minimum=0.1),
        minimum=0.1,
    )
    strict_pan_import = _to_bool(
        payload.get("strict_pan_import"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_STRICT_PAN_IMPORT"), False),
    )
    vbs_retry = _to_int(
        payload.get("vbs_retry"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_VBS_RETRY"), 3, minimum=1),
        minimum=1,
    )
    vbs_timeout_backoff = _to_int(
        payload.get("vbs_timeout_backoff"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_VBS_TIMEOUT_BACKOFF"), 300, minimum=0),
        minimum=0,
    )
    strict_vbs_export = _to_bool(
        payload.get("strict_vbs_export"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_STRICT_VBS_EXPORT"), False),
    )
    vbs_timeout = _to_int(
        payload.get("vbs_timeout"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_VBS_TIMEOUT"), 1800, minimum=30),
        minimum=30,
    )
    phase_retry = _to_int(
        payload.get("phase_retry"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_PHASE_RETRY"), 3, minimum=1),
        minimum=1,
    )
    phase_retry_sleep = _to_float(
        payload.get("phase_retry_sleep"),
        _to_float(os.getenv("MEEMEE_TXT_UPDATE_PHASE_RETRY_SLEEP"), 1.5, minimum=0.1),
        minimum=0.1,
    )
    ingest_retry = _to_int(
        payload.get("ingest_retry"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_INGEST_RETRY"), 3, minimum=1),
        minimum=1,
    )
    ingest_retry_sleep = _to_float(
        payload.get("ingest_retry_sleep"),
        _to_float(os.getenv("MEEMEE_TXT_UPDATE_INGEST_RETRY_SLEEP"), 1.5, minimum=0.1),
        minimum=0.1,
    )
    backfill_lookback_days = _to_int(
        payload.get("backfill_lookback_days"),
        int(os.getenv("MEEMEE_NIGHTLY_BACKFILL_LOOKBACK_DAYS", "130")),
        minimum=20,
    )
    backfill_max_missing_days = _to_int(
        payload.get("backfill_max_missing_days"),
        int(os.getenv("MEEMEE_NIGHTLY_BACKFILL_MAX_MISSING_DAYS", "260")),
        minimum=1,
    )
    max_stale_export_hours = _to_float(
        payload.get("max_stale_export_hours"),
        _to_float(os.getenv("MEEMEE_TXT_UPDATE_MAX_STALE_EXPORT_HOURS"), 36.0, minimum=1.0),
        minimum=1.0,
    )
    auto_walkforward_gate = _to_bool(
        payload.get("auto_walkforward_gate"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_AUTO_WALKFORWARD_GATE"), True),
    )
    walkforward_gate_monthly_only = _to_bool(
        payload.get("walkforward_gate_monthly_only"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_GATE_MONTHLY_ONLY"), True),
    )
    walkforward_gate_strict = _to_bool(
        payload.get("walkforward_gate_strict"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_GATE_STRICT"), False),
    )
    walkforward_gate_min_oos_total = _to_float(
        payload.get("walkforward_gate_min_oos_total_realized_unit_pnl"),
        _to_float(
            os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_GATE_MIN_OOS_TOTAL_REALIZED_UNIT_PNL"),
            0.0,
            minimum=-1_000_000_000.0,
        ),
        minimum=-1_000_000_000.0,
    )
    walkforward_gate_min_oos_pf = _to_float(
        payload.get("walkforward_gate_min_oos_mean_profit_factor"),
        _to_float(
            os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_GATE_MIN_OOS_MEAN_PROFIT_FACTOR"),
            1.05,
            minimum=0.0,
        ),
        minimum=0.0,
    )
    walkforward_gate_min_oos_pos_ratio = _to_float(
        payload.get("walkforward_gate_min_oos_positive_window_ratio"),
        _to_float(
            os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_GATE_MIN_OOS_POSITIVE_WINDOW_RATIO"),
            0.40,
            minimum=0.0,
        ),
        minimum=0.0,
    )
    auto_walkforward_run = _to_bool(
        payload.get("auto_walkforward_run"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_AUTO_WALKFORWARD_RUN"), True),
    )
    walkforward_run_monthly_only = _to_bool(
        payload.get("walkforward_run_monthly_only"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_MONTHLY_ONLY"), True),
    )
    walkforward_run_strict = _to_bool(
        payload.get("walkforward_run_strict"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_STRICT"), False),
    )
    walkforward_run_start_dt = _to_optional_int(payload.get("walkforward_run_start_dt"))
    walkforward_run_end_dt = _to_optional_int(payload.get("walkforward_run_end_dt"))
    walkforward_run_max_codes = _to_int(
        payload.get("walkforward_run_max_codes"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_MAX_CODES"), 500, minimum=50),
        minimum=50,
    )
    walkforward_run_train_months = _to_int(
        payload.get("walkforward_run_train_months"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_TRAIN_MONTHS"), 24, minimum=1),
        minimum=1,
    )
    walkforward_run_test_months = _to_int(
        payload.get("walkforward_run_test_months"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_TEST_MONTHS"), 3, minimum=1),
        minimum=1,
    )
    walkforward_run_step_months = _to_int(
        payload.get("walkforward_run_step_months"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_STEP_MONTHS"), 12, minimum=1),
        minimum=1,
    )
    walkforward_run_min_windows = _to_int(
        payload.get("walkforward_run_min_windows"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_MIN_WINDOWS"), 1, minimum=1),
        minimum=1,
    )
    walkforward_run_allowed_sides = str(
        payload.get("walkforward_run_allowed_sides")
        or os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_ALLOWED_SIDES")
        or "long"
    ).strip().lower()
    if walkforward_run_allowed_sides not in {"both", "long", "short"}:
        walkforward_run_allowed_sides = "long"
    raw_walkforward_run_allowed_long_setups = (
        payload.get("walkforward_run_allowed_long_setups")
        if payload.get("walkforward_run_allowed_long_setups") is not None
        else os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_ALLOWED_LONG_SETUPS")
    )
    walkforward_run_allowed_long_setups: tuple[str, ...]
    if raw_walkforward_run_allowed_long_setups is None:
        walkforward_run_allowed_long_setups = ("long_breakout_p2",)
    elif isinstance(raw_walkforward_run_allowed_long_setups, (list, tuple, set)):
        parsed = [str(v).strip() for v in raw_walkforward_run_allowed_long_setups if str(v).strip()]
        walkforward_run_allowed_long_setups = tuple(parsed) if parsed else ("long_breakout_p2",)
    else:
        parsed = [
            s.strip()
            for s in str(raw_walkforward_run_allowed_long_setups).split(",")
            if s.strip()
        ]
        walkforward_run_allowed_long_setups = tuple(parsed) if parsed else ("long_breakout_p2",)
    walkforward_run_use_regime_filter = _to_bool(
        payload.get("walkforward_run_use_regime_filter"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_USE_REGIME_FILTER"), True),
    )
    walkforward_run_min_long_score = _to_float(
        payload.get("walkforward_run_min_long_score"),
        _to_float(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_MIN_LONG_SCORE"), 2.0, minimum=-1000.0),
        minimum=-1000.0,
    )
    walkforward_run_min_short_score = _to_float(
        payload.get("walkforward_run_min_short_score"),
        _to_float(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_MIN_SHORT_SCORE"), 99.0, minimum=-1000.0),
        minimum=-1000.0,
    )
    walkforward_run_max_new_entries_per_day = _to_int(
        payload.get("walkforward_run_max_new_entries_per_day"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_MAX_NEW_ENTRIES_PER_DAY"), 1, minimum=1),
        minimum=1,
    )
    walkforward_run_regime_long_min_breadth_above60 = _to_float(
        payload.get("walkforward_run_regime_long_min_breadth_above60"),
        _to_float(
            os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_REGIME_LONG_MIN_BREADTH_ABOVE60"),
            0.57,
            minimum=0.0,
        ),
        minimum=0.0,
    )
    walkforward_run_range_bias_width_min = _to_float(
        payload.get("walkforward_run_range_bias_width_min"),
        _to_float(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_RANGE_BIAS_WIDTH_MIN"), 0.08, minimum=0.0),
        minimum=0.0,
    )
    walkforward_run_range_bias_long_pos_min = _to_float(
        payload.get("walkforward_run_range_bias_long_pos_min"),
        _to_float(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_RANGE_BIAS_LONG_POS_MIN"), 0.60, minimum=0.0),
        minimum=0.0,
    )
    walkforward_run_range_bias_short_pos_max = _to_float(
        payload.get("walkforward_run_range_bias_short_pos_max"),
        _to_float(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_RANGE_BIAS_SHORT_POS_MAX"), 0.40, minimum=0.0),
        minimum=0.0,
    )
    walkforward_run_ma20_count20_min_long = _to_int(
        payload.get("walkforward_run_ma20_count20_min_long"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_MA20_COUNT20_MIN_LONG"), 12, minimum=1),
        minimum=1,
    )
    walkforward_run_ma60_count60_min_long = _to_int(
        payload.get("walkforward_run_ma60_count60_min_long"),
        _to_int(os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_MA60_COUNT60_MIN_LONG"), 30, minimum=1),
        minimum=1,
    )
    state = _load_update_state()
    state["last_pipeline_status"] = "running"
    state["last_pipeline_started_at"] = datetime.now().isoformat()
    state.pop("last_pipeline_finished_at", None)
    state.pop("last_error", None)
    state.pop("last_error_message", None)
    _set_pipeline_stage(state, "init", message="Initializing update...")

    job_manager._update_db(job_id, "txt_update", "running", message="Initializing update...", progress=0)
    code_path = _pan_code_txt_path()
    out_dir = _pan_out_txt_dir()

    if _exit_if_canceled(job_id, state, stage="init", message="Canceled before start"):
        return

    if not os.path.isfile(code_path):
        error_msg = f"code.txt not found at {code_path}"
        print(f"[txt_update_job] ERROR: {error_msg}")
        _record_pipeline_failure(state, stage="init", error=error_msg)
        job_manager._update_db(
            job_id, "txt_update", "failed", error=error_msg, message=error_msg, finished_at=datetime.now()
        )
        return

    os.makedirs(out_dir, exist_ok=True)

    # Step 0: Import latest data into Pan database (pandtmgr F5)
    if _exit_if_canceled(job_id, state, stage="pan_import", message="Canceled before Pan import"):
        return

    _set_pipeline_stage(state, "pan_import", message="Launching Pan and importing latest data...")
    job_manager._update_db(
        job_id,
        "txt_update",
        "running",
        message="Launching Pan import...",
        progress=0,
    )

    try:
        from app.backend.infra.panrolling.pan_import import run_pan_import
    except Exception as exc:
        error_msg = f"Pan import module load failed: {exc}"
        logger.exception(error_msg)
        _record_pipeline_failure(state, stage="pan_import", error=error_msg, message="Pan import failed")
        job_manager._update_db(
            job_id,
            "txt_update",
            "failed",
            error="Pan import failed",
            message=error_msg,
            finished_at=datetime.now(),
        )
        return

    pan_import_ok = False
    pan_import_error: str | None = None
    for attempt in range(1, pan_retry + 1):
        if _exit_if_canceled(job_id, state, stage="pan_import", message="Canceled during Pan import"):
            return
        try:
            pan_import_ok = run_pan_import(str(config.PAN_DTMGR_PATH))
            if pan_import_ok:
                break
            pan_import_error = "Pan import returned False"
        except Exception as exc:
            pan_import_error = str(exc)
            logger.warning("Pan import error (attempt %s/%s): %s", attempt, pan_retry, exc)

        if attempt < pan_retry:
            job_manager._update_db(
                job_id,
                "txt_update",
                "running",
                message=f"Retrying Pan import ({attempt}/{pan_retry})...",
                progress=3,
            )
            time.sleep(float(pan_retry_sleep))

    if not pan_import_ok:
        error_msg = f"Pan import failed: {pan_import_error or 'unknown error'}"
        is_fresh, freshness_status = _is_existing_txt_data_fresh(
            out_dir,
            max_age_hours=max_stale_export_hours,
        )
        if not is_fresh:
            stale_msg = (
                f"{error_msg} (stale_txt_data: {freshness_status}, "
                f"max_stale_export_hours={max_stale_export_hours:.1f})"
            )
            _record_pipeline_failure(state, stage="pan_import", error=stale_msg, message="Pan import failed")
            job_manager._update_db(
                job_id,
                "txt_update",
                "failed",
                error="Pan import failed",
                message=stale_msg,
                finished_at=datetime.now(),
            )
            return
        if strict_pan_import:
            _record_pipeline_failure(state, stage="pan_import", error=error_msg, message="Pan import failed")
            job_manager._update_db(
                job_id,
                "txt_update",
                "failed",
                error="Pan import failed",
                message=error_msg,
                finished_at=datetime.now(),
            )
            return
        warning_msg = f"{error_msg} ({freshness_status})"
        logger.warning("Pan import failed but continuing update in non-strict mode: %s", warning_msg)
        state["last_pan_import_warning"] = warning_msg
        _set_pipeline_stage(
            state,
            "pan_import",
            status="warning",
            message="Pan import failed. Continuing with export of existing data.",
        )
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message="PAN import failed. Continuing with existing TXT data.",
            progress=5,
        )

    if _exit_if_canceled(job_id, state, stage="pan_import", message="Canceled after Pan import"):
        return

    # Step 1: VBS export (Pan -> TXT)
    _set_pipeline_stage(state, "export", message="Running Pan Rolling export...")
    job_manager._update_db(job_id, "txt_update", "running", message="Running Pan Rolling export...", progress=10)

    output_lines: list[str] = []
    vbs_code = -1
    for attempt in range(1, vbs_retry + 1):
        attempt_timeout = int(vbs_timeout + (attempt - 1) * vbs_timeout_backoff)
        vbs_code, output_lines = run_vbs_export(
            code_path,
            out_dir,
            timeout=attempt_timeout,
            should_cancel=lambda: _is_job_canceled(job_id),
        )
        if vbs_code in (0, -2):
            break
        if attempt < vbs_retry:
            retry_message = f"Pan Rolling export retry {attempt}/{vbs_retry}..."
            logger.warning("VBS export failed (attempt %s/%s): code=%s", attempt, vbs_retry, vbs_code)
            job_manager._update_db(
                job_id,
                "txt_update",
                "running",
                message=retry_message,
                progress=12,
            )
            time.sleep(1.0)
    summary_line = next((line for line in output_lines if "SUMMARY:" in line), "Export completed")

    if vbs_code == -2:
        _mark_job_canceled(
            job_id,
            "Canceled during Pan Rolling export",
            state=state,
            stage="export",
        )
        return

    if vbs_code != 0:
        msg = output_lines[-1] if output_lines else "VBS failed"
        if _is_job_canceled(job_id):
            _mark_job_canceled(
                job_id,
                "Canceled during Pan Rolling export",
                state=state,
                stage="export",
            )
            return
        if strict_vbs_export:
            _record_pipeline_failure(state, stage="export", error=f"VBS failed with code {vbs_code}", message=msg)
            job_manager._update_db(
                job_id,
                "txt_update",
                "failed",
                message=f"{summary_line}: {msg}",
                error=f"VBS failed with code {vbs_code}",
                finished_at=datetime.now(),
            )
            return
        is_fresh, freshness_status = _is_existing_txt_data_fresh(
            out_dir,
            max_age_hours=max_stale_export_hours,
        )
        if not is_fresh:
            stale_msg = (
                f"VBS export failed with code {vbs_code}: {msg} "
                f"(stale_txt_data: {freshness_status}, "
                f"max_stale_export_hours={max_stale_export_hours:.1f})"
            )
            _record_pipeline_failure(
                state,
                stage="export",
                error=f"VBS failed with code {vbs_code}",
                message=stale_msg,
            )
            job_manager._update_db(
                job_id,
                "txt_update",
                "failed",
                message=stale_msg,
                error=f"VBS failed with code {vbs_code}",
                finished_at=datetime.now(),
            )
            return
        warning_msg = f"VBS export failed with code {vbs_code}: {msg} ({freshness_status})"
        logger.warning("VBS export failed but continuing update in non-strict mode: %s", warning_msg)
        state["last_vbs_export_warning"] = warning_msg
        _set_pipeline_stage(
            state,
            "export",
            status="warning",
            message="VBS export failed. Continuing with existing TXT data.",
        )
        summary_line = "EXPORT_WARNING: using existing TXT data"
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message="VBS export failed. Continuing with existing TXT data.",
            progress=68,
        )

    if _exit_if_canceled(job_id, state, stage="export", message="Canceled after Pan Rolling export"):
        return

    job_manager._update_db(
        job_id,
        "txt_update",
        "running",
        message=f"{summary_line}. Export completed.",
        progress=70,
    )

    if _exit_if_canceled(job_id, state, stage="ingest", message="Canceled before ingest"):
        return

    _set_pipeline_stage(state, "ingest", message="Ingesting incremental TXT data...")
    job_manager._update_db(job_id, "txt_update", "running", message="Ingesting (Incremental)...", progress=85)
    _ingest_out, ingest_err, ingest_stats, ingest_attempts, ingest_error_kind = _run_ingest_with_retry(
        incremental=True,
        max_attempts=ingest_retry,
        sleep_seconds=ingest_retry_sleep,
    )
    state["last_ingest_attempts"] = int(ingest_attempts)
    state["last_ingest_retry_sleep_sec"] = float(ingest_retry_sleep)
    state["last_ingest_error_kind"] = ingest_error_kind
    if _exit_if_canceled(job_id, state, stage="ingest", message="Canceled during ingest"):
        return
    if ingest_err:
        state["last_ingest_error"] = str(ingest_err)
        state["last_ingest_failed_at"] = datetime.now().isoformat()
        _record_pipeline_failure(state, stage="ingest", error=ingest_err, message="Ingest failed")
        job_manager._update_db(
            job_id,
            "txt_update",
            "failed",
            error="Ingest Failed",
            message=f"Ingest Error: {ingest_err}",
            finished_at=datetime.now(),
        )
        return
    state["last_ingest_at"] = datetime.now().isoformat()
    state["last_ingest_stats"] = ingest_stats
    state.pop("last_ingest_error", None)
    state.pop("last_ingest_failed_at", None)
    changed_files = _to_int(ingest_stats.get("changed"), 0, minimum=0)

    if _exit_if_canceled(job_id, state, stage="phase", message="Canceled before phase update"):
        return

    _set_pipeline_stage(state, "phase", message="Rebuilding latest phase snapshot...")
    job_manager._update_db(job_id, "txt_update", "running", message="Refreshing phase snapshot...", progress=92)
    try:
        phase_dt = _run_phase_with_retry(max_attempts=phase_retry, sleep_seconds=phase_retry_sleep)
        state["last_phase_dt"] = int(phase_dt)
        state["last_phase_at"] = datetime.now().isoformat()
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message=f"Phase snapshot refreshed (dt={phase_dt})",
            progress=95,
        )
    except Exception as exc:
        _record_pipeline_failure(state, stage="phase", error=str(exc), message="Phase update failed")
        job_manager._update_db(
            job_id,
            "txt_update",
            "failed",
            error="Phase update failed",
            message=f"Phase update failed: {exc}",
            finished_at=datetime.now(),
        )
        return

    if _exit_if_canceled(job_id, state, stage="phase", message="Canceled after phase update"):
        return

    ml_note_parts: list[str] = []
    try:
        from app.backend.services import ml_service

        if auto_ml_train:
            if _exit_if_canceled(job_id, state, stage="ml_train", message="Canceled before ML training"):
                return
            latest_pred_dt = _to_optional_int(state.get("last_ml_predict_dt"))
            has_prior_ml = bool(state.get("last_ml_train_at") or state.get("last_ml_model_version"))
            skip_train = (
                (not force_ml_train)
                and bool(skip_ml_train_if_no_change)
                and int(changed_files) == 0
                and has_prior_ml
            )
            if skip_train:
                if latest_pred_dt is not None and int(latest_pred_dt) == int(phase_dt):
                    skip_message = f"Skipping ML training (no data change, dt={int(phase_dt)})"
                else:
                    skip_message = (
                        "Skipping ML training (no data change; "
                        f"prediction refresh only, dt={int(phase_dt)})"
                    )
                _set_pipeline_stage(state, "ml_train", message=skip_message)
                job_manager._update_db(
                    job_id,
                    "txt_update",
                    "running",
                    message=skip_message,
                    progress=97,
                )
                ml_note_parts.append("ml_train=skip(no_change)")
            else:
                _set_pipeline_stage(state, "ml_train", message="Refreshing ML training...")
                job_manager._update_db(
                    job_id, "txt_update", "running", message="Refreshing ML training...", progress=97
                )
                ml_report = {"progress": -1, "at": 0.0}

                def _on_ml_train_progress(progress: int, message: str) -> None:
                    progress_clamped = max(0, min(100, int(progress)))
                    now_ts = time.monotonic()
                    prev_progress = int(ml_report["progress"])
                    prev_ts = float(ml_report["at"])
                    if (
                        progress_clamped < 100
                        and prev_progress >= 0
                        and (progress_clamped - prev_progress) < 2
                        and (now_ts - prev_ts) < 1.5
                    ):
                        return
                    ml_report["progress"] = progress_clamped
                    ml_report["at"] = now_ts
                    total_progress = 93 + int(round(progress_clamped * 5 / 100))
                    total_progress = max(93, min(98, total_progress))
                    detail = f"Refreshing ML training... {message} ({progress_clamped}%)"
                    _set_pipeline_stage(state, "ml_train", message=detail)
                    job_manager._update_db(
                        job_id,
                        "txt_update",
                        "running",
                        message=detail,
                        progress=total_progress,
                    )

                train_result = ml_service.train_models(dry_run=False, progress_cb=_on_ml_train_progress)
                state["last_ml_train_at"] = datetime.now().isoformat()
                model_version = train_result.get("model_version")
                if model_version:
                    state["last_ml_model_version"] = str(model_version)
                ml_note_parts.append("ml_train=ok")

        if auto_ml_predict:
            if _exit_if_canceled(job_id, state, stage="ml_predict", message="Canceled before ML prediction"):
                return
            _set_pipeline_stage(state, "ml_predict", message="Refreshing ML prediction...")
            job_manager._update_db(
                job_id, "txt_update", "running", message="Refreshing ML prediction...", progress=98
            )
            pred_result = ml_service.predict_for_dt(dt=phase_dt)
            state["last_ml_predict_at"] = datetime.now().isoformat()
            state["last_ml_predict_dt"] = int(pred_result.get("dt") or phase_dt)
            state["last_ml_predict_rows"] = int(pred_result.get("rows") or 0)
            ml_note_parts.append(f"ml_predict=ok(rows={state['last_ml_predict_rows']})")

            if _exit_if_canceled(job_id, state, stage="ml_live_guard", message="Canceled before ML live guard"):
                return
            _set_pipeline_stage(state, "ml_live_guard", message="Evaluating live guard...")
            job_manager._update_db(
                job_id, "txt_update", "running", message="Evaluating ML live guard...", progress=99
            )
            guard_result = ml_service.enforce_live_guard()
            state["last_ml_live_guard_at"] = datetime.now().isoformat()
            state["last_ml_live_guard_action"] = str(guard_result.get("action") or "unknown")
            state["last_ml_live_guard_reason"] = str(guard_result.get("reason") or "")
            rolled_back_to = guard_result.get("rolled_back_to")
            if rolled_back_to:
                state["last_ml_model_version"] = str(rolled_back_to)
                ml_note_parts.append(f"ml_live_guard=rollback({rolled_back_to})")
            else:
                ml_note_parts.append(f"ml_live_guard={state['last_ml_live_guard_action']}")
        else:
            ml_note_parts.append("ml_predict=skip")
    except Exception as exc:
        print(f"[txt_update_job] ml predict refresh failed: {exc}")
        state["last_ml_error"] = str(exc)
        ml_note_parts.append(f"ml=failed({exc})")
    else:
        state.pop("last_ml_error", None)

    try:
        if _exit_if_canceled(job_id, state, stage="scoring", message="Canceled before scoring refresh"):
            return
        _set_pipeline_stage(state, "scoring", message="Refreshing short scores...")
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message="Refreshing short scores...",
            progress=99,
        )
        from app.backend.api.dependencies import get_stock_repo, init_resources
        from app.backend.jobs.scoring_job import ScoringJob

        # Ensure repository bindings point to the current runtime data dir.
        init_resources(str(config.DATA_DIR))
        score_repo = get_stock_repo()
        scoring_results = ScoringJob(score_repo).run()
        scoring_rows = len(scoring_results) if isinstance(scoring_results, list) else 0
        state["last_scoring_at"] = datetime.now().isoformat()
        state["last_scoring_rows"] = int(scoring_rows)
        ml_note_parts.append(f"scoring=ok(rows={scoring_rows})")
    except Exception as exc:
        logger.exception("Scoring refresh failed: %s", exc)
        _record_pipeline_failure(state, stage="scoring", error=str(exc), message="Scoring refresh failed")
        job_manager._update_db(
            job_id,
            "txt_update",
            "failed",
            error="Scoring refresh failed",
            message=f"Scoring refresh failed: {exc}",
            finished_at=datetime.now(),
        )
        return

    try:
        if _exit_if_canceled(
            job_id,
            state,
            stage="sell_analysis_accum",
            message="Canceled before sell analysis accumulation",
        ):
            return
        _set_pipeline_stage(state, "sell_analysis_accum", message="Accumulating sell analysis data...")
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message="Accumulating sell analysis data...",
            progress=99,
        )
        from app.backend.services.sell_analysis_accumulator import accumulate_sell_analysis

        sell_result = accumulate_sell_analysis(lookback_days=3)
        sell_rows = int(sell_result.get("rows_last_dt") or 0)
        sell_dt = sell_result.get("last_dt")
        state["last_sell_analysis_at"] = datetime.now().isoformat()
        state["last_sell_analysis_rows"] = sell_rows
        state["last_sell_analysis_dt"] = int(sell_dt) if sell_dt is not None else None
        state.pop("last_sell_analysis_error", None)
        ml_note_parts.append(
            f"sell_analysis=ok(dt={state.get('last_sell_analysis_dt')},rows={sell_rows})"
        )
    except Exception as exc:
        logger.exception("Sell analysis accumulation failed: %s", exc)
        state["last_sell_analysis_error"] = str(exc)
        ml_note_parts.append(f"sell_analysis=failed({exc})")

    if auto_fill_missing_history:
        try:
            if _exit_if_canceled(
                job_id,
                state,
                stage="analysis_backfill",
                message="Canceled before analysis backfill",
            ):
                return
            _set_pipeline_stage(
                state,
                "analysis_backfill",
                message=(
                    "Backfilling missing analysis history "
                    f"(lookback={backfill_lookback_days}, max_missing={backfill_max_missing_days})..."
                ),
            )
            job_manager._update_db(
                job_id,
                "txt_update",
                "running",
                message=(
                    "Backfilling missing analysis history "
                    f"(lookback={backfill_lookback_days}, max_missing={backfill_max_missing_days})..."
                ),
                progress=99,
            )
            from app.backend.services.analysis_backfill_service import backfill_missing_analysis_history

            backfill_result = backfill_missing_analysis_history(
                lookback_days=backfill_lookback_days,
                max_missing_days=backfill_max_missing_days,
                include_sell=True,
                include_phase=False,
            )
            state["last_analysis_backfill_at"] = datetime.now().isoformat()
            state["last_analysis_backfill_result"] = {
                "anchor_dt": backfill_result.get("anchor_dt"),
                "missing_ml_total": backfill_result.get("missing_ml_total"),
                "missing_ml_selected": backfill_result.get("missing_ml_selected"),
                "predicted": len(backfill_result.get("predicted_dates") or []),
                "sell_refreshed": len(backfill_result.get("sell_refreshed_dates") or []),
                "errors": len(backfill_result.get("errors") or []),
            }
            state.pop("last_analysis_backfill_error", None)
            ml_note_parts.append(
                "analysis_backfill="
                f"ok(pred={state['last_analysis_backfill_result']['predicted']},"
                f"sell={state['last_analysis_backfill_result']['sell_refreshed']},"
                f"errors={state['last_analysis_backfill_result']['errors']})"
            )
        except Exception as exc:
            logger.exception("Analysis backfill failed: %s", exc)
            state["last_analysis_backfill_error"] = str(exc)
            ml_note_parts.append(f"analysis_backfill=failed({exc})")

    try:
        if _exit_if_canceled(job_id, state, stage="cache_refresh", message="Canceled before cache refresh"):
            return
        _set_pipeline_stage(state, "cache_refresh", message="Refreshing rankings cache...")
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message="Refreshing rankings cache...",
            progress=99,
        )
        from app.backend.services import rankings_cache

        rankings_cache.refresh_cache()
        state["last_cache_refresh_at"] = datetime.now().isoformat()
    except Exception as exc:
        logger.exception("Rankings cache refresh failed: %s", exc)
        _record_pipeline_failure(
            state,
            stage="cache_refresh",
            error=str(exc),
            message="Rankings cache refresh failed",
        )
        job_manager._update_db(
            job_id,
            "txt_update",
            "failed",
            error="Rankings cache refresh failed",
            message=f"Rankings cache refresh failed: {exc}",
            finished_at=datetime.now(),
        )
        return

    walkforward_run_failed = False
    try:
        if _exit_if_canceled(
            job_id,
            state,
            stage="walkforward_run",
            message="Canceled before walkforward run",
        ):
            return
        run_now = datetime.now()
        run_month_key = run_now.strftime("%Y-%m")
        if not auto_walkforward_run:
            state["last_walkforward_run_skipped_at"] = run_now.isoformat()
            state["last_walkforward_run_skipped_reason"] = "disabled"
            state.pop("last_walkforward_run_error", None)
            state.pop("last_walkforward_run_error_at", None)
            ml_note_parts.append("walkforward_run=skip(disabled)")
        elif walkforward_run_monthly_only and str(state.get("last_walkforward_run_month_key") or "") == run_month_key:
            state["last_walkforward_run_skipped_at"] = run_now.isoformat()
            state["last_walkforward_run_skipped_reason"] = f"already_ran_month:{run_month_key}"
            state.pop("last_walkforward_run_error", None)
            state.pop("last_walkforward_run_error_at", None)
            ml_note_parts.append(f"walkforward_run=skip(month={run_month_key})")
        else:
            _set_pipeline_stage(state, "walkforward_run", message="Running strategy walkforward...")
            job_manager._update_db(
                job_id,
                "txt_update",
                "running",
                message="Running strategy walkforward...",
                progress=99,
            )
            from app.backend.services import strategy_backtest_service

            walkforward_cfg = strategy_backtest_service.StrategyBacktestConfig(
                min_long_score=float(walkforward_run_min_long_score),
                min_short_score=float(walkforward_run_min_short_score),
                max_new_entries_per_day=int(walkforward_run_max_new_entries_per_day),
                allowed_sides=str(walkforward_run_allowed_sides),
                allowed_long_setups=tuple(walkforward_run_allowed_long_setups),
                use_regime_filter=bool(walkforward_run_use_regime_filter),
                regime_long_min_breadth_above60=float(walkforward_run_regime_long_min_breadth_above60),
                range_bias_width_min=float(walkforward_run_range_bias_width_min),
                range_bias_long_pos_min=float(walkforward_run_range_bias_long_pos_min),
                range_bias_short_pos_max=float(walkforward_run_range_bias_short_pos_max),
                ma20_count20_min_long=int(walkforward_run_ma20_count20_min_long),
                ma60_count60_min_long=int(walkforward_run_ma60_count60_min_long),
            )
            run_result = strategy_backtest_service.run_strategy_walkforward(
                start_dt=walkforward_run_start_dt,
                end_dt=walkforward_run_end_dt,
                max_codes=int(walkforward_run_max_codes),
                dry_run=False,
                config=walkforward_cfg,
                train_months=int(walkforward_run_train_months),
                test_months=int(walkforward_run_test_months),
                step_months=int(walkforward_run_step_months),
                min_windows=int(walkforward_run_min_windows),
            )
            run_id = str(run_result.get("run_id") or "")
            run_summary = run_result.get("summary") if isinstance(run_result.get("summary"), dict) else {}
            state["last_walkforward_run_at"] = datetime.now().isoformat()
            state["last_walkforward_run_month_key"] = run_month_key
            state["last_walkforward_run_run_id"] = run_id
            state["last_walkforward_run_windowing"] = run_result.get("windowing") or {}
            state["last_walkforward_run_summary"] = run_summary
            state.pop("last_walkforward_run_error", None)
            state.pop("last_walkforward_run_error_at", None)
            state.pop("last_walkforward_run_skipped_at", None)
            state.pop("last_walkforward_run_skipped_reason", None)
            ml_note_parts.append(
                "walkforward_run="
                f"ok(run={run_id or 'unknown'},"
                f"oos_pnl={run_summary.get('oos_total_realized_unit_pnl')},"
                f"oos_pf={run_summary.get('oos_mean_profit_factor')})"
            )
    except Exception as exc:
        logger.exception("Walkforward run failed: %s", exc)
        state["last_walkforward_run_error"] = str(exc)
        state["last_walkforward_run_error_at"] = datetime.now().isoformat()
        walkforward_run_failed = True
        ml_note_parts.append(f"walkforward_run=failed({exc})")
        if walkforward_run_strict:
            _record_pipeline_failure(
                state,
                stage="walkforward_run",
                error=str(exc),
                message="Walkforward run failed",
            )
            job_manager._update_db(
                job_id,
                "txt_update",
                "failed",
                error="Walkforward run failed",
                message=f"Walkforward run failed: {exc}",
                finished_at=datetime.now(),
            )
            return

    try:
        if _exit_if_canceled(
            job_id,
            state,
            stage="walkforward_gate",
            message="Canceled before walkforward gate",
        ):
            return
        gate_now = datetime.now()
        gate_month_key = gate_now.strftime("%Y-%m")
        latest_run_id = str(state.get("last_walkforward_run_run_id") or "")
        last_gate_source_run_id = str(state.get("last_walkforward_gate_source_run_id") or "")
        if walkforward_run_failed:
            state["last_walkforward_gate_skipped_at"] = gate_now.isoformat()
            state["last_walkforward_gate_skipped_reason"] = "walkforward_run_failed"
            state.pop("last_walkforward_gate_error", None)
            state.pop("last_walkforward_gate_error_at", None)
            ml_note_parts.append("walkforward_gate=skip(run_failed)")
        elif not auto_walkforward_gate:
            state["last_walkforward_gate_skipped_at"] = gate_now.isoformat()
            state["last_walkforward_gate_skipped_reason"] = "disabled"
            state.pop("last_walkforward_gate_error", None)
            state.pop("last_walkforward_gate_error_at", None)
            ml_note_parts.append("walkforward_gate=skip(disabled)")
        elif (
            walkforward_gate_monthly_only
            and str(state.get("last_walkforward_gate_month_key") or "") == gate_month_key
            and ((not latest_run_id) or latest_run_id == last_gate_source_run_id)
        ):
            state["last_walkforward_gate_skipped_at"] = gate_now.isoformat()
            state["last_walkforward_gate_skipped_reason"] = f"already_ran_month:{gate_month_key}"
            state.pop("last_walkforward_gate_error", None)
            state.pop("last_walkforward_gate_error_at", None)
            ml_note_parts.append(f"walkforward_gate=skip(month={gate_month_key})")
        else:
            _set_pipeline_stage(state, "walkforward_gate", message="Evaluating strategy walkforward gate...")
            job_manager._update_db(
                job_id,
                "txt_update",
                "running",
                message="Evaluating strategy walkforward gate...",
                progress=99,
            )
            from app.backend.services import strategy_backtest_service

            gate_result = strategy_backtest_service.run_strategy_walkforward_gate(
                min_oos_total_realized_unit_pnl=walkforward_gate_min_oos_total,
                min_oos_mean_profit_factor=walkforward_gate_min_oos_pf,
                min_oos_positive_window_ratio=walkforward_gate_min_oos_pos_ratio,
                dry_run=False,
                note=f"txt_update_job:{job_id}:run={latest_run_id or 'unknown'}",
            )
            source = gate_result.get("source") if isinstance(gate_result.get("source"), dict) else {}
            source_run_id = str(source.get("run_id") or "")
            state["last_walkforward_gate_at"] = datetime.now().isoformat()
            state["last_walkforward_gate_month_key"] = gate_month_key
            state["last_walkforward_gate_gate_id"] = str(gate_result.get("gate_id") or "")
            state["last_walkforward_gate_source_run_id"] = source_run_id
            state["last_walkforward_gate_source_finished_at"] = source.get("finished_at")
            state["last_walkforward_gate_status"] = str(gate_result.get("status") or "")
            state["last_walkforward_gate_passed"] = bool(gate_result.get("passed"))
            state["last_walkforward_gate_thresholds"] = gate_result.get("thresholds") or {}
            state.pop("last_walkforward_gate_error", None)
            state.pop("last_walkforward_gate_error_at", None)
            state.pop("last_walkforward_gate_skipped_at", None)
            state.pop("last_walkforward_gate_skipped_reason", None)
            passed = bool(gate_result.get("passed"))
            ml_note_parts.append(
                f"walkforward_gate={'pass' if passed else 'fail'}"
                f"(run={source_run_id or 'unknown'})"
            )
            if walkforward_gate_strict and not passed:
                error_msg = "Walkforward gate failed"
                _record_pipeline_failure(
                    state,
                    stage="walkforward_gate",
                    error=error_msg,
                    message=f"{error_msg} (source_run_id={source_run_id or 'unknown'})",
                )
                job_manager._update_db(
                    job_id,
                    "txt_update",
                    "failed",
                    error=error_msg,
                    message=f"{error_msg} (source_run_id={source_run_id or 'unknown'})",
                    finished_at=datetime.now(),
                )
                return
    except Exception as exc:
        logger.exception("Walkforward gate evaluation failed: %s", exc)
        state["last_walkforward_gate_error"] = str(exc)
        state["last_walkforward_gate_error_at"] = datetime.now().isoformat()
        ml_note_parts.append(f"walkforward_gate=failed({exc})")
        if walkforward_gate_strict:
            _record_pipeline_failure(
                state,
                stage="walkforward_gate",
                error=str(exc),
                message="Walkforward gate failed",
            )
            job_manager._update_db(
                job_id,
                "txt_update",
                "failed",
                error="Walkforward gate failed",
                message=f"Walkforward gate failed: {exc}",
                finished_at=datetime.now(),
            )
            return

    if _exit_if_canceled(job_id, state, stage="finalize", message="Canceled before finalize"):
        return

    completion_ts = datetime.now()
    state.update(
        {
            "last_txt_update_at": completion_ts.isoformat(),
            "last_txt_update_date": completion_ts.date().isoformat(),
        }
    )
    ml_note = f" [{' / '.join(ml_note_parts)}]" if ml_note_parts else ""
    _record_pipeline_success(
        state,
        stage="finalize",
        message=f"{summary_line}. Ingest + Phase + Scoring completed.{ml_note}",
    )
    job_manager._update_db(
        job_id,
        "txt_update",
        "success",
        message=f"{summary_line}. Ingest + Phase + Scoring completed.{ml_note}",
        progress=100,
        finished_at=completion_ts,
    )


def run_vbs_update(job_id: str, code_path: str, out_dir: str, *, timeout: int = 1800) -> tuple[int, list[str]]:
    """Legacy wrapper so callers can keep passing job_id first."""
    return run_vbs_export(code_path, out_dir, timeout=timeout)

