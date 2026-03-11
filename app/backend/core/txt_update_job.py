from __future__ import annotations

import io
import json
import logging
import os
import queue
import random
import subprocess
import threading
import time
import traceback
from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime
from typing import Any, Callable

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
_RETRY_TRACE_MAX = 200
_RETRY_JITTER_RATIO = 0.20
_VBS_PROGRESS_FILE_NAME = "vbs_progress.json"
_TXT_UPDATE_JOB_TYPE = "txt_update"
_TXT_FOLLOWUP_JOB_TYPE = "txt_followup"
_COMPLETION_MODE_FULL = "full"
_COMPLETION_MODE_PRACTICAL_FAST = "practical_fast"


def _update_vbs_path() -> str:
    return os.path.abspath(str(config.PAN_EXPORT_VBS_PATH))


def _pan_out_txt_dir() -> str:
    return os.path.abspath(str(config.PAN_OUT_TXT_DIR))


def _scale_progress(progress: int, start: int, end: int) -> int:
    progress_clamped = max(0, min(100, int(progress)))
    if end <= start:
        return int(start)
    return int(start) + int(round((int(end) - int(start)) * progress_clamped / 100))


def _read_vbs_progress(out_dir: str) -> dict[str, Any] | None:
    progress_path = os.path.join(str(out_dir), _VBS_PROGRESS_FILE_NAME)
    if not os.path.isfile(progress_path):
        return None
    try:
        with open(progress_path, "r", encoding="utf-8-sig") as handle:
            payload = json.load(handle)
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


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


def _trim_retry_trace(state: dict) -> None:
    trace = state.get("retry_trace")
    if not isinstance(trace, list):
        state["retry_trace"] = []
        return
    if len(trace) > _RETRY_TRACE_MAX:
        state["retry_trace"] = trace[-_RETRY_TRACE_MAX:]


def _append_retry_trace(
    state: dict,
    *,
    stage: str,
    operation: str,
    attempt: int,
    max_attempts: int,
    kind: str,
    error: str,
    will_retry: bool,
    sleep_seconds: float | None,
) -> None:
    _trim_retry_trace(state)
    trace = state.get("retry_trace")
    if not isinstance(trace, list):
        trace = []
        state["retry_trace"] = trace
    trace.append(
        {
            "at": datetime.now().isoformat(),
            "stage": stage,
            "operation": operation,
            "attempt": int(attempt),
            "max_attempts": int(max_attempts),
            "kind": kind,
            "error": str(error),
            "will_retry": bool(will_retry),
            "sleep_seconds": float(sleep_seconds) if sleep_seconds is not None else None,
        }
    )
    _trim_retry_trace(state)


def _set_retry_summary(
    state: dict,
    *,
    stage: str,
    operation: str,
    attempts: int,
    status: str,
    kind: str,
    error: str | None = None,
) -> None:
    now_iso = datetime.now().isoformat()
    state["last_retry_summary"] = {
        "at": now_iso,
        "stage": stage,
        "operation": operation,
        "attempts": int(attempts),
        "status": status,
        "kind": kind,
        "error": str(error) if error else None,
    }
    state["last_retry_stage"] = stage
    state["last_retry_reason"] = kind
    state["last_retry_count"] = int(attempts)
    if status == "failed":
        state["last_retry_exhausted_stage"] = stage
        state["last_retry_exhausted_kind"] = kind
    else:
        state.pop("last_retry_exhausted_stage", None)
        state.pop("last_retry_exhausted_kind", None)


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


def _set_followup_stage(
    state: dict,
    stage: str,
    *,
    status: str = "running",
    message: str | None = None,
    save: bool = True,
) -> None:
    now_iso = datetime.now().isoformat()
    state["last_followup_stage"] = stage
    state["last_followup_stage_status"] = status
    state["last_followup_stage_at"] = now_iso
    state["last_followup_status"] = status
    if message is not None:
        state["last_followup_message"] = message
    if save:
        _save_update_state(state)


def _record_followup_failure(state: dict, *, stage: str, error: str, message: str | None = None) -> None:
    now_iso = datetime.now().isoformat()
    state["last_followup_status"] = "failed"
    state["last_followup_finished_at"] = now_iso
    state["last_followup_failed_at"] = now_iso
    state["last_followup_failed_stage"] = stage
    state["last_followup_error"] = str(error)
    state["last_followup_error_message"] = message or str(error)
    _set_followup_stage(state, stage, status="failed", message=message or str(error), save=False)
    _save_update_state(state)


def _record_followup_canceled(state: dict, *, stage: str, message: str) -> None:
    now_iso = datetime.now().isoformat()
    state["last_followup_status"] = "canceled"
    state["last_followup_finished_at"] = now_iso
    state["last_followup_canceled_at"] = now_iso
    state["last_followup_error"] = "canceled"
    state["last_followup_error_message"] = message
    _set_followup_stage(state, stage, status="canceled", message=message, save=False)
    _save_update_state(state)


def _record_followup_success(state: dict, *, stage: str, message: str) -> None:
    now_iso = datetime.now().isoformat()
    state["last_followup_status"] = "success"
    state["last_followup_finished_at"] = now_iso
    state.pop("last_followup_error", None)
    state.pop("last_followup_error_message", None)
    _set_followup_stage(state, stage, status="success", message=message, save=False)
    _save_update_state(state)


def _normalize_completion_mode(value: object) -> str:
    text = str(value or "").strip().lower()
    if text == _COMPLETION_MODE_PRACTICAL_FAST:
        return _COMPLETION_MODE_PRACTICAL_FAST
    return _COMPLETION_MODE_FULL


def _record_followup_enqueued(state: dict, *, source_job_id: str, followup_job_id: str) -> None:
    now_iso = datetime.now().isoformat()
    state["last_followup_job_id"] = str(followup_job_id)
    state["last_followup_enqueued_at"] = now_iso
    state["last_followup_source_txt_job_id"] = str(source_job_id)
    state["last_followup_status"] = "queued"
    state.pop("last_followup_error", None)
    _save_update_state(state)


def _queue_txt_followup(
    state: dict,
    *,
    source_job_id: str,
    payload: dict[str, Any],
) -> str | None:
    followup_job_id = job_manager.submit(_TXT_FOLLOWUP_JOB_TYPE, payload, unique=False)
    if followup_job_id:
        _record_followup_enqueued(
            state,
            source_job_id=str(source_job_id),
            followup_job_id=str(followup_job_id),
        )
    return followup_job_id


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
    progress_cb: Callable[[dict[str, Any]], None] | None = None,
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
    last_progress_key: tuple[Any, ...] | None = None
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
                if progress_cb is not None:
                    snapshot = _read_vbs_progress(out_dir)
                    if snapshot is not None:
                        progress_key = (
                            snapshot.get("phase"),
                            snapshot.get("current"),
                            snapshot.get("started"),
                            snapshot.get("processed"),
                            snapshot.get("ok"),
                            snapshot.get("err"),
                            snapshot.get("split"),
                            snapshot.get("error"),
                        )
                        if progress_key != last_progress_key:
                            last_progress_key = progress_key
                            progress_cb(snapshot)
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


def run_ingest(
    incremental: bool = True,
    run_id: str | None = None,
    progress_cb: Callable[[int, str], None] | None = None,
) -> tuple[str, str, dict]:
    print(f"[txt_update_job] run_ingest called incremental={incremental}")
    if not ingest_txt:
        error = "ingest_txt module not found"
        print(f"[txt_update_job] ERROR: {error}")
        return "", error, {}

    buffer = io.StringIO()
    stats: dict[str, int | str] = {}
    try:
        with redirect_stdout(buffer), redirect_stderr(buffer):
            result = ingest_txt.ingest(incremental=incremental, run_id=run_id, progress_cb=progress_cb)
        output = buffer.getvalue()
        if isinstance(result, dict):
            for key in ("changed_files", "changed", "skipped_files", "skipped", "rows", "pan_finalized_rows"):
                if key in result:
                    stats[key] = result[key]  # type: ignore[index]
        if not stats:
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


def _classify_retry_exception(exc: Exception) -> str:
    if _is_transient_db_lock_error(exc):
        return "db_lock"
    return "other"


def _compute_retry_sleep_seconds(base_sleep_seconds: float, attempt: int) -> float:
    base = max(0.1, float(base_sleep_seconds))
    exponent = max(0, int(attempt) - 1)
    jitter = 1.0 + random.uniform(-_RETRY_JITTER_RATIO, _RETRY_JITTER_RATIO)
    return max(0.1, base * (2 ** exponent) * jitter)


def _execute_with_retry(
    *,
    stage: str,
    operation: str,
    max_attempts: int,
    sleep_seconds: float,
    state: dict | None,
    run_once: Callable[[], Any],
    classify_error: Callable[[Exception], str],
    retry_if: Callable[[Exception], bool],
) -> tuple[bool, Any, int, str, str | None]:
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        try:
            value = run_once()
        except Exception as exc:
            kind = classify_error(exc)
            should_retry = retry_if(exc)
            will_retry = attempt < max_attempts and should_retry
            sleep_for = _compute_retry_sleep_seconds(sleep_seconds, attempt) if will_retry else None
            if state is not None:
                _append_retry_trace(
                    state,
                    stage=stage,
                    operation=operation,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    kind=kind,
                    error=str(exc),
                    will_retry=will_retry,
                    sleep_seconds=sleep_for,
                )
                _set_retry_summary(
                    state,
                    stage=stage,
                    operation=operation,
                    attempts=attempt,
                    status="retrying" if will_retry else "failed",
                    kind=kind,
                    error=str(exc),
                )
                _save_update_state(state)
            if not will_retry:
                logger.warning(
                    "%s failed (%s/%s, kind=%s): %s",
                    operation,
                    attempt,
                    max_attempts,
                    kind,
                    exc,
                )
                return False, None, attempt, kind, str(exc)
            logger.warning(
                "%s retry (%s/%s, kind=%s) after %.2fs: %s",
                operation,
                attempt,
                max_attempts,
                kind,
                float(sleep_for or 0.0),
                exc,
            )
            time.sleep(max(0.1, float(sleep_for or 0.1)))
            continue
        if state is not None:
            _set_retry_summary(
                state,
                stage=stage,
                operation=operation,
                attempts=attempt,
                status="success",
                kind="none",
            )
            _save_update_state(state)
        return True, value, attempt, "none", None
    return False, None, attempt, "other", "retry_exhausted"


def _run_phase_with_retry(
    *,
    max_attempts: int,
    sleep_seconds: float,
    state: dict | None = None,
    stage: str = "phase",
) -> int:
    ok, value, _attempts, _kind, error_text = _execute_with_retry(
        stage=stage,
        operation="phase_update",
        max_attempts=max_attempts,
        sleep_seconds=sleep_seconds,
        state=state,
        run_once=_run_phase_batch_latest,
        classify_error=_classify_retry_exception,
        retry_if=_is_transient_db_lock_error,
    )
    if ok:
        return int(value)
    raise RuntimeError(error_text or "phase update failed")


def _run_ingest_with_retry(
    *,
    incremental: bool,
    max_attempts: int,
    sleep_seconds: float,
    state: dict | None = None,
    stage: str = "ingest",
    run_id: str | None = None,
    progress_cb: Callable[[int, str], None] | None = None,
) -> tuple[str, str, dict, int, str]:
    last_output = ""
    last_stats: dict = {}

    def _run_once() -> tuple[str, dict]:
        nonlocal last_output, last_stats
        out, err, stats = run_ingest(incremental=incremental, run_id=run_id, progress_cb=progress_cb)
        last_output = out
        last_stats = stats
        if err:
            raise RuntimeError(err)
        return out, stats

    ok, value, attempts, error_kind, error_text = _execute_with_retry(
        stage=stage,
        operation="ingest_incremental" if incremental else "ingest_full",
        max_attempts=max_attempts,
        sleep_seconds=sleep_seconds,
        state=state,
        run_once=_run_once,
        classify_error=lambda exc: _classify_ingest_error_text(str(exc)),
        retry_if=lambda exc: _classify_ingest_error_text(str(exc)) == "db_lock",
    )
    if ok and value is not None:
        out, stats = value
        return out, "", stats, attempts, "none"
    return last_output, str(error_text or "ingest_failed"), last_stats, attempts, error_kind


def _is_job_canceled(job_id: str) -> bool:
    return job_manager.is_cancel_requested(job_id)


def _mark_job_canceled(
    job_id: str,
    message: str = "Canceled",
    *,
    state: dict | None = None,
    stage: str = "cancel",
    job_type: str = _TXT_UPDATE_JOB_TYPE,
) -> None:
    if state is not None:
        _record_pipeline_canceled(state, stage=stage, message=message)
    job_manager._update_db(
        job_id,
        job_type,
        "canceled",
        message=message,
        error="canceled",
        finished_at=datetime.now(),
    )


def _exit_if_canceled(
    job_id: str,
    state: dict,
    *,
    stage: str,
    message: str,
    job_type: str = _TXT_UPDATE_JOB_TYPE,
) -> bool:
    if not _is_job_canceled(job_id):
        return False
    _mark_job_canceled(job_id, message, state=state, stage=stage, job_type=job_type)
    return True


def _mark_followup_canceled(
    job_id: str,
    message: str = "Canceled",
    *,
    state: dict | None = None,
    stage: str = "cancel",
) -> None:
    if state is not None:
        _record_followup_canceled(state, stage=stage, message=message)
    job_manager._update_db(
        job_id,
        _TXT_FOLLOWUP_JOB_TYPE,
        "canceled",
        message=message,
        error="canceled",
        finished_at=datetime.now(),
    )


def _exit_followup_if_canceled(job_id: str, state: dict, *, stage: str, message: str) -> bool:
    if not _is_job_canceled(job_id):
        return False
    _mark_followup_canceled(job_id, message, state=state, stage=stage)
    return True


def handle_txt_update(job_id: str, payload: dict) -> None:
    completion_mode = _normalize_completion_mode(payload.get("completion_mode"))
    auto_ml_predict = _to_bool(payload.get("auto_ml_predict"), True)
    auto_ml_train = _to_bool(payload.get("auto_ml_train"), True)
    force_ml_train = _to_bool(payload.get("force_ml_train"), False)
    force_recompute_on_pan_finalize = _to_bool(payload.get("force_recompute_on_pan_finalize"), True)
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
    walkforward_gate_min_oos_worst_dd = _to_float(
        payload.get("walkforward_gate_min_oos_worst_max_drawdown_unit"),
        _to_float(
            os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_GATE_MIN_OOS_WORST_MAX_DRAWDOWN_UNIT"),
            -0.12,
            minimum=-1.0,
        ),
        minimum=-1.0,
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
    raw_walkforward_run_allowed_short_setups = (
        payload.get("walkforward_run_allowed_short_setups")
        if payload.get("walkforward_run_allowed_short_setups") is not None
        else os.getenv("MEEMEE_TXT_UPDATE_WALKFORWARD_RUN_ALLOWED_SHORT_SETUPS")
    )
    walkforward_run_allowed_short_setups: tuple[str, ...]
    if raw_walkforward_run_allowed_short_setups is None:
        walkforward_run_allowed_short_setups = (
            "short_crash_top_p3",
            "short_downtrend_p4",
            "short_failed_high_p1",
            "short_box_fail_p2",
            "short_ma20_break_p5",
            "short_decision_down",
            "short_entry",
        )
    elif isinstance(raw_walkforward_run_allowed_short_setups, (list, tuple, set)):
        parsed = [str(v).strip() for v in raw_walkforward_run_allowed_short_setups if str(v).strip()]
        walkforward_run_allowed_short_setups = (
            tuple(parsed)
            if parsed
            else (
                "short_crash_top_p3",
                "short_downtrend_p4",
                "short_failed_high_p1",
                "short_box_fail_p2",
                "short_ma20_break_p5",
                "short_decision_down",
                "short_entry",
            )
        )
    else:
        parsed = [
            s.strip()
            for s in str(raw_walkforward_run_allowed_short_setups).split(",")
            if s.strip()
        ]
        walkforward_run_allowed_short_setups = (
            tuple(parsed)
            if parsed
            else (
                "short_crash_top_p3",
                "short_downtrend_p4",
                "short_failed_high_p1",
                "short_box_fail_p2",
                "short_ma20_break_p5",
                "short_decision_down",
                "short_entry",
            )
        )
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
    _trim_retry_trace(state)
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
    EXPORT_PROGRESS_START = 10
    EXPORT_PROGRESS_END = 68

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
            pan_dt_path = getattr(config, "PAN_DTMGR_PATH", None)
            pan_import_ok = run_pan_import(str(pan_dt_path) if pan_dt_path else None)
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
                progress=min(4, 1 + attempt),
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
    job_manager._update_db(
        job_id,
        "txt_update",
        "running",
        message="Running Pan Rolling export...",
        progress=EXPORT_PROGRESS_START,
    )

    output_lines: list[str] = []
    vbs_code = -1
    vbs_progress_report = {"progress": -1, "message": ""}

    def _on_vbs_export_progress(snapshot: dict[str, Any]) -> None:
        phase = str(snapshot.get("phase") or "").strip().lower()
        current = str(snapshot.get("current") or "").strip()
        started = max(0, _to_int(snapshot.get("started"), 0, minimum=0))
        processed = max(0, _to_int(snapshot.get("processed"), 0, minimum=0))
        ok_count = max(0, _to_int(snapshot.get("ok"), 0, minimum=0))
        err_count = max(0, _to_int(snapshot.get("err"), 0, minimum=0))
        split_count = max(0, _to_int(snapshot.get("split"), 0, minimum=0))
        if started > 0:
            export_pct = int(round(100 * min(processed, started) / max(1, started)))
        elif phase == "done":
            export_pct = 100
        elif phase in {"starting", "exporting"}:
            export_pct = 5
        else:
            export_pct = 0
        total_progress = _scale_progress(export_pct, EXPORT_PROGRESS_START, EXPORT_PROGRESS_END)
        if phase == "done":
            detail = f"Pan Rolling export completed ({ok_count}/{max(1, started)} ok, err={err_count})"
        elif phase == "exporting":
            code_label = f" code={current}" if current else ""
            detail = (
                "Pan Rolling export "
                f"{processed}/{max(1, started)}{code_label} "
                f"(ok={ok_count}, err={err_count}, split={split_count})"
            )
        elif phase == "starting":
            detail = f"Preparing Pan Rolling export target list ({started} codes)..."
        elif phase == "booting":
            detail = "Starting Pan Rolling export..."
        elif phase == "error":
            error_text = str(snapshot.get("error") or "unknown error")
            detail = f"Pan Rolling export progress failed: {error_text}"
        else:
            detail = "Running Pan Rolling export..."
        if (
            int(vbs_progress_report["progress"]) == int(total_progress)
            and str(vbs_progress_report["message"]) == detail
        ):
            return
        vbs_progress_report["progress"] = int(total_progress)
        vbs_progress_report["message"] = detail
        _set_pipeline_stage(state, "export", message=detail)
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message=detail,
            progress=int(total_progress),
        )

    for attempt in range(1, vbs_retry + 1):
        attempt_timeout = int(vbs_timeout + (attempt - 1) * vbs_timeout_backoff)
        vbs_code, output_lines = run_vbs_export(
            code_path,
            out_dir,
            timeout=attempt_timeout,
            should_cancel=lambda: _is_job_canceled(job_id),
            progress_cb=_on_vbs_export_progress,
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
    ingest_report = {"message": "", "progress": -1}

    def _on_ingest_progress(progress: int, message: str) -> None:
        total_progress = _scale_progress(progress, 85, 92)
        detail = f"Ingesting incremental TXT data... {message}"
        if (
            int(ingest_report["progress"]) == int(total_progress)
            and str(ingest_report["message"]) == detail
        ):
            return
        ingest_report["progress"] = int(total_progress)
        ingest_report["message"] = detail
        _set_pipeline_stage(state, "ingest", message=detail)
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message=detail,
            progress=int(total_progress),
        )

    _ingest_out, ingest_err, ingest_stats, ingest_attempts, ingest_error_kind = _run_ingest_with_retry(
        incremental=True,
        max_attempts=ingest_retry,
        sleep_seconds=ingest_retry_sleep,
        state=state,
        stage="ingest",
        run_id=job_id,
        progress_cb=_on_ingest_progress,
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
    changed_files = _to_int(
        ingest_stats.get("changed_files"),
        _to_int(ingest_stats.get("changed"), 0, minimum=0),
        minimum=0,
    )
    pan_finalized_rows = _to_int(ingest_stats.get("pan_finalized_rows"), 0, minimum=0)
    state["last_pan_finalize_rows"] = int(pan_finalized_rows)
    state["last_force_recompute_on_pan_finalize"] = bool(force_recompute_on_pan_finalize)
    if pan_finalized_rows > 0:
        state["last_pan_finalize_at"] = datetime.now().isoformat()

    force_recompute_due_to_pan_finalize = bool(force_recompute_on_pan_finalize and pan_finalized_rows > 0)
    effective_auto_ml_train = bool(auto_ml_train or force_recompute_due_to_pan_finalize)
    effective_auto_ml_predict = bool(auto_ml_predict or force_recompute_due_to_pan_finalize)
    effective_auto_walkforward_run = bool(auto_walkforward_run or force_recompute_due_to_pan_finalize)
    effective_auto_walkforward_gate = bool(auto_walkforward_gate or force_recompute_due_to_pan_finalize)
    if force_recompute_due_to_pan_finalize:
        state["last_forced_recompute_at"] = datetime.now().isoformat()

    if _exit_if_canceled(job_id, state, stage="phase", message="Canceled before phase update"):
        return

    _set_pipeline_stage(state, "phase", message="Rebuilding latest phase snapshot...")
    job_manager._update_db(job_id, "txt_update", "running", message="Refreshing phase snapshot...", progress=92)
    try:
        phase_dt = _run_phase_with_retry(
            max_attempts=phase_retry,
            sleep_seconds=phase_retry_sleep,
            state=state,
            stage="phase",
        )
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
    ML_TRAIN_PROGRESS_START = 93
    ML_TRAIN_PROGRESS_DONE = 94
    ML_PREDICT_PROGRESS = 95
    ML_LIVE_GUARD_PROGRESS = 96
    SCORING_PROGRESS = 97
    SELL_ANALYSIS_PROGRESS = 97
    ANALYSIS_BACKFILL_PROGRESS = 98
    CACHE_REFRESH_PROGRESS = 98
    WALKFORWARD_RUN_PROGRESS = 98
    WALKFORWARD_GATE_PROGRESS = 98
    FINALIZING_PROGRESS = 99

    if completion_mode == _COMPLETION_MODE_PRACTICAL_FAST:
        if force_recompute_due_to_pan_finalize:
            ml_note_parts.append(f"pan_finalize_force_recompute(rows={int(pan_finalized_rows)})")
        ml_note_parts.append("ml=queued(background)")
    else:
        try:
            from app.backend.services import ml_service

            if force_recompute_due_to_pan_finalize:
                ml_note_parts.append(f"pan_finalize_force_recompute(rows={int(pan_finalized_rows)})")

            if effective_auto_ml_train:
                if _exit_if_canceled(job_id, state, stage="ml_train", message="Canceled before ML training"):
                    return
                latest_pred_dt = _to_optional_int(state.get("last_ml_predict_dt"))
                has_prior_ml = bool(state.get("last_ml_train_at") or state.get("last_ml_model_version"))
                skip_train = (
                    (not force_ml_train)
                    and bool(skip_ml_train_if_no_change)
                    and (not force_recompute_due_to_pan_finalize)
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
                        progress=ML_TRAIN_PROGRESS_DONE,
                    )
                    ml_note_parts.append("ml_train=skip(no_change)")
                else:
                    _set_pipeline_stage(state, "ml_train", message="Refreshing ML training...")
                    job_manager._update_db(
                        job_id,
                        "txt_update",
                        "running",
                        message="Refreshing ML training...",
                        progress=ML_TRAIN_PROGRESS_START,
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
                        total_progress = ML_TRAIN_PROGRESS_START + int(round(progress_clamped / 100))
                        total_progress = max(ML_TRAIN_PROGRESS_START, min(ML_TRAIN_PROGRESS_DONE, total_progress))
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
            else:
                ml_note_parts.append("ml_train=skip(disabled)")

            if effective_auto_ml_predict:
                if _exit_if_canceled(job_id, state, stage="ml_predict", message="Canceled before ML prediction"):
                    return
                _set_pipeline_stage(state, "ml_predict", message="Refreshing ML prediction...")
                job_manager._update_db(
                    job_id,
                    "txt_update",
                    "running",
                    message="Refreshing ML prediction...",
                    progress=ML_PREDICT_PROGRESS,
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
                    job_id,
                    "txt_update",
                    "running",
                    message="Evaluating ML live guard...",
                    progress=ML_LIVE_GUARD_PROGRESS,
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
            progress=SCORING_PROGRESS,
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
            progress=SELL_ANALYSIS_PROGRESS,
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

    if completion_mode != _COMPLETION_MODE_PRACTICAL_FAST and auto_fill_missing_history:
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
                progress=ANALYSIS_BACKFILL_PROGRESS,
            )
            from app.backend.services.analysis_backfill_service import backfill_missing_analysis_history

            analysis_backfill_report = {"message": ""}

            def _on_analysis_backfill_progress(progress: int, message: str) -> None:
                detail = f"Backfilling missing analysis history... {message}"
                if str(analysis_backfill_report["message"]) == detail:
                    return
                analysis_backfill_report["message"] = detail
                _set_pipeline_stage(state, "analysis_backfill", message=detail)
                job_manager._update_db(
                    job_id,
                    "txt_update",
                    "running",
                    message=detail,
                    progress=ANALYSIS_BACKFILL_PROGRESS,
                )

            backfill_result = backfill_missing_analysis_history(
                lookback_days=backfill_lookback_days,
                max_missing_days=backfill_max_missing_days,
                include_sell=True,
                include_phase=False,
                progress_cb=_on_analysis_backfill_progress,
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

    if completion_mode != _COMPLETION_MODE_PRACTICAL_FAST:
        try:
            from app.backend.core.analysis_prewarm_job import schedule_analysis_prewarm_if_needed

            prewarm_job_id = schedule_analysis_prewarm_if_needed(source=f"txt_update:{job_id}")
            state["last_analysis_prewarm_submit_at"] = datetime.now().isoformat()
            state["last_analysis_prewarm_job_id"] = prewarm_job_id
            if prewarm_job_id:
                ml_note_parts.append(f"analysis_prewarm=queued({prewarm_job_id})")
            else:
                ml_note_parts.append("analysis_prewarm=skip(covered_or_active)")
        except Exception as exc:
            logger.warning("Analysis prewarm submit skipped: %s", exc)
            state["last_analysis_prewarm_error"] = str(exc)
            ml_note_parts.append(f"analysis_prewarm=failed({exc})")

    try:
        if _exit_if_canceled(job_id, state, stage="cache_refresh", message="Canceled before cache refresh"):
            return
        _set_pipeline_stage(state, "cache_refresh", message="Refreshing rankings cache...")
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message="Refreshing rankings cache...",
            progress=CACHE_REFRESH_PROGRESS,
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

    if completion_mode == _COMPLETION_MODE_PRACTICAL_FAST:
        should_queue_followup = bool(
            effective_auto_ml_train
            or effective_auto_ml_predict
            or auto_fill_missing_history
            or effective_auto_walkforward_run
            or effective_auto_walkforward_gate
        )
        followup_job_id: str | None = None
        if should_queue_followup:
            followup_payload = dict(payload)
            followup_payload.update(
                {
                    "source_txt_job_id": str(job_id),
                    "phase_dt": int(phase_dt),
                    "changed_files": int(changed_files),
                    "pan_finalized_rows": int(pan_finalized_rows),
                    "summary_line": str(summary_line),
                }
            )
            followup_job_id = _queue_txt_followup(
                state,
                source_job_id=str(job_id),
                payload=followup_payload,
            )
            if followup_job_id:
                ml_note_parts.append(f"followup=queued({followup_job_id})")
            else:
                ml_note_parts.append("followup=skip(queue_rejected)")
        completion_ts = datetime.now()
        _set_pipeline_stage(state, "finalize", message="Finalizing update status...")
        job_manager._update_db(
            job_id,
            _TXT_UPDATE_JOB_TYPE,
            "running",
            message="Finalizing update status...",
            progress=FINALIZING_PROGRESS,
        )
        state.update(
            {
                "last_txt_update_at": completion_ts.isoformat(),
                "last_txt_update_date": completion_ts.date().isoformat(),
            }
        )
        base_message = (
            f"{summary_line}. 日次更新は完了。重い後続処理はバックグラウンドで継続中。"
            if followup_job_id
            else f"{summary_line}. Ingest + Phase + Scoring completed."
        )
        ml_note = f" [{' / '.join(ml_note_parts)}]" if ml_note_parts else ""
        final_message = f"{base_message}{ml_note}"
        _record_pipeline_success(state, stage="finalize", message=final_message)
        job_manager._update_db(
            job_id,
            _TXT_UPDATE_JOB_TYPE,
            "success",
            message=final_message,
            progress=100,
            finished_at=completion_ts,
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
        if not effective_auto_walkforward_run:
            state["last_walkforward_run_skipped_at"] = run_now.isoformat()
            state["last_walkforward_run_skipped_reason"] = "disabled"
            state.pop("last_walkforward_run_error", None)
            state.pop("last_walkforward_run_error_at", None)
            ml_note_parts.append("walkforward_run=skip(disabled)")
        elif (
            (not force_recompute_due_to_pan_finalize)
            and walkforward_run_monthly_only
            and str(state.get("last_walkforward_run_month_key") or "") == run_month_key
        ):
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
                progress=WALKFORWARD_RUN_PROGRESS,
            )
            from app.backend.services import strategy_backtest_service

            walkforward_report = {"message": "", "progress": -1}

            def _on_walkforward_run_progress(progress: int, message: str) -> None:
                total_progress = _scale_progress(progress, WALKFORWARD_RUN_PROGRESS - 1, WALKFORWARD_RUN_PROGRESS)
                detail = f"Running strategy walkforward... {message}"
                if (
                    int(walkforward_report["progress"]) == int(total_progress)
                    and str(walkforward_report["message"]) == detail
                ):
                    return
                walkforward_report["progress"] = int(total_progress)
                walkforward_report["message"] = detail
                _set_pipeline_stage(state, "walkforward_run", message=detail)
                job_manager._update_db(
                    job_id,
                    "txt_update",
                    "running",
                    message=detail,
                    progress=int(total_progress),
                )

            walkforward_cfg = strategy_backtest_service.StrategyBacktestConfig(
                min_long_score=float(walkforward_run_min_long_score),
                min_short_score=float(walkforward_run_min_short_score),
                max_new_entries_per_day=int(walkforward_run_max_new_entries_per_day),
                allowed_sides=str(walkforward_run_allowed_sides),
                allowed_long_setups=tuple(walkforward_run_allowed_long_setups),
                allowed_short_setups=tuple(walkforward_run_allowed_short_setups),
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
                progress_cb=_on_walkforward_run_progress,
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
        elif not effective_auto_walkforward_gate:
            state["last_walkforward_gate_skipped_at"] = gate_now.isoformat()
            state["last_walkforward_gate_skipped_reason"] = "disabled"
            state.pop("last_walkforward_gate_error", None)
            state.pop("last_walkforward_gate_error_at", None)
            ml_note_parts.append("walkforward_gate=skip(disabled)")
        elif (
            (not force_recompute_due_to_pan_finalize)
            and
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
                progress=WALKFORWARD_GATE_PROGRESS,
            )
            from app.backend.services import strategy_backtest_service

            gate_result = strategy_backtest_service.run_strategy_walkforward_gate(
                min_oos_total_realized_unit_pnl=walkforward_gate_min_oos_total,
                min_oos_mean_profit_factor=walkforward_gate_min_oos_pf,
                min_oos_positive_window_ratio=walkforward_gate_min_oos_pos_ratio,
                min_oos_worst_max_drawdown_unit=walkforward_gate_min_oos_worst_dd,
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

    try:
        from app.backend.services import strategy_backtest_service

        research_snapshot = strategy_backtest_service.save_daily_walkforward_research_snapshot()
        if bool(research_snapshot.get("saved")):
            state["last_walkforward_research_snapshot_at"] = datetime.now().isoformat()
            state["last_walkforward_research_source_run_id"] = str(research_snapshot.get("source_run_id") or "")
            state["last_walkforward_research_snapshot_date"] = research_snapshot.get("snapshot_date")
            ml_note_parts.append(
                f"walkforward_research_snapshot=ok(date={research_snapshot.get('snapshot_date')})"
            )
    except Exception as exc:
        logger.warning("Walkforward research snapshot skipped: %s", exc)
        ml_note_parts.append(f"walkforward_research_snapshot=skip({exc})")

    if _exit_if_canceled(job_id, state, stage="finalize", message="Canceled before finalize"):
        return

    completion_ts = datetime.now()
    _set_pipeline_stage(state, "finalize", message="Finalizing update status...")
    job_manager._update_db(
        job_id,
        "txt_update",
        "running",
        message="Finalizing update status...",
        progress=FINALIZING_PROGRESS,
    )
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

