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
    auto_fill_missing_history = _to_bool(payload.get("auto_fill_missing_history"), True)
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

    # ── Step 0: Import latest data into Pan database (pandtmgr F5) ──
    if _exit_if_canceled(job_id, state, stage="pan_import", message="Canceled before Pan import"):
        return

    _set_pipeline_stage(state, "pan_import", message="Launching Pan and importing latest data...")
    job_manager._update_db(
        job_id,
        "txt_update",
        "running",
        message="PANを起動してデータ取り込み中...",
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
    for attempt in (1, 2):
        if _exit_if_canceled(job_id, state, stage="pan_import", message="Canceled during Pan import"):
            return
        try:
            pan_import_ok = run_pan_import(str(config.PAN_DTMGR_PATH))
            if pan_import_ok:
                break
            pan_import_error = "Pan import returned False"
        except Exception as exc:
            pan_import_error = str(exc)
            logger.warning("Pan import error (attempt %s/2): %s", attempt, exc)

        if attempt == 1:
            job_manager._update_db(
                job_id,
                "txt_update",
                "running",
                message="PAN取り込みを再試行中...",
                progress=3,
            )
            time.sleep(1.0)

    if not pan_import_ok:
        error_msg = f"PAN起動または取り込みに失敗しました: {pan_import_error or 'unknown error'}"
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

    if _exit_if_canceled(job_id, state, stage="pan_import", message="Canceled after Pan import"):
        return

    # ── Step 1: VBS Export (Pan → TXT) ──
    _set_pipeline_stage(state, "export", message="Running Pan Rolling export...")
    job_manager._update_db(job_id, "txt_update", "running", message="Running Pan Rolling export...", progress=10)

    vbs_code, output_lines = run_vbs_export(
        code_path,
        out_dir,
        should_cancel=lambda: _is_job_canceled(job_id),
    )
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
    _ingest_out, ingest_err, ingest_stats = run_ingest(incremental=True)
    if _exit_if_canceled(job_id, state, stage="ingest", message="Canceled during ingest"):
        return
    if ingest_err:
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

    if _exit_if_canceled(job_id, state, stage="phase", message="Canceled before phase update"):
        return

    _set_pipeline_stage(state, "phase", message="Rebuilding latest phase snapshot...")
    job_manager._update_db(job_id, "txt_update", "running", message="Phase予測を更新中...", progress=92)
    try:
        phase_dt = _run_phase_batch_latest()
        state["last_phase_dt"] = int(phase_dt)
        state["last_phase_at"] = datetime.now().isoformat()
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message=f"Phase予測を更新しました (dt={phase_dt})",
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
            _set_pipeline_stage(state, "ml_train", message="Refreshing ML training...")
            job_manager._update_db(
                job_id, "txt_update", "running", message="Refreshing ML training...", progress=97
            )
            train_result = ml_service.train_models(dry_run=False)
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
        from app.backend.api.dependencies import get_stock_repo
        from app.backend.jobs.scoring_job import ScoringJob

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
