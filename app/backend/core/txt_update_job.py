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
from datetime import datetime, timedelta
from typing import Any, Callable

from .config import config
from .jobs import job_manager
from app.backend.core.legacy_analysis_control import (
    is_legacy_analysis_disabled,
    legacy_analysis_disabled_log_value,
)

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
_TRACKING_REFRESH_PROGRESS_BASE = 99.0
_TRACKING_REFRESH_PROGRESS_SPAN = 0.9
_DAILY_UPDATE_PROFILE_DIR_NAME = "jobs"
_TXT_SOURCE_MANIFEST_FILE_NAME = "txt_update_source_manifest.json"
_HEAVY_REFRESH_REASON_VALUES = {
    "manual_full_refresh",
    "missing_tracking_artifact",
    "schema_version_changed",
    "algorithm_version_changed",
    "repair_migration",
    "full_rebuild_flag",
}


def _hidden_process_kwargs() -> dict[str, object]:
    kwargs: dict[str, object] = {
        "creationflags": getattr(subprocess, "CREATE_NO_WINDOW", 0),
    }
    startupinfo_factory = getattr(subprocess, "STARTUPINFO", None)
    if startupinfo_factory is not None:
        startupinfo = startupinfo_factory()
        startupinfo.dwFlags |= getattr(subprocess, "STARTF_USESHOWWINDOW", 0)
        startupinfo.wShowWindow = getattr(subprocess, "SW_HIDE", 0)
        kwargs["startupinfo"] = startupinfo
    return kwargs


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


def _seed_ingest_state_hashes_for_export(out_dir: str) -> dict[str, int | str]:
    if ingest_txt is None or not hasattr(ingest_txt, "seed_ingest_state_hashes"):
        return {
            "seeded_files": 0,
            "total_bytes": 0,
            "elapsed_ms": 0,
            "state_path": "",
        }
    return ingest_txt.seed_ingest_state_hashes(out_dir)


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


def _new_daily_update_profile(job_id: str) -> dict[str, Any]:
    started_at = datetime.now()
    return {
        "job_id": str(job_id),
        "job_type": "txt_daily_update",
        "mode": "fast_path",
        "status": "running",
        "started_at": started_at.isoformat(),
        "ended_at": None,
        "duration_sec": 0.0,
        "source_latest_date": None,
        "db_latest_before": None,
        "db_latest_after": None,
        "changed_dates_count": None,
        "changed_symbols_count": None,
        "changed_files_count": None,
        "pan_finalized_rows": None,
        "export_required": None,
        "export_reason": None,
        "import_required": None,
        "import_reason": None,
        "phases": [],
        "skipped": {
            "export": False,
            "import": False,
            "ranking_refresh": False,
            "tracking_refresh": True,
        },
        "heavy_refresh_required": False,
        "heavy_refresh_reason": None,
        "_started_monotonic": time.monotonic(),
    }


def _record_profile_phase(
    profile: dict[str, Any],
    name: str,
    *,
    started_at: float,
    status: str = "done",
    **extra: Any,
) -> None:
    phase = {
        "name": str(name),
        "duration_sec": round(max(0.0, time.monotonic() - float(started_at)), 3),
        "status": str(status),
    }
    phase.update(extra)
    phases = profile.setdefault("phases", [])
    if isinstance(phases, list):
        phases.append(phase)


def _record_update_stage_duration(
    state: dict,
    profile: dict[str, Any],
    name: str,
    *,
    started_at: float,
    status: str = "done",
    **extra: Any,
) -> float:
    duration_sec = round(max(0.0, time.monotonic() - float(started_at)), 3)
    durations = state.get("last_pipeline_stage_durations")
    if not isinstance(durations, dict):
        durations = {}
        state["last_pipeline_stage_durations"] = durations
    durations[str(name)] = {
        "duration_sec": duration_sec,
        "status": str(status),
        "recorded_at": datetime.now().isoformat(),
        **extra,
    }
    _record_profile_phase(profile, name, started_at=started_at, status=status, **extra)
    return duration_sec


def _write_daily_update_profile(profile: dict[str, Any], *, status: str) -> str | None:
    ended_at = datetime.now()
    started_monotonic = float(profile.pop("_started_monotonic", time.monotonic()))
    profile["status"] = str(status)
    profile["ended_at"] = ended_at.isoformat()
    profile["duration_sec"] = round(max(0.0, time.monotonic() - started_monotonic), 3)
    root = config.DATA_DIR / _DAILY_UPDATE_PROFILE_DIR_NAME
    path = root / f"daily_update_profile_{ended_at.strftime('%Y%m%d_%H%M%S')}_{profile.get('job_id')}.json"
    try:
        root.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(profile, handle, ensure_ascii=False, indent=2)
        return str(path)
    except Exception as exc:
        logger.warning("Failed to write daily update profile (%s): %s", path, exc)
        return None


def _txt_source_manifest_path() -> str:
    return os.path.abspath(str(config.DATA_DIR / _DAILY_UPDATE_PROFILE_DIR_NAME / _TXT_SOURCE_MANIFEST_FILE_NAME))


def _file_manifest_entry(path: str) -> dict[str, Any] | None:
    try:
        stat = os.stat(path)
    except OSError:
        return None
    return {
        "path": os.path.abspath(path),
        "mtime_ns": int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
        "size": int(stat.st_size),
        "sha256": None,
    }


def _list_export_output_entries(out_dir: str) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    if not os.path.isdir(out_dir):
        return entries
    try:
        names = sorted(os.listdir(out_dir))
    except OSError:
        return entries
    for name in names:
        if not name.lower().endswith(".txt"):
            continue
        entry = _file_manifest_entry(os.path.join(out_dir, name))
        if entry is not None:
            entries.append(entry)
    return entries


def _load_txt_source_manifest() -> dict[str, Any] | None:
    path = _txt_source_manifest_path()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return payload if isinstance(payload, dict) else None
    except Exception as exc:
        logger.warning("Failed to load TXT source manifest (%s): %s", path, exc)
        return None


def _save_txt_source_manifest(manifest: dict[str, Any]) -> None:
    path = _txt_source_manifest_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, ensure_ascii=False, indent=2)
    except Exception as exc:
        logger.warning("Failed to save TXT source manifest (%s): %s", path, exc)


def _manifest_file_signature(entries: object) -> list[tuple[str, int, int]]:
    if not isinstance(entries, list):
        return []
    signature: list[tuple[str, int, int]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        path = os.path.abspath(str(entry.get("path") or ""))
        if not path:
            continue
        try:
            mtime_ns = int(entry.get("mtime_ns") or 0)
            size = int(entry.get("size") or 0)
        except (TypeError, ValueError):
            mtime_ns = 0
            size = 0
        signature.append((path, mtime_ns, size))
    return sorted(signature)


def _format_ymd_key(value: int | None) -> str | None:
    if value is None:
        return None
    text = f"{int(value):08d}"
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}"


def _parse_txt_ymd_key(value: object) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) != 8:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def _expected_latest_confirmed_date_key(now: datetime | None = None) -> int:
    current = (now or datetime.now()).date()
    expected = current - timedelta(days=1)
    while expected.weekday() >= 5:
        expected -= timedelta(days=1)
    return int(expected.strftime("%Y%m%d"))


def _preflight_manifest_is_current_enough(manifest: dict[str, Any]) -> tuple[bool, str | None]:
    expected_key = _expected_latest_confirmed_date_key()
    source_key = _parse_txt_ymd_key(manifest.get("source_latest_date"))
    db_key = _parse_txt_ymd_key(manifest.get("db_latest_date"))
    latest_key = max((key for key in (source_key, db_key) if key is not None), default=None)
    if latest_key is None:
        return False, "latest_date_missing"
    if latest_key < expected_key:
        return False, "source_behind_expected_trading_day"
    return True, None


def _latest_txt_export_date_key_from_file(path: str) -> int | None:
    try:
        with open(path, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - 65536), os.SEEK_SET)
            raw = handle.read()
    except OSError:
        return None
    if not raw:
        return None
    text = raw.decode("cp932", errors="ignore")
    latest_key: int | None = None
    for line in reversed(text.splitlines()):
        parts = line.strip().split(",")
        if len(parts) < 2:
            continue
        key = _parse_txt_ymd_key(parts[1])
        if key is None:
            continue
        latest_key = key if latest_key is None else max(latest_key, key)
        break
    return latest_key


def _latest_txt_export_date_key(out_dir: str) -> int | None:
    if not os.path.isdir(out_dir):
        return None
    latest_key: int | None = None
    try:
        names = sorted(os.listdir(out_dir))
    except OSError:
        return None
    for name in names:
        if not name.lower().endswith(".txt"):
            continue
        key = _latest_txt_export_date_key_from_file(os.path.join(out_dir, name))
        if key is None:
            continue
        latest_key = key if latest_key is None else max(latest_key, key)
    return latest_key


def _latest_confirmed_db_date_key() -> int | None:
    try:
        from app.backend.db import get_conn
        from app.backend.services.data.yahoo_provisional import normalize_date_key
    except Exception as exc:
        logger.warning("Failed to load DB date helpers: %s", exc)
        return None
    try:
        with get_conn() as conn:
            row = conn.execute(
                """
                SELECT MAX(date)
                FROM daily_bars
                WHERE COALESCE(source, 'pan') <> 'yahoo'
                """
            ).fetchone()
    except Exception as exc:
        logger.warning("Failed to read latest confirmed DB date: %s", exc)
        return None
    return normalize_date_key(row[0]) if row and row[0] is not None else None


def _build_txt_source_manifest_snapshot(
    *,
    code_path: str,
    out_dir: str,
    db_latest_key: int | None,
    ranking_snapshot_key: int | None,
) -> dict[str, Any]:
    source_files = []
    for path in (code_path, _update_vbs_path()):
        entry = _file_manifest_entry(path)
        if entry is not None:
            source_files.append(entry)
    db_latest_date = _format_ymd_key(db_latest_key)
    source_latest_date = _format_ymd_key(_latest_txt_export_date_key(out_dir)) or db_latest_date
    return {
        "schema_version": 1,
        "updated_at": datetime.now().isoformat(),
        "source_kind": "pan_txt",
        "source_files": source_files,
        "export_outputs": _list_export_output_entries(out_dir),
        "source_latest_date": source_latest_date,
        "db_latest_date": db_latest_date,
        "ranking_snapshot_as_of": _format_ymd_key(ranking_snapshot_key),
    }


def _manifests_match_for_noop(previous: dict[str, Any] | None, current: dict[str, Any]) -> tuple[bool, str | None]:
    if not previous:
        return False, "manifest_missing"
    if int(previous.get("schema_version") or 0) != 1:
        return False, "manifest_schema_changed"
    current_source_key = _parse_txt_ymd_key(current.get("source_latest_date"))
    current_db_key = _parse_txt_ymd_key(current.get("db_latest_date"))
    if current_source_key is not None and current_db_key is not None and current_source_key > current_db_key:
        return False, "source_newer_than_db"
    if _manifest_file_signature(previous.get("source_files")) != _manifest_file_signature(current.get("source_files")):
        return False, "source_changed"
    if _manifest_file_signature(previous.get("export_outputs")) != _manifest_file_signature(current.get("export_outputs")):
        return False, "output_changed"
    if not current.get("export_outputs"):
        return False, "output_missing"
    if str(previous.get("source_latest_date") or "") != str(current.get("source_latest_date") or ""):
        return False, "source_latest_changed"
    if str(previous.get("db_latest_date") or "") != str(current.get("db_latest_date") or ""):
        return False, "db_latest_changed"
    if str(previous.get("ranking_snapshot_as_of") or "") != str(current.get("db_latest_date") or ""):
        return False, "ranking_snapshot_stale"
    return True, None


def _manifest_supports_postprocess_only_resume(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
) -> bool:
    matches, reason = _manifests_match_for_noop(previous, current)
    if matches:
        return False
    return reason == "ranking_snapshot_stale"


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


def _tracking_refresh_message(progress: dict[str, Any]) -> str:
    substage = str(progress.get("substage") or f"tracking_refresh.{progress.get('phase') or 'unknown'}").strip()
    processed = progress.get("processed")
    total = progress.get("total")
    detail = str(progress.get("detail") or "").strip()
    message = f"Refreshing signal/ranking tracking... {substage}"
    if processed is not None and total not in (None, 0):
        message = f"{message} {processed}/{total}"
    if detail:
        message = f"{message} - {detail}"
    return message


def _tracking_refresh_progress_value(progress: dict[str, Any]) -> float:
    phase = str(progress.get("phase") or "")
    status = str(progress.get("status") or "running")
    processed = progress.get("processed")
    total = progress.get("total")
    if phase == "finalize" and status == "done":
        return 99.9
    fraction = 0.0
    if isinstance(processed, (int, float)) and isinstance(total, (int, float)) and float(total) > 0:
        fraction = max(0.0, min(1.0, float(processed) / float(total)))
    return round(min(99.9, _TRACKING_REFRESH_PROGRESS_BASE + 0.1 + (_TRACKING_REFRESH_PROGRESS_SPAN * fraction)), 1)


def _record_tracking_refresh_progress(state: dict, *, job_id: str, progress: dict[str, Any]) -> None:
    now_iso = datetime.now().isoformat()
    substage = str(progress.get("substage") or f"tracking_refresh.{progress.get('phase') or 'unknown'}").strip()
    phase = str(progress.get("phase") or "unknown").strip()
    status = str(progress.get("status") or "running").strip()
    heartbeat_at = str(progress.get("heartbeat_at") or now_iso)
    progress_value = _tracking_refresh_progress_value(progress)
    message = _tracking_refresh_message(progress)
    detail_payload = {
        "current_phase": phase,
        "substage": substage,
        "substage_status": status,
        "processed": progress.get("processed"),
        "total": progress.get("total"),
        "processed_dates": progress.get("processed"),
        "total_dates": progress.get("total"),
        "current_market_ymd": progress.get("current_market_ymd"),
        "current_market_date": progress.get("current_market_date"),
        "current_side": progress.get("current_side"),
        "detail": progress.get("detail"),
        "trigger_reason": progress.get("trigger_reason"),
        "heartbeat_at": heartbeat_at,
        "progress": progress_value,
    }
    _set_pipeline_stage(state, "tracking_refresh", message=message, save=False)
    state["last_pipeline_substage"] = substage
    state["last_pipeline_substage_status"] = status
    state["last_pipeline_substage_at"] = now_iso
    state["last_pipeline_heartbeat_at"] = heartbeat_at
    state["last_pipeline_progress_detail"] = detail_payload
    state["last_pipeline_progress_percent"] = progress_value
    try:
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message=message,
            progress=progress_value,
        )
    except Exception as exc:
        logger.warning("Failed to publish tracking refresh heartbeat: %s", exc)
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
    followup_job_id = job_manager.submit(
        _TXT_FOLLOWUP_JOB_TYPE,
        payload,
        unique=False,
        lane="maintenance",
        dedupe_key=f"{_TXT_FOLLOWUP_JOB_TYPE}:{str(source_job_id).strip() or 'latest'}",
    )
    if followup_job_id:
        _record_followup_enqueued(
            state,
            source_job_id=str(source_job_id),
            followup_job_id=str(followup_job_id),
        )
        state["last_followup_lane_stats"] = job_manager.get_lane_stats()
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
            **_hidden_process_kwargs(),
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
    if "incremental history validation failed" in lowered:
        return "history_guard"
    if "module not found" in lowered:
        return "missing_module"
    if "permission" in lowered:
        return "permission"
    return "other"


def _classify_retry_exception(exc: Exception) -> str:
    if _is_transient_db_lock_error(exc):
        return "db_lock"
    return "other"


def _classify_pan_import_error_text(error_text: str) -> str:
    lowered = str(error_text or "").strip().lower()
    if not lowered:
        return "none"
    if "libstock database is in use" in lowered or "database is in use" in lowered:
        return "db_lock"
    if "already running" in lowered and "pan data manager" in lowered:
        return "already_running"
    if "blocked by pan-side error dialog" in lowered:
        return "pan_dialog"
    if "timeout" in lowered:
        return "timeout"
    return "other"


def _classify_pan_import_exception(exc: Exception) -> str:
    return _classify_pan_import_error_text(str(exc))


def _is_transient_pan_import_error(exc: Exception) -> bool:
    return _classify_pan_import_exception(exc) in {"db_lock", "already_running"}


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


def _run_pan_import_with_retry(
    *,
    run_pan_import_once: Callable[[], bool],
    max_attempts: int,
    sleep_seconds: float,
    state: dict | None = None,
    job_id: str | None = None,
) -> tuple[bool, int, str, str | None]:
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        try:
            ok = bool(run_pan_import_once())
            if ok:
                if state is not None:
                    _set_retry_summary(
                        state,
                        stage="pan_import",
                        operation="pan_import",
                        attempts=attempt,
                        status="success",
                        kind="none",
                    )
                    _save_update_state(state)
                return True, attempt, "none", None
            error_text = "Pan import returned False"
            error_kind = _classify_pan_import_error_text(error_text)
        except Exception as exc:
            error_text = str(exc)
            error_kind = _classify_pan_import_exception(exc)

        will_retry = attempt < max_attempts and _is_transient_pan_import_error(RuntimeError(error_text))
        sleep_for = _compute_retry_sleep_seconds(sleep_seconds, attempt) if will_retry else None
        if state is not None:
            _append_retry_trace(
                state,
                stage="pan_import",
                operation="pan_import",
                attempt=attempt,
                max_attempts=max_attempts,
                kind=error_kind,
                error=error_text,
                will_retry=will_retry,
                sleep_seconds=sleep_for,
            )
            _set_retry_summary(
                state,
                stage="pan_import",
                operation="pan_import",
                attempts=attempt,
                status="retrying" if will_retry else "failed",
                kind=error_kind,
                error=error_text,
            )
            _save_update_state(state)
        logger.warning("Pan import error (attempt %s/%s, kind=%s): %s", attempt, max_attempts, error_kind, error_text)
        if not will_retry:
            return False, attempt, error_kind, error_text
        if job_id:
            wait_sec = max(0.1, float(sleep_for or 0.1))
            if error_kind == "db_lock":
                wait_msg = f"PAN DB lock detected. Waiting {wait_sec:.1f}s before retry {attempt}/{max_attempts}..."
            else:
                wait_msg = (
                    f"PAN process is still active. Waiting {wait_sec:.1f}s before retry {attempt}/{max_attempts}..."
                )
            job_manager._update_db(
                job_id,
                "txt_update",
                "running",
                message=wait_msg,
                progress=min(4, 1 + attempt),
            )
        time.sleep(max(0.1, float(sleep_for or 0.1)))
    return False, attempt, "other", "retry_exhausted"


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
    profile = _new_daily_update_profile(job_id)

    def _finish_profile(status: str) -> None:
        path = _write_daily_update_profile(profile, status=status)
        if path:
            state["last_daily_update_profile_path"] = path
            _save_update_state(state)

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
    run_tracking_refresh = _to_bool(
        payload.get("run_tracking_refresh"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_RUN_TRACKING_REFRESH"), False),
    )
    tracking_refresh_trigger_reason = str(payload.get("tracking_refresh_trigger_reason") or "").strip()
    full_rebuild_requested = _to_bool(payload.get("full_rebuild"), False) or _to_bool(
        payload.get("full_rebuild_flag"),
        False,
    )
    if full_rebuild_requested and not tracking_refresh_trigger_reason:
        tracking_refresh_trigger_reason = "full_rebuild_flag"
    if run_tracking_refresh and tracking_refresh_trigger_reason not in _HEAVY_REFRESH_REASON_VALUES:
        state = _load_update_state()
        _trim_retry_trace(state)
        reason = tracking_refresh_trigger_reason or "unknown"
        error_msg = (
            "Tracking refresh requires an explicit trigger reason "
            f"({', '.join(sorted(_HEAVY_REFRESH_REASON_VALUES))}); got {reason!r}"
        )
        state["last_pipeline_status"] = "failed"
        state["last_failed_stage"] = "tracking_refresh_trigger"
        state["last_error"] = error_msg
        profile["heavy_refresh_required"] = True
        profile["heavy_refresh_reason"] = reason
        _finish_profile("failed")
        job_manager._update_db(
            job_id,
            "txt_update",
            "failed",
            error="Invalid tracking refresh trigger",
            message=error_msg,
            finished_at=datetime.now(),
        )
        return
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
    profile["tracking_refresh_trigger_reason"] = tracking_refresh_trigger_reason or None
    db_latest_before_key = _latest_confirmed_db_date_key()
    profile["db_latest_before"] = _format_ymd_key(db_latest_before_key)
    profile["source_latest_date"] = profile["db_latest_before"]

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

    previous_manifest = _load_txt_source_manifest()
    previous_ranking_key = _to_optional_int(state.get("last_cache_refresh_db_latest_key"))
    preflight_manifest = _build_txt_source_manifest_snapshot(
        code_path=code_path,
        out_dir=out_dir,
        db_latest_key=db_latest_before_key,
        ranking_snapshot_key=previous_ranking_key,
    )
    allow_postprocess_only_resume = _to_bool(
        payload.get("allow_postprocess_only_resume"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_ALLOW_POSTPROCESS_ONLY_RESUME"), True),
    )
    allow_manifest_fast_noop = _to_bool(
        payload.get("allow_manifest_fast_noop"),
        _to_bool(os.getenv("MEEMEE_TXT_UPDATE_ALLOW_MANIFEST_FAST_NOOP"), False),
    )
    force_export = _to_bool(payload.get("force_export"), False) or full_rebuild_requested
    repair_mode = bool(auto_fill_missing_history or force_export)
    preflight_manifest_matches, _preflight_manifest_miss_reason = _manifests_match_for_noop(
        previous_manifest,
        preflight_manifest,
    )
    preflight_manifest_current, preflight_manifest_stale_reason = _preflight_manifest_is_current_enough(
        preflight_manifest
    )
    if not preflight_manifest_current:
        profile["preflight_noop_blocked_reason"] = preflight_manifest_stale_reason
        state["last_txt_update_preflight_noop_blocked_at"] = datetime.now().isoformat()
        state["last_txt_update_preflight_noop_blocked_reason"] = preflight_manifest_stale_reason
    if (
        allow_manifest_fast_noop
        and (not force_export)
        and (not repair_mode)
        and preflight_manifest_matches
        and preflight_manifest_current
    ):
        completion_ts = datetime.now()
        profile["status"] = "no_change"
        profile["export_required"] = False
        profile["export_reason"] = None
        profile["import_required"] = False
        profile["import_reason"] = None
        profile["changed_files_count"] = 0
        profile["changed_dates_count"] = 0
        profile["changed_symbols_count"] = 0
        profile["pan_finalized_rows"] = 0
        profile["db_latest_after"] = preflight_manifest.get("db_latest_date")
        profile["source_latest_date"] = preflight_manifest.get("source_latest_date")
        profile["skipped"]["pan_import"] = True
        profile["skipped"]["export"] = True
        profile["skipped"]["import"] = True
        profile["skipped"]["ranking_refresh"] = True
        profile["skipped"]["tracking_refresh"] = True
        state["last_pan_import_skipped_at"] = completion_ts.isoformat()
        state["last_pan_import_skipped_reason"] = "source_manifest_unchanged"
        state["last_txt_update_no_change_at"] = completion_ts.isoformat()
        state["last_txt_update_no_change_reason"] = "source_manifest_unchanged"
        state["last_tracking_refresh_skipped_at"] = completion_ts.isoformat()
        state["last_tracking_refresh_skipped_reason"] = "no_confirmed_change"
        state.update(
            {
                "last_txt_update_at": completion_ts.isoformat(),
                "last_txt_update_date": completion_ts.date().isoformat(),
            }
        )
        _record_profile_phase(
            profile,
            "preflight_no_change",
            started_at=time.monotonic(),
            status="no_change",
        )
        final_message = "No confirmed TXT/PAN source changes detected. Daily update fast path completed."
        _record_pipeline_success(state, stage="finalize", message=final_message)
        _save_txt_source_manifest(preflight_manifest)
        _finish_profile("no_change")
        job_manager._update_db(
            job_id,
            "txt_update",
            "success",
            message=final_message,
            progress=100,
            finished_at=completion_ts,
        )
        return
    if (
        allow_postprocess_only_resume
        and (not force_export)
        and (not repair_mode)
        and preflight_manifest_current
        and _manifest_supports_postprocess_only_resume(previous_manifest, preflight_manifest)
    ):
        completion_ts = datetime.now()
        postprocess_message = "Pan/TXT import already current. Queuing post-processing only..."
        _set_pipeline_stage(state, "postprocess_resume", message=postprocess_message)
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message=postprocess_message,
            progress=92,
        )
        followup_payload = dict(payload)
        followup_payload.update(
            {
                "source_txt_job_id": str(job_id),
                "phase_dt": None,
                "db_latest_after_key": int(db_latest_before_key) if db_latest_before_key is not None else None,
                "changed_files": 0,
                "pan_finalized_rows": 0,
                "summary_line": "Pan/TXT import skipped; post-processing only",
            }
        )
        followup_job_id = _queue_txt_followup(
            state,
            source_job_id=str(job_id),
            payload=followup_payload,
        )
        profile["mode"] = "postprocess_only_resume"
        profile["status"] = "queued_followup" if followup_job_id else "followup_queue_rejected"
        profile["export_required"] = False
        profile["export_reason"] = None
        profile["import_required"] = False
        profile["import_reason"] = None
        profile["changed_files_count"] = 0
        profile["changed_dates_count"] = 0
        profile["changed_symbols_count"] = 0
        profile["pan_finalized_rows"] = 0
        profile["db_latest_after"] = _format_ymd_key(db_latest_before_key)
        profile["source_latest_date"] = preflight_manifest.get("source_latest_date")
        profile["skipped"]["export"] = True
        profile["skipped"]["import"] = True
        profile["skipped"]["ranking_refresh"] = True
        profile["skipped"]["tracking_refresh"] = True
        _record_profile_phase(
            profile,
            "postprocess_only_resume",
            started_at=time.monotonic(),
            status="queued" if followup_job_id else "queue_rejected",
            followup_job_id=followup_job_id,
        )
        state["last_pan_import_skipped_at"] = completion_ts.isoformat()
        state["last_pan_import_skipped_reason"] = "postprocess_only_resume"
        state["last_txt_update_postprocess_resume_at"] = completion_ts.isoformat()
        state["last_txt_update_postprocess_resume_reason"] = "ranking_snapshot_stale"
        state["last_tracking_refresh_skipped_at"] = completion_ts.isoformat()
        state["last_tracking_refresh_skipped_reason"] = "postprocess_only_resume_followup"
        state.update(
            {
                "last_txt_update_at": completion_ts.isoformat(),
                "last_txt_update_date": completion_ts.date().isoformat(),
            }
        )
        _save_txt_source_manifest(preflight_manifest)
        _finish_profile("queued_followup" if followup_job_id else "followup_queue_rejected")
        if followup_job_id:
            final_message = f"Pan/TXT import already current. Post-processing queued ({followup_job_id})."
            _record_pipeline_success(state, stage="postprocess_resume", message=final_message)
            job_manager._update_db(
                job_id,
                "txt_update",
                "success",
                message=final_message,
                progress=100,
                finished_at=completion_ts,
            )
            return
        error_message = "Pan/TXT import already current, but post-processing queue was rejected."
        _record_pipeline_failure(state, stage="postprocess_resume", error=error_message, message=error_message)
        job_manager._update_db(
            job_id,
            "txt_update",
            "failed",
            error="Post-processing queue rejected",
            message=error_message,
            finished_at=completion_ts,
        )
        return

    # Step 0: Import latest data into Pan database (pandtmgr F5)
    if _exit_if_canceled(job_id, state, stage="pan_import", message="Canceled before Pan import"):
        return

    pan_import_started = time.monotonic()
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

    if _exit_if_canceled(job_id, state, stage="pan_import", message="Canceled during Pan import"):
        return

    pan_dt_path = getattr(config, "PAN_DTMGR_PATH", None)
    pan_import_ok, pan_import_attempts, pan_import_error_kind, pan_import_error = _run_pan_import_with_retry(
        run_pan_import_once=lambda: run_pan_import(str(pan_dt_path) if pan_dt_path else None),
        max_attempts=pan_retry,
        sleep_seconds=pan_retry_sleep,
        state=state,
        job_id=job_id,
    )
    state["last_pan_import_attempts"] = int(pan_import_attempts)
    state["last_pan_import_error_kind"] = pan_import_error_kind
    if pan_import_ok:
        state.pop("last_pan_import_warning", None)
        _save_update_state(state)
        _record_profile_phase(
            profile,
            "detect_changes",
            started_at=pan_import_started,
            status="done",
        )

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

    manifest_started = time.monotonic()
    current_manifest = _build_txt_source_manifest_snapshot(
        code_path=code_path,
        out_dir=out_dir,
        db_latest_key=db_latest_before_key,
        ranking_snapshot_key=previous_ranking_key,
    )
    manifest_matches, manifest_miss_reason = _manifests_match_for_noop(previous_manifest, current_manifest)
    preflight_source_behind_expected = preflight_manifest_stale_reason == "source_behind_expected_trading_day"
    export_required = bool(
        force_export
        or repair_mode
        or preflight_source_behind_expected
        or (not allow_manifest_fast_noop)
        or not manifest_matches
    )
    if force_export:
        export_reason = "forced_export"
    elif repair_mode:
        export_reason = "repair_mode"
    elif preflight_source_behind_expected:
        export_reason = "source_behind_expected_trading_day"
    elif not allow_manifest_fast_noop:
        export_reason = "manual_refresh_after_pan_import"
    elif manifest_matches:
        export_reason = None
    else:
        export_reason = manifest_miss_reason or "manifest_changed"
    source_latest_key = _parse_txt_ymd_key(current_manifest.get("source_latest_date"))
    manifest_db_latest_key = _parse_txt_ymd_key(current_manifest.get("db_latest_date"))
    source_newer_than_db = (
        source_latest_key is not None
        and manifest_db_latest_key is not None
        and source_latest_key > manifest_db_latest_key
    )
    profile["export_required"] = export_required
    profile["export_reason"] = export_reason
    profile["source_latest_date"] = current_manifest.get("source_latest_date")
    profile["db_latest_before"] = current_manifest.get("db_latest_date")
    _record_profile_phase(
        profile,
        "detect_source_manifest",
        started_at=manifest_started,
        status="done",
        export_required=export_required,
        export_reason=export_reason,
        export_outputs_count=len(current_manifest.get("export_outputs") or []),
    )
    if not export_required:
        completion_ts = datetime.now()
        profile["status"] = "no_change"
        profile["import_required"] = False
        profile["import_reason"] = None
        profile["changed_files_count"] = 0
        profile["changed_dates_count"] = 0
        profile["changed_symbols_count"] = 0
        profile["pan_finalized_rows"] = 0
        profile["db_latest_after"] = current_manifest.get("db_latest_date")
        profile["skipped"]["export"] = True
        profile["skipped"]["import"] = True
        profile["skipped"]["ranking_refresh"] = True
        profile["skipped"]["tracking_refresh"] = True
        state["last_txt_update_no_change_at"] = completion_ts.isoformat()
        state["last_txt_update_no_change_reason"] = "source_manifest_unchanged"
        state["last_tracking_refresh_skipped_at"] = completion_ts.isoformat()
        state["last_tracking_refresh_skipped_reason"] = "no_confirmed_change"
        _record_profile_phase(
            profile,
            "finalize_status",
            started_at=time.monotonic(),
            status="no_change",
        )
        state.update(
            {
                "last_txt_update_at": completion_ts.isoformat(),
                "last_txt_update_date": completion_ts.date().isoformat(),
            }
        )
        final_message = "No confirmed TXT/PAN source changes detected. Daily update fast path completed."
        _record_pipeline_success(state, stage="finalize", message=final_message)
        no_change_manifest = _build_txt_source_manifest_snapshot(
            code_path=code_path,
            out_dir=out_dir,
            db_latest_key=db_latest_before_key,
            ranking_snapshot_key=db_latest_before_key,
        )
        _save_txt_source_manifest(no_change_manifest)
        _finish_profile("no_change")
        job_manager._update_db(
            job_id,
            "txt_update",
            "success",
            message=final_message,
            progress=100,
            finished_at=completion_ts,
        )
        return

    hash_seed_started = time.monotonic()
    if source_newer_than_db:
        seed_result = {
            "seeded_files": 0,
            "total_bytes": 0,
            "elapsed_ms": 0,
            "state_path": "",
            "reason": "source_newer_than_db",
        }
        state["last_ingest_hash_seed_at"] = datetime.now().isoformat()
        state["last_ingest_hash_seed_status"] = "skipped"
        state["last_ingest_hash_seed_result"] = seed_result
        state.pop("last_ingest_hash_seed_error", None)
        _record_profile_phase(
            profile,
            "seed_ingest_hash_baseline",
            started_at=hash_seed_started,
            status="skipped",
            reason="source_newer_than_db",
        )
        _save_update_state(state)
    else:
        try:
            seed_result = _seed_ingest_state_hashes_for_export(out_dir)
            state["last_ingest_hash_seed_at"] = datetime.now().isoformat()
            state["last_ingest_hash_seed_status"] = "done"
            state["last_ingest_hash_seed_result"] = seed_result
            state.pop("last_ingest_hash_seed_error", None)
            _record_profile_phase(
                profile,
                "seed_ingest_hash_baseline",
                started_at=hash_seed_started,
                status="done",
                seeded_files=seed_result.get("seeded_files"),
                total_bytes=seed_result.get("total_bytes"),
            )
            _save_update_state(state)
        except Exception as exc:
            logger.warning("TXT ingest hash seed skipped before export: %s", exc)
            state["last_ingest_hash_seed_at"] = datetime.now().isoformat()
            state["last_ingest_hash_seed_status"] = "failed"
            state["last_ingest_hash_seed_error"] = str(exc)
            _record_profile_phase(
                profile,
                "seed_ingest_hash_baseline",
                started_at=hash_seed_started,
                status="failed",
                error=str(exc),
            )
            _save_update_state(state)

    # Step 1: VBS export (Pan -> TXT)
    export_started = time.monotonic()
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

    _record_profile_phase(
        profile,
        "export_confirmed_txt",
        started_at=export_started,
        status="done" if vbs_code == 0 else "warning",
    )

    job_manager._update_db(
        job_id,
        "txt_update",
        "running",
        message=f"{summary_line}. Export completed.",
        progress=70,
    )

    if _exit_if_canceled(job_id, state, stage="ingest", message="Canceled before ingest"):
        return

    ingest_incremental = not source_newer_than_db
    ingest_mode_label = "full" if not ingest_incremental else "incremental"
    _set_pipeline_stage(state, "ingest", message=f"Ingesting {ingest_mode_label} TXT data...")
    job_manager._update_db(
        job_id,
        "txt_update",
        "running",
        message=f"Ingesting ({ingest_mode_label.title()})...",
        progress=85,
    )
    ingest_report = {"message": "", "progress": -1}
    ingest_started = time.monotonic()

    def _on_ingest_progress(progress: int, message: str) -> None:
        total_progress = _scale_progress(progress, 85, 92)
        detail = f"Ingesting {ingest_mode_label} TXT data... {message}"
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
        incremental=ingest_incremental,
        max_attempts=ingest_retry,
        sleep_seconds=ingest_retry_sleep,
        state=state,
        stage="ingest",
        run_id=job_id,
        progress_cb=_on_ingest_progress,
    )
    if ingest_err and ingest_error_kind == "history_guard":
        logger.warning("TXT update incremental ingest blocked by history guard; retrying with full ingest.")
        state["last_ingest_guard_triggered_at"] = datetime.now().isoformat()
        state["last_ingest_guard_reason"] = str(ingest_err)
        _set_pipeline_stage(state, "ingest", message="Incremental ingest blocked; retrying full rebuild...")
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message="Incremental ingest blocked; retrying full rebuild...",
            progress=78,
        )
        _ingest_out, ingest_err, ingest_stats, full_ingest_attempts, ingest_error_kind = _run_ingest_with_retry(
            incremental=False,
            max_attempts=max(1, ingest_retry),
            sleep_seconds=ingest_retry_sleep,
            state=state,
            stage="ingest_full_recovery",
            run_id=job_id,
            progress_cb=_on_ingest_progress,
        )
        ingest_attempts += int(full_ingest_attempts)
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
    profile["changed_files_count"] = int(changed_files)
    profile["pan_finalized_rows"] = int(pan_finalized_rows)
    profile["changed_dates_count"] = _to_optional_int(ingest_stats.get("changed_dates_count"))
    if profile["changed_dates_count"] is None:
        profile["changed_dates_count"] = 0 if int(changed_files) == 0 else None
    profile["changed_symbols_count"] = _to_optional_int(ingest_stats.get("changed_symbols_count"))
    if profile["changed_symbols_count"] is None:
        profile["changed_symbols_count"] = int(changed_files)
    profile["import_required"] = int(changed_files) > 0 or int(pan_finalized_rows) > 0
    profile["import_reason"] = "changed_export_outputs" if profile["import_required"] else None
    _record_profile_phase(
        profile,
        "import_confirmed_bars",
        started_at=ingest_started,
        status="done",
        changed_files=int(changed_files),
        pan_finalized_rows=int(pan_finalized_rows),
    )
    state["last_force_recompute_on_pan_finalize"] = bool(force_recompute_on_pan_finalize)
    if pan_finalized_rows > 0:
        state["last_pan_finalize_at"] = datetime.now().isoformat()

    no_confirmed_change = int(changed_files) == 0 and int(pan_finalized_rows) == 0
    can_fast_no_change_exit = no_confirmed_change and bool(state.get("last_cache_refresh_at"))
    if can_fast_no_change_exit:
        completion_ts = datetime.now()
        db_latest_after_key = _latest_confirmed_db_date_key()
        profile["db_latest_after"] = _format_ymd_key(db_latest_after_key)
        profile["source_latest_date"] = profile["db_latest_after"] or profile.get("source_latest_date")
        profile["changed_dates_count"] = 0
        profile["changed_symbols_count"] = 0
        state["last_txt_update_no_change_at"] = completion_ts.isoformat()
        state["last_tracking_refresh_skipped_at"] = completion_ts.isoformat()
        state["last_tracking_refresh_skipped_reason"] = "no_confirmed_change"
        profile["status"] = "no_change"
        profile["skipped"]["import"] = True
        profile["skipped"]["ranking_refresh"] = True
        profile["skipped"]["tracking_refresh"] = True
        _record_profile_phase(
            profile,
            "finalize_status",
            started_at=time.monotonic(),
            status="no_change",
        )
        state.update(
            {
                "last_txt_update_at": completion_ts.isoformat(),
                "last_txt_update_date": completion_ts.date().isoformat(),
            }
        )
        final_message = "No confirmed TXT/PAN changes detected. Daily update fast path completed."
        _record_pipeline_success(state, stage="finalize", message=final_message)
        no_change_manifest = _build_txt_source_manifest_snapshot(
            code_path=code_path,
            out_dir=out_dir,
            db_latest_key=db_latest_after_key,
            ranking_snapshot_key=db_latest_after_key,
        )
        _save_txt_source_manifest(no_change_manifest)
        _finish_profile("no_change")
        job_manager._update_db(
            job_id,
            "txt_update",
            "success",
            message=final_message,
            progress=100,
            finished_at=completion_ts,
        )
        return

    db_latest_after_ingest_key = _latest_confirmed_db_date_key()
    profile["db_latest_after"] = _format_ymd_key(db_latest_after_ingest_key)
    profile["source_latest_date"] = profile["db_latest_after"] or profile.get("source_latest_date")

    if completion_mode == _COMPLETION_MODE_PRACTICAL_FAST and not run_tracking_refresh:
        should_queue_followup = bool(
            auto_ml_train
            or auto_ml_predict
            or auto_fill_missing_history
            or auto_walkforward_run
            or auto_walkforward_gate
            or changed_files > 0
            or (force_recompute_on_pan_finalize and pan_finalized_rows > 0)
        )
        followup_job_id: str | None = None
        if should_queue_followup:
            followup_payload = dict(payload)
            followup_payload.update(
                {
                    "source_txt_job_id": str(job_id),
                    "phase_dt": None,
                    "db_latest_after_key": (
                        int(db_latest_after_ingest_key) if db_latest_after_ingest_key is not None else None
                    ),
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
        completion_ts = datetime.now()
        finalize_started = time.monotonic()
        profile["mode"] = "chart_first_fast_path"
        profile["skipped"]["ranking_refresh"] = True
        profile["skipped"]["tracking_refresh"] = True
        if not should_queue_followup:
            state["last_followup_skipped_at"] = completion_ts.isoformat()
            state["last_followup_skipped_reason"] = "chart_first_no_requested_followup"
            state["last_followup_status"] = "skipped"
            state.pop("last_followup_error", None)
        state["last_tracking_refresh_skipped_at"] = completion_ts.isoformat()
        state["last_tracking_refresh_skipped_reason"] = "daily_fast_path"
        state.pop("last_tracking_refresh_trigger_reason", None)
        state.update(
            {
                "last_txt_update_at": completion_ts.isoformat(),
                "last_txt_update_date": completion_ts.date().isoformat(),
            }
        )
        _set_pipeline_stage(state, "finalize", message="Finalizing chart-first daily update...")
        job_manager._update_db(
            job_id,
            _TXT_UPDATE_JOB_TYPE,
            "running",
            message="Finalizing chart-first daily update...",
            progress=99,
        )
        notes = [
            "chart_refresh=ready",
            "tracking=skip(daily_fast_path)",
        ]
        if followup_job_id:
            notes.extend(
                [
                    "phase=queued(background)",
                    "scoring=queued(background)",
                    "ranking_cache=queued(background)",
                ]
            )
            notes.append(f"followup=queued({followup_job_id})")
        elif should_queue_followup:
            notes.append("followup=skip(queue_rejected)")
        else:
            notes.extend(
                [
                    "phase=skip(chart_first)",
                    "scoring=skip(chart_first)",
                    "ranking_cache=skip(chart_first)",
                    "followup=skip(chart_first_no_requested_followup)",
                ]
            )
        final_message = f"{summary_line}. Confirmed TXT/PAN bars imported; chart refresh is ready. [{' / '.join(notes)}]"
        _record_pipeline_success(state, stage="finalize", message=final_message)
        _record_profile_phase(profile, "finalize_status", started_at=finalize_started)
        current_manifest = _build_txt_source_manifest_snapshot(
            code_path=code_path,
            out_dir=out_dir,
            db_latest_key=db_latest_after_ingest_key,
            ranking_snapshot_key=previous_ranking_key,
        )
        _save_txt_source_manifest(current_manifest)
        _finish_profile("done")
        job_manager._update_db(
            job_id,
            _TXT_UPDATE_JOB_TYPE,
            "success",
            message=final_message,
            progress=100,
            finished_at=completion_ts,
        )
        return

    phase_dt = _to_optional_int(state.get("last_phase_dt"))
    if phase_dt is None:
        phase_dt = db_latest_after_ingest_key
    force_recompute_due_to_pan_finalize = bool(force_recompute_on_pan_finalize and pan_finalized_rows > 0)
    legacy_analysis_disabled = is_legacy_analysis_disabled()
    effective_auto_ml_train = False if legacy_analysis_disabled else bool(auto_ml_train or force_recompute_due_to_pan_finalize)
    effective_auto_ml_predict = False if legacy_analysis_disabled else bool(auto_ml_predict or force_recompute_due_to_pan_finalize)
    effective_auto_walkforward_run = bool(auto_walkforward_run or force_recompute_due_to_pan_finalize)
    effective_auto_walkforward_gate = bool(auto_walkforward_gate or force_recompute_due_to_pan_finalize)
    if force_recompute_due_to_pan_finalize:
        state["last_forced_recompute_at"] = datetime.now().isoformat()

    if legacy_analysis_disabled:
        logger.info("TXT update skipping legacy phase refresh (%s)", legacy_analysis_disabled_log_value())
        _set_pipeline_stage(state, "phase", message="Skipping legacy phase update (external analysis active)...")
        state["last_phase_skip_reason"] = "legacy_analysis_disabled"
    else:
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
    FEATURE_REFRESH_PROGRESS = 98.1
    LABEL_REFRESH_PROGRESS = 98.2
    PREDICTION_REFRESH_PROGRESS = 98.3
    CACHE_REFRESH_PROGRESS = 98.4
    TRACKING_REFRESH_PROGRESS = 99
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

    if legacy_analysis_disabled:
        logger.info("TXT update skipping legacy sell analysis accumulation (%s)", legacy_analysis_disabled_log_value())
        ml_note_parts.append("sell_analysis=skip(disabled)")
    else:
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
            from app.backend.services.analysis.sell_analysis_accumulator import accumulate_sell_analysis

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

    if completion_mode != _COMPLETION_MODE_PRACTICAL_FAST and auto_fill_missing_history and not legacy_analysis_disabled:
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
            from app.backend.services.analysis.analysis_backfill_service import backfill_missing_analysis_history

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
    elif legacy_analysis_disabled:
        logger.info("TXT update skipping legacy analysis backfill (%s)", legacy_analysis_disabled_log_value())
        ml_note_parts.append("analysis_backfill=skip(disabled)")

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
        if _exit_if_canceled(job_id, state, stage="feature_refresh", message="Canceled before feature refresh"):
            return
        feature_refresh_started = time.monotonic()
        feature_refresh_message = "feature_refresh: Refreshing confirmed ML features..."
        _set_pipeline_stage(state, "feature_refresh", message=feature_refresh_message)
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message=feature_refresh_message,
            progress=FEATURE_REFRESH_PROGRESS,
        )
        from app.backend.db import get_conn as get_backend_conn
        from app.backend.services.ml import ml_service

        with get_backend_conn() as conn:
            feature_refresh_result = ml_service.refresh_ml_features_incremental(conn)
        state["last_ml_feature_refresh_at"] = datetime.now().isoformat()
        state["last_ml_feature_refresh_result"] = feature_refresh_result
        state.pop("last_ml_feature_refresh_error", None)
        ml_note_parts.append(
            "ml_features="
            f"ok(rows={feature_refresh_result.get('rows')},latest={feature_refresh_result.get('latest_feature_dt')})"
        )
        feature_refresh_duration_sec = _record_update_stage_duration(
            state,
            profile,
            "refresh_ml_features",
            started_at=feature_refresh_started,
            rows=feature_refresh_result.get("rows"),
            latest=feature_refresh_result.get("latest_feature_dt"),
        )
        feature_refresh_done_message = (
            "feature_refresh: ML features refreshed "
            f"(rows={feature_refresh_result.get('rows')}, "
            f"latest={feature_refresh_result.get('latest_feature_dt')}, "
            f"{feature_refresh_duration_sec:.3f}s)"
        )
        _set_pipeline_stage(state, "feature_refresh", status="success", message=feature_refresh_done_message)
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message=feature_refresh_done_message,
            progress=FEATURE_REFRESH_PROGRESS,
        )
    except Exception as exc:
        logger.exception("Incremental ML feature refresh failed: %s", exc)
        if "feature_refresh_started" in locals():
            _record_update_stage_duration(
                state,
                profile,
                "refresh_ml_features",
                started_at=feature_refresh_started,
                status="failed",
                error=str(exc),
            )
        state["last_ml_feature_refresh_error"] = str(exc)
        ml_note_parts.append(f"ml_features=failed({exc})")
        feature_refresh_failed_message = f"feature_refresh: ML feature refresh failed; continuing ({exc})"
        _set_pipeline_stage(state, "feature_refresh", status="failed", message=feature_refresh_failed_message)
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message=feature_refresh_failed_message,
            progress=FEATURE_REFRESH_PROGRESS,
        )

    try:
        if _exit_if_canceled(job_id, state, stage="label_refresh", message="Canceled before label refresh"):
            return
        label_refresh_started = time.monotonic()
        label_refresh_message = "label_refresh: Refreshing mature 5/10/20-day labels..."
        _set_pipeline_stage(state, "label_refresh", message=label_refresh_message)
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message=label_refresh_message,
            progress=LABEL_REFRESH_PROGRESS,
        )
        from app.backend.db import get_conn as get_backend_conn
        from app.backend.services.ml import ml_service

        with get_backend_conn() as conn:
            label_refresh_result = ml_service.refresh_ml_labels_incremental(conn)
        state["last_ml_label_refresh_at"] = datetime.now().isoformat()
        state["last_ml_label_refresh_result"] = label_refresh_result
        state.pop("last_ml_label_refresh_error", None)
        ml_note_parts.append(
            "ml_labels="
            f"ok(rows={label_refresh_result.get('rows')},latest={label_refresh_result.get('latest_label_dt')})"
        )
        label_refresh_duration_sec = _record_update_stage_duration(
            state,
            profile,
            "refresh_ml_labels",
            started_at=label_refresh_started,
            rows=label_refresh_result.get("rows"),
            latest=label_refresh_result.get("latest_label_dt"),
        )
        label_refresh_done_message = (
            "label_refresh: ML labels refreshed "
            f"(rows={label_refresh_result.get('rows')}, "
            f"latest={label_refresh_result.get('latest_label_dt')}, "
            f"{label_refresh_duration_sec:.3f}s)"
        )
        _set_pipeline_stage(state, "label_refresh", status="success", message=label_refresh_done_message)
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message=label_refresh_done_message,
            progress=LABEL_REFRESH_PROGRESS,
        )
    except Exception as exc:
        logger.exception("Incremental ML label refresh failed: %s", exc)
        if "label_refresh_started" in locals():
            _record_update_stage_duration(
                state,
                profile,
                "refresh_ml_labels",
                started_at=label_refresh_started,
                status="failed",
                error=str(exc),
            )
        state["last_ml_label_refresh_error"] = str(exc)
        ml_note_parts.append(f"ml_labels=failed({exc})")
        label_refresh_failed_message = f"label_refresh: ML label refresh failed; continuing ({exc})"
        _set_pipeline_stage(state, "label_refresh", status="failed", message=label_refresh_failed_message)
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message=label_refresh_failed_message,
            progress=LABEL_REFRESH_PROGRESS,
        )

    try:
        if _exit_if_canceled(job_id, state, stage="prediction_refresh", message="Canceled before prediction refresh"):
            return
        prediction_refresh_started = time.monotonic()
        prediction_refresh_message = "prediction_refresh: Refreshing confirmed ML predictions..."
        _set_pipeline_stage(state, "prediction_refresh", message=prediction_refresh_message)
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message=prediction_refresh_message,
            progress=PREDICTION_REFRESH_PROGRESS,
        )
        from app.backend.db import get_conn as get_backend_conn
        from app.backend.services.ml import ml_service

        with get_backend_conn() as conn:
            prediction_refresh_result = ml_service.refresh_ml_predictions_incremental(conn)
        state["last_ml_prediction_refresh_at"] = datetime.now().isoformat()
        state["last_ml_prediction_refresh_result"] = prediction_refresh_result
        state.pop("last_ml_prediction_refresh_error", None)
        ml_note_parts.append(
            "ml_predictions="
            f"ok(rows={prediction_refresh_result.get('rows')},latest={prediction_refresh_result.get('latest_prediction_dt')})"
        )
        prediction_refresh_duration_sec = _record_update_stage_duration(
            state,
            profile,
            "refresh_ml_predictions",
            started_at=prediction_refresh_started,
            rows=prediction_refresh_result.get("rows"),
            latest=prediction_refresh_result.get("latest_prediction_dt"),
        )
        prediction_refresh_done_message = (
            "prediction_refresh: ML predictions refreshed "
            f"(rows={prediction_refresh_result.get('rows')}, "
            f"latest={prediction_refresh_result.get('latest_prediction_dt')}, "
            f"{prediction_refresh_duration_sec:.3f}s)"
        )
        _set_pipeline_stage(state, "prediction_refresh", status="success", message=prediction_refresh_done_message)
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message=prediction_refresh_done_message,
            progress=PREDICTION_REFRESH_PROGRESS,
        )
    except Exception as exc:
        logger.exception("Incremental ML prediction refresh failed: %s", exc)
        if "prediction_refresh_started" in locals():
            _record_update_stage_duration(
                state,
                profile,
                "refresh_ml_predictions",
                started_at=prediction_refresh_started,
                status="failed",
                error=str(exc),
            )
        state["last_ml_prediction_refresh_error"] = str(exc)
        ml_note_parts.append(f"ml_predictions=failed({exc})")
        prediction_refresh_failed_message = f"prediction_refresh: ML prediction refresh failed; continuing ({exc})"
        _set_pipeline_stage(state, "prediction_refresh", status="failed", message=prediction_refresh_failed_message)
        job_manager._update_db(
            job_id,
            "txt_update",
            "running",
            message=prediction_refresh_failed_message,
            progress=PREDICTION_REFRESH_PROGRESS,
        )

    try:
        if _exit_if_canceled(job_id, state, stage="cache_refresh", message="Canceled before cache refresh"):
            return
        cache_refresh_started = time.monotonic()
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
        state["last_cache_refresh_db_latest_key"] = int(db_latest_after_ingest_key) if db_latest_after_ingest_key is not None else None
        try:
            from app.backend.core.config import config as app_config
            from app.backend.services.dev_db_sync import (
                record_dev_db_sync_state,
                sync_updated_stock_db_to_local_peer,
            )

            sync_result = sync_updated_stock_db_to_local_peer(source_db_path=app_config.DB_PATH)
            record_dev_db_sync_state(state, sync_result)
            if sync_result.get("synced"):
                ml_note_parts.append(
                    f"dev_db_sync=ok(confirmed={sync_result.get('confirmed_latest_date')})"
                )
            elif sync_result.get("skipped_reason"):
                ml_note_parts.append(f"dev_db_sync=skip({sync_result.get('skipped_reason')})")
            else:
                ml_note_parts.append(f"dev_db_sync=failed({sync_result.get('error')})")
        except Exception as exc:
            logger.warning("Production-to-dev DB sync skipped after txt_update cache refresh: %s", exc)
            state["last_dev_db_sync_at"] = datetime.now().isoformat()
            state["last_dev_db_sync_error"] = str(exc)
            ml_note_parts.append(f"dev_db_sync=failed({exc})")
        _record_profile_phase(profile, "refresh_latest_ranking", started_at=cache_refresh_started)
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

    if run_tracking_refresh:
        try:
            if _exit_if_canceled(job_id, state, stage="tracking_refresh", message="Canceled before tracking refresh"):
                return
            tracking_refresh_started = time.monotonic()
            profile["mode"] = "explicit_heavy_path"
            profile["skipped"]["tracking_refresh"] = False
            profile["heavy_refresh_required"] = True
            profile["heavy_refresh_reason"] = tracking_refresh_trigger_reason
            _set_pipeline_stage(
                state,
                "tracking_refresh",
                message=f"Running explicit signal/ranking tracking refresh ({tracking_refresh_trigger_reason})...",
            )
            job_manager._update_db(
                job_id,
                "txt_update",
                "running",
                message=f"Running explicit signal/ranking tracking refresh ({tracking_refresh_trigger_reason})...",
                progress=TRACKING_REFRESH_PROGRESS,
            )
            from app.backend.services import signal_tracking_service

            def _on_tracking_refresh_progress(progress: dict[str, Any]) -> None:
                try:
                    progress = dict(progress)
                    progress["trigger_reason"] = tracking_refresh_trigger_reason
                    _record_tracking_refresh_progress(state, job_id=job_id, progress=progress)
                except Exception as exc:
                    logger.warning("Tracking refresh heartbeat skipped: %s", exc)

            tracking_result = signal_tracking_service.refresh_daily_tracking_window(
                progress_cb=_on_tracking_refresh_progress
            )
            state["last_tracking_refresh_at"] = datetime.now().isoformat()
            state["last_tracking_refresh_trigger_reason"] = tracking_refresh_trigger_reason
            state["last_tracking_refresh_result"] = {
                "market_day_window": tracking_result.get("market_day_window"),
                "from": tracking_result.get("from"),
                "to": tracking_result.get("to"),
                "basis_dates_processed": ((tracking_result.get("result") or {}).get("basis") or {}).get("dates_processed"),
                "ranking_appearance_upserted": ((tracking_result.get("result") or {}).get("ranking") or {}).get("appearance_upserted"),
            }
            _record_profile_phase(
                profile,
                "full_tracking_refresh",
                started_at=tracking_refresh_started,
                trigger_reason=tracking_refresh_trigger_reason,
            )
            ml_note_parts.append(
                "tracking="
                f"ok(from={state['last_tracking_refresh_result']['from']},"
                f"to={state['last_tracking_refresh_result']['to']},"
                f"appearance={state['last_tracking_refresh_result']['ranking_appearance_upserted']},"
                f"reason={tracking_refresh_trigger_reason})"
            )
        except Exception as exc:
            logger.exception("Tracking refresh failed: %s", exc)
            _record_pipeline_failure(
                state,
                stage="tracking_refresh",
                error=str(exc),
                message="Tracking refresh failed",
            )
            _finish_profile("failed")
            job_manager._update_db(
                job_id,
                "txt_update",
                "failed",
                error="Tracking refresh failed",
                message=f"Tracking refresh failed: {exc}",
                finished_at=datetime.now(),
            )
            return
    else:
        skipped_at = datetime.now().isoformat()
        state["last_tracking_refresh_skipped_at"] = skipped_at
        state["last_tracking_refresh_skipped_reason"] = "daily_fast_path"
        state.pop("last_tracking_refresh_trigger_reason", None)
        ml_note_parts.append("tracking=skip(daily_fast_path)")

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
                    "phase_dt": int(phase_dt) if phase_dt is not None else None,
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
        finalize_started = time.monotonic()
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
        _record_profile_phase(profile, "finalize_status", started_at=finalize_started)
        final_manifest = _build_txt_source_manifest_snapshot(
            code_path=code_path,
            out_dir=out_dir,
            db_latest_key=db_latest_after_ingest_key,
            ranking_snapshot_key=db_latest_after_ingest_key,
        )
        _save_txt_source_manifest(final_manifest)
        _finish_profile("done")
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
                source_run_id=latest_run_id or None,
                source_finished_at=None,
                source_status="success" if latest_run_id else None,
                source_report={
                    "summary": state.get("last_walkforward_run_summary") or {},
                    "windowing": state.get("last_walkforward_run_windowing") or {},
                }
                if latest_run_id and isinstance(state.get("last_walkforward_run_summary"), dict)
                else None,
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

    if legacy_analysis_disabled:
        logger.info(
            "TXT update skipping walkforward research snapshot (%s)",
            legacy_analysis_disabled_log_value(),
        )
        ml_note_parts.append("walkforward_research_snapshot=skip(legacy_analysis_disabled)")
    else:
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
    finalize_started = time.monotonic()
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
    _record_profile_phase(profile, "finalize_status", started_at=finalize_started)
    final_manifest = _build_txt_source_manifest_snapshot(
        code_path=code_path,
        out_dir=out_dir,
        db_latest_key=db_latest_after_ingest_key,
        ranking_snapshot_key=db_latest_after_ingest_key,
    )
    _save_txt_source_manifest(final_manifest)
    _finish_profile("done")
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

