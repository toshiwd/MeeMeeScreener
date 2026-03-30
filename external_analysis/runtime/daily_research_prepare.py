from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from external_analysis.contracts.paths import resolve_export_db_path, resolve_label_db_path
from external_analysis.exporter.research_prepare_export import run_research_prepare_export
from external_analysis.exporter.snapshot_status import (
    EXPORT_SNAPSHOT_STATUS_COMPLETE,
    build_export_snapshot,
    probe_export_snapshot_readiness,
)
from external_analysis.labels.rolling_labels import POLICY_VERSION, build_rolling_labels
from external_analysis.runtime.incremental_cache import LABEL_RELEVANT_EXPORT_TABLES, probe_label_cache

DAILY_RESEARCH_PREPARE_SCHEMA_VERSION = "tradex_daily_research_prepare_v1"


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


def resolve_daily_research_prepare_manifest_path(export_db_path: str | Path | None) -> Path:
    resolved = resolve_export_db_path(str(export_db_path) if export_db_path is not None else None)
    return resolved.with_name(f"{resolved.name}.daily_research_prepare.json")


def resolve_daily_research_prepare_progress_path(export_db_path: str | Path | None) -> Path:
    resolved = resolve_export_db_path(str(export_db_path) if export_db_path is not None else None)
    return resolved.with_name(f"{resolved.name}.daily_research_prepare.progress.json")


def _resolve_latest_trade_date_from_export(export_db_path: str | None) -> str | None:
    resolved = resolve_export_db_path(export_db_path)
    if not resolved.exists():
        return None
    conn = duckdb.connect(str(resolved), read_only=True)
    try:
        row = conn.execute("SELECT MAX(trade_date) FROM bars_daily_export").fetchone()
    finally:
        conn.close()
    if not row or row[0] is None:
        return None
    return str(int(row[0]))


def probe_daily_research_prepared_environment(
    *,
    source_db_path: str | None = None,
    export_db_path: str | None = None,
    label_db_path: str | None = None,
) -> dict[str, Any]:
    export_probe = probe_export_snapshot_readiness(source_db_path, export_db_path)
    label_probe = probe_label_cache(
        export_db_path=export_db_path,
        label_db_path=label_db_path,
        generation_key="rolling_labels",
        dependency_version=POLICY_VERSION,
        relevant_tables=LABEL_RELEVANT_EXPORT_TABLES,
    )
    reusable = bool(export_probe.get("reusable")) and str(label_probe.get("action") or "") == "skip"
    reason_code = "prepared_complete"
    if str(export_probe.get("status") or "") != EXPORT_SNAPSHOT_STATUS_COMPLETE:
        reason_code = str(export_probe.get("reason_code") or "export_prepare_required")
    elif str(label_probe.get("action") or "") != "skip":
        reason_code = f"label_{str(label_probe.get('reason') or 'prepare_required')}"
    manifest_path = resolve_daily_research_prepare_manifest_path(export_db_path)
    payload = {
        "schema_version": DAILY_RESEARCH_PREPARE_SCHEMA_VERSION,
        "prepared": reusable,
        "reason_code": reason_code,
        "source_signature": export_probe.get("source_signature"),
        "source_db_path": export_probe.get("source_db_path"),
        "export_db_path": export_probe.get("export_db_path"),
        "label_db_path": str(resolve_label_db_path(label_db_path)),
        "source_snapshot_db_path": export_probe.get("source_db_path"),
        "export_status": export_probe.get("status"),
        "label_status": str(label_probe.get("action") or ""),
        "latest_trade_date": _resolve_latest_trade_date_from_export(export_db_path),
        "prepared_at": None,
        "progress_path": str(resolve_daily_research_prepare_progress_path(export_db_path)),
        "manifest_path": str(manifest_path),
        "export_probe": export_probe,
        "label_probe": label_probe,
    }
    if manifest_path.exists():
        try:
            saved = json.loads(manifest_path.read_text(encoding="utf-8"))
            if isinstance(saved, dict):
                payload["prepared_at"] = saved.get("prepared_at")
        except (OSError, ValueError, json.JSONDecodeError):
            payload["prepared_at"] = None
    return payload


def run_daily_research_prepare(
    *,
    source_db_path: str | None = None,
    export_db_path: str | None = None,
    label_db_path: str | None = None,
    progress_path: str | None = None,
    manifest_path: str | None = None,
) -> dict[str, Any]:
    resolved_progress_path = Path(progress_path).expanduser().resolve() if progress_path else resolve_daily_research_prepare_progress_path(export_db_path)
    resolved_manifest_path = Path(manifest_path).expanduser().resolve() if manifest_path else resolve_daily_research_prepare_manifest_path(export_db_path)
    progress = {
        "schema_version": DAILY_RESEARCH_PREPARE_SCHEMA_VERSION,
        "mode": "prepare",
        "status": "running",
        "current_phase": "export_probe",
        "started_at": _utcnow_iso(),
        "finished_at": None,
        "export_status": None,
        "label_status": None,
        "eta_seconds": None,
        "manifest_path": str(resolved_manifest_path),
    }
    _write_json(resolved_progress_path, progress)
    export_payload: dict[str, Any] | None = None
    label_payload: dict[str, Any] | None = None
    try:
        export_probe = probe_export_snapshot_readiness(source_db_path, export_db_path)
        progress["export_status"] = export_probe.get("status")
        if not bool(export_probe.get("reusable")):
            progress["current_phase"] = "export_prepare"
            _write_json(resolved_progress_path, progress)
            export_ready = build_export_snapshot(
                source_db_path=source_db_path,
                export_db_path=export_db_path,
                export_runner=run_research_prepare_export,
            )
            export_payload = dict(export_ready)
        else:
            export_payload = dict(export_probe)

        progress["current_phase"] = "label_probe"
        _write_json(resolved_progress_path, progress)
        label_probe = probe_label_cache(
            export_db_path=export_db_path,
            label_db_path=label_db_path,
            generation_key="rolling_labels",
            dependency_version=POLICY_VERSION,
            relevant_tables=LABEL_RELEVANT_EXPORT_TABLES,
        )
        progress["label_status"] = str(label_probe.get("action") or "")
        if str(label_probe.get("action") or "") != "skip":
            progress["current_phase"] = "label_prepare"
            _write_json(resolved_progress_path, progress)
            label_payload = build_rolling_labels(export_db_path=export_db_path, label_db_path=label_db_path)
        else:
            label_payload = {
                "ok": True,
                "skipped": True,
                "cache_state": label_probe.get("cache_state"),
                "reason": label_probe.get("reason"),
                "dirty_ranges": [],
                "source_signature": label_probe.get("source_signature"),
            }

        progress["current_phase"] = "manifest"
        _write_json(resolved_progress_path, progress)
        prepared_probe = probe_daily_research_prepared_environment(
            source_db_path=source_db_path,
            export_db_path=export_db_path,
            label_db_path=label_db_path,
        )
        manifest_payload = {
            "schema_version": DAILY_RESEARCH_PREPARE_SCHEMA_VERSION,
            "prepared": bool(prepared_probe.get("prepared")),
            "reason_code": prepared_probe.get("reason_code"),
            "source_signature": prepared_probe.get("source_signature"),
            "source_snapshot_db_path": prepared_probe.get("source_snapshot_db_path"),
            "export_status": prepared_probe.get("export_status"),
            "label_status": prepared_probe.get("label_status"),
            "latest_trade_date": prepared_probe.get("latest_trade_date"),
            "prepared_at": _utcnow_iso(),
            "progress_path": str(resolved_progress_path),
            "export_probe": prepared_probe.get("export_probe"),
            "label_probe": prepared_probe.get("label_probe"),
            "export_prepare": export_payload,
            "label_prepare": label_payload,
        }
        _write_json(resolved_manifest_path, manifest_payload)
        progress.update(
            {
                "status": "complete",
                "current_phase": "completed",
                "finished_at": _utcnow_iso(),
                "export_status": manifest_payload["export_status"],
                "label_status": manifest_payload["label_status"],
                "eta_seconds": 0,
            }
        )
        _write_json(resolved_progress_path, progress)
        return {
            "ok": bool(manifest_payload["prepared"]),
            "prepared": bool(manifest_payload["prepared"]),
            "reason_code": manifest_payload["reason_code"],
            "source_signature": manifest_payload["source_signature"],
            "source_snapshot_db_path": manifest_payload["source_snapshot_db_path"],
            "export_status": manifest_payload["export_status"],
            "label_status": manifest_payload["label_status"],
            "latest_trade_date": manifest_payload["latest_trade_date"],
            "prepared_at": manifest_payload["prepared_at"],
            "progress_path": str(resolved_progress_path),
            "manifest_path": str(resolved_manifest_path),
            "export_prepare": export_payload,
            "label_prepare": label_payload,
            "export_probe": manifest_payload["export_probe"],
            "label_probe": manifest_payload["label_probe"],
        }
    except Exception as exc:
        progress.update(
            {
                "status": "failed",
                "current_phase": "failed",
                "finished_at": _utcnow_iso(),
                "error_class": exc.__class__.__name__,
                "error_message": str(exc),
            }
        )
        _write_json(resolved_progress_path, progress)
        raise
