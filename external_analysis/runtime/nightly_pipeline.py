from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from external_analysis.exporter.diff_export import run_diff_export
from external_analysis.labels.rolling_labels import build_rolling_labels
from external_analysis.models.candidate_baseline import run_candidate_baseline
from external_analysis.ops.store import insert_quarantine_record, upsert_job_run
from external_analysis.runtime.load_control import resolve_research_runtime_budget
from external_analysis.runtime.source_snapshot import create_source_snapshot

JOB_TYPE = "nightly_candidate_pipeline"
MAX_ATTEMPTS = 3
logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _run_id(as_of_date: str) -> str:
    return _utcnow().strftime(f"nightly_candidate_{as_of_date}_%Y%m%dT%H%M%S%fZ")


def run_nightly_candidate_pipeline(
    *,
    source_db_path: str | None = None,
    export_db_path: str | None = None,
    label_db_path: str | None = None,
    result_db_path: str | None = None,
    similarity_db_path: str | None = None,
    ops_db_path: str | None = None,
    as_of_date: str,
    publish_id: str | None = None,
    freshness_state: str = "fresh",
    max_attempts: int = MAX_ATTEMPTS,
    load_control: dict[str, Any] | None = None,
    snapshot_source: bool = True,
    snapshot_root: str | None = None,
) -> dict[str, Any]:
    run_id = _run_id(str(as_of_date))
    started_at = _utcnow()
    attempt = 1
    snapshot_payload = (
        create_source_snapshot(
            source_db_path=source_db_path,
            snapshot_root=snapshot_root or (str(Path(str(export_db_path)).expanduser().resolve().parent / "source_snapshots") if export_db_path else None),
            label=f"nightly_candidate_{as_of_date}",
        )
        if snapshot_source
        else None
    )
    effective_source_db_path = str((snapshot_payload or {}).get("snapshot_db_path") or source_db_path or "")
    upsert_job_run(
        job_id=run_id,
        job_type=JOB_TYPE,
        status="running",
        as_of_date=str(as_of_date),
        publish_id=publish_id,
        attempt=attempt,
        started_at=started_at,
        details={
            "freshness_state": freshness_state,
            "load_control": load_control or {},
            "source_snapshot": snapshot_payload,
        },
        ops_db_path=ops_db_path,
    )
    logger.info("nightly_candidate_pipeline start run_id=%s as_of_date=%s", run_id, as_of_date)
    runtime_budget = resolve_research_runtime_budget(load_control)
    export_payload = run_diff_export(source_db_path=effective_source_db_path, export_db_path=export_db_path)
    label_payload = build_rolling_labels(export_db_path=export_db_path, label_db_path=label_db_path)
    baseline_payload = run_candidate_baseline(
        export_db_path=export_db_path,
        label_db_path=label_db_path,
        result_db_path=result_db_path,
        as_of_date=as_of_date,
        publish_id=publish_id,
        freshness_state=freshness_state,
        ops_db_path=ops_db_path,
        candidate_limit_per_side=int(runtime_budget["candidate_limit_per_side"]),
        similarity_db_path=similarity_db_path,
    )
    status = "success"
    quarantine_reason = None
    if not baseline_payload.get("metrics_saved", False):
        status = "published_with_metrics_failure"
        quarantine_reason = "nightly_metrics_persist_failed"
        insert_quarantine_record(
            quarantine_id=f"{run_id}_metrics",
            job_type=JOB_TYPE,
            as_of_date=str(as_of_date),
            publish_id=str(baseline_payload.get("publish_id")),
            attempt_count=int(baseline_payload.get("metrics_attempts") or max_attempts),
            reason=quarantine_reason,
            payload={
                "metrics_error_class": baseline_payload.get("metrics_error_class"),
                "publish_id": baseline_payload.get("publish_id"),
                "nightly_metrics_run_id": baseline_payload.get("nightly_metrics_run_id"),
            },
            ops_db_path=ops_db_path,
        )
        logger.warning(
            "nightly_candidate_pipeline metrics quarantined run_id=%s publish_id=%s",
            run_id,
            baseline_payload.get("publish_id"),
        )
    finished_at = _utcnow()
    upsert_job_run(
        job_id=run_id,
        job_type=JOB_TYPE,
        status=status,
        as_of_date=str(as_of_date),
        publish_id=str(baseline_payload.get("publish_id") or publish_id or ""),
        attempt=attempt,
        started_at=started_at,
        finished_at=finished_at,
        error_class=str(baseline_payload.get("metrics_error_class") or "") or None,
        details={
            "export_run_id": export_payload.get("run_id"),
            "label_run_id": label_payload.get("run_id"),
            "publish_id": baseline_payload.get("publish_id"),
            "metrics_saved": baseline_payload.get("metrics_saved"),
            "metrics_attempts": baseline_payload.get("metrics_attempts"),
            "state_eval_count": baseline_payload.get("state_eval_count"),
            "state_eval_shadow_saved": baseline_payload.get("state_eval_shadow_saved"),
            "state_eval_readiness_pass": baseline_payload.get("state_eval_readiness_pass"),
            "load_control": load_control or {},
            "runtime_budget": runtime_budget,
            "quarantine_reason": quarantine_reason,
            "source_snapshot": snapshot_payload,
        },
        ops_db_path=ops_db_path,
    )
    return {
        "ok": True,
        "run_id": run_id,
        "job_type": JOB_TYPE,
        "status": status,
        "export": export_payload,
        "labels": label_payload,
        "baseline": baseline_payload,
        "load_control": load_control or {},
        "runtime_budget": runtime_budget,
        "quarantine_reason": quarantine_reason,
        "source_snapshot": snapshot_payload,
    }
