from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from external_analysis.ops.store import insert_quarantine_record, upsert_job_run
from external_analysis.runtime.load_control import resolve_research_runtime_budget
from external_analysis.similarity.baseline import run_similarity_baseline

JOB_TYPE = "nightly_similarity_pipeline"
MAX_ATTEMPTS = 3
logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _run_id(as_of_date: str) -> str:
    return _utcnow().strftime(f"nightly_similarity_{as_of_date}_%Y%m%dT%H%M%S%fZ")


def run_nightly_similarity_pipeline(
    *,
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
) -> dict[str, Any]:
    run_id = _run_id(str(as_of_date))
    started_at = _utcnow()
    attempt = 1
    upsert_job_run(
        job_id=run_id,
        job_type=JOB_TYPE,
        status="running",
        as_of_date=str(as_of_date),
        publish_id=publish_id,
        attempt=attempt,
        started_at=started_at,
        details={"freshness_state": freshness_state, "load_control": load_control or {}},
        ops_db_path=ops_db_path,
    )
    logger.info("nightly_similarity_pipeline start run_id=%s as_of_date=%s", run_id, as_of_date)
    runtime_budget = resolve_research_runtime_budget(load_control)
    quarantine_reason = None
    try:
        similarity_payload = run_similarity_baseline(
            export_db_path=export_db_path,
            label_db_path=label_db_path,
            result_db_path=result_db_path,
            similarity_db_path=similarity_db_path,
            as_of_date=as_of_date,
            publish_id=publish_id,
            freshness_state=freshness_state,
            top_k=int(runtime_budget["similarity_top_k"]),
        )
    except Exception as exc:
        finished_at = _utcnow()
        quarantine_reason = "similarity_publish_failed"
        insert_quarantine_record(
            quarantine_id=f"{run_id}_publish",
            job_type=JOB_TYPE,
            as_of_date=str(as_of_date),
            publish_id=str(publish_id or ""),
            attempt_count=attempt,
            reason=quarantine_reason,
            payload={"error_class": exc.__class__.__name__, "message": str(exc)},
            ops_db_path=ops_db_path,
        )
        upsert_job_run(
            job_id=run_id,
            job_type=JOB_TYPE,
            status="failed",
            as_of_date=str(as_of_date),
            publish_id=str(publish_id or ""),
            attempt=attempt,
            started_at=started_at,
            finished_at=finished_at,
            error_class=exc.__class__.__name__,
            details={"quarantine_reason": quarantine_reason},
            ops_db_path=ops_db_path,
        )
        logger.exception("nightly_similarity_pipeline publish failed run_id=%s", run_id)
        return {
            "ok": False,
            "run_id": run_id,
            "job_type": JOB_TYPE,
            "status": "failed",
            "similarity": None,
            "quarantine_reason": quarantine_reason,
            "error_class": exc.__class__.__name__,
        }
    status = "success"
    if not similarity_payload.get("metrics_saved", False):
        status = "published_with_metrics_failure"
        quarantine_reason = "similarity_metrics_persist_failed"
        insert_quarantine_record(
            quarantine_id=f"{run_id}_metrics",
            job_type=JOB_TYPE,
            as_of_date=str(as_of_date),
            publish_id=str(similarity_payload.get("publish_id") or publish_id or ""),
            attempt_count=int(similarity_payload.get("metrics_attempts") or max_attempts),
            reason=quarantine_reason,
            payload={
                "metrics_error_class": similarity_payload.get("metrics_error_class"),
                "publish_id": similarity_payload.get("publish_id"),
                "similarity_metrics_run_id": similarity_payload.get("similarity_metrics_run_id"),
            },
            ops_db_path=ops_db_path,
        )
        logger.warning(
            "nightly_similarity_pipeline metrics quarantined run_id=%s publish_id=%s",
            run_id,
            similarity_payload.get("publish_id"),
        )
    finished_at = _utcnow()
    upsert_job_run(
        job_id=run_id,
        job_type=JOB_TYPE,
        status=status,
        as_of_date=str(as_of_date),
        publish_id=str(similarity_payload.get("publish_id") or publish_id or ""),
        attempt=attempt,
        started_at=started_at,
        finished_at=finished_at,
        error_class=str(similarity_payload.get("metrics_error_class") or "") or None,
        details={
            "publish_id": similarity_payload.get("publish_id"),
            "similar_case_count": similarity_payload.get("similar_case_count"),
            "similar_path_count": similarity_payload.get("similar_path_count"),
            "metrics_saved": similarity_payload.get("metrics_saved"),
            "metrics_attempts": similarity_payload.get("metrics_attempts"),
            "load_control": load_control or {},
            "runtime_budget": runtime_budget,
            "quarantine_reason": quarantine_reason,
        },
        ops_db_path=ops_db_path,
    )
    return {
        "ok": True,
        "run_id": run_id,
        "job_type": JOB_TYPE,
        "status": status,
        "similarity": similarity_payload,
        "load_control": load_control or {},
        "runtime_budget": runtime_budget,
        "quarantine_reason": quarantine_reason,
    }
