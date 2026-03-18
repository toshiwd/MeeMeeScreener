from __future__ import annotations

from typing import Any

from external_analysis.ops.store import insert_quarantine_record, upsert_job_run
from external_analysis.runtime.challenger_eval import run_challenger_eval
from external_analysis.runtime.load_control import resolve_research_runtime_budget

JOB_TYPE = "nightly_similarity_challenger_pipeline"


def run_nightly_similarity_challenger_pipeline(
    *,
    export_db_path: str | None = None,
    label_db_path: str | None = None,
    result_db_path: str | None = None,
    similarity_db_path: str | None = None,
    ops_db_path: str | None = None,
    as_of_date: str,
    publish_id: str | None = None,
    max_attempts: int = 3,
    load_control: dict[str, Any] | None = None,
) -> dict[str, Any]:
    del max_attempts
    job_id = f"{JOB_TYPE}_{publish_id or as_of_date}"
    runtime_budget = resolve_research_runtime_budget(load_control)
    upsert_job_run(
        job_id=job_id,
        job_type=JOB_TYPE,
        status="running",
        as_of_date=str(as_of_date),
        publish_id=publish_id,
        details={"mode": "wrapper", "load_control": load_control or {}, "runtime_budget": runtime_budget},
        ops_db_path=ops_db_path,
    )
    try:
        payload = run_challenger_eval(
            export_db_path=export_db_path,
            label_db_path=label_db_path,
            result_db_path=result_db_path,
            similarity_db_path=similarity_db_path,
            ops_db_path=ops_db_path,
            scope_type="nightly",
            scope_id="nightly",
            as_of_date=str(as_of_date),
            publish_id=publish_id,
            payload={
                "query_case_limit": int(runtime_budget["challenger_query_case_limit"]),
                "candidate_pool_limit": int(runtime_budget["challenger_candidate_pool_limit"]),
            },
        )
    except Exception as exc:
        insert_quarantine_record(
            quarantine_id=f"{job_id}_failed",
            job_type=JOB_TYPE,
            as_of_date=str(as_of_date),
            publish_id=publish_id,
            attempt_count=1,
            reason="challenger_eval_failed",
            payload={"error_class": exc.__class__.__name__, "message": str(exc)},
            ops_db_path=ops_db_path,
        )
        payload = {
            "ok": False,
            "run_id": None,
            "status": "failed",
            "error_class": exc.__class__.__name__,
            "review_work_id": None,
        }
    upsert_job_run(
        job_id=job_id,
        job_type=JOB_TYPE,
        status=str(payload.get("status") or "failed"),
        as_of_date=str(as_of_date),
        publish_id=publish_id,
        error_class=None if payload.get("ok", False) else str(payload.get("error_class") or "RuntimeError"),
        details={
            "run_id": payload.get("run_id"),
            "review_work_id": payload.get("review_work_id"),
            "load_control": load_control or {},
            "runtime_budget": runtime_budget,
        },
        ops_db_path=ops_db_path,
    )
    return {
        "ok": bool(payload.get("ok", False)),
        "run_id": payload.get("run_id"),
        "job_type": JOB_TYPE,
        "status": payload.get("status"),
        "challenger": payload.get("challenger"),
        "review_work_id": payload.get("review_work_id"),
        "quarantine_reason": None if payload.get("ok", False) else "challenger_eval_failed",
    }
