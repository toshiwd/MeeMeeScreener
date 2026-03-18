from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from external_analysis.ops.store import insert_quarantine_record, load_work_item, upsert_job_run, upsert_work_item
from external_analysis.runtime.review_summary import build_review_summary
from external_analysis.runtime.rolling_comparison import aggregate_comparison_windows

JOB_TYPE = "review_build_runner"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _make_work_id(*, scope_type: str, scope_id: str, suffix: str | None = None) -> str:
    parts = ["review_build", scope_type, scope_id]
    if suffix:
        parts.append(suffix)
    return "_".join(parts)


def run_review_build(
    *,
    result_db_path: str,
    similarity_db_path: str,
    ops_db_path: str,
    work_id: str | None = None,
    scope_type: str | None = None,
    scope_id: str | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    work_item = load_work_item(work_id=work_id, ops_db_path=ops_db_path) if work_id else None
    resolved_payload = dict(work_item["payload"]) if work_item else {}
    if payload:
        resolved_payload.update(payload)
    resolved_scope_type = str(scope_type or (work_item or {}).get("scope_type") or "nightly")
    resolved_scope_id = str(scope_id or (work_item or {}).get("scope_id") or "nightly")
    resolved_work_id = work_id or _make_work_id(
        scope_type=resolved_scope_type,
        scope_id=resolved_scope_id,
        suffix=str(resolved_payload.get("publish_id") or ""),
    )
    started_at = _utcnow()
    upsert_work_item(
        work_id=resolved_work_id,
        work_type="review_build",
        scope_type=resolved_scope_type,
        scope_id=resolved_scope_id,
        status="running",
        depends_on=(work_item or {}).get("depends_on"),
        payload=resolved_payload,
        started_at=started_at,
        ops_db_path=ops_db_path,
    )
    upsert_job_run(
        job_id=resolved_work_id,
        job_type=JOB_TYPE,
        status="running",
        publish_id=str(resolved_payload.get("publish_id") or ""),
        as_of_date=str(resolved_payload.get("as_of_date") or ""),
        started_at=started_at,
        details={"scope_type": resolved_scope_type, "scope_id": resolved_scope_id},
        ops_db_path=ops_db_path,
    )
    try:
        updated_scopes: dict[str, Any] = {}
        if resolved_scope_type == "replay":
            updated_scopes["replay"] = aggregate_comparison_windows(
                result_db_path=result_db_path,
                similarity_db_path=similarity_db_path,
                ops_db_path=ops_db_path,
                scope_type="replay",
                scope_id=resolved_scope_id,
            )
            updated_scopes["combined"] = aggregate_comparison_windows(
                result_db_path=result_db_path,
                similarity_db_path=similarity_db_path,
                ops_db_path=ops_db_path,
                scope_type="combined",
                scope_id="global",
            )
        else:
            updated_scopes["nightly"] = aggregate_comparison_windows(
                result_db_path=result_db_path,
                similarity_db_path=similarity_db_path,
                ops_db_path=ops_db_path,
                scope_type="nightly",
                scope_id="nightly",
            )
            updated_scopes["combined"] = aggregate_comparison_windows(
                result_db_path=result_db_path,
                similarity_db_path=similarity_db_path,
                ops_db_path=ops_db_path,
                scope_type="combined",
                scope_id="global",
            )
        review_summary = build_review_summary(ops_db_path=ops_db_path)
        finished_at = _utcnow()
        upsert_work_item(
            work_id=resolved_work_id,
            work_type="review_build",
            scope_type=resolved_scope_type,
            scope_id=resolved_scope_id,
            status="success",
            depends_on=(work_item or {}).get("depends_on"),
            payload=resolved_payload,
            started_at=started_at,
            finished_at=finished_at,
            ops_db_path=ops_db_path,
        )
        upsert_job_run(
            job_id=resolved_work_id,
            job_type=JOB_TYPE,
            status="success",
            publish_id=str(resolved_payload.get("publish_id") or ""),
            as_of_date=str(resolved_payload.get("as_of_date") or ""),
            started_at=started_at,
            finished_at=finished_at,
            details={
                "scope_type": resolved_scope_type,
                "scope_id": resolved_scope_id,
                "updated_scopes": updated_scopes,
                "review_id": review_summary.get("review_id"),
            },
            ops_db_path=ops_db_path,
        )
        return {
            "ok": True,
            "run_id": resolved_work_id,
            "job_type": JOB_TYPE,
            "status": "success",
            "scope_type": resolved_scope_type,
            "scope_id": resolved_scope_id,
            "updated_scopes": updated_scopes,
            "review_id": review_summary.get("review_id"),
        }
    except Exception as exc:
        finished_at = _utcnow()
        insert_quarantine_record(
            quarantine_id=f"{resolved_work_id}_failed",
            job_type=JOB_TYPE,
            as_of_date=str(resolved_payload.get("as_of_date") or ""),
            publish_id=str(resolved_payload.get("publish_id") or ""),
            attempt_count=1,
            reason="review_build_failed",
            payload={"error_class": exc.__class__.__name__, "message": str(exc)},
            ops_db_path=ops_db_path,
        )
        upsert_work_item(
            work_id=resolved_work_id,
            work_type="review_build",
            scope_type=resolved_scope_type,
            scope_id=resolved_scope_id,
            status="failed",
            depends_on=(work_item or {}).get("depends_on"),
            payload=resolved_payload,
            started_at=started_at,
            finished_at=finished_at,
            error_class=exc.__class__.__name__,
            ops_db_path=ops_db_path,
        )
        upsert_job_run(
            job_id=resolved_work_id,
            job_type=JOB_TYPE,
            status="failed",
            publish_id=str(resolved_payload.get("publish_id") or ""),
            as_of_date=str(resolved_payload.get("as_of_date") or ""),
            started_at=started_at,
            finished_at=finished_at,
            error_class=exc.__class__.__name__,
            details={"scope_type": resolved_scope_type, "scope_id": resolved_scope_id},
            ops_db_path=ops_db_path,
        )
        return {
            "ok": False,
            "run_id": resolved_work_id,
            "job_type": JOB_TYPE,
            "status": "failed",
            "scope_type": resolved_scope_type,
            "scope_id": resolved_scope_id,
            "error_class": exc.__class__.__name__,
        }
