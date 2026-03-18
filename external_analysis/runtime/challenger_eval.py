from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from external_analysis.ops.store import insert_quarantine_record, load_work_item, upsert_job_run, upsert_work_item
from external_analysis.similarity.baseline import (
    CHALLENGER_EMBEDDING_VERSION,
    EMBEDDING_VERSION,
    materialize_challenger_metrics,
    prepare_challenger_template,
    run_similarity_challenger_shadow,
)

JOB_TYPE = "challenger_eval_runner"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _make_work_id(*, work_type: str, scope_type: str, scope_id: str, suffix: str | None = None) -> str:
    parts = [work_type, scope_type, scope_id]
    if suffix:
        parts.append(suffix)
    return "_".join(parts)


def _ensure_work_item(
    *,
    work_id: str | None,
    work_type: str,
    scope_type: str,
    scope_id: str,
    payload: dict[str, Any],
    ops_db_path: str | None,
) -> str:
    resolved = work_id or _make_work_id(
        work_type=work_type,
        scope_type=scope_type,
        scope_id=scope_id,
        suffix=str(payload.get("publish_id") or payload.get("replay_id") or payload.get("as_of_date") or ""),
    )
    upsert_work_item(
        work_id=resolved,
        work_type=work_type,
        scope_type=scope_type,
        scope_id=scope_id,
        status="pending",
        payload=payload,
        ops_db_path=ops_db_path,
    )
    return resolved


def _queue_review_work_item(
    *,
    scope_type: str,
    scope_id: str,
    payload: dict[str, Any],
    depends_on: list[str],
    ops_db_path: str | None,
) -> str:
    if scope_type == "nightly":
        suffix = str(payload.get("publish_id") or payload.get("as_of_date") or "latest")
    else:
        suffix = None
    review_work_id = _make_work_id(work_type="review_build", scope_type=scope_type, scope_id=scope_id, suffix=suffix)
    upsert_work_item(
        work_id=review_work_id,
        work_type="review_build",
        scope_type=scope_type,
        scope_id=scope_id,
        status="pending",
        depends_on=depends_on,
        payload=payload,
        ops_db_path=ops_db_path,
    )
    return review_work_id


def run_challenger_eval(
    *,
    export_db_path: str | None = None,
    label_db_path: str | None = None,
    result_db_path: str | None = None,
    similarity_db_path: str | None = None,
    ops_db_path: str | None = None,
    work_id: str | None = None,
    scope_type: str | None = None,
    scope_id: str | None = None,
    as_of_date: str | None = None,
    publish_id: str | None = None,
    replay_id: str | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    work_item = load_work_item(work_id=work_id, ops_db_path=ops_db_path) if work_id else None
    resolved_payload = dict(work_item["payload"]) if work_item else {}
    if payload:
        resolved_payload.update(payload)
    resolved_scope_type = str(scope_type or (work_item or {}).get("scope_type") or ("replay" if replay_id else "nightly"))
    resolved_scope_id = str(scope_id or (work_item or {}).get("scope_id") or (replay_id or "nightly"))
    if publish_id:
        resolved_payload["publish_id"] = publish_id
    if as_of_date:
        resolved_payload["as_of_date"] = str(as_of_date)
    if replay_id:
        resolved_payload["replay_id"] = replay_id
    resolved_work_id = _ensure_work_item(
        work_id=work_id,
        work_type="challenger_eval",
        scope_type=resolved_scope_type,
        scope_id=resolved_scope_id,
        payload=resolved_payload,
        ops_db_path=ops_db_path,
    )
    started_at = _utcnow()
    upsert_work_item(
        work_id=resolved_work_id,
        work_type="challenger_eval",
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
        as_of_date=str(resolved_payload.get("as_of_date") or ""),
        publish_id=str(resolved_payload.get("publish_id") or ""),
        started_at=started_at,
        details={"scope_type": resolved_scope_type, "scope_id": resolved_scope_id},
        ops_db_path=ops_db_path,
    )
    try:
        if resolved_scope_type == "replay":
            replay_days = list(resolved_payload.get("days") or [])
            if not replay_days:
                raise RuntimeError("replay_days_missing")
            template_payload = prepare_challenger_template(
                export_db_path=export_db_path,
                label_db_path=label_db_path,
                similarity_db_path=similarity_db_path,
                as_of_date=str(replay_days[0]["as_of_date"]),
                top_k=int(resolved_payload.get("top_k") or 5),
                cached_metrics_template=resolved_payload.get("metrics_template"),
            )
            resolved_payload.update(
                {
                    "template_key": template_payload["template_key"],
                    "source_signature": template_payload["source_signature"],
                    "metrics_template": template_payload["metrics_template"],
                    "shadow_case_count": int(template_payload["shadow_case_count"]),
                }
            )
            upsert_work_item(
                work_id=resolved_work_id,
                work_type="challenger_eval",
                scope_type=resolved_scope_type,
                scope_id=resolved_scope_id,
                status="running",
                depends_on=(work_item or {}).get("depends_on"),
                payload=resolved_payload,
                started_at=started_at,
                ops_db_path=ops_db_path,
            )
            metrics_saved_count = 0
            metric_results: list[dict[str, Any]] = []
            for day in replay_days:
                result = materialize_challenger_metrics(
                    similarity_db_path=similarity_db_path,
                    publish_id=str(day["publish_id"]),
                    as_of_date=str(day["as_of_date"]),
                    metrics_template=template_payload["metrics_template"],
                    scope_type="replay",
                    scope_id=resolved_scope_id,
                    producer_work_id=resolved_work_id,
                )
                metric_results.append(result)
                if result["metrics_saved"]:
                    metrics_saved_count += 1
            status = "success" if metrics_saved_count == len(replay_days) else "shadow_with_metrics_failure"
            review_work_id = None
            if metrics_saved_count == len(replay_days):
                review_work_id = _queue_review_work_item(
                    scope_type="replay",
                    scope_id=resolved_scope_id,
                    payload={"replay_id": resolved_scope_id},
                    depends_on=[resolved_work_id],
                    ops_db_path=ops_db_path,
                )
            finished_at = _utcnow()
            upsert_work_item(
                work_id=resolved_work_id,
                work_type="challenger_eval",
                scope_type=resolved_scope_type,
                scope_id=resolved_scope_id,
                status=status,
                depends_on=(work_item or {}).get("depends_on"),
                payload=resolved_payload,
                started_at=started_at,
                finished_at=finished_at,
                error_class=None if status == "success" else "MetricsPersistFailure",
                ops_db_path=ops_db_path,
            )
            upsert_job_run(
                job_id=resolved_work_id,
                job_type=JOB_TYPE,
                status=status,
                attempt=1,
                started_at=started_at,
                finished_at=finished_at,
                error_class=None if status == "success" else "MetricsPersistFailure",
                details={
                    "scope_type": resolved_scope_type,
                    "scope_id": resolved_scope_id,
                    "template_key": template_payload["template_key"],
                    "evaluated_publish_count": len(replay_days),
                    "metrics_saved_count": metrics_saved_count,
                    "review_work_id": review_work_id,
                },
                ops_db_path=ops_db_path,
            )
            return {
                "ok": True,
                "run_id": resolved_work_id,
                "job_type": JOB_TYPE,
                "status": status,
                "scope_type": resolved_scope_type,
                "scope_id": resolved_scope_id,
                "template_key": template_payload["template_key"],
                "evaluated_publish_count": len(replay_days),
                "metrics_saved_count": metrics_saved_count,
                "shadow_case_count": int(template_payload["shadow_case_count"]),
                "review_work_id": review_work_id,
                "embedding_version": CHALLENGER_EMBEDDING_VERSION,
                "comparison_target_version": EMBEDDING_VERSION,
            }
        persist_shadow_rows = bool(resolved_payload.get("persist_shadow_rows", False))
        query_case_limit = resolved_payload.get("query_case_limit")
        candidate_pool_limit = resolved_payload.get("candidate_pool_limit")
        nightly_payload = run_similarity_challenger_shadow(
            export_db_path=export_db_path,
            label_db_path=label_db_path,
            result_db_path=result_db_path,
            similarity_db_path=similarity_db_path,
            as_of_date=str(resolved_payload.get("as_of_date") or as_of_date or ""),
            publish_id=str(resolved_payload.get("publish_id") or publish_id or ""),
            query_case_limit=None if query_case_limit in (None, "") else int(query_case_limit),
            candidate_pool_limit=None if candidate_pool_limit in (None, "") else int(candidate_pool_limit),
            scope_type="nightly",
            scope_id="nightly",
            producer_work_id=resolved_work_id,
            persist_shadow_rows=persist_shadow_rows,
            cached_metrics_template=resolved_payload.get("metrics_template"),
        )
        resolved_payload.update(
            {
                "publish_id": nightly_payload["publish_id"],
                "as_of_date": nightly_payload["as_of_date"],
                "template_key": nightly_payload.get("template_key"),
                "source_signature": nightly_payload.get("source_signature"),
                "metrics_template": nightly_payload.get("metrics_template"),
                "query_case_limit": nightly_payload.get("query_case_limit"),
                "query_case_count": nightly_payload.get("query_case_count"),
                "candidate_pool_limit": nightly_payload.get("candidate_pool_limit"),
            }
        )
        review_work_id = None
        if nightly_payload.get("metrics_saved", False):
            review_work_id = _queue_review_work_item(
                scope_type="nightly",
                scope_id="nightly",
                payload={
                    "publish_id": nightly_payload["publish_id"],
                    "as_of_date": nightly_payload["as_of_date"],
                },
                depends_on=[resolved_work_id],
                ops_db_path=ops_db_path,
            )
        finished_at = _utcnow()
        status = "success" if nightly_payload.get("metrics_saved", False) else "shadow_with_metrics_failure"
        upsert_work_item(
            work_id=resolved_work_id,
            work_type="challenger_eval",
            scope_type=resolved_scope_type,
            scope_id=resolved_scope_id,
            status=status,
            depends_on=(work_item or {}).get("depends_on"),
            payload=resolved_payload,
            started_at=started_at,
            finished_at=finished_at,
            error_class=None if nightly_payload.get("metrics_saved", False) else str(nightly_payload.get("metrics_error_class") or "MetricsPersistFailure"),
            ops_db_path=ops_db_path,
        )
        upsert_job_run(
            job_id=resolved_work_id,
            job_type=JOB_TYPE,
            status=status,
            as_of_date=str(nightly_payload.get("as_of_date") or ""),
            publish_id=str(nightly_payload.get("publish_id") or ""),
            started_at=started_at,
            finished_at=finished_at,
            error_class=None if nightly_payload.get("metrics_saved", False) else str(nightly_payload.get("metrics_error_class") or "MetricsPersistFailure"),
            details={
                "scope_type": resolved_scope_type,
                "scope_id": resolved_scope_id,
                "template_key": nightly_payload.get("template_key"),
                "shadow_case_count": nightly_payload.get("shadow_case_count"),
                "persist_shadow_rows": persist_shadow_rows,
                "query_case_limit": nightly_payload.get("query_case_limit"),
                "query_case_count": nightly_payload.get("query_case_count"),
                "candidate_pool_limit": nightly_payload.get("candidate_pool_limit"),
                "review_work_id": review_work_id,
            },
            ops_db_path=ops_db_path,
        )
        return {
            "ok": True,
            "run_id": resolved_work_id,
            "job_type": JOB_TYPE,
            "status": status,
            "scope_type": resolved_scope_type,
            "scope_id": resolved_scope_id,
            "review_work_id": review_work_id,
            "challenger": nightly_payload,
            "embedding_version": CHALLENGER_EMBEDDING_VERSION,
            "comparison_target_version": EMBEDDING_VERSION,
        }
    except Exception as exc:
        finished_at = _utcnow()
        insert_quarantine_record(
            quarantine_id=f"{resolved_work_id}_failed",
            job_type=JOB_TYPE,
            as_of_date=str(resolved_payload.get("as_of_date") or ""),
            publish_id=str(resolved_payload.get("publish_id") or ""),
            attempt_count=1,
            reason="challenger_eval_failed",
            payload={"error_class": exc.__class__.__name__, "message": str(exc)},
            ops_db_path=ops_db_path,
        )
        upsert_work_item(
            work_id=resolved_work_id,
            work_type="challenger_eval",
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
            as_of_date=str(resolved_payload.get("as_of_date") or ""),
            publish_id=str(resolved_payload.get("publish_id") or ""),
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
