from __future__ import annotations

import logging
from datetime import datetime

from app.backend.core.jobs import job_manager
from app.db.session import get_conn
from external_analysis.runtime.load_control import ResearchLoadDecision, evaluate_research_load_control, load_decision_payload
from external_analysis.runtime.nightly_pipeline import run_nightly_candidate_pipeline

EXTERNAL_ANALYSIS_PUBLISH_JOB_TYPE = "external_analysis_publish_latest"
logger = logging.getLogger(__name__)


def _to_int(value: object, default: int | None = None) -> int | None:
    try:
        if value is None:
            result = default
        else:
            result = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        result = default
    return None if result is None else int(result)


def schedule_external_analysis_publish_latest(
    *,
    source: str,
    as_of: int | None = None,
    freshness_state: str = "fresh",
) -> str | None:
    payload: dict[str, object] = {
        "source": source,
        "freshness_state": str(freshness_state or "fresh").strip() or "fresh",
    }
    resolved_as_of = _to_int(as_of)
    if resolved_as_of is not None:
        payload["as_of"] = resolved_as_of
    job_id = job_manager.submit(
        EXTERNAL_ANALYSIS_PUBLISH_JOB_TYPE,
        payload=payload,
        unique=True,
        message="Waiting in queue...",
        progress=0,
    )
    if job_id:
        logger.info(
            "Submitted external analysis publish job id=%s source=%s as_of=%s",
            job_id,
            source,
            payload.get("as_of"),
        )
    return job_id


def resolve_latest_external_analysis_as_of_date() -> int | None:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT
                MAX(
                    CASE
                        WHEN date BETWEEN 19000101 AND 20991231 THEN date
                        WHEN date >= 1000000000000 THEN CAST(strftime(to_timestamp(date / 1000), '%Y%m%d') AS INTEGER)
                        WHEN date >= 1000000000 THEN CAST(strftime(to_timestamp(date), '%Y%m%d') AS INTEGER)
                        ELSE NULL
                    END
                ) AS max_ymd
            FROM daily_bars
            """
        ).fetchone()
    if not row or row[0] is None:
        return None
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return None


def _resolve_effective_load_decision(*, payload: dict, load_decision: ResearchLoadDecision) -> ResearchLoadDecision:
    if load_decision.mode != "deferred" or str(load_decision.reason or "").strip().lower() != "outside_heavy_window":
        return load_decision
    source = str(payload.get("source") or "").strip().lower()
    override_reason = "manual_override_outside_heavy_window" if source == "manual_api" else "background_override_outside_heavy_window"
    return ResearchLoadDecision(
        mode="throttled",
        reason=override_reason,
        active_window_title=load_decision.active_window_title,
        active_process_name=load_decision.active_process_name,
        within_heavy_window=load_decision.within_heavy_window,
        interaction_detected=load_decision.interaction_detected,
    )


def handle_external_analysis_publish_latest(job_id: str, payload: dict) -> None:
    as_of = _to_int(payload.get("as_of"))
    if as_of is None:
        as_of = resolve_latest_external_analysis_as_of_date()
    if as_of is None:
        raise RuntimeError("latest_as_of_not_found")

    freshness_state = str(payload.get("freshness_state") or "fresh").strip() or "fresh"
    load_decision = _resolve_effective_load_decision(
        payload=payload,
        load_decision=evaluate_research_load_control(),
    )
    if load_decision.mode == "deferred":
        job_manager._update_db(
            job_id,
            EXTERNAL_ANALYSIS_PUBLISH_JOB_TYPE,
            "skipped",
            progress=100,
            message=f"external_analysis deferred reason={load_decision.reason}",
            finished_at=datetime.now(),
        )
        return

    job_manager._update_db(
        job_id,
        EXTERNAL_ANALYSIS_PUBLISH_JOB_TYPE,
        "running",
        started_at=datetime.now(),
        progress=5,
        message=f"external_analysis start as_of={as_of} mode={load_decision.mode}",
    )
    job_manager.update_status_cache_only(
        job_id=job_id,
        job_type=EXTERNAL_ANALYSIS_PUBLISH_JOB_TYPE,
        status="running",
        progress=20,
        message=f"external_analysis nightly pipeline mode={load_decision.mode}",
    )

    result = run_nightly_candidate_pipeline(
        as_of_date=str(as_of),
        publish_id=str(payload.get("publish_id") or "").strip() or None,
        freshness_state=freshness_state,
        load_control=load_decision_payload(load_decision),
    )
    baseline = result.get("baseline") if isinstance(result, dict) else {}
    publish_id = baseline.get("publish_id") if isinstance(baseline, dict) else None
    status = "success" if bool(result.get("ok", False)) else "failed"
    message = f"external_analysis finished as_of={as_of}"
    if publish_id:
        message += f" publish_id={publish_id}"
    error = None
    if status != "success":
        error = str(result)
    job_manager._update_db(
        job_id,
        EXTERNAL_ANALYSIS_PUBLISH_JOB_TYPE,
        status,
        progress=100,
        message=message,
        error=error,
        finished_at=datetime.now(),
    )
