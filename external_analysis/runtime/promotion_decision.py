from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.backend.services.analysis_bridge.reader import (
    get_internal_state_eval_promotion_review,
    save_internal_state_eval_promotion_decision,
)
from external_analysis.runtime.daily_research import _temporary_analysis_reader_paths


def run_promotion_decision_command(
    *,
    result_db_path: str | None = None,
    ops_db_path: str | None = None,
    decision: str,
    note: str | None = None,
    actor: str | None = "codex_cli",
    report_path: str | None = None,
) -> dict[str, Any]:
    with _temporary_analysis_reader_paths(result_db_path=result_db_path, ops_db_path=ops_db_path):
        payload = save_internal_state_eval_promotion_decision(
            decision=decision,
            note=note,
            actor=actor,
            ops_db_path=ops_db_path,
        )
        review = payload.get("review") or {}
        result = {
            "ok": not bool(payload.get("degraded")),
            "publish": payload.get("publish"),
            "as_of_date": payload.get("as_of_date"),
            "freshness_state": payload.get("freshness_state"),
            "decision": (review.get("approval_decision") or {}),
            "review": {
                "readiness_pass": review.get("readiness_pass"),
                "expectancy_delta": review.get("expectancy_delta"),
                "sample_count": review.get("sample_count"),
                "champion_version": review.get("champion_version"),
                "challenger_version": review.get("challenger_version"),
            },
        }
    if report_path:
        Path(str(report_path)).expanduser().resolve().write_text(
            json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    return result


def format_promotion_decision_text_report(payload: dict[str, Any]) -> str:
    decision = payload.get("decision") or {}
    review = payload.get("review") or {}
    publish = payload.get("publish") or {}
    lines = [
        "Tradex Promotion Decision",
        f"publish_id: {publish.get('publish_id') or '--'}",
        f"as_of_date: {payload.get('as_of_date') or '--'}",
        f"decision: {decision.get('decision') or '--'}",
        f"actor: {decision.get('actor') or '--'}",
        f"note: {decision.get('note') or '--'}",
        f"readiness_pass: {'yes' if review.get('readiness_pass') else 'no'}",
        f"expectancy_delta: {review.get('expectancy_delta') if review else '--'}",
        f"samples: {review.get('sample_count') if review else '--'}",
    ]
    return "\n".join(lines)
