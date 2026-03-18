from __future__ import annotations

import os
from typing import Any

from fastapi.responses import JSONResponse

LEGACY_ANALYSIS_DISABLE_ENV = "MEEMEE_DISABLE_LEGACY_ANALYSIS"
LEGACY_ANALYSIS_DISABLED_JOB_TYPES: frozenset[str] = frozenset(
    {
        "phase_rebuild",
        "ml_train",
        "ml_predict",
        "ml_live_guard",
        "analysis_backfill",
    }
)


def is_legacy_analysis_disabled() -> bool:
    raw = os.getenv(LEGACY_ANALYSIS_DISABLE_ENV, "1")
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def legacy_analysis_disabled_log_value() -> str:
    return f"legacy_analysis_disabled={str(is_legacy_analysis_disabled()).lower()}"


def is_legacy_analysis_job_type(job_type: str) -> bool:
    return str(job_type).strip() in LEGACY_ANALYSIS_DISABLED_JOB_TYPES


def legacy_analysis_disabled_payload(*, job_type: str, source: str) -> dict[str, Any]:
    return {
        "ok": False,
        "disabled": True,
        "error": "legacy_analysis_disabled",
        "message": "Legacy analysis is disabled in Phase 1. Use external analysis publish results.",
        "job_type": str(job_type),
        "source": str(source),
    }


def legacy_analysis_disabled_response(*, job_type: str, source: str) -> JSONResponse:
    return JSONResponse(
        status_code=410,
        content=legacy_analysis_disabled_payload(job_type=job_type, source=source),
    )


def legacy_analysis_status_payload(*, source: str) -> dict[str, Any]:
    return {
        "ok": True,
        "disabled": True,
        "error": None,
        "message": "Legacy analysis is disabled in Phase 1. External analysis publish is the active path.",
        "source": str(source),
    }
