from __future__ import annotations

from app.backend.services.analysis_bridge.contracts import (
    DEGRADE_REASON_HARD_STALE,
    DEGRADE_REASON_MANIFEST_MISMATCH,
    DEGRADE_REASON_NO_PUBLISH,
    DEGRADE_REASON_POINTER_CORRUPTION,
    DEGRADE_REASON_RESULT_DB_MISSING,
    DEGRADE_REASON_REGIME_ROW_CORRUPTION,
    DEGRADE_REASON_SCHEMA_MISMATCH,
    DEGRADE_REASON_WARNING_STALE,
)


def build_degrade_payload(reason: str) -> dict:
    messages = {
        DEGRADE_REASON_NO_PUBLISH: "外付け解析結果は未公開",
        DEGRADE_REASON_WARNING_STALE: "解析結果は最新ではありません",
        DEGRADE_REASON_HARD_STALE: "解析結果が古いため参考表示に切替中",
        DEGRADE_REASON_POINTER_CORRUPTION: "解析結果ポインタが破損しています",
        DEGRADE_REASON_MANIFEST_MISMATCH: "解析結果 manifest が不整合です",
        DEGRADE_REASON_SCHEMA_MISMATCH: "解析結果 schema が非互換です",
        DEGRADE_REASON_RESULT_DB_MISSING: "解析結果 DB が見つかりません",
        DEGRADE_REASON_REGIME_ROW_CORRUPTION: "regime_daily の公開行数が不正です",
    }
    hard_block = reason in {
        DEGRADE_REASON_NO_PUBLISH,
        DEGRADE_REASON_POINTER_CORRUPTION,
        DEGRADE_REASON_MANIFEST_MISMATCH,
        DEGRADE_REASON_SCHEMA_MISMATCH,
        DEGRADE_REASON_RESULT_DB_MISSING,
    }
    return {
        "degraded": True,
        "degrade_reason": reason,
        "stale_message": messages.get(reason, "解析結果を利用できません"),
        "cta_suppressed": True,
        "show_candidates": not hard_block,
        "show_similar_cases": not hard_block,
        "show_state_evaluation": reason == DEGRADE_REASON_WARNING_STALE,
        "app_continues": True,
    }
