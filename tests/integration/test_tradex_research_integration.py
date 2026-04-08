from __future__ import annotations

import pytest

from app.backend.services import tradex_experiment_service as service
from app.backend.services import tradex_research_contracts as contracts


def test_compare_payload_rejects_same_condition_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        service,
        "_build_champion_challenger_evaluation",
        lambda **kwargs: {
            "regime_tag": "trend_long",
            "artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
            "fallback_status": contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
            "promote_ready": True,
            "promote_reasons": ["passed"],
            "status_reasons": [],
        },
    )

    family = {
        "family_id": "family-a",
        "universe": ["1001", "1002"],
        "period": {"segments": [{"label": "core", "start_date": "2025-01-01", "end_date": "2025-01-31"}]},
    }
    baseline = {
        "plan_id": "baseline",
        "plan_version": "v1",
        "label": "baseline",
        "method_id": "baseline",
        "method_title": "baseline",
        "method_thesis": "test",
        "method_family": "baseline-family",
        "feature_family": "common_pattern",
        "effective_config": {"effective_parameters": {"top_k": 5, "cost_model": contracts.TRADEX_DEFAULT_COST_MODEL}},
        "metrics": {},
        "summary": {},
    }
    candidate = {
        "plan_id": "candidate-a",
        "plan_version": "v1",
        "label": "candidate",
        "method_id": "candidate-a",
        "method_title": "candidate",
        "method_thesis": "test",
        "method_family": "candidate-family",
        "feature_family": "common_pattern",
        "effective_config": {"effective_parameters": {"top_k": 6, "cost_model": contracts.TRADEX_DEFAULT_COST_MODEL}},
        "metrics": {},
        "summary": {},
    }

    with pytest.raises(ValueError, match="same-condition compare mismatch"):
        service._compare_payload(family, baseline, candidate)
