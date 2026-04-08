from __future__ import annotations

import pytest

from app.backend.services import tradex_research_contracts as contracts


def _same_condition() -> dict[str, object]:
    return contracts.build_same_condition_contract(
        universe=["1001", "1002"],
        period_segments=[{"label": "core", "start_date": "2025-01-01", "end_date": "2025-01-31"}],
        top_k=5,
        regime="trend_long",
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        feature_family="common_pattern",
    ).to_dict()


def _candidate_row(*, feature_family: str | None = "common_pattern") -> dict[str, object]:
    row = {
        "plan_id": "candidate-a",
        "plan_version": "v1",
        "label": "Candidate A",
        "method_id": "candidate_a",
        "method_title": "Candidate A",
        "method_thesis": "test",
        "method_family": "family-a",
        "decision": "keep",
        "candidate_local_decision": "keep",
        "decision_reasons": [{"code": "top5", "status": "pass"}],
        "artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        "fallback_status": contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
        "victory_metrics": {metric: None for metric in contracts.TRADEX_VICTORY_METRICS},
        "long_horizon_regime_score": 0.1,
        "recent_adaptation_score": 0.2,
        "same_condition_contract": _same_condition(),
    }
    if feature_family is not None:
        row["feature_family"] = feature_family
    return row


def test_run_manifest_shape_and_validation() -> None:
    manifest = contracts.build_run_manifest(
        session_id="tradex-v11",
        seed=7,
        random_seed=7,
        input_artifacts=[{"name": "compare.json", "path": "tmp/compare.json"}],
        asof="2025-03-23",
        config={"mode": "smoke"},
        universe=["1001", "1002"],
        period={"start_date": "2025-01-01", "end_date": "2025-03-23"},
        horizon="20d",
        artifact_detail_level=contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
        fallback_status=contracts.TRADEX_FALLBACK_STATUS_RESEARCH,
        cost_model=contracts.TRADEX_DEFAULT_COST_MODEL,
    )

    contracts.validate_run_manifest(manifest)
    assert manifest["artifact_detail_level"] == contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE
    assert manifest["fallback_status"] == contracts.TRADEX_FALLBACK_STATUS_RESEARCH
    assert manifest["cost_model"]["mode"] == contracts.TRADEX_DEFAULT_COST_MODEL["mode"]


def test_compare_artifact_rejects_missing_feature_family() -> None:
    payload = {
        "schema_version": "tradex_experiment_compare_v1",
        "diagnostics_schema_version": "tradex_diagnostics_v1",
        "family_id": "family-a",
        "generated_at": "2025-03-23T00:00:00+09:00",
        "baseline_run_id": "baseline",
        "same_condition_contract": _same_condition(),
        "candidate_results": [
            _candidate_row(feature_family=None),
        ],
    }

    with pytest.raises(ValueError, match="feature_family invalid"):
        contracts.validate_compare_artifact(payload)


def test_compare_artifact_rejects_mixed_decision_aliases() -> None:
    payload = {
        "schema_version": "tradex_experiment_compare_v1",
        "diagnostics_schema_version": "tradex_diagnostics_v1",
        "family_id": "family-a",
        "generated_at": "2025-03-23T00:00:00+09:00",
        "baseline_run_id": "baseline",
        "same_condition_contract": _same_condition(),
        "candidate_results": [
            {
                **_candidate_row(),
                "decision": "drop",
                "candidate_local_decision": "keep",
            },
        ],
    }

    with pytest.raises(ValueError, match="must match candidate_local_decision"):
        contracts.validate_compare_artifact(payload)


def test_family_and_rollup_contracts_require_explicit_decisions() -> None:
    family_payload = {
        "schema_version": "tradex_experiment_family_v1",
        "session_meta": {"session_id": "s1"},
        "source_compare_path": "compare.json",
        "coverage_waterfall": {},
        "overview": {},
        "family_summary": [
            {
                "method_family": "family-a",
                "method_title": "family-a",
                "method_thesis": "test",
                "decision": "keep",
                "session_aggregate_decision": "keep",
                "decision_reasons": [{"code": "candidate_keep_present"}],
            }
        ],
        "candidate_rows": [
            {
                "method_family": "family-a",
                "method_title": "candidate-a",
                "method_thesis": "test",
                "decision": "keep",
                "candidate_local_decision": "keep",
                "method_signature_hash": "hash-a",
                "decision_reasons": [{"code": "top5", "status": "pass"}],
                "feature_family": "common_pattern",
                "artifact_detail_level": contracts.TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
                "fallback_status": contracts.TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
                "victory_metrics": {metric: None for metric in contracts.TRADEX_VICTORY_METRICS},
            }
        ],
        "authoritative_rollup_decision": "keep",
    }
    contracts.validate_family_leaderboard_artifact(family_payload)

    family_payload["candidate_rows"][0]["candidate_local_decision"] = "drop"
    with pytest.raises(ValueError, match="must match candidate_local_decision"):
        contracts.validate_family_leaderboard_artifact(family_payload)


def test_scope_rollup_contract_requires_explicit_session_decision() -> None:
    payload = {
        "schema_version": "tradex_scope_stability_rollup_v1",
        "overview": {},
        "session_rows": [
            {
                "session_scope_id": "scope-1",
                "decision": "keep",
                "session_aggregate_decision": "keep",
                "decision_reasons": [{"code": "candidate_keep_present"}],
            }
        ],
        "authoritative_rollup_decision": "keep",
    }

    contracts.validate_scope_rollup_artifact(payload)

    payload["session_rows"][0]["decision"] = "drop"
    with pytest.raises(ValueError, match="must match session_aggregate_decision"):
        contracts.validate_scope_rollup_artifact(payload)
