from __future__ import annotations

from external_analysis.contracts.analysis_output import (
    AnalysisOutputContract,
    analysis_output_from_decision,
    analysis_output_from_result,
    build_override_state,
    build_publish_readiness,
)


def test_tradex_analysis_output_contract_maps_existing_decision_shape() -> None:
    decision = {
        "tone": "up",
        "patternLabel": "pattern-a",
        "environmentLabel": "environment-b",
        "version": "2026-03-20",
        "confidence": 0.72,
        "buyProb": 0.81,
        "neutralProb": 0.12,
        "sellProb": 0.07,
        "scenarios": [
            {"key": "up", "label": "buy", "tone": "up", "score": 0.81},
            {"key": "range", "label": "neutral", "tone": "neutral", "score": 0.12},
            {"key": "down", "label": "sell", "tone": "down", "score": 0.07},
        ],
    }
    output = analysis_output_from_decision(
        symbol="7203",
        asof="2026-03-19",
        decision=decision,
        publish_readiness=build_publish_readiness(
            ready=True,
            status="approved",
            reasons=("validation_ok",),
            candidate_key="logic_family_a:v2",
            approved=True,
        ),
        override_state=build_override_state(
            present=True,
            source="operator_override",
            logic_key="logic_family_a:v4",
            logic_version="v4",
            reason="operator pin",
        ),
    )

    assert isinstance(output, AnalysisOutputContract)
    assert output.symbol == "7203"
    assert output.asof == "2026-03-19"
    assert output.side_ratios.buy == 0.81
    assert output.side_ratios.neutral == 0.12
    assert output.side_ratios.sell == 0.07
    assert output.confidence == 0.72
    assert "tone=up" in output.reasons
    assert output.publish_readiness.ready is True
    assert output.publish_readiness.status == "approved"
    assert output.publish_readiness.approved is True
    assert output.override_state.present is True
    assert output.override_state.logic_key == "logic_family_a:v4"
    assert len(output.candidate_comparisons) == 3
    assert output.candidate_comparisons[0].candidate_key == "up"
    assert output.candidate_comparisons[0].comparison_scope == "decision_scenarios"
    assert output.candidate_comparisons[0].score == 0.81
    assert output.to_dict()["schema_version"] == "tradex_analysis_output_v1"


def test_tradex_analysis_output_contract_maps_existing_result_payload_shape() -> None:
    result_payload = {
        "symbol": "7203",
        "asof": "2026-03-19",
        "decision": {
            "tone": "up",
            "patternLabel": "pattern-a",
            "environmentLabel": "environment-b",
            "version": "2026-03-20",
            "confidence": 0.72,
            "buyProb": 0.81,
            "neutralProb": 0.12,
            "sellProb": 0.07,
            "scenarios": [
                {"key": "up", "label": "buy", "tone": "up", "score": 0.81},
                {"key": "range", "label": "neutral", "tone": "neutral", "score": 0.12},
                {"key": "down", "label": "sell", "tone": "down", "score": 0.07},
            ],
        },
        "candidate_comparisons": [
            {
                "candidate_key": "logic_family_a:v2",
                "baseline_key": "logic_family_a:v1",
                "comparison_scope": "external_result",
                "score": 0.74,
                "score_delta": 0.03,
                "rank": 1,
                "reasons": ["validation_ok"],
                "publish_ready": True,
            }
        ],
        "publish_readiness": {
            "ready": True,
            "status": "approved",
            "reasons": ["validation_ok"],
            "candidate_key": "logic_family_a:v2",
            "approved": True,
        },
        "override_state": {
            "present": True,
            "source": "operator_override",
            "logic_key": "logic_family_a:v4",
            "logic_version": "v4",
            "reason": "operator pin",
        },
    }

    output = analysis_output_from_result(result=result_payload)

    assert isinstance(output, AnalysisOutputContract)
    assert output.symbol == "7203"
    assert output.asof == "2026-03-19"
    assert output.side_ratios.buy == 0.81
    assert output.side_ratios.neutral == 0.12
    assert output.side_ratios.sell == 0.07
    assert output.confidence == 0.72
    assert output.publish_readiness.ready is True
    assert output.publish_readiness.status == "approved"
    assert output.override_state.present is True
    assert output.override_state.logic_key == "logic_family_a:v4"
    assert len(output.candidate_comparisons) == 1
    assert output.candidate_comparisons[0].candidate_key == "logic_family_a:v2"
    assert output.candidate_comparisons[0].comparison_scope == "external_result"
