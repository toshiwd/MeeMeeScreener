from __future__ import annotations

from external_analysis.contracts.analysis_input import AnalysisInputContract
from external_analysis.contracts.analysis_output import AnalysisOutputContract, analysis_output_from_result, build_override_state, build_publish_readiness
from external_analysis.runtime import orchestrator as tradex_orchestrator
from external_analysis.runtime.orchestrator import run_tradex_analysis


def test_tradex_analysis_orchestrator_returns_typed_output_end_to_end() -> None:
    input_contract = AnalysisInputContract(
        symbol="7203",
        asof="2026-03-19",
        analysis_p_up=0.81,
        analysis_p_down=0.07,
        analysis_p_turn_up=0.54,
        analysis_p_turn_down=0.31,
        analysis_ev_net=0.012,
        playbook_up_score_bonus=0.02,
        playbook_down_score_bonus=0.01,
        additive_signals={"bonusEstimate": 0.03},
        sell_analysis={"trendDown": False},
        scenarios=(
            {"key": "up", "label": "buy", "tone": "up", "score": 0.81, "publish_ready": True},
            {"key": "range", "label": "neutral", "tone": "neutral", "score": 0.12, "publish_ready": False},
            {"key": "down", "label": "sell", "tone": "down", "score": 0.07, "publish_ready": False},
        ),
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

    output = run_tradex_analysis(input_contract)
    normalized = tradex_orchestrator.normalize_tradex_analysis_input(input_contract)
    decision_payload = tradex_orchestrator.build_tradex_analysis_payload(normalized)
    expected = analysis_output_from_result(
        result=decision_payload,
    )

    assert isinstance(output, AnalysisOutputContract)
    assert output.symbol == "7203"
    assert output.asof == "2026-03-19"
    assert output.side_ratios.buy == expected.side_ratios.buy
    assert output.side_ratios.neutral == expected.side_ratios.neutral
    assert output.side_ratios.sell == expected.side_ratios.sell
    assert output.confidence == expected.confidence
    assert output.reasons == expected.reasons
    assert output.publish_readiness.ready is True
    assert output.publish_readiness.status == "approved"
    assert output.publish_readiness.approved is True
    assert output.override_state.present is True
    assert output.override_state.logic_key == "logic_family_a:v4"
    assert [item.to_dict() for item in output.candidate_comparisons] == [item.to_dict() for item in expected.candidate_comparisons]
    assert output.to_dict() == expected.to_dict()


def test_tradex_analysis_orchestrator_mapping_stays_stable_for_same_input() -> None:
    input_contract = AnalysisInputContract(
        symbol="9432",
        asof="2026-03-19",
        analysis_p_up=0.61,
        analysis_p_down=0.25,
        analysis_p_turn_up=0.42,
        analysis_p_turn_down=0.23,
        analysis_ev_net=0.004,
        playbook_up_score_bonus=0.01,
        playbook_down_score_bonus=0.0,
        additive_signals={"bonusEstimate": 0.015},
        sell_analysis={"trendDown": True},
        scenarios=(
            {"key": "down", "label": "sell", "tone": "down", "score": 0.54},
            {"key": "up", "label": "buy", "tone": "up", "score": 0.39},
        ),
        publish_readiness=build_publish_readiness(
            ready=False,
            status="not_ready",
            reasons=("validation_missing",),
            candidate_key="logic_family_b:v1",
            approved=False,
        ),
        override_state=build_override_state(
            present=False,
        ),
    )

    first = run_tradex_analysis(input_contract)
    second = run_tradex_analysis(input_contract)

    assert first.to_dict() == second.to_dict()


def test_tradex_analysis_orchestrator_calls_normalization_adapter_assembler_in_order(monkeypatch) -> None:
    calls: list[str] = []

    def fake_normalize(input_contract: AnalysisInputContract):
        calls.append("normalize")
        return input_contract

    def fake_adapter(normalized_input):
        calls.append("adapter")
        return {
            "symbol": normalized_input.symbol,
            "asof": normalized_input.asof,
            "decision": {
                "tone": "neutral",
                "patternLabel": "pattern",
                "environmentLabel": "environment",
                "version": "2026-03-20",
                "confidence": 0.5,
                "buyProb": 0.4,
                "neutralProb": 0.3,
                "sellProb": 0.3,
                "scenarios": [{"key": "range", "label": "neutral", "tone": "neutral", "score": 0.3}],
            },
            "candidate_comparisons": [
                {
                    "candidate_key": "range",
                    "baseline_key": "neutral",
                    "comparison_scope": "decision_scenarios",
                    "score": 0.3,
                    "score_delta": 0.0,
                    "rank": 1,
                    "reasons": ["key=range"],
                    "publish_ready": False,
                }
            ],
            "publish_readiness": {"ready": True, "status": "approved", "reasons": ["validation_ok"]},
            "override_state": {"present": False},
        }

    def fake_assemble(*, result_payload):
        calls.append("assembler")
        assert result_payload["symbol"] == "7203"
        assert result_payload["decision"]["tone"] == "neutral"
        return analysis_output_from_result(result=result_payload)

    monkeypatch.setattr(tradex_orchestrator, "normalize_tradex_analysis_input", fake_normalize)
    monkeypatch.setattr(tradex_orchestrator, "build_tradex_analysis_payload", fake_adapter)
    monkeypatch.setattr(tradex_orchestrator, "assemble_tradex_analysis_output", fake_assemble)

    output = run_tradex_analysis(
        AnalysisInputContract(
            symbol="7203",
            asof="2026-03-19",
        )
    )

    assert calls == ["normalize", "adapter", "assembler"]
    assert output.symbol == "7203"
