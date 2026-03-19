from __future__ import annotations

from app.backend.services.analysis.analysis_decision import build_analysis_decision
from external_analysis.contracts.analysis_input import AnalysisInputContract
from external_analysis.contracts.analysis_output import AnalysisOutputContract, analysis_output_from_result, build_override_state, build_publish_readiness
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
    runtime_kwargs = input_contract.to_runtime_kwargs()
    decision = build_analysis_decision(
        analysis_p_up=runtime_kwargs["analysis_p_up"],
        analysis_p_down=runtime_kwargs["analysis_p_down"],
        analysis_p_turn_up=runtime_kwargs["analysis_p_turn_up"],
        analysis_p_turn_down=runtime_kwargs["analysis_p_turn_down"],
        analysis_ev_net=runtime_kwargs["analysis_ev_net"],
        playbook_up_score_bonus=runtime_kwargs["playbook_up_score_bonus"],
        playbook_down_score_bonus=runtime_kwargs["playbook_down_score_bonus"],
        additive_signals=runtime_kwargs["additive_signals"],
        sell_analysis=runtime_kwargs["sell_analysis"],
    )
    expected = analysis_output_from_result(
        result={
            "symbol": "7203",
            "asof": "2026-03-19",
            "decision": decision,
            "publish_readiness": input_contract.publish_readiness,
            "override_state": input_contract.override_state,
        }
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
