from __future__ import annotations

from external_analysis.contracts.analysis_input import ANALYSIS_INPUT_SCHEMA_VERSION, AnalysisInputContract
from external_analysis.contracts.analysis_output import build_override_state, build_publish_readiness
from external_analysis.runtime.input_normalization import normalize_tradex_analysis_input


def test_tradex_analysis_input_contract_serializes_stably() -> None:
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
            {"key": "up", "label": "buy", "tone": "up", "score": 0.81},
            {"key": "down", "label": "sell", "tone": "down", "score": 0.07},
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

    payload = input_contract.to_dict()
    runtime_kwargs = input_contract.to_runtime_kwargs()

    assert input_contract.schema_version == ANALYSIS_INPUT_SCHEMA_VERSION
    assert payload["schema_version"] == ANALYSIS_INPUT_SCHEMA_VERSION
    assert payload["symbol"] == "7203"
    assert payload["asof"] == "2026-03-19"
    assert payload["scenarios"][0]["key"] == "up"
    assert payload["publish_readiness"]["status"] == "approved"
    assert payload["override_state"]["logic_key"] == "logic_family_a:v4"
    assert runtime_kwargs["symbol"] == "7203"
    assert runtime_kwargs["asof"] == "2026-03-19"
    assert runtime_kwargs["playbook_up_score_bonus"] == 0.02
    assert runtime_kwargs["playbook_down_score_bonus"] == 0.01
    assert runtime_kwargs["scenarios"][1]["key"] == "down"


def test_tradex_analysis_input_contract_normalizes_missing_playbook_bonus_to_zero() -> None:
    omitted = AnalysisInputContract(symbol="7203", asof="2026-03-19")
    explicit_zero = AnalysisInputContract(symbol="7203", asof="2026-03-19", playbook_up_score_bonus=0.0)

    omitted_kwargs = normalize_tradex_analysis_input(omitted).decision_kwargs
    zero_kwargs = normalize_tradex_analysis_input(explicit_zero).decision_kwargs

    assert omitted_kwargs["playbook_up_score_bonus"] == 0.0
    assert zero_kwargs["playbook_up_score_bonus"] == 0.0
    assert omitted_kwargs == zero_kwargs
