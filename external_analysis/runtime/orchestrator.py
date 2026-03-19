from __future__ import annotations

from typing import Any

from app.backend.services.analysis.analysis_decision import build_analysis_decision

from external_analysis.contracts.analysis_input import AnalysisInputContract
from external_analysis.contracts.analysis_output import AnalysisOutputContract, analysis_output_from_result


def run_tradex_analysis(input_contract: AnalysisInputContract) -> AnalysisOutputContract:
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
    result_payload: dict[str, Any] = {
        "symbol": runtime_kwargs["symbol"],
        "asof": runtime_kwargs["asof"],
        "decision": decision,
    }
    if runtime_kwargs.get("scenarios"):
        result_payload["scenarios"] = list(runtime_kwargs["scenarios"])
    if input_contract.publish_readiness is not None:
        result_payload["publish_readiness"] = input_contract.publish_readiness
    if input_contract.override_state is not None:
        result_payload["override_state"] = input_contract.override_state
    return analysis_output_from_result(result=result_payload)
