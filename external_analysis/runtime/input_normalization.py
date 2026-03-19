from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from external_analysis.contracts.analysis_input import AnalysisInputContract
from external_analysis.contracts.analysis_output import AnalysisOverrideState, AnalysisPublishReadiness


@dataclass(frozen=True)
class NormalizedTradexAnalysisInput:
    symbol: str
    asof: str
    decision_kwargs: dict[str, Any]
    publish_readiness: AnalysisPublishReadiness | dict[str, Any] | None
    override_state: AnalysisOverrideState | dict[str, Any] | None
    scenarios: tuple[dict[str, Any], ...]
    source: str


def normalize_tradex_analysis_input(input_contract: AnalysisInputContract) -> NormalizedTradexAnalysisInput:
    runtime_kwargs = input_contract.to_runtime_kwargs()
    return NormalizedTradexAnalysisInput(
        symbol=str(runtime_kwargs["symbol"]),
        asof=str(runtime_kwargs["asof"]),
        decision_kwargs={
            "analysis_p_up": runtime_kwargs["analysis_p_up"],
            "analysis_p_down": runtime_kwargs["analysis_p_down"],
            "analysis_p_turn_up": runtime_kwargs["analysis_p_turn_up"],
            "analysis_p_turn_down": runtime_kwargs["analysis_p_turn_down"],
            "analysis_ev_net": runtime_kwargs["analysis_ev_net"],
            "playbook_up_score_bonus": runtime_kwargs["playbook_up_score_bonus"],
            "playbook_down_score_bonus": runtime_kwargs["playbook_down_score_bonus"],
            "additive_signals": runtime_kwargs["additive_signals"],
            "sell_analysis": runtime_kwargs["sell_analysis"],
        },
        publish_readiness=input_contract.publish_readiness,
        override_state=input_contract.override_state,
        scenarios=tuple(dict(item) for item in runtime_kwargs["scenarios"]),
        source=str(input_contract.source),
    )
