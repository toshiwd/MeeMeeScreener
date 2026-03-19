from __future__ import annotations

from typing import Any

from external_analysis.runtime.score_context import prepare_tradex_score_context
from external_analysis.runtime.score_axes import (
    score_ev_upside_downside,
    score_short_bias_penalty,
    score_trend_direction,
    score_turning_momentum,
)
from external_analysis.runtime.score_aggregate import aggregate_tradex_score_decision
from external_analysis.runtime.score_finalize import finalize_tradex_score_output


def compute_analysis_decision_core(score_context: dict[str, Any]) -> dict[str, Any]:
    trend_axis = score_trend_direction(score_context)
    turning_axis = score_turning_momentum(score_context)
    ev_axis = score_ev_upside_downside(score_context)
    short_axis = score_short_bias_penalty(score_context)
    return aggregate_tradex_score_decision(
        trend_axis=trend_axis,
        turning_axis=turning_axis,
        ev_axis=ev_axis,
        short_axis=short_axis,
    )


def build_analysis_decision(
    *,
    analysis_p_up: float | None,
    analysis_p_down: float | None,
    analysis_p_turn_up: float | None,
    analysis_p_turn_down: float | None,
    analysis_ev_net: float | None,
    playbook_up_score_bonus: float | None,
    playbook_down_score_bonus: float | None,
    additive_signals: dict[str, Any] | None,
    sell_analysis: dict[str, Any] | None,
) -> dict[str, Any]:
    score_context = prepare_tradex_score_context(
        analysis_p_up=analysis_p_up,
        analysis_p_down=analysis_p_down,
        analysis_p_turn_up=analysis_p_turn_up,
        analysis_p_turn_down=analysis_p_turn_down,
        analysis_ev_net=analysis_ev_net,
        playbook_up_score_bonus=playbook_up_score_bonus,
        playbook_down_score_bonus=playbook_down_score_bonus,
        additive_signals=additive_signals,
        sell_analysis=sell_analysis,
    )
    core = compute_analysis_decision_core(score_context)
    return finalize_tradex_score_output(core)
