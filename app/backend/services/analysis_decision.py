from __future__ import annotations

import math
from typing import Any

DECISION_VERSION = "2026-03-03-v1"


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return out


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def _clamp(value: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, value)))


def _first_finite(*values: Any) -> float | None:
    for value in values:
        out = _to_float(value)
        if out is not None:
            return out
    return None


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
    up_prob = _clamp(
        analysis_p_up if analysis_p_up is not None else (1.0 - analysis_p_down if analysis_p_down is not None else 0.5),
        0.0,
        1.0,
    )
    down_prob_raw = (
        analysis_p_down
        if analysis_p_down is not None
        else _first_finite(
            sell_analysis.get("pDown") if isinstance(sell_analysis, dict) else None,
            1.0 - up_prob,
            0.5,
        )
    )
    down_prob = _clamp(float(down_prob_raw or 0.5), 0.0, 1.0)
    turn_up = _clamp(_first_finite(analysis_p_turn_up, 0.5) or 0.5, 0.0, 1.0)
    sell_turn_down = _first_finite(sell_analysis.get("pTurnDown") if isinstance(sell_analysis, dict) else None)
    turn_down = _clamp(
        0.5 * (_first_finite(analysis_p_turn_down, 0.5) or 0.5)
        + 0.5 * (sell_turn_down if sell_turn_down is not None else (_first_finite(analysis_p_turn_down, 0.5) or 0.5)),
        0.0,
        1.0,
    )
    ev_bias = 0.0
    if analysis_ev_net is not None:
        ev_bias = _clamp(float(analysis_ev_net) / 0.06, -1.0, 1.0)
    additive_bonus = (
        _to_float(additive_signals.get("bonusEstimate"))
        if isinstance(additive_signals, dict)
        else None
    )
    additive_bias = _clamp((additive_bonus or 0.0) / 0.06, -1.0, 1.0) if additive_bonus is not None else 0.0
    up_playbook_bias = _clamp((_to_float(playbook_up_score_bonus) or 0.0) / 0.04, -0.35, 0.35)
    down_playbook_bias = _clamp((_to_float(playbook_down_score_bonus) or 0.0) / 0.04, -0.35, 0.35)

    trend_down = _to_bool((sell_analysis or {}).get("trendDown"))
    trend_down_strict = _to_bool((sell_analysis or {}).get("trendDownStrict"))
    trend_down_penalty = 0.08 if trend_down_strict else 0.04 if trend_down else 0.0
    trend_down_boost = 1.0 if trend_down_strict else 0.7 if trend_down else 0.3
    short_score = _first_finite((sell_analysis or {}).get("shortScore"), 70.0) or 70.0
    short_score_norm = _clamp((short_score - 70.0) / 90.0, 0.0, 1.0)
    bullish_structure = bool(
        (not trend_down)
        and (_first_finite((sell_analysis or {}).get("distMa20Signed"), 0.0) or 0.0) > 0
        and (_first_finite((sell_analysis or {}).get("ma20Slope"), 0.0) or 0.0) >= 0
        and (_first_finite((sell_analysis or {}).get("ma60Slope"), 0.0) or 0.0) >= 0
    )
    strong_up_context = bool(
        _to_bool((additive_signals or {}).get("trendUpStrict"))
        and (_first_finite((additive_signals or {}).get("monthlyBreakoutUpProb"), 0.0) or 0.0) >= 0.8
    )

    up_score = _clamp(
        0.5 * up_prob
        + 0.18 * turn_up
        + 0.17 * (0.5 + ev_bias * 0.5)
        + 0.15 * (0.5 + additive_bias * 0.5)
        - 0.06 * down_playbook_bias
        + 0.08 * up_playbook_bias
        - trend_down_penalty,
        0.0,
        1.0,
    )
    down_score = _clamp(
        0.45 * down_prob
        + 0.22 * turn_down
        + 0.18 * (0.5 - ev_bias * 0.5)
        + 0.1 * trend_down_boost
        + 0.08 * down_playbook_bias
        - 0.06 * up_playbook_bias
        + 0.05 * (0.5 - additive_bias * 0.5),
        0.0,
        1.0,
    )
    sell_signal_quality = _clamp(
        0.38 * down_prob
        + 0.22 * turn_down
        + 0.14 * _clamp((-(analysis_ev_net or 0.0) + 0.005) / 0.04, 0.0, 1.0)
        + 0.16 * (1.0 if trend_down_strict else 0.72 if trend_down else 0.2)
        + 0.1 * short_score_norm
        - 0.12 * (1.0 if bullish_structure else 0.0),
        0.0,
        1.0,
    )
    range_score = _clamp(
        0.4 * (1.0 - abs(up_prob - down_prob))
        + 0.3 * min(turn_up, turn_down)
        + 0.3 * (1.0 - abs(ev_bias)),
        0.0,
        1.0,
    )

    force_up_reclaim = bool(
        strong_up_context
        and _to_bool((additive_signals or {}).get("mtfStrongAligned"))
        and up_score >= down_score
        and turn_up >= turn_down - 0.10
    )
    force_down_confirm = bool(
        (trend_down_strict and down_prob >= 0.58 and turn_down >= 0.56 and (analysis_ev_net or 0.0) <= 0.0)
        or (down_prob >= 0.68 and turn_down >= 0.64 and (analysis_ev_net or 0.0) <= -0.01)
    )
    down_confirm = bool(
        trend_down
        or trend_down_strict
        or (down_prob - up_prob) >= 0.10
        or down_prob >= 0.62
    )
    down_threshold = 0.66 if strong_up_context else 0.56

    scenarios = [
        {"key": "up", "label": "上昇継続（押し目再開）", "tone": "up", "score": float(up_score)},
        {"key": "down", "label": "下落継続（戻り売り優位）", "tone": "down", "score": float(down_score)},
        {"key": "range", "label": "往復レンジ（上下振れ）", "tone": "neutral", "score": float(range_score)},
    ]
    scenarios.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    top = scenarios[0] if scenarios else None
    pre_surge_long = bool(
        (not trend_down_strict)
        and (
            _to_bool((additive_signals or {}).get("boxBottomAligned"))
            or (
                (_first_finite((additive_signals or {}).get("monthlyRangeProb"), 0.0) or 0.0) >= 0.6
                and (_first_finite((additive_signals or {}).get("monthlyRangePos"), 1.0) or 1.0) <= 0.45
            )
        )
        and turn_up >= turn_down - 0.08
        and up_score >= down_score - 0.04
    )
    pre_surge_short = bool(
        down_confirm
        and turn_down >= turn_up - 0.08
        and down_score >= up_score - 0.04
    )
    environment_label = "方向感拮抗"
    tone = "neutral"
    if force_up_reclaim and up_score >= 0.56:
        tone = "up"
        environment_label = "上昇優位"
    elif force_down_confirm:
        tone = "down"
        environment_label = "下落優位"
    elif top and top.get("key") == "up" and float(top.get("score") or 0.0) >= 0.56:
        tone = "up"
        environment_label = "上昇優位"
    elif (
        top
        and top.get("key") == "down"
        and float(top.get("score") or 0.0) >= down_threshold
        and down_confirm
        and sell_signal_quality >= 0.52
    ):
        tone = "down"
        environment_label = "下落優位"
    elif top and top.get("key") == "range" and float(top.get("score") or 0.0) >= 0.56:
        tone = "neutral"
        if pre_surge_long and not pre_surge_short:
            environment_label = "レンジ優位（先回り買い監視）"
        elif pre_surge_short and not pre_surge_long:
            environment_label = "レンジ優位（戻り売り監視）"
        else:
            environment_label = "レンジ優位"

    scenario_map = {str(item.get("key")): item for item in scenarios}
    buy_scenario = scenario_map.get("up")
    sell_scenario = scenario_map.get("down")
    neutral_scenario = scenario_map.get("range")
    selected = (
        buy_scenario
        if tone == "up"
        else sell_scenario
        if tone == "down"
        else neutral_scenario or (scenarios[0] if scenarios else None)
    )
    side_label = "買い" if tone == "up" else "売り" if tone == "down" else "中立"
    pattern_label = str(selected.get("label")) if isinstance(selected, dict) and selected.get("label") else "--"
    confidence = _first_finite((selected or {}).get("score"))
    buy_prob = _first_finite((buy_scenario or {}).get("score"))
    sell_prob = _first_finite((sell_scenario or {}).get("score"))
    neutral_prob = _first_finite((neutral_scenario or {}).get("score"))
    return {
        "tone": tone,
        "sideLabel": side_label,
        "patternLabel": pattern_label,
        "environmentLabel": environment_label,
        "confidence": confidence,
        "buyProb": buy_prob,
        "sellProb": sell_prob,
        "neutralProb": neutral_prob,
        "version": DECISION_VERSION,
        "scenarios": scenarios,
    }
