from __future__ import annotations

import math
from typing import Any


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


def prepare_tradex_score_context(
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
    additive_signals = dict(additive_signals or {})
    sell_analysis = dict(sell_analysis or {})
    up_prob = _clamp(
        analysis_p_up if analysis_p_up is not None else (1.0 - analysis_p_down if analysis_p_down is not None else 0.5),
        0.0,
        1.0,
    )
    down_prob_raw = (
        analysis_p_down
        if analysis_p_down is not None
        else _first_finite(
            sell_analysis.get("pDown"),
            1.0 - up_prob,
            0.5,
        )
    )
    down_prob = _clamp(float(down_prob_raw or 0.5), 0.0, 1.0)
    turn_up = _clamp(_first_finite(analysis_p_turn_up, 0.5) or 0.5, 0.0, 1.0)
    sell_turn_down = _first_finite(sell_analysis.get("pTurnDown"))
    turn_down = _clamp(
        0.5 * (_first_finite(analysis_p_turn_down, 0.5) or 0.5)
        + 0.5 * (sell_turn_down if sell_turn_down is not None else (_first_finite(analysis_p_turn_down, 0.5) or 0.5)),
        0.0,
        1.0,
    )
    ev_bias = 0.0
    if analysis_ev_net is not None:
        ev_bias = _clamp(float(analysis_ev_net) / 0.06, -1.0, 1.0)
    additive_bonus = _to_float(additive_signals.get("bonusEstimate"))
    additive_bias = _clamp((additive_bonus or 0.0) / 0.06, -1.0, 1.0) if additive_bonus is not None else 0.0
    up_playbook_bias = _clamp((_to_float(playbook_up_score_bonus) or 0.0) / 0.04, -0.35, 0.35)
    down_playbook_bias = _clamp((_to_float(playbook_down_score_bonus) or 0.0) / 0.04, -0.35, 0.35)
    trend_down = _to_bool(sell_analysis.get("trendDown"))
    trend_down_strict = _to_bool(sell_analysis.get("trendDownStrict"))
    trend_down_penalty = 0.08 if trend_down_strict else 0.04 if trend_down else 0.0
    trend_down_boost = 1.0 if trend_down_strict else 0.7 if trend_down else 0.3
    short_score = _first_finite(sell_analysis.get("shortScore"))
    if short_score is None:
        a_score = _first_finite(sell_analysis.get("aScore"))
        b_score = _first_finite(sell_analysis.get("bScore"))
        if a_score is not None or b_score is not None:
            short_score = float(a_score or 0.0) + float(b_score or 0.0)
    if short_score is None:
        short_score = 70.0
    short_score_norm = _clamp((short_score - 70.0) / 90.0, 0.0, 1.0)
    bullish_structure = bool(
        (not trend_down)
        and (_first_finite(sell_analysis.get("distMa20Signed"), 0.0) or 0.0) > 0
        and (_first_finite(sell_analysis.get("ma20Slope"), 0.0) or 0.0) >= 0
        and (_first_finite(sell_analysis.get("ma60Slope"), 0.0) or 0.0) >= 0
    )
    short_signal_confirmed = bool(
        trend_down
        or trend_down_strict
        or short_score_norm >= 0.34
    )
    strong_up_context = bool(
        _to_bool(additive_signals.get("trendUpStrict"))
        and (_first_finite(additive_signals.get("monthlyBreakoutUpProb"), 0.0) or 0.0) >= 0.8
    )
    return {
        "analysis_ev_net": analysis_ev_net,
        "additive_signals": additive_signals,
        "sell_analysis": sell_analysis,
        "up_prob": up_prob,
        "down_prob": down_prob,
        "turn_up": turn_up,
        "turn_down": turn_down,
        "ev_bias": ev_bias,
        "additive_bias": additive_bias,
        "up_playbook_bias": up_playbook_bias,
        "down_playbook_bias": down_playbook_bias,
        "trend_down": trend_down,
        "trend_down_strict": trend_down_strict,
        "trend_down_penalty": trend_down_penalty,
        "trend_down_boost": trend_down_boost,
        "short_score": short_score,
        "short_score_norm": short_score_norm,
        "bullish_structure": bullish_structure,
        "short_signal_confirmed": short_signal_confirmed,
        "strong_up_context": strong_up_context,
    }
