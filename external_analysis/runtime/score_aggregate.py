from __future__ import annotations

from typing import Any


DECISION_VERSION = "2026-03-04-v2"


def _clamp(value: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, value)))


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        out = float(value)
    except Exception:
        return default
    if out != out:
        return default
    return out


def _scenario_set(*, up_score: float, down_score: float, range_score: float) -> list[dict[str, Any]]:
    scenarios = [
        {"key": "up", "label": "上昇継続（押し目再開）", "tone": "up", "score": float(up_score)},
        {"key": "down", "label": "下落継続（戻り売り優位）", "tone": "down", "score": float(down_score)},
        {"key": "range", "label": "往復レンジ（上下振れ）", "tone": "neutral", "score": float(range_score)},
    ]
    scenarios.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    return scenarios


def aggregate_tradex_score_decision(
    *,
    trend_axis: dict[str, Any],
    turning_axis: dict[str, Any],
    ev_axis: dict[str, Any],
    short_axis: dict[str, Any],
) -> dict[str, Any]:
    trend_axis = dict(trend_axis or {})
    turning_axis = dict(turning_axis or {})
    ev_axis = dict(ev_axis or {})
    short_axis = dict(short_axis or {})

    up_prob = _to_float(trend_axis.get("up_prob"), 0.5)
    down_prob = _to_float(trend_axis.get("down_prob"), 0.5)
    turn_up = _to_float(turning_axis.get("turn_up"), 0.5)
    turn_down = _to_float(turning_axis.get("turn_down"), 0.5)
    trend_down = bool(trend_axis.get("trend_down"))
    trend_down_strict = bool(trend_axis.get("trend_down_strict"))
    short_score_norm = _to_float(short_axis.get("short_score_norm"), 0.0)
    bullish_structure = bool(short_axis.get("bullish_structure"))
    short_signal_confirmed = bool(short_axis.get("short_signal_confirmed"))
    down_threshold = _to_float(trend_axis.get("down_threshold"), 0.58)
    analysis_ev_net = short_axis.get("analysis_ev_net")

    up_score = _clamp(
        _to_float(trend_axis.get("direction_up_component"), 0.0)
        + _to_float(turning_axis.get("turn_up_component"), 0.0)
        + _to_float(ev_axis.get("up_ev_component"), 0.0),
        0.0,
        1.0,
    )
    down_score = _clamp(
        _to_float(trend_axis.get("direction_down_component"), 0.0)
        + _to_float(turning_axis.get("turn_down_component"), 0.0)
        + _to_float(ev_axis.get("down_ev_component"), 0.0),
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
        _to_float(trend_axis.get("range_balance_component"), 0.0)
        + _to_float(turning_axis.get("range_turn_component"), 0.0)
        + _to_float(ev_axis.get("range_ev_component"), 0.0),
        0.0,
        1.0,
    )

    force_up_reclaim = bool(
        trend_axis.get("strong_up_signal")
        and up_score >= down_score
        and turn_up >= turn_down - 0.10
    )
    force_down_confirm = bool(
        (trend_down_strict and down_prob >= 0.58 and turn_down >= 0.56 and (analysis_ev_net or 0.0) <= 0.0)
        or (
            down_prob >= 0.70
            and turn_down >= 0.66
            and (analysis_ev_net or 0.0) <= -0.01
            and short_score_norm >= 0.34
        )
    )
    down_confirm = bool(
        trend_down
        or trend_down_strict
        or (((down_prob - up_prob) >= 0.10 or down_prob >= 0.62) and short_signal_confirmed)
    )

    scenarios = _scenario_set(up_score=up_score, down_score=down_score, range_score=range_score)
    top = scenarios[0] if scenarios else None
    pre_surge_long = bool(
        (not trend_down_strict)
        and (
            bool(trend_axis.get("box_bottom_aligned"))
            or (
                _to_float(trend_axis.get("monthly_range_prob"), 0.0) >= 0.6
                and _to_float(trend_axis.get("monthly_range_pos"), 1.0) <= 0.45
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
    confidence = _to_float((selected or {}).get("score"), 0.0)
    return {
        "tone": tone,
        "side_label": side_label,
        "pattern_label": pattern_label,
        "environment_label": environment_label,
        "confidence": confidence,
        "buy_prob": _to_float((buy_scenario or {}).get("score"), 0.0),
        "sell_prob": _to_float((sell_scenario or {}).get("score"), 0.0),
        "neutral_prob": _to_float((neutral_scenario or {}).get("score"), 0.0),
        "version": DECISION_VERSION,
        "scenarios": scenarios,
    }
