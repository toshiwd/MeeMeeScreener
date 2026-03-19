from __future__ import annotations

from typing import Any

from .score_axes import AxisScore


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
    scenarios.sort(key=lambda item: (-float(item.get("score") or 0.0), str(item.get("key") or "")))
    return scenarios


def _axis_signal(axis: dict[str, Any] | AxisScore | None, name: str, default: Any = None) -> Any:
    if isinstance(axis, AxisScore):
        return axis.signal(name, default)
    payload = dict(axis or {})
    signals = payload.get("signals")
    if isinstance(signals, dict) and name in signals:
        return signals.get(name, default)
    if name in payload:
        return payload.get(name, default)
    return default


def _axis_component(axis: dict[str, Any] | AxisScore | None, name: str, default: float = 0.0) -> float:
    if isinstance(axis, AxisScore):
        return axis.component(name, default)
    payload = dict(axis or {})
    components = payload.get("components")
    if isinstance(components, dict) and name in components:
        return _to_float(components.get(name), default)
    if name in payload:
        return _to_float(payload.get(name), default)
    return default


def _rule_matches(rule: dict[str, Any], *, tone: str, context: dict[str, Any]) -> bool:
    if rule["tone"] != tone:
        return False
    return bool(rule["when"](context))


def _resolve_tone_and_environment(*, context: dict[str, Any], scenarios: list[dict[str, Any]]) -> tuple[str, str]:
    rule_table = (
        {
            "tone": "up",
            "when": lambda ctx: ctx["force_up_reclaim"] and ctx["up_score"] >= 0.56,
            "environment": "????",
        },
        {
            "tone": "down",
            "when": lambda ctx: ctx["force_down_confirm"],
            "environment": "????",
        },
        {
            "tone": "up",
            "when": lambda ctx: ctx["top_key"] == "up" and ctx["top_score"] >= 0.56,
            "environment": "????",
        },
        {
            "tone": "down",
            "when": (
                lambda ctx: (
                    ctx["top_key"] == "down"
                    and ctx["top_score"] >= ctx["down_threshold"]
                    and ctx["down_confirm"]
                    and ctx["sell_signal_quality"] >= 0.52
                )
            ),
            "environment": "????",
        },
        {
            "tone": "neutral",
            "when": lambda ctx: ctx["top_key"] == "range" and ctx["top_score"] >= 0.56,
            "environment": "?????",
        },
    )
    context = dict(context)
    context["top_key"] = str((scenarios[0].get("key") if scenarios else "") or "")
    context["top_score"] = float(scenarios[0].get("score") or 0.0) if scenarios else 0.0
    for rule in rule_table:
        if _rule_matches(rule, tone=rule["tone"], context=context):
            tone = rule["tone"]
            if tone == "neutral":
                if context.get("pre_surge_long") and not context.get("pre_surge_short"):
                    return tone, "??????????????"
                if context.get("pre_surge_short") and not context.get("pre_surge_long"):
                    return tone, "??????????????"
            return tone, rule["environment"]
    if context.get("pre_surge_long") and not context.get("pre_surge_short"):
        return "neutral", "??????????????"
    if context.get("pre_surge_short") and not context.get("pre_surge_long"):
        return "neutral", "??????????????"
    return "neutral", "?????"


def aggregate_tradex_score_decision(
    *,
    trend_axis: dict[str, Any] | AxisScore,
    turning_axis: dict[str, Any] | AxisScore,
    ev_axis: dict[str, Any] | AxisScore,
    short_axis: dict[str, Any] | AxisScore,
) -> dict[str, Any]:
    up_prob = _to_float(_axis_signal(trend_axis, "up_prob"), 0.5)
    down_prob = _to_float(_axis_signal(trend_axis, "down_prob"), 0.5)
    turn_up = _to_float(_axis_signal(turning_axis, "turn_up"), 0.5)
    turn_down = _to_float(_axis_signal(turning_axis, "turn_down"), 0.5)
    trend_down = bool(_axis_signal(trend_axis, "trend_down"))
    trend_down_strict = bool(_axis_signal(trend_axis, "trend_down_strict"))
    short_score_norm = _to_float(_axis_signal(short_axis, "short_score_norm"), 0.0)
    bullish_structure = bool(_axis_signal(short_axis, "bullish_structure"))
    short_signal_confirmed = bool(_axis_signal(short_axis, "short_signal_confirmed"))
    down_threshold = _to_float(_axis_signal(trend_axis, "down_threshold"), 0.58)
    analysis_ev_net = _axis_signal(short_axis, "analysis_ev_net")

    up_score = _clamp(
        _axis_component(trend_axis, "direction_up_component", 0.0)
        + _axis_component(turning_axis, "turn_up_component", 0.0)
        + _axis_component(ev_axis, "up_ev_component", 0.0),
        0.0,
        1.0,
    )
    down_score = _clamp(
        _axis_component(trend_axis, "direction_down_component", 0.0)
        + _axis_component(turning_axis, "turn_down_component", 0.0)
        + _axis_component(ev_axis, "down_ev_component", 0.0),
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
        _axis_component(trend_axis, "range_balance_component", 0.0)
        + _axis_component(turning_axis, "range_turn_component", 0.0)
        + _axis_component(ev_axis, "range_ev_component", 0.0),
        0.0,
        1.0,
    )

    force_up_reclaim = bool(
        bool(_axis_signal(trend_axis, "strong_up_signal"))
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
    pre_surge_long = bool(
        (not trend_down_strict)
        and (
            bool(_axis_signal(trend_axis, "box_bottom_aligned"))
            or (
                _to_float(_axis_signal(trend_axis, "monthly_range_prob"), 0.0) >= 0.6
                and _to_float(_axis_signal(trend_axis, "monthly_range_pos"), 1.0) <= 0.45
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
    tone, environment_label = _resolve_tone_and_environment(
        context={
            "force_up_reclaim": force_up_reclaim,
            "force_down_confirm": force_down_confirm,
            "down_confirm": down_confirm,
            "down_threshold": down_threshold,
            "sell_signal_quality": sell_signal_quality,
            "pre_surge_long": pre_surge_long,
            "pre_surge_short": pre_surge_short,
        },
        scenarios=scenarios,
    )
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
