from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any


@dataclass(frozen=True)
class AxisScore:
    """内部用の軸スコア要約。

    score は public contract ではなく、aggregator が各軸の相対強度を
    比較するための内部的なサマリ値としてだけ使う。
    signals / components / reasons は pure な補助情報で、typed public
    output にはそのまま露出しない。
    """
    score: float
    signals: tuple[tuple[str, Any], ...] = ()
    components: tuple[tuple[str, float], ...] = ()
    reasons: tuple[str, ...] = ()

    def signal(self, name: str, default: Any = None) -> Any:
        for key, value in self.signals:
            if key == name:
                return value
        return default

    def component(self, name: str, default: float = 0.0) -> float:
        for key, value in self.components:
            if key == name:
                return value
        return default

    def to_dict(self) -> dict[str, Any]:
        return {
            "score": float(self.score),
            "signals": {key: value for key, value in self.signals},
            "components": {key: float(value) for key, value in self.components},
            "reasons": list(self.reasons),
        }

    def __getitem__(self, key: str) -> Any:
        if key == "score":
            return self.score
        if key == "signals":
            return {k: v for k, v in self.signals}
        if key == "components":
            return {k: v for k, v in self.components}
        if key == "reasons":
            return list(self.reasons)
        value = self.signal(key, default=None)
        if value is not None:
            return value
        value = self.component(key, default=None)
        if value is not None:
            return value
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default


def _axis_score(
    *,
    score: float,
    signals: list[tuple[str, Any]] | tuple[tuple[str, Any], ...] = (),
    components: list[tuple[str, float]] | tuple[tuple[str, float], ...] = (),
    reasons: list[str] | tuple[str, ...] = (),
) -> AxisScore:
    return AxisScore(
        score=float(score),
        signals=tuple(signals),
        components=tuple((str(key), float(value)) for key, value in components),
        reasons=tuple(str(reason) for reason in reasons if str(reason).strip()),
    )


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


def score_trend_direction(score_context: dict[str, Any]) -> dict[str, Any]:
    ctx = dict(score_context or {})
    additive_signals = dict(ctx.get("additive_signals") or {})
    up_prob = _clamp(_to_float(ctx.get("up_prob")) or 0.5, 0.0, 1.0)
    down_prob = _clamp(_to_float(ctx.get("down_prob")) or 0.5, 0.0, 1.0)
    trend_down = _to_bool(ctx.get("trend_down"))
    trend_down_strict = _to_bool(ctx.get("trend_down_strict"))
    trend_down_penalty = _clamp(_to_float(ctx.get("trend_down_penalty")) or 0.0, 0.0, 1.0)
    trend_down_boost = _clamp(_to_float(ctx.get("trend_down_boost")) or 0.0, 0.0, 1.0)
    up_playbook_bias = _clamp(_to_float(ctx.get("up_playbook_bias")) or 0.0, -0.35, 0.35)
    down_playbook_bias = _clamp(_to_float(ctx.get("down_playbook_bias")) or 0.0, -0.35, 0.35)
    strong_up_context = _to_bool(ctx.get("strong_up_context"))
    down_threshold = 0.68 if strong_up_context else 0.58
    box_bottom_aligned = _to_bool(additive_signals.get("boxBottomAligned"))
    monthly_range_prob = _clamp(_to_float(additive_signals.get("monthlyRangeProb")) or 0.0, 0.0, 1.0)
    monthly_range_pos = _clamp(_to_float(additive_signals.get("monthlyRangePos")) or 1.0, 0.0, 1.0)
    direction_up_component = 0.5 * up_prob - 0.06 * down_playbook_bias + 0.08 * up_playbook_bias - trend_down_penalty
    direction_down_component = 0.45 * down_prob + 0.1 * trend_down_boost + 0.08 * down_playbook_bias - 0.06 * up_playbook_bias
    range_balance_component = 0.4 * (1.0 - abs(up_prob - down_prob))
    strong_up_signal = bool(strong_up_context and _to_bool(additive_signals.get("mtfStrongAligned")))
    score = max(direction_up_component, direction_down_component, range_balance_component)
    return _axis_score(
        score=score,
        signals=[
            ("up_prob", up_prob),
            ("down_prob", down_prob),
            ("trend_down", trend_down),
            ("trend_down_strict", trend_down_strict),
            ("trend_down_penalty", trend_down_penalty),
            ("trend_down_boost", trend_down_boost),
            ("up_playbook_bias", up_playbook_bias),
            ("down_playbook_bias", down_playbook_bias),
            ("strong_up_context", strong_up_context),
            ("down_threshold", down_threshold),
            ("box_bottom_aligned", box_bottom_aligned),
            ("monthly_range_prob", monthly_range_prob),
            ("monthly_range_pos", monthly_range_pos),
            ("strong_up_signal", strong_up_signal),
        ],
        components=[
            ("direction_up_component", direction_up_component),
            ("direction_down_component", direction_down_component),
            ("range_balance_component", range_balance_component),
        ],
        reasons=[
            "direction=up" if direction_up_component >= direction_down_component else "direction=down",
            "range=balanced" if range_balance_component >= 0.25 else "range=unbalanced",
        ],
    )


def score_turning_momentum(score_context: dict[str, Any]) -> dict[str, Any]:
    ctx = dict(score_context or {})
    turn_up = _clamp(_to_float(ctx.get("turn_up")) or 0.5, 0.0, 1.0)
    turn_down = _clamp(_to_float(ctx.get("turn_down")) or 0.5, 0.0, 1.0)
    turn_up_component = 0.18 * turn_up
    turn_down_component = 0.22 * turn_down
    range_turn_component = 0.3 * min(turn_up, turn_down)
    score = max(turn_up_component, turn_down_component, range_turn_component)
    return _axis_score(
        score=score,
        signals=[
            ("turn_up", turn_up),
            ("turn_down", turn_down),
        ],
        components=[
            ("turn_up_component", turn_up_component),
            ("turn_down_component", turn_down_component),
            ("range_turn_component", range_turn_component),
        ],
        reasons=[
            "momentum=up" if turn_up >= turn_down else "momentum=down",
        ],
    )


def score_ev_upside_downside(score_context: dict[str, Any]) -> dict[str, Any]:
    ctx = dict(score_context or {})
    analysis_ev_net = ctx.get("analysis_ev_net")
    ev_bias = _clamp(_to_float(ctx.get("ev_bias")) or 0.0, -1.0, 1.0)
    additive_bias = _clamp(_to_float(ctx.get("additive_bias")) or 0.0, -1.0, 1.0)
    up_ev_component = 0.17 * (0.5 + ev_bias * 0.5) + 0.15 * (0.5 + additive_bias * 0.5)
    down_ev_component = 0.18 * (0.5 - ev_bias * 0.5) + 0.05 * (0.5 - additive_bias * 0.5)
    range_ev_component = 0.3 * (1.0 - abs(ev_bias))
    score = max(up_ev_component, down_ev_component, range_ev_component)
    return _axis_score(
        score=score,
        signals=[
            ("analysis_ev_net", analysis_ev_net),
            ("ev_bias", ev_bias),
            ("additive_bias", additive_bias),
        ],
        components=[
            ("up_ev_component", up_ev_component),
            ("down_ev_component", down_ev_component),
            ("range_ev_component", range_ev_component),
        ],
        reasons=[
            "ev=up" if up_ev_component >= down_ev_component else "ev=down",
        ],
    )


def score_short_bias_penalty(score_context: dict[str, Any]) -> dict[str, Any]:
    ctx = dict(score_context or {})
    sell_analysis = dict(ctx.get("sell_analysis") or {})
    trend_down = _to_bool(ctx.get("trend_down"))
    trend_down_strict = _to_bool(ctx.get("trend_down_strict"))
    analysis_ev_net = ctx.get("analysis_ev_net")
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
    score = 1.0 - short_score_norm
    return _axis_score(
        score=score,
        signals=[
            ("analysis_ev_net", analysis_ev_net),
            ("short_score", short_score),
            ("short_score_norm", short_score_norm),
            ("bullish_structure", bullish_structure),
            ("short_signal_confirmed", short_signal_confirmed),
        ],
        components=[
            ("short_penalty_component", short_score_norm),
        ],
        reasons=[
            "short=confirmed" if short_signal_confirmed else "short=unconfirmed",
            "structure=bullish" if bullish_structure else "structure=neutral",
        ],
    )
