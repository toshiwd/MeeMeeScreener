from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .analysis_output import AnalysisOverrideState, AnalysisPublishReadiness

ANALYSIS_INPUT_SCHEMA_VERSION = "tradex_analysis_input_v1"


def _to_text(value: Any, fallback: str = "") -> str:
    text = str(value or "").strip()
    return text or fallback


def _as_dict(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return dict(value)
    return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if out != out:
        return None
    return out


@dataclass(frozen=True)
class AnalysisInputContract:
    symbol: str
    asof: str
    analysis_p_up: float | None = None
    analysis_p_down: float | None = None
    analysis_p_turn_up: float | None = None
    analysis_p_turn_down: float | None = None
    analysis_ev_net: float | None = None
    playbook_up_score_bonus: float | None = None
    playbook_down_score_bonus: float | None = None
    additive_signals: dict[str, Any] | None = None
    sell_analysis: dict[str, Any] | None = None
    scenarios: tuple[dict[str, Any], ...] = ()
    publish_readiness: AnalysisPublishReadiness | dict[str, Any] | None = None
    override_state: AnalysisOverrideState | dict[str, Any] | None = None
    diagnostics: dict[str, Any] | None = None
    source: str = "tradex_analysis_input"
    schema_version: str = ANALYSIS_INPUT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["scenarios"] = [dict(item) for item in self.scenarios]
        if isinstance(self.publish_readiness, AnalysisPublishReadiness):
            payload["publish_readiness"] = self.publish_readiness.to_dict()
        else:
            payload["publish_readiness"] = _as_dict(self.publish_readiness)
        if isinstance(self.override_state, AnalysisOverrideState):
            payload["override_state"] = self.override_state.to_dict()
        else:
            payload["override_state"] = _as_dict(self.override_state)
        if self.diagnostics is not None:
            payload["diagnostics"] = dict(self.diagnostics)
        else:
            payload.pop("diagnostics", None)
        return payload

    def to_runtime_kwargs(self) -> dict[str, Any]:
        return {
            "symbol": _to_text(self.symbol, fallback="unknown"),
            "asof": _to_text(self.asof, fallback="unknown"),
            "analysis_p_up": self.analysis_p_up,
            "analysis_p_down": self.analysis_p_down,
            "analysis_p_turn_up": self.analysis_p_turn_up,
            "analysis_p_turn_down": self.analysis_p_turn_down,
            "analysis_ev_net": self.analysis_ev_net,
            "playbook_up_score_bonus": _to_float(self.playbook_up_score_bonus) or 0.0,
            "playbook_down_score_bonus": _to_float(self.playbook_down_score_bonus) or 0.0,
            "additive_signals": _as_dict(self.additive_signals),
            "sell_analysis": _as_dict(self.sell_analysis),
            "scenarios": [dict(item) for item in self.scenarios],
        }
