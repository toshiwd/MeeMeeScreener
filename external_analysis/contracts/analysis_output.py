from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable

ANALYSIS_OUTPUT_SCHEMA_VERSION = "tradex_analysis_output_v1"


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if out != out:  # NaN guard
        return None
    return out


def _to_text(value: Any, fallback: str = "") -> str:
    text = str(value or "").strip()
    return text or fallback


def _as_str_tuple(values: Iterable[Any] | None) -> tuple[str, ...]:
    if values is None:
        return ()
    items: list[str] = []
    for value in values:
        text = _to_text(value)
        if text:
            items.append(text)
    return tuple(items)


def _candidate_comparison_from_any(
    value: Any,
    *,
    default_scope: str = "decision_scenarios",
) -> AnalysisCandidateComparison:
    payload = value if isinstance(value, dict) else {}
    reasons = payload.get("reasons")
    return AnalysisCandidateComparison(
        candidate_key=_to_text(payload.get("candidate_key") or payload.get("key"), fallback="candidate"),
        baseline_key=_to_text(payload.get("baseline_key")) or None,
        comparison_scope=_to_text(payload.get("comparison_scope"), fallback=default_scope),
        score=_to_float(payload.get("score")),
        score_delta=_to_float(payload.get("score_delta")),
        rank=int(float(payload.get("rank"))) if payload.get("rank") is not None else None,
        reasons=_as_str_tuple(reasons),
        publish_ready=bool(payload.get("publish_ready")) if payload.get("publish_ready") is not None else None,
    )


@dataclass(frozen=True)
class AnalysisSideRatios:
    buy: float
    neutral: float
    sell: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AnalysisCandidateComparison:
    candidate_key: str
    baseline_key: str | None
    comparison_scope: str
    score: float | None
    score_delta: float | None
    rank: int | None
    reasons: tuple[str, ...] = ()
    publish_ready: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reasons"] = list(self.reasons)
        return payload


@dataclass(frozen=True)
class AnalysisPublishReadiness:
    ready: bool
    status: str
    reasons: tuple[str, ...] = ()
    candidate_key: str | None = None
    approved: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reasons"] = list(self.reasons)
        return payload


@dataclass(frozen=True)
class AnalysisOverrideState:
    present: bool
    source: str | None = None
    logic_key: str | None = None
    logic_version: str | None = None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AnalysisOutputContract:
    symbol: str
    asof: str
    side_ratios: AnalysisSideRatios
    confidence: float | None
    reasons: tuple[str, ...]
    candidate_comparisons: tuple[AnalysisCandidateComparison, ...]
    publish_readiness: AnalysisPublishReadiness
    override_state: AnalysisOverrideState
    source: str = "tradex_analysis"
    schema_version: str = ANALYSIS_OUTPUT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["side_ratios"] = self.side_ratios.to_dict()
        payload["candidate_comparisons"] = [item.to_dict() for item in self.candidate_comparisons]
        payload["publish_readiness"] = self.publish_readiness.to_dict()
        payload["override_state"] = self.override_state.to_dict()
        payload["reasons"] = list(self.reasons)
        return payload


def build_side_ratios(*, buy: Any, neutral: Any, sell: Any) -> AnalysisSideRatios:
    return AnalysisSideRatios(
        buy=_to_float(buy) or 0.0,
        neutral=_to_float(neutral) or 0.0,
        sell=_to_float(sell) or 0.0,
    )


def build_candidate_comparisons(
    scenarios: Iterable[dict[str, Any]] | None,
    *,
    comparison_scope: str,
    baseline_key: str | None = None,
) -> tuple[AnalysisCandidateComparison, ...]:
    rows: list[AnalysisCandidateComparison] = []
    if scenarios is None:
        return ()
    scenario_list = list(scenarios)
    if not scenario_list:
        return ()
    selected_score = _to_float(next((item.get("score") for item in scenario_list if bool(item.get("selected"))), None))
    if selected_score is None:
        selected_score = _to_float(scenario_list[0].get("score"))
    for index, scenario in enumerate(scenario_list, start=1):
        score = _to_float(scenario.get("score"))
        reasons = []
        if scenario.get("key") is not None:
            reasons.append(f"key={_to_text(scenario.get('key'))}")
        if scenario.get("label") is not None:
            reasons.append(f"label={_to_text(scenario.get('label'))}")
        if scenario.get("tone") is not None:
            reasons.append(f"tone={_to_text(scenario.get('tone'))}")
        rows.append(
            AnalysisCandidateComparison(
                candidate_key=_to_text(scenario.get("key"), fallback=f"candidate_{index}"),
                baseline_key=baseline_key,
                comparison_scope=comparison_scope,
                score=score,
                score_delta=(score - selected_score) if score is not None and selected_score is not None else None,
                rank=index,
                reasons=tuple(reasons),
                publish_ready=bool(scenario.get("publish_ready")) if scenario.get("publish_ready") is not None else None,
            )
        )
    return tuple(rows)


def build_publish_readiness(
    *,
    ready: bool,
    status: str,
    reasons: Iterable[Any] | None = None,
    candidate_key: str | None = None,
    approved: bool | None = None,
) -> AnalysisPublishReadiness:
    return AnalysisPublishReadiness(
        ready=bool(ready),
        status=_to_text(status, fallback="unknown"),
        reasons=_as_str_tuple(reasons),
        candidate_key=_to_text(candidate_key) or None,
        approved=approved,
    )


def build_override_state(
    *,
    present: bool,
    source: str | None = None,
    logic_key: str | None = None,
    logic_version: str | None = None,
    reason: str | None = None,
) -> AnalysisOverrideState:
    return AnalysisOverrideState(
        present=bool(present),
        source=_to_text(source) or None,
        logic_key=_to_text(logic_key) or None,
        logic_version=_to_text(logic_version) or None,
        reason=_to_text(reason) or None,
    )


def analysis_output_from_decision(
    *,
    symbol: str,
    asof: str,
    decision: dict[str, Any] | None,
    publish_readiness: AnalysisPublishReadiness | dict[str, Any] | None = None,
    override_state: AnalysisOverrideState | dict[str, Any] | None = None,
    candidate_comparisons: Iterable[AnalysisCandidateComparison] | None = None,
) -> AnalysisOutputContract:
    payload = decision or {}
    ratios = build_side_ratios(
        buy=payload.get("buyProb"),
        neutral=payload.get("neutralProb"),
        sell=payload.get("sellProb"),
    )
    if candidate_comparisons is None:
        candidate_comparisons = build_candidate_comparisons(
            payload.get("scenarios"),
            comparison_scope="decision_scenarios",
            baseline_key=_to_text(payload.get("tone")) or None,
        )
    if isinstance(publish_readiness, dict):
        publish_readiness = build_publish_readiness(
            ready=bool(publish_readiness.get("ready")),
            status=_to_text(publish_readiness.get("status"), fallback="unknown"),
            reasons=publish_readiness.get("reasons"),
            candidate_key=publish_readiness.get("candidate_key"),
            approved=publish_readiness.get("approved") if publish_readiness.get("approved") is not None else None,
        )
    elif publish_readiness is None:
        publish_readiness = build_publish_readiness(ready=False, status="not_evaluated")
    if isinstance(override_state, dict):
        override_state = build_override_state(
            present=bool(override_state.get("present")),
            source=override_state.get("source"),
            logic_key=override_state.get("logic_key"),
            logic_version=override_state.get("logic_version"),
            reason=override_state.get("reason"),
        )
    elif override_state is None:
        override_state = build_override_state(present=False)
    reasons = tuple(
        reason
        for reason in (
            f"tone={_to_text(payload.get('tone')) or 'unknown'}",
            f"pattern={_to_text(payload.get('patternLabel')) or 'unknown'}",
            f"environment={_to_text(payload.get('environmentLabel')) or 'unknown'}",
            f"version={_to_text(payload.get('version')) or ANALYSIS_OUTPUT_SCHEMA_VERSION}",
        )
        if reason
    )
    return AnalysisOutputContract(
        symbol=_to_text(symbol, fallback="unknown"),
        asof=_to_text(asof, fallback="unknown"),
        side_ratios=ratios,
        confidence=_to_float(payload.get("confidence")),
        reasons=reasons,
        candidate_comparisons=tuple(candidate_comparisons),
        publish_readiness=publish_readiness,
        override_state=override_state,
    )


def analysis_output_from_result(
    *,
    result: dict[str, Any] | None,
) -> AnalysisOutputContract:
    payload = dict(result or {})
    decision = payload.get("decision")
    if not isinstance(decision, dict):
        decision = payload
    decision = dict(decision)
    if isinstance(payload.get("scenarios"), list) and "scenarios" not in decision:
        decision["scenarios"] = list(payload.get("scenarios") or [])
    publish_readiness = payload.get("publish_readiness")
    override_state = payload.get("override_state")
    candidate_comparisons = payload.get("candidate_comparisons")
    if isinstance(candidate_comparisons, list):
        candidate_comparisons = tuple(
            _candidate_comparison_from_any(item, default_scope="external_result")
            for item in candidate_comparisons
        )
    else:
        candidate_comparisons = None
    return analysis_output_from_decision(
        symbol=_to_text(payload.get("symbol"), fallback=_to_text(decision.get("symbol"), fallback="unknown")),
        asof=_to_text(payload.get("asof"), fallback=_to_text(decision.get("asof"), fallback="unknown")),
        decision=decision,
        publish_readiness=publish_readiness if isinstance(publish_readiness, dict) or publish_readiness is None else publish_readiness,
        override_state=override_state if isinstance(override_state, dict) or override_state is None else override_state,
        candidate_comparisons=candidate_comparisons,
    )
