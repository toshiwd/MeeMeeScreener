from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable

from external_analysis.runtime.decision_parts import (
    build_candidate_comparison_payloads,
    normalize_candidate_comparison_payloads,
    normalize_override_state_payload,
    normalize_publish_readiness_payload,
)
from external_analysis.runtime.score_finalize import build_tradex_score_reasons

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
    diagnostics: dict[str, Any] | None = None
    source: str = "tradex_analysis"
    schema_version: str = ANALYSIS_OUTPUT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["side_ratios"] = self.side_ratios.to_dict()
        payload["candidate_comparisons"] = [item.to_dict() for item in self.candidate_comparisons]
        payload["publish_readiness"] = self.publish_readiness.to_dict()
        payload["override_state"] = self.override_state.to_dict()
        payload["reasons"] = list(self.reasons)
        if self.diagnostics is not None:
            payload["diagnostics"] = dict(self.diagnostics)
        else:
            payload.pop("diagnostics", None)
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
    payloads = build_candidate_comparison_payloads(
        scenarios,
        comparison_scope=comparison_scope,
        baseline_key=baseline_key,
    )
    return tuple(
        AnalysisCandidateComparison(
            candidate_key=_to_text(payload.get("candidate_key"), fallback="candidate"),
            baseline_key=_to_text(payload.get("baseline_key")) or None,
            comparison_scope=_to_text(payload.get("comparison_scope"), fallback=comparison_scope),
            score=_to_float(payload.get("score")),
            score_delta=_to_float(payload.get("score_delta")),
            rank=int(payload["rank"]) if payload.get("rank") is not None else None,
            reasons=_as_str_tuple(payload.get("reasons")),
            publish_ready=bool(payload.get("publish_ready")) if payload.get("publish_ready") is not None else None,
        )
        for payload in payloads
    )


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
    diagnostics: dict[str, Any] | None = None,
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
    publish_readiness_payload = normalize_publish_readiness_payload(publish_readiness)
    if publish_readiness_payload is not None:
        publish_readiness = build_publish_readiness(
            ready=bool(publish_readiness_payload.get("ready")),
            status=_to_text(publish_readiness_payload.get("status"), fallback="unknown"),
            reasons=publish_readiness_payload.get("reasons"),
            candidate_key=publish_readiness_payload.get("candidate_key"),
            approved=publish_readiness_payload.get("approved") if publish_readiness_payload.get("approved") is not None else None,
        )
    elif publish_readiness is None:
        publish_readiness = build_publish_readiness(ready=False, status="not_evaluated")
    override_state_payload = normalize_override_state_payload(override_state)
    if override_state_payload is not None:
        override_state = build_override_state(
            present=bool(override_state_payload.get("present")),
            source=override_state_payload.get("source"),
            logic_key=override_state_payload.get("logic_key"),
            logic_version=override_state_payload.get("logic_version"),
            reason=override_state_payload.get("reason"),
        )
    elif override_state is None:
        override_state = build_override_state(present=False)
    reasons = build_tradex_score_reasons(decision)
    return AnalysisOutputContract(
        symbol=_to_text(symbol, fallback="unknown"),
        asof=_to_text(asof, fallback="unknown"),
        side_ratios=ratios,
        confidence=_to_float(payload.get("confidence")),
        reasons=reasons,
        candidate_comparisons=tuple(candidate_comparisons),
        publish_readiness=publish_readiness,
        override_state=override_state,
        diagnostics=dict(diagnostics) if diagnostics is not None else None,
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
    diagnostics = payload.get("diagnostics")
    if diagnostics is not None and not isinstance(diagnostics, dict):
        diagnostics = None
    if isinstance(candidate_comparisons, list):
        candidate_comparisons = tuple(
            AnalysisCandidateComparison(
                candidate_key=_to_text(item.get("candidate_key"), fallback="candidate"),
                baseline_key=_to_text(item.get("baseline_key")) or None,
                comparison_scope=_to_text(item.get("comparison_scope"), fallback="external_result"),
                score=_to_float(item.get("score")),
                score_delta=_to_float(item.get("score_delta")),
                rank=int(item["rank"]) if item.get("rank") is not None else None,
                reasons=_as_str_tuple(item.get("reasons")),
                publish_ready=bool(item.get("publish_ready")) if item.get("publish_ready") is not None else None,
            )
            for item in normalize_candidate_comparison_payloads(candidate_comparisons, default_scope="external_result")
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
        diagnostics=diagnostics,
    )
