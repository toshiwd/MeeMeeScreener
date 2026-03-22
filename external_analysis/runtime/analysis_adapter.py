from __future__ import annotations

from app.backend.services.analysis.analysis_decision import compute_analysis_decision_core

from .input_normalization import NormalizedTradexAnalysisInput
from .decision_parts import (
    build_candidate_comparison_payloads,
    normalize_override_state_payload,
    normalize_publish_readiness_payload,
)
from .score_context import prepare_tradex_score_context
from .score_finalize import finalize_tradex_score_output


def build_tradex_analysis_payload(normalized_input: NormalizedTradexAnalysisInput) -> dict[str, object]:
    score_context = prepare_tradex_score_context(**normalized_input.decision_kwargs)
    core = compute_analysis_decision_core(score_context)
    decision = finalize_tradex_score_output(core)
    payload: dict[str, object] = {
        "symbol": normalized_input.symbol,
        "asof": normalized_input.asof,
        "decision": decision,
        "candidate_comparisons": build_candidate_comparison_payloads(
            decision.get("scenarios"),
            comparison_scope="decision_scenarios",
            baseline_key=str(decision.get("tone") or "").strip() or None,
        ),
    }
    publish_readiness = normalize_publish_readiness_payload(normalized_input.publish_readiness)
    if publish_readiness is not None:
        payload["publish_readiness"] = publish_readiness
    override_state = normalize_override_state_payload(normalized_input.override_state)
    if override_state is not None:
        payload["override_state"] = override_state
    if normalized_input.diagnostics is not None:
        payload["diagnostics"] = dict(normalized_input.diagnostics)
    return payload
