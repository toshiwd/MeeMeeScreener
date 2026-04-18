# Boundary-Aware Readiness

## Current State
- Active deep-dive: `bp_liquidity_trap_penalty/v1`
- Research contract: `provisional_keep_with_soft_warning`
- Turnover warning: `shadow_only` / `soft_warning_non_blocking`
- Top10 boundary: `thin`
- Top20 boundary: `not_instrumented`
- Boundary-aware compare run: `not_executed`

## Allowed
- Measure `top5/top10` cutoff replacement quality
- Measure boundary score gap and thickness
- Measure selection divergence only near the cutoff

## Not Allowed
- Compensate for unresolved liquidity warning issues
- Re-rank the whole top-K broadly
- Use boundary-aware as a substitute for top20 instrumentation
- Hide condition drift or upgrade readiness implicitly

## Measured Facts
- `top10_boundary_thickness_measure.status = thin`
- `top10_boundary_score_gap_measure.value = -0.00010290647860106833`
- `changed_top5_near_boundary_measure.value = 6`
- `changed_top10_near_boundary_measure.value = 12`
- `top20_boundary_thickness_measure.status = not_instrumented`
- `top20_boundary_thickness_measure.observability_state = contract_limited_not_observable`
- `top20_boundary_score_gap_measure.status = not_instrumented`
- `top20_boundary_score_gap_measure.observability_state = contract_limited_not_observable`
- `changed_top20_near_boundary_measure.status = not_instrumented`
- `changed_top20_near_boundary_measure.observability_state = contract_limited_not_observable`
- `top20_boundary_instrumentation_status.status = not_instrumented`
- `top20_boundary_instrumentation_status.observability_state = contract_limited_not_observable`
- `top20_boundary_instrumentation_requirement.status = not_instrumented`
- `top20_boundary_instrumentation_requirement.observability_state = contract_limited_not_observable`

## Readiness Result
- `readiness_decision = not_ready_for_execution`
- `blocking_reasons = top10_boundary_thin, top20_not_instrumented`
- `next_allowed_step = complete_top20_boundary_instrumentation`

## Decision Layering
- `compare_engine_local_decision = hold`
- `authoritative_candidate_gate = keep`
- `research_contract_status = provisional_keep_with_soft_warning`

## Interpretation
The current boundary measurements are sufficient to document the operating envelope, but they are not sufficient to execute boundary-aware fairly. Top10 is measurable and thin; top20 remains contract-limited and not observable in the current authoritative compare summary, so boundary-aware remains backlog-only.
