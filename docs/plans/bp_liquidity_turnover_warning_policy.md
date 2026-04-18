# Liquidity Turnover Warning Policy

- Candidate: `bp_liquidity_trap_penalty/v1`
- Current classification: `soft_warning_non_blocking`
- Research contract status: `provisional_keep_with_soft_warning`

## Blocker Promotion Rule

Promote `turnover_too_high` to blocker only when turnover coincides with a real loss of keep quality.

Typed blocker conditions:

- `turnover_proxy >= 0.60 and (top10_uplift <= 0 or worst_regime_delta < 0)`
- `turnover_proxy >= 0.60 and (evaluation_window_count < 3 or coverage_status.status_reasons is not empty)`
- `turnover_proxy >= 0.60 and bad_pick_removal <= 0`
- `turnover_proxy >= 0.60 and top5_boundary_score_gap <= 0`

## Non-Blocking Conditions

- `bad_pick_removal > 0`
- `top10_uplift > 0`
- `evaluation_window_count == 3`
- `coverage_status.status_reasons` is empty
- `worst_regime_delta >= 0`
- observed high-turnover months still retain positive capture

## Shadow Evaluation Result

- Current warning remains soft.
- Current keep stays authoritative keep.
- The blocker rule is typed, but it is not triggered by the current envelope.

## Decision Examples

- Current keep: local hold, authoritative keep, warning remains soft.
- Future blocker: local hold, but negative uplift or regime collapse promotes the warning to a blocker.
