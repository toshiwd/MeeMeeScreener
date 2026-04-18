# Bad Pick Removal Feature Notes

## Keep Candidate
- `bp_liquidity_trap_penalty/v1` stays authoritative `keep`.
- Compare engine local decision remains `hold` with reason `turnover_too_high`.
- The authoritative override is explicit: `coverage_restored_keep_with_turnover_warning`.

## Liquidity Deep Dive
- `liq_adv20_quality_penalty_v1` -> drop
- `liq_atr_turnover_penalty_v1` -> drop
- `liq_breakout_followthrough_penalty_v1` -> drop
- These branches are compare-ready, but each remained `topk_boundary_absent` / no-op in this round.

## Frozen Branches
- `bp_weak_recovery_penalty/v1` stays dropped and frozen.
- `bp_breakout_failure_penalty/v1` stays dropped and frozen.

## Interpretation
- The liquidity keep is currently stable enough to keep as the active branch.
- The lightweight liquidity sub-branches did not isolate a distinct causal driver yet.
- `boundary-aware` remains the next backlog axis, but it is not run in this round.
