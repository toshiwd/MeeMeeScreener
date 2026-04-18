# Outcome-Only Branching Redesign

## Current Baseline
- The first compare-ready outcome-only baseline is closed as `drop/no_op`.
- Measured evidence: `top5_uplift = 0.0`, `top10_uplift = 0.0`, `bad_pick_removal = 0.0`, `changed_top10_members_count = 0`, `changed_top20_members_count = 0`.
- The compare-engine local reason was `topk_boundary_absent`.
- The authoritative gate decision was `drop` with `no_meaningful_branching_no_op`.

## One Challenger
- Candidate: `outcome_only_cutoff_margin_branch_v1`.
- Purpose: improve top-K branching near the cutoff.
- Scoring idea: add a small cutoff-margin pressure term derived only from outcome-only and market features so near-boundary items can swap membership.
- Historical buy/sell judgment stays excluded from core scoring.

## Compare Contract
- Same universe.
- Same period.
- Same top-K.
- Same regime.
- Same cost.
- Same artifact detail level.
- Leakage-safe out-of-sample evaluation.
- Time-block split, embargo, and no shuffle stay on.

## Success Gate
- `changed_top10_members_count > 0`.
- `top10_uplift >= 0`.
- `bad_pick_removal >= 0`.
- No obvious worst-regime collapse.
- No historical judgment dependence.
- If the gate is not met, the challenger is dropped.

## Explicit Non-Scope
- No broad family sweep.
- No boundary-aware compare execution.
- No MeeMee UI, publish, or promote_ready changes.
- No weak_recovery or breakout_failure redesign.
- No reintroduction of historical judgment into core scoring.
