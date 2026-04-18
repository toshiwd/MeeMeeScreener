# Next Candidate Design Round

## Constraint
- One axis per candidate.
- Same universe, same period, same top-K, same regime, same cost, same artifact detail level.
- No silent fallback.
- No MeeMee UI changes.

## Proposed Directions

| candidate_name | target_failure_bucket | feature_class | expected_to_move | must_not_change | acceptance_criteria | decision |
| --- | --- | --- | --- | --- | --- | --- |
| bad-pick-removal | liquidity_trap / weak_recovery / breakout_failure | bad_pick_removal | Lightweight penalty composite that pushes known loser patterns out of top-K | comparison contract and artifact detail level | `bad_pick_removal > 0`, `changed_top10_count > 0`, `top10_uplift >= 0`, no worst-regime collapse, overlap not excessive | execute first |
| boundary-aware | ranking_no_op | boundary_feature | Top5 / top10 cutoff replacements and near-boundary ordering | comparison contract and artifact detail level | `changed_top10_count >= 1`, positive `top10_uplift`, no worst-regime collapse | backlog |
| regime-differential-adjustment | regime_mismatch | regime_adjustment | Score only when the effect clearly changes by regime | comparison contract and artifact detail level | positive uplift in at least one comparable scope, explicit selection divergence reason, no collapse in worst regime | hold |

## Exclusions
- Symbol-specific adjustment is not first priority.
- Image-first ranking is not part of this round.
- Multi-axis sweeps are not allowed until the inventory and loss ledger are stable.
- `ranking_no_op` is a research-side pruning / branching diagnostic bucket, not a market phenotype bucket.

## Execution Order
- Run `bad-pick-removal` first.
- Keep `boundary-aware` as the second branch only after the bad-pick-removal run is comparable.
- Keep `regime-differential-adjustment` as a later branch unless the bad-pick-removal run fails to branch.

## Evidence Gap
- The current comparable session still looks overlap-heavy.
- The champion is not yet clearly beaten by a one-axis keep candidate.
- The next three directions should be treated as hold until they produce a measurable top-K swap.
- `bad-pick-prune` still needs a comparable run; hold must not be indefinite.
