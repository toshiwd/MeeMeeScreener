# TRADEX Buy Judgment Revision R4 - Reclaim Quality Gate

## Current State
- confirmed: `buy_judgment_revision_r1_weak_liquidity_gate` remains a kept prior result.
- confirmed: `buy_judgment_revision_r2_regime_mismatch_gate` remains a kept prior result.
- confirmed: `buy_judgment_revision_r3_liquidity_plus_regime_gate` is `drop` and is being treated as a closed prior round.
- confirmed: `bp_liquidity_trap_penalty/v1` remains the active deep-dive.
- confirmed: overwrite/live status remains `verified_live`.
- confirmed: `boundary_aware_compare_executed = false`.
- confirmed baseline 20d metrics remain the same fixed comparison contract for this round.

## Problem
- R1 and R2 are both useful individually, but R3 showed that combining good gates naively over-prunes.
- The remaining buy-side weakness is a cluster of weak reclaim contexts, especially `early reversal / failed breakdown recovery` and `MA recovery`.
- The next useful step is a single subtractive reclaim-quality gate, not another combination test and not another liquidity or regime retune.

## Change Policy
- TRADEX only.
- One axis only: `buy_judgment_revision_r4_reclaim_quality_gate`.
- Keep R1 and R2 as prior kept references.
- Keep R3 marked as dropped and closed.
- No boundary-aware compare execution.
- No overwrite/provenance implementation work.
- No model retraining.
- No MeeMee UI / publish / promote_ready changes.
- Use confirmed-data-only, judgment-time-only inputs.

## Concrete Instructions
- Baseline: `MA20_RECLAIM_INITIAL`.
- Challenger: baseline plus one reclaim-quality veto only.
- Gate features: `diff20_pct`, `weekly_breakout_up_prob`, `monthly_breakout_up_prob`.
- Gate logic: veto when `diff20_pct <= 0.015` and `weekly_breakout_up_prob <= 0.40` and `monthly_breakout_up_prob <= 0.40`.
- Target loss buckets: `early reversal / failed breakdown recovery`, `MA recovery`.
- Required artifacts: [`buy_judgment_revision_r4_reclaim_quality_gate.json`](C:\work\meemee-screener\artifacts\research_inventory\buy_judgment_revision_r4_reclaim_quality_gate.json), [`buy_judgment_revision_r4_gate_decision.json`](C:\work\meemee-screener\artifacts\research_inventory\buy_judgment_revision_r4_gate_decision.json).
- Decision rule: keep if bad-loss improves versus baseline, upside is preserved better than R1, and the removed rows are concentrated in the weak reclaim cluster.
- Explicitly not changing: liquidity gate, regime gate, late-entry gate, breakout_failure, weak_recovery, or any broad bullish score expansion.

## Evidence Basis
- confirmed-data-only threshold search favored a 3-feature reclaim gate that removes only the `very_weak_reclaim` tier.
- removed rows are concentrated in the weak reclaim cluster and are not a broad branch-wide prune.
- the gate preserves all three `>=20%` winners in the confirmed audit cohort.
- this round treats `reclaim_quality_bucket` as audit-only and provisional.

## Verify
- JSON load validation for the new plan and decision artifacts.
- confirm exactly one new challenger exists.
- confirm 5d / 10d / 20d metrics are populated.
- confirm removed events are measurable and bucket-tagged.
- confirm no MeeMee UI files are modified.
- confirm no overwrite/provenance regression is introduced.
- confirm `boundary_aware_compare_executed = false` remains intact.
