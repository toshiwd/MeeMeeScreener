# TRADEX Buy Surface R11 - Operational Validation of R1 Defensive Challenger

## Current State
- confirmed: the selected default operational baseline remains `buy_judgment_revision_r4_reclaim_quality_gate`.
- confirmed: the source challenger under validation is `buy_judgment_revision_r1_weak_liquidity_gate`.
- confirmed: the direct compare was executed on the confirmed symbol-level universe with `compare_unit = symbol`.
- confirmed: compare engine local decision is `keep` and promote-ready is `true`.
- confirmed: `boundary_aware_compare_executed = false`.
- confirmed: `bp_liquidity_trap_penalty/v1` remains the active deep-dive.

## Problem
- R1 is the strongest audit-side defensive gate, but it still needs operational validation against the current baseline.
- The key question is whether its liquidity filtering improves real symbol-level selection without introducing an unacceptable downside trade-off.
- The compare contract is narrower than the audit contract, so the validation has to be read from symbol-level compare results, not from the earlier audit summary alone.

## Change Policy
- TRADEX only.
- One axis only: `buy_surface_operational_validation_r11_r1_defensive`.
- No new gate family.
- No new threshold.
- No gate recombination.
- No policy reselection.
- Preserve R1/R2/R3/R4/R5/R6/R7/R8/R9/R10 role assignments as fixed.
- No overwrite/provenance work unless a new regression appears.
- No boundary-aware compare execution.
- No model retraining.
- No MeeMee UI / publish / promote_ready changes.

## Concrete Instructions
- Validate the R1 defensive gate as a compare-ready operational lane.
- Compare the current baseline path against the R1-derived challenger.
- Use the confirmed symbol-level universe and same-condition contract.
- Record top-K branching, liquidity-quality delta, and regime-wise deltas explicitly.
- Keep the result machine-readable and decision-bearing.
- If top20 is not observable because the candidate symbol set is too small, mark that explicitly instead of inventing coverage.
