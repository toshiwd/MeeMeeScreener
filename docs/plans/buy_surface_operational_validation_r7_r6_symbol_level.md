# TRADEX Buy Surface R7 - Symbol-Level Operational Validation for R6

## Current State
- confirmed: `buy_surface_operational_challenger_r6_r4_default` is the kept compare-ready challenger source.
- confirmed: the broad confirmed symbol universe contains 671 symbols and 23,600 `up` appearances.
- confirmed: boundary-aware compare remains not executed.
- confirmed: overwrite/live remains `verified_live`.

## Problem
- R6 proved real branching on the audit corpus, but symbol-level operational confidence was still unproven.
- This round checks whether that branch remains meaningful on the broader confirmed symbol universe.
- The broader universe branches, but the uplift is flat and worst-regime behavior regresses, so the question is validation rather than redesign.

## Change Policy
- TRADEX only.
- One axis only: symbol-level operational validation of the kept R6 challenger.
- No new gate family, no new threshold, no recombination, no policy reselection.
- Keep same evaluation contract: same universe, same period, same top-K, same regime, same cost, same artifact detail level.
- Non-scope: overwrite/provenance implementation, boundary-aware compare, model retraining, MeeMee UI changes.

## Concrete Instructions
- Validate `buy_surface_operational_challenger_r6_r4_default` against the current baseline path on the confirmed symbol universe.
- Compare unit: `symbol`.
- Required outputs: 5d / 10d / 20d hit rate, mean / median return, bad-loss rate, `>=10%` and `>=20%` counts, top5 / top10 / top20 branching, regime-wise deltas, and liquidity-quality delta.
- Decision policy: `keep` if symbol-level branching is real and the contract does not regress materially; `hold` if branching exists but uplift is flat or the worst regime regresses; `drop` if symbol-level validation weakens materially.
- What will not change: R1/R2/R3/R4/R5/R6 role split, active liquidity deep-dive, overwrite/live contract, boundary-aware backlog.
