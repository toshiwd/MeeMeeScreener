# Buy Judgment Revision R3: Liquidity Plus Regime Gate

## Current State
- confirmed: `buy_judgment_revision_r1_weak_liquidity_gate` remains a kept prior result.
- confirmed: `buy_judgment_revision_r2_regime_mismatch_gate` remains a kept prior result.
- confirmed: baseline 20d bad-loss rate is `0.1724137931034483`.
- confirmed: R1 20d bad-loss rate is `0.1569459172852598`.
- confirmed: R2 20d bad-loss rate is `0.16789667896678967`.
- confirmed: overwrite/live status remains `verified_live`.
- confirmed: `boundary_aware_compare_executed = false`.

## Problem
- R1 and R2 are both valid keep results, but they pull in different directions.
- R1 is stronger defensively but destroys the rare `>=20%` winners.
- R2 preserves the rare `>=20%` winners better, but is weaker than R1 on central tendency.
- The remaining question is whether combining them is genuine complementarity or just cumulative over-pruning.

## Change Policy
- Scope: TRADEX only.
- Axis: one only, combined-gate validation.
- Non-scope: no third loss-axis, no threshold retuning, no late-entry gate, no early-reversal gate, no weak_recovery redesign, no breakout_failure redesign, no overwrite/provenance implementation work, no boundary-aware compare execution, no model retraining, no MeeMee UI, no publish/promote changes.
- Boundary check: TRADEX owns this combined validation; MeeMee stays unchanged.
- Risk: the combined gate may over-prune and destroy too much upside.

## Concrete Instructions
- Baseline: current `MA20_RECLAIM_INITIAL` buy surface.
- Prior kept results: R1 weak-liquidity gate and R2 regime-mismatch gate.
- Challenger: baseline plus both kept gates, with no retuning.
- Evaluate exactly four paths only: baseline, R1, R2, and R3 combined.
- R3 combined rule: keep only rows that pass both inherited gates.
- Report 5d / 10d / 20d hit rate, mean return, median return, bad-loss rate, `>=10%` count, `>=20%` count, removed-event count, overlap of removed events between R1 and R2, incremental removed events unique to R3, and before/after liquidity and regime breakdown.
- Decision rule: keep only if the combination improves bad-loss rate while preserving upside better than the single gates; hold if the trade-off is mixed; drop if the combination is mostly cumulative over-pruning.

## Evidence
- baseline 20d mean return: `-0.00038369650105161995`
- baseline 20d median return: `0.004686550702916459`
- baseline bad-loss rate: `0.1724137931034483`
- R1 20d mean return: `0.001662253842114863`
- R1 20d median return: `0.005334449035092392`
- R1 bad-loss rate: `0.1569459172852598`
- R2 20d mean return: `9.974919964107984e-05`
- R2 20d median return: `0.004417831798196881`
- R2 bad-loss rate: `0.16789667896678967`
- R3 20d mean return: `0.0009127375766411659`
- R3 20d median return: `0.004581026913533126`
- R3 bad-loss rate: `0.15830546265328874`
- R3 removed events: `263`
- removed by R1 only: `187`
- removed by R2 only: `46`
- removed by both: `30`
- R3 `>=10%` count: `21`
- R3 `>=20%` count: `0`

## Decision
- Decision: drop
- Reason: the combined gate is mostly cumulative over-pruning. It removes `263` events, but still destroys all `>=20%` winners and does not beat R1 on mean or median 20d return.
- Residual risk: the separate gates remain valid individually, but their combination is not a better net buy surface in this cohort.
