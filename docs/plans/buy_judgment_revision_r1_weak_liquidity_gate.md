# Buy Judgment Revision R1: Weak Liquidity Gate

## Current State
- confirmed: the historical buy-judgment audit corpus exists with 1,162 events.
- confirmed: 5d and 10d horizons are complete for all 1,162 events; 20d is populated for 1,160 events.
- confirmed: the realized buy surface in the frozen replay cohort is monolithic `MA20_RECLAIM_INITIAL`.
- confirmed: the main loss bucket is `weak liquidity continuation`.
- confirmed: overwrite/live validation remains `verified_live`.
- confirmed: boundary-aware compare remains not executed.

## Problem
- The current buy surface is too broad to claim durable practical edge.
- The audit says the main issue is not lack of bullish triggers but insufficient gating of loss-making low-liquidity contexts.
- The first revision should be subtractive: remove weak liquidity reclaim cases before mixing in any other axis.

## Change Policy
- Scope: TRADEX only.
- Axis: one only, `weak_liquidity_continuation_gate`.
- Non-scope: no overwrite/provenance implementation work, no boundary-aware compare execution, no model retraining, no MeeMee UI, no publish/promote changes, no regime mismatch gate, no late/stretched entry gate.
- Boundary check: TRADEX owns this revision; MeeMee stays unchanged.
- Risk: the three 20%+ winners in the cohort are very low-liquidity names and are removed by any meaningful liquidity floor.

## Concrete Instructions
- Baseline: current realized `MA20_RECLAIM_INITIAL` buy surface.
- Challenger: baseline plus one gate only.
- Gate definition: `liquidity20d >= 438345.4875`.
- Threshold basis: weak liquidity continuation bucket Q1 on confirmed 20d-available audit rows.
- Produce one challenger only.
- Evaluate on the same confirmed-data audit contract.
- Report 5d / 10d / 20d hit rate, mean and median return, bad-loss rate, >=10% and >=20% counts, removed-event count, removed-bucket enrichment, and regime/liquidity breakdown.
- Decision rule: keep if bad-loss rate improves and mean/median do not worsen materially; hold if upside tradeoff is still too uncertain; drop if the gate does not concentrate losses.

## Evidence
- baseline 20d hit rate: 0.5413793103448276
- challenger 20d hit rate: 0.5514316012725344
- baseline 20d mean return: -0.00038369650105161995
- challenger 20d mean return: 0.001662253842114863
- baseline 20d median return: 0.004686550702916459
- challenger 20d median return: 0.005334449035092392
- baseline bad-loss rate: 0.1724137931034483
- challenger bad-loss rate: 0.1569459172852598
- removed events: 217
- removed weak liquidity continuation events: 107
- removed target bucket share: 0.4930875576036866
- removed target bucket enrichment: 1.3426797343198977
- 20d >= 10% count: 35 -> 26
- 20d >= 20% count: 3 -> 0

## Decision
- Decision: keep
- Reason: the gate is subtractive, audit-derived, and reduces the confirmed bad-loss rate while improving mean and median 20d returns.
- Residual risk: the rare large winners in this cohort are all very low liquidity and are sacrificed by the floor; later rounds may need a separate upside-preservation pass.
