# Buy Judgment Revision R2: Regime Mismatch Gate

## Current State
- confirmed: one subtractive buy revision exists already, `buy_judgment_revision_r1_weak_liquidity_gate`.
- confirmed: R1 decision is `keep`.
- confirmed: baseline 20d metrics are hit rate `0.5413793103448276`, mean return `-0.00038369650105161995`, median return `0.004686550702916459`, bad-loss rate `0.1724137931034483`, `>=10%` count `35`, `>=20%` count `3`.
- confirmed: R1 20d metrics are hit rate `0.5514316012725344`, mean return `0.001662253842114863`, median return `0.005334449035092392`, bad-loss rate `0.1569459172852598`, `>=10%` count `26`, `>=20%` count `0`.
- confirmed: overwrite/live validation remains `verified_live`.
- confirmed: boundary-aware compare remains not executed.

## Problem
- R1 is a valid keep, but it removes all `>=20%` winners in this cohort.
- The next useful step should not be more liquidity tightening.
- The next single axis should target wrong market background so losses can be reduced without further broad upside suppression.

## Change Policy
- Scope: TRADEX only.
- Axis: one only, `buy_judgment_revision_r2_regime_mismatch_gate`.
- Non-scope: no liquidity + regime combination, no late-entry gate, no early-reversal gate, no overwrite/provenance implementation work, no boundary-aware compare, no model retraining, no MeeMee UI, no publish/promote changes.
- Boundary check: TRADEX owns this revision; MeeMee remains unchanged.
- Risk: the regime bucket is still proxy-based, and later rounds may still be needed for the remaining loss buckets.

## Concrete Instructions
- Baseline: current buy surface.
- Prior kept result: `buy_judgment_revision_r1_weak_liquidity_gate` remains intact and is only a reference path.
- Challenger: baseline plus one gate only.
- Gate definition: veto when `market_ret20 < -0.01` and `breadth_above_ma20 <= 0.50`.
- Gate purpose: reject buy judgments in the wrong market background, without adding broad bullish scoring.
- Produce exactly one challenger only.
- Evaluate three paths only: baseline, R1 weak-liquidity gate, R2 regime-mismatch gate.
- Report 5d / 10d / 20d hit rate, mean and median return, bad-loss rate, `>=10%` and `>=20%` counts, removed-event count, removed-bucket concentration, and before/after regime breakdown.
- Decision rule: keep if bad-loss rate improves versus baseline and `>=20%` upside is preserved better than in R1; hold if the downside is real but upside tradeoff is still unclear; drop if the gate does not concentrate regime-mismatch losses.

## Evidence
- baseline 20d mean return: -0.00038369650105161995
- baseline 20d median return: 0.004686550702916459
- baseline bad-loss rate: 0.1724137931034483
- R1 20d mean return: 0.001662253842114863
- R1 20d median return: 0.005334449035092392
- R1 bad-loss rate: 0.1569459172852598
- R2 20d mean return: 9.974919964107984e-05
- R2 20d median return: 0.004417831798196881
- R2 bad-loss rate: 0.16789667896678967
- R2 removed events: 76
- R2 removed regime mismatch events: 76
- R2 removed target share: 1.0
- R2 target enrichment: 15.26315789473684
- R2 `>=10%` count: 30
- R2 `>=20%` count: 3
- R2 kept regime breakdown: {'neutral': 733, 'risk_on': 351}
- R2 removed regime breakdown: {'neutral': 43, 'risk_off': 33}

## Decision
- Decision: keep
- Reason: the gate removes a pure regime-mismatch cluster, improves bad-loss rate versus baseline, and preserves the three `>=20%` winners that R1 removed.
- Residual risk: regime buckets are still proxy-based, so a later round may still need more precise background instrumentation.
