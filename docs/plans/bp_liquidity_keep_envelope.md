# bp_liquidity_trap_penalty/v1 Keep Envelope

This note defines where the current liquidity keep still works and where it becomes fragile.

## Decision Layering

- Local decision: `hold`
- Local reason: `turnover_too_high`
- Authoritative gate: `keep`
- Gate reason: `positive_bad_pick_removal_with_completed_coverage`
- Override code: `coverage_restored_keep_with_turnover_warning`
- Research contract: `provisional_keep_with_soft_warning`

## Turnover Envelope

| Bucket | Range | Months | Mean turnover | Mean capture rate | Mean capture count |
| --- | --- | ---: | ---: | ---: | ---: |
| low_turnover | 0.0 - 0.2 | 2 | 0.090909 | 0.518939 | 6.00 |
| mid_turnover | 0.2 - 0.5 | 2 | 0.400641 | 0.552083 | 5.50 |
| high_turnover | 0.5+ | 2 | 0.769231 | 0.533333 | 4.00 |

## Liquidity Envelope

Quartiles are derived from `liquidity20d` across the current keep run samples.

| Bucket | Sample count | Mean liquidity20d | Mean shortRet20 | Mean confidence | Dominant failure reason |
| --- | ---: | ---: | ---: | ---: | --- |
| q1_low_liquidity | 525 | 240117.03 | 0.041459 | 0.647398 | data_missing |
| q2_mid_low_liquidity | 525 | 714140.57 | 0.046710 | 0.667995 | data_missing |
| q3_mid_high_liquidity | 525 | 1643810.24 | 0.056642 | 0.672241 | liquidity_fail |
| q4_high_liquidity | 525 | 28015913.77 | 0.050659 | 0.688331 | data_missing |

## Regime Envelope

| Regime | Sample count | Signal rate | Ready rate | Overall score | Dominant reason |
| --- | ---: | ---: | ---: | ---: | --- |
| up | 1350 | 0.0 | 0.0 | 0.218986 | version=2026-03-04-v2 |
| down | 240 | 0.0 | 0.0 | 0.229544 | missing_feature |
| flat | 510 | 0.0 | 0.0 | 0.230534 | version=2026-03-04-v2 |

## Top-K Bands

| Band | Status | Changed members | Uplift | Boundary gap |
| --- | --- | ---: | ---: | ---: |
| top5 | stable_positive_branching | 6 | 0.027392 | 0.026039 |
| top10 | stable_but_thin | 12 | 0.017542 | -0.000103 |
| top20 | not_instrumented_in_current_contract | current top_k is 5 | n/a | n/a |

## Keep-Valid Regions

- Mid and high turnover bands remain positive.
- All three regimes remain covered and worst-regime delta does not collapse.
- q3 mid-high liquidity is the strongest sample-level region.

## Keep-Risk Regions

- The top10 boundary is thin.
- q1 low-liquidity samples are the weakest sample-level region.
- High-turnover months remain the first place to watch.

## Blocker-Candidate Regions

- `turnover_proxy >= 0.60 and (top10_uplift <= 0 or worst_regime_delta < 0)`
- `turnover_proxy >= 0.60 and (evaluation_window_count < 3 or coverage_status.status_reasons is not empty)`
- `turnover_proxy >= 0.60 and bad_pick_removal <= 0`
- `turnover_proxy >= 0.60 and top5_boundary_score_gap <= 0`
