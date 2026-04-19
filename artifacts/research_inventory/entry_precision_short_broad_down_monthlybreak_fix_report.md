# Entry Precision Short Broad Down MonthlyBreak Audit

## Current State
- confirmed: monthly_alignment_failed_was_confirmed_direct, monthly_range_prob_hypothesis_was_rejected, long_logic_remained_frozen
- provisional: monthly_breakout_down_min_may_be_too_strict, ladder_needed_to_find_minimal_relaxation, regime_labels_are_proxy_derived

## Problem
The task is to validate whether the frozen monthly breakout threshold is too strict for broad-down followthrough shorts, using a minimal relaxation ladder on the same frozen reference.

## Change Policy
TRADEX research only, monthly_breakout_down_min ladder only, keep long logic frozen, keep MeeMee untouched, keep all other gates frozen, and do not review other failure regimes.

## Concrete Changes
- frozen failure session: `entry-short-broad-down-alignmentpath-20260419-141647`
- ladder variants: `broad_down_monthlybreak_0p60_v1, broad_down_monthlybreak_0p45_v1, broad_down_monthlybreak_0p30_v1`

## Verify
- baseline count: `10`
- frozen reference count: `6`
- long freeze confirmed: `True`
- profitable reentries by ladder: `{'broad_down_monthlybreak_0p60_v1': 1, 'broad_down_monthlybreak_0p45_v1': 1, 'broad_down_monthlybreak_0p30_v1': 2}`

## Decision
- overall: `hold`
- reasons: frozen_reference_count=6, best_variant=broad_down_monthlybreak_0p30_v1, best_variant_count=8, best_variant_hit_rate=0.5, best_variant_median_ret20=-0.017756346160049025, best_variant_mean_ret20=-0.027910710374672465, profitable_reentered_0p60=1, profitable_reentered_0p45=1, profitable_reentered_0p30=2, partial_repair_only

## Remaining Risks
- monthly_breakout_relaxation_did_not_yet_prove_a_clean_keep
- sample_size_is_still_small_relative_to_the_full_history
- close_pos_and_midrange_remain_blockers_for_4684

## Next One Thing
If this lane continues, the next axis would be close_pos_max only if the ladder proves monthly breakout is not the main limiter.
