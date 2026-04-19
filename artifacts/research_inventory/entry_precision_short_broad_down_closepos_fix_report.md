# Entry Precision Short Broad Down ClosePos Audit

## Current State
- confirmed: monthly_breakout_down_min_was_partially_repaired, monthly_alignment_failed_is_direct_gate, long_logic_remained_frozen
- provisional: close_pos_may_only_be_a_partial_blocker, require_midrange_off_may_be_the_remaining_blocker_for_4684, regime_labels_are_proxy_derived

## Problem
The task is to validate whether the remaining missing broad-down short is still blocked by close_pos_max after the monthly breakout repair.

## Change Policy
TRADEX research only, close_pos_max ladder only, keep long logic frozen, keep MeeMee untouched, keep require_midrange_off frozen, and do not change monthly gates again.

## Concrete Changes
- frozen failure session: `entry-short-broad-down-monthlybreak-20260419-142728`
- ladder variants: `broad_down_closepos_0p25_v1, broad_down_closepos_0p30_v1`

## Verify
- baseline count: `10`
- frozen reference count: `8`
- long freeze confirmed: `True`
- profitable reentries by ladder: `{'broad_down_closepos_0p25_v1': 2, 'broad_down_closepos_0p30_v1': 2}`

## Decision
- overall: `drop`
- reasons: frozen_reference_count=8, best_variant=broad_down_closepos_0p25_v1, best_variant_count=8, best_variant_hit_rate=0.5, best_variant_median_ret20=-0.017756346160049025, best_variant_mean_ret20=-0.027910710374672465, profitable_reentered_0p25=2, profitable_reentered_0p30=2, close_pos_not_the_remaining_shared_blocker

## Remaining Risks
- close_pos_relaxation_did_not_prove_a_clean_keep
- sample_size_is_still_small_relative_to_the_full_history
- require_midrange_off_still_blocks_4684

## Next One Thing
If this lane continues, the next single axis would be require_midrange_off only if close_pos proves insufficient.
