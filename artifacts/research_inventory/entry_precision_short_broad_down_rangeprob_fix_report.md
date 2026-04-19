# Entry Precision Short Broad Down RangeProb Audit

## Current State
- confirmed: short_trend_alignment_v1_was_dropped_for_the_broad_down_lane, long_logic_remained_frozen, monthly_alignment_was_rejected_as_the_main_cause
- provisional: monthly_range_prob_is_the_next_smallest_suspect, broad_down_sample_is_not_large, regime_labels_are_proxy_derived

## Problem
The broad-down failure may be caused by the monthly range-probability gate, but that needs isolated evidence before touching close-position or midrange logic.

## Change Policy
TRADEX research only, monthly_range_prob_max ablation only, keep long logic frozen, keep MeeMee untouched, keep close_pos_max and require_midrange_off frozen, and do not review other failure regimes.

## Concrete Changes
- frozen failure session: `entry-short-broad-down-monthly-fix-20260419-131348`
- monthly rangeprob variants tested: `broad_down_monthly_rangeprob_relaxed_v1, broad_down_monthly_rangeprob_off_v1`

## Verify
- frozen challenger hit rate: `0.4`
- relaxed profitable reentries: `0`
- off profitable reentries: `0`
- long freeze confirmed: `True`

## Decision
- overall: `drop`
- reasons: frozen_challenger_count=5, relaxed_count=6, off_count=6, profitable_reentered_relaxed=0, profitable_reentered_off=0, losing_kept_relaxed=3, losing_kept_off=3, monthly_range_prob_not_primary_cause

## Remaining Risks
- monthly_rangeprob_ablation_did_not_demonstrate_recovery_of_the_removed_profitable_shorts
- sample_size_is_still_small_relative_to_the_full_history
- close_pos_or_midrange_may_still_block_the_reentry_candidates

## Next One Thing
If the range-prob gate is not the culprit, the next single axis would be close_pos_max, not a broader redesign.
