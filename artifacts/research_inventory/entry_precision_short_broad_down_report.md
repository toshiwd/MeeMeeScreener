# Entry Precision Short Broad Down Audit

## Current State
- confirmed: short_trend_alignment_v1_was_kept_frozen_from_the_prior_regime_pass, long_logic_remained_frozen, broad_down_regime_was_the_only_bucket_reviewed
- provisional: trend_alignment_is_proxy_based, broad_down_rows_remain_sample_thin_for_some_subsets, the_failure_may_be_a_mix_of_overfiltering_and_wrong_keeps

## Problem
The challenger is degrading quality inside broad_down_regime, which should be one of the most short-supportive environments. This pass isolates whether it is removing the wrong shorts, keeping the wrong shorts, or both.

## Change Policy
TRADEX research only, frozen challenger logic, single-bucket review for broad_down_regime only, same-condition comparison fixed, long logic frozen, and no new feature families or threshold changes.

## Concrete Changes
- focused regime: `broad_down_regime`
- baseline count: `10`
- challenger count: `5`
- helpful removals: `2`
- harmful removals: `3`

## Verify
- broad-down baseline hit rate: `0.5`
- broad-down challenger hit rate: `0.4`
- long freeze confirmed: `True`
- failure buckets reviewed: `overfiltered_valid_breakdown, removed_good_followthrough, kept_late_short_after_extension, kept_weak_continuation, removed_high_quality_short_too_early, selected_noise_inside_downtrend, no_material_difference`

## Decision
- overall: `drop`
- reasons: broad_down_baseline_count=10, broad_down_challenger_count=5, broad_down_hit_rate_delta=-0.09999999999999998, broad_down_median_ret20_delta=-0.02107860529626165, broad_down_mean_ret20_delta=-0.010144246007346223, helpful_removals=2, harmful_removals=3, unchanged_core_count=5, challenger_degrades_broad_down_quality

## Remaining Risks
- broad_down_failure_is_clear_but_the_classifier_is_still_proxy_based
- sample_size_is_small_relative_to_the_full_history
- future_passes_should_not_reprice_the_gate_without_more_context

## Next One Thing
If anything changes next, it should be a targeted fix for the specific broad_down failure pattern rather than a new global threshold retune.
