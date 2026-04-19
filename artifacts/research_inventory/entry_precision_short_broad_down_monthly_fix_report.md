# Entry Precision Short Broad Down Monthly Fix Audit

## Current State
- confirmed: short_trend_alignment_v1_was_dropped_for_the_broad_down_lane, long_logic_remained_frozen, the_broad_down_failure_was_already_diagnosed
- provisional: monthly_alignment_is_the_first_suspect, broad_down_sample_is_not_large, regime_labels_are_proxy_derived

## Problem
The broad-down failure may be partly caused by the monthly-alignment filter, but that needs direct ablation evidence before any larger redesign.

## Change Policy
TRADEX research only, monthly-alignment ablation only, keep long logic frozen, keep MeeMee untouched, keep the rest of the challenger structure frozen, and do not review other failure regimes.

## Concrete Changes
- frozen failure session: `entry-short-broad-down-20260419-130000`
- monthly variants tested: `broad_down_monthly_alignment_relaxed_v1, broad_down_monthly_alignment_off_v1`

## Verify
- frozen challenger hit rate: `0.4`
- relaxed reentries: `0`
- off reentries: `0`
- long freeze confirmed: `True`

## Decision
- overall: `drop`
- reasons: frozen_challenger_count=5, relaxed_count=5, off_count=5, profitable_reentered_relaxed=0, profitable_reentered_off=0, losing_kept_relaxed=3, losing_kept_off=3, monthly_alignment_not_primary_cause

## Remaining Risks
- monthly_alignment_ablation_did_not_demonstrate_recovery_of_the_removed_profitable_shorts
- sample_size_is_still_small_relative_to_the_full_history
- other_gates_still_block_the_removed_names

## Next One Thing
If the monthly gate is not the culprit, the next useful axis would be a single broader-down false-keep diagnostic rather than another threshold retune.
