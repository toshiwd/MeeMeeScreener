# Entry Precision Short Audit

## Current State
- confirmed: entry_precision_short_audit_completed, baseline_short_rows_are_replayed_from_snapshot_db, short_side_was_evaluated_separately
- provisional: borrow_cost_is_proxy_only, false_neutral_recovery_is_subset_based, monthly_regime_stability_is_partial

## Problem
Short candidates still include weak closes and mixed followthrough; the goal is fewer but more actionable shorts, not broader coverage.

## Change Policy
TRADEX only, short-side stage-A cleanup only, long logic frozen, same window and artifact detail level fixed, no multi-axis redesign.

## Concrete Changes
- baseline short selected count: `27`
- followthrough selected count: `17`
- late extension selected count: `10`
- bottom risk selected count: `14`

## Verify
- short baseline hit rate: `0.4444444444444444`
- short baseline median ret20: `-0.007709604882749759`
- long freeze confirmed: `True`

## Decision
- short_cleanup_followthrough_v1: `keep`
- short_cleanup_late_extension_v1: `hold`
- short_cleanup_bottom_risk_v1: `keep`

## Remaining Risks
- baseline_short_sample_is_small
- borrow_or_cost_has_no_direct_table
- event_risk_can_be_sparse_in_this_window
- monthly_stability_is_only_partially_observed

## Next One Thing
Move one axis only: either tighten the followthrough gate further or widen the historical slice, not both.
