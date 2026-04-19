# Entry Precision Short Trend Audit

## Current State
- confirmed: short_side_cleanup_completed, baseline_short_rows_replayed_from_snapshot_db, long_logic_remained_frozen
- provisional: borrow_cost_is_proxy_only, false_neutral_recovery_is_not_yet_demonstrated, monthly_regime_stability_is_partial

## Problem
The remaining short-side noise is structural: trend-misaligned names, range-middle shorts, and weak continuation zones still leak through the gate.

## Change Policy
TRADEX only, short-side trend-alignment cleanup only, long logic frozen, same window and artifact detail level fixed, and no multi-axis redesign.

## Concrete Changes
- baseline short selected count: `27`
- trend alignment selected count: `13`
- range suppression selected count: `10`
- trend+followthrough selected count: `10`

## Verify
- short baseline hit rate: `0.4444444444444444`
- short baseline median ret20: `-0.007709604882749759`
- long freeze confirmed: `True`

## Decision
- short_trend_alignment_v1: `keep`
- short_range_middle_suppression_v1: `drop`
- short_trend_alignment_plus_followthrough_v1: `drop`

## Remaining Risks
- baseline_short_sample_is_small
- borrow_or_cost_has_no_direct_table
- event_risk_can_be_sparse_in_this_window
- trend_alignment_is_still_proxy_based

## Next One Thing
Move one axis only: either tighten higher-timeframe alignment further or widen the historical slice, but not both.
