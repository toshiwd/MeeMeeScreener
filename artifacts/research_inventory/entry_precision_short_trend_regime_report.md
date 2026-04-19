# Entry Precision Short Trend Regime Audit

## Current State
- confirmed: local_uplift_was_verified_before_this_pass, wide_slice_was_already_tested, the_logic_is_frozen_for_this_pass
- provisional: regime_support_is_not_uniform, 2024_is_sample_thin, trend_edge_is_proxy_based

## Problem
The frozen short-trend edge may depend on regime; this pass decomposes which regimes support it and which regimes break it.

## Change Policy
TRADEX research only, frozen challenger logic, regime split only, same-condition comparison fixed, long logic frozen, and no new feature families or threshold changes.

## Concrete Changes
- wide window: `20240101..20260226`
- local reference window: `20250101..20260226`

## Verify
- regime buckets: `6`
- 2024 challenger rows: `2`
- long freeze confirmed: `True`

## Decision
- overall: `hold`
- reasons: support_regimes=broad_range_sideways_regime, failure_regimes=broad_down_regime,broad_up_countertrend_regime,weak_trend_noisy_regime, selected_2024_rows=2, selected_2025_rows=13, mixed_regime_response

## Remaining Risks
- support_buckets_are_small_and_may_not_generalize
- failure_buckets_are_not_uniform_across_years
- trend_logic_is_still_proxy_based
- future_passes_should_not_tune_without_more_context

## Next One Thing
If anything changes next, it should be a regime-targeted review of the failure buckets, not a threshold retune.
