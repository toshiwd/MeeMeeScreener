# Entry Precision Short Trend Wide Audit

## Current State
- confirmed: short_trend_alignment_v1_was_kept_in_the_local_window, long_logic_remained_frozen, wider_history_is_now_tested
- provisional: local_keep_was_based_on_a_thin_sample, trend_alignment_is_proxy_based, wide_year_2024_behavior_is_mixed

## Problem
The local keep needs a durability check: once the slice is widened, the challenger may lose its edge or only work in part of the history.

## Change Policy
TRADEX research only, frozen challenger logic, wider historical evaluation only, same-condition comparison fixed, long logic frozen, and no new feature families.

## Concrete Changes
- local reference selected count: `13`
- wide slice selected count: `15`
- wide worst subwindow: `2024`

## Verify
- local hit rate: `0.5384615384615384`
- wide hit rate: `0.5333333333333333`
- long freeze confirmed: `True`

## Decision
- wide decision: `hold`
- reasons: wide_hit_rate_weaker, wide_median_ret20_not_weaker, wide_mean_ret20_weaker, worst_subwindow_2024, wide_monthly_balance_positive, local_uplift_but_wide_instability

## Remaining Risks
- wide_slice_reveals_year_to_year_instability
- trend_alignment_logic_is_still_proxy_based
- short_sample_remains_thin_in_the_earlier_year
- future_passes_should_not_tune_on_this_result_without_additional_context

## Next One Thing
Do not retune yet; if this lane continues, the next useful check is a regime-split review of the same frozen logic rather than another feature change.
