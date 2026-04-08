# TRADEX Scope Stability Rollup

- generated_at: `2026-04-03T01:47:30.729696+00:00`
- scope_count: `3`
- session_count: `6`
- scope_ids: `scope-bad, scope-good, scope-mixed`
- eval_window_mode_counts: standard=`0`, fallback=`6`, unknown=`0`
- ret20_source_mode_counts: precomputed=`0`, derived=`0`, unknown=`6`
- scope_filter_applied_stage: `unknown`
- key_normalization_mode: `unknown`
- future_ret20 stage counts: before_guard=`0` / after_guard=`0` / joinable=`0` / compare_emitted=`0` / retained=`0`
- candidate_in_scope_before_build_count: `0` / candidate_in_scope_after_build_count: `0`
- candidate_removed_by_scope_boundary_count: `0`

## Overview

| usable | unstable | unusable | sessions | sample_min | sample_max | sample_mean |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 1 | 6 | 0 | 11 | 5.50 |

## Scope Summary

| scope | decision | sessions | sample_min | sample_max | sample_mean | first_zero_stage |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| scope-bad | unusable | 2 | 0 | 0 | 0.00 | eligibility_passed |
- decision_reasons: `[{"code": "no_sessions_have_samples", "sample_count_max": 0}]`
- first_zero_stage_counts: `{"eligibility_passed": 2}`
- eval_window_mode_counts: `{"fallback": 2, "standard": 0, "unknown": 0}`
- ret20_source_mode_counts: `{"derived_from_daily_bars": 0, "precomputed": 0, "unknown": 2}`
- scope_filter_applied_stage: `unknown`
- key_normalization_mode: `unknown`
- future_ret20 stage counts: before_guard=`0` / after_guard=`0` / joinable=`0` / compare_emitted=`0` / retained=`0`
- candidate_in_scope_before_build_count=`0` / candidate_in_scope_after_build_count=`0`
- candidate_removed_by_scope_boundary_count: `0`
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
- future_ret20_join_gap_coverage: `{"after_scope_filter_count": 0, "reason_counts": {}}`
| scope-good | usable | 2 | 11 | 11 | 11.00 | passed |
- decision_reasons: `[{"code": "all_sessions_have_samples", "sample_count_min": 11}]`
- first_zero_stage_counts: `{"passed": 2}`
- eval_window_mode_counts: `{"fallback": 2, "standard": 0, "unknown": 0}`
- ret20_source_mode_counts: `{"derived_from_daily_bars": 0, "precomputed": 0, "unknown": 2}`
- scope_filter_applied_stage: `unknown`
- key_normalization_mode: `unknown`
- future_ret20 stage counts: before_guard=`0` / after_guard=`0` / joinable=`0` / compare_emitted=`0` / retained=`0`
- candidate_in_scope_before_build_count=`0` / candidate_in_scope_after_build_count=`0`
- candidate_removed_by_scope_boundary_count: `0`
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
- future_ret20_join_gap_coverage: `{"after_scope_filter_count": 0, "reason_counts": {}}`
| scope-mixed | unstable | 2 | 0 | 11 | 5.50 | eligibility_passed |
- decision_reasons: `[{"code": "mixed_sample_presence", "sample_count_min": 0, "sample_count_max": 11, "sample_count_positive": 1, "sample_count_total": 2}]`
- first_zero_stage_counts: `{"eligibility_passed": 1, "passed": 1}`
- eval_window_mode_counts: `{"fallback": 2, "standard": 0, "unknown": 0}`
- ret20_source_mode_counts: `{"derived_from_daily_bars": 0, "precomputed": 0, "unknown": 2}`
- scope_filter_applied_stage: `unknown`
- key_normalization_mode: `unknown`
- future_ret20 stage counts: before_guard=`0` / after_guard=`0` / joinable=`0` / compare_emitted=`0` / retained=`0`
- candidate_in_scope_before_build_count=`0` / candidate_in_scope_after_build_count=`0`
- candidate_removed_by_scope_boundary_count: `0`
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
- future_ret20_join_gap_coverage: `{"after_scope_filter_count": 0, "reason_counts": {}}`

## Session Rows

| scope | seed | mode | sample | best | first_zero | top5Δ | worstΔ | ddΔ | turnoverΔ | liquidityΔ |
| --- | ---: | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| scope-bad | 7 | fallback | 0 | no | eligibility_passed | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
| scope-bad | 19 | fallback | 0 | no | eligibility_passed | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
| scope-good | 7 | fallback | 11 | yes | passed | 0.0100 | 0.0100 | -0.0100 | -0.0100 | -0.0100 |
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
| scope-good | 19 | fallback | 11 | yes | passed | 0.0100 | 0.0100 | -0.0100 | -0.0100 | -0.0100 |
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
| scope-mixed | 7 | fallback | 11 | yes | passed | 0.0100 | 0.0100 | -0.0100 | -0.0100 | -0.0100 |
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
| scope-mixed | 19 | fallback | 0 | no | eligibility_passed | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`

## Notes

- legacy analysis env must be `0` for research runs (`MEEMEE_DISABLE_LEGACY_ANALYSIS`).
- standard window min days: `60`
- fallback window min days: `20`
- sample_count=0 or scope_decision != usable sessions must not be used for pruning.
