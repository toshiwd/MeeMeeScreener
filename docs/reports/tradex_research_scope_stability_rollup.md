# TRADEX Scope Stability Rollup

- generated_at: `2026-03-24T01:45:23.926091+00:00`
- scope_count: `3`
- session_count: `15`
- scope_ids: `rr_confirmed_20260323_fix5, rr_confirmed_20260323_fix5_near_period, rr_confirmed_20260323_fix5_regime_shift`
- eval_window_mode_counts: standard=`0`, fallback=`15`, unknown=`0`
- ret20_source_mode_counts: precomputed=`15`, derived=`0`, unknown=`0`
- scope_filter_applied_stage: `unknown`
- key_normalization_mode: `unknown`
- future_ret20 stage counts: before_guard=`0` / after_guard=`0` / joinable=`0` / compare_emitted=`0` / retained=`0`
- future_ret20_failure_reason_counts_by_source_mode: `{"precomputed": {"ret20_source_missing": 4450}}`
- candidate_in_scope_before_build_count: `0` / candidate_in_scope_after_build_count: `0`
- candidate_removed_by_scope_boundary_count: `0`

## Overview

| usable | unstable | unusable | sessions | sample_min | sample_max | sample_mean |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 3 | 0 | 15 | 0 | 89 | 29.67 |

## Scope Summary

| scope | decision | sessions | sample_min | sample_max | sample_mean | first_zero_stage |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| rr_confirmed_20260323_fix5 | unstable | 5 | 0 | 89 | 17.80 | future_ret20_computable |
- decision_reasons: `[{"code": "mixed_sample_presence", "sample_count_min": 0, "sample_count_max": 89, "sample_count_positive": 1, "sample_count_total": 5}]`
- first_zero_stage_counts: `{"future_ret20_computable": 4, "passed": 1}`
- eval_window_mode_counts: `{"fallback": 5, "standard": 0, "unknown": 0}`
- ret20_source_mode_counts: `{"derived_from_daily_bars": 0, "precomputed": 5, "unknown": 0}`
- scope_filter_applied_stage: `unknown`
- key_normalization_mode: `unknown`
- future_ret20 stage counts: before_guard=`0` / after_guard=`0` / joinable=`0` / compare_emitted=`0` / retained=`0`
- candidate_in_scope_before_build_count=`0` / candidate_in_scope_after_build_count=`0`
- candidate_removed_by_scope_boundary_count: `0`
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 1490}`
- future_ret20_join_gap_coverage: `{"after_scope_filter_count": 2980, "reason_counts": {}}`
| rr_confirmed_20260323_fix5_near_period | unstable | 5 | 0 | 89 | 35.60 | future_ret20_computable |
- decision_reasons: `[{"code": "mixed_sample_presence", "sample_count_min": 0, "sample_count_max": 89, "sample_count_positive": 2, "sample_count_total": 5}]`
- first_zero_stage_counts: `{"future_ret20_computable": 3, "passed": 2}`
- eval_window_mode_counts: `{"fallback": 5, "standard": 0, "unknown": 0}`
- ret20_source_mode_counts: `{"derived_from_daily_bars": 0, "precomputed": 5, "unknown": 0}`
- scope_filter_applied_stage: `unknown`
- key_normalization_mode: `unknown`
- future_ret20 stage counts: before_guard=`0` / after_guard=`0` / joinable=`0` / compare_emitted=`0` / retained=`0`
- candidate_in_scope_before_build_count=`0` / candidate_in_scope_after_build_count=`0`
- candidate_removed_by_scope_boundary_count: `0`
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 1480}`
- future_ret20_join_gap_coverage: `{"after_scope_filter_count": 2960, "reason_counts": {}}`
| rr_confirmed_20260323_fix5_regime_shift | unstable | 5 | 0 | 89 | 35.60 | future_ret20_computable |
- decision_reasons: `[{"code": "mixed_sample_presence", "sample_count_min": 0, "sample_count_max": 89, "sample_count_positive": 2, "sample_count_total": 5}]`
- first_zero_stage_counts: `{"future_ret20_computable": 3, "passed": 2}`
- eval_window_mode_counts: `{"fallback": 5, "standard": 0, "unknown": 0}`
- ret20_source_mode_counts: `{"derived_from_daily_bars": 0, "precomputed": 5, "unknown": 0}`
- scope_filter_applied_stage: `unknown`
- key_normalization_mode: `unknown`
- future_ret20 stage counts: before_guard=`0` / after_guard=`0` / joinable=`0` / compare_emitted=`0` / retained=`0`
- candidate_in_scope_before_build_count=`0` / candidate_in_scope_after_build_count=`0`
- candidate_removed_by_scope_boundary_count: `0`
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 1480}`
- future_ret20_join_gap_coverage: `{"after_scope_filter_count": 2960, "reason_counts": {}}`

## Session Rows

| scope | seed | mode | sample | best | first_zero | top5Δ | worstΔ | ddΔ | turnoverΔ | liquidityΔ |
| --- | ---: | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| rr_confirmed_20260323_fix5 | 7 | fallback | 0 | no | future_ret20_computable | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 300}`
| rr_confirmed_20260323_fix5 | 11 | fallback | 0 | no | future_ret20_computable | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 300}`
| rr_confirmed_20260323_fix5 | 19 | fallback | 89 | yes | passed | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
- future_ret20: candidate_day_count=`890`, passed_count=`890`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 290}`
| rr_confirmed_20260323_fix5 | 23 | fallback | 0 | no | future_ret20_computable | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 300}`
| rr_confirmed_20260323_fix5 | 29 | fallback | 0 | no | future_ret20_computable | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 300}`
| rr_confirmed_20260323_fix5_near_period | 7 | fallback | 89 | yes | passed | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
- future_ret20: candidate_day_count=`890`, passed_count=`890`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 290}`
| rr_confirmed_20260323_fix5_near_period | 11 | fallback | 0 | no | future_ret20_computable | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 300}`
| rr_confirmed_20260323_fix5_near_period | 19 | fallback | 0 | no | future_ret20_computable | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 300}`
| rr_confirmed_20260323_fix5_near_period | 23 | fallback | 0 | no | future_ret20_computable | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 300}`
| rr_confirmed_20260323_fix5_near_period | 29 | fallback | 89 | yes | passed | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
- future_ret20: candidate_day_count=`890`, passed_count=`890`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 290}`
| rr_confirmed_20260323_fix5_regime_shift | 7 | fallback | 89 | yes | passed | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
- future_ret20: candidate_day_count=`890`, passed_count=`890`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 290}`
| rr_confirmed_20260323_fix5_regime_shift | 11 | fallback | 0 | no | future_ret20_computable | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 300}`
| rr_confirmed_20260323_fix5_regime_shift | 19 | fallback | 89 | yes | passed | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
- future_ret20: candidate_day_count=`890`, passed_count=`890`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 290}`
| rr_confirmed_20260323_fix5_regime_shift | 23 | fallback | 0 | no | future_ret20_computable | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 300}`
| rr_confirmed_20260323_fix5_regime_shift | 29 | fallback | 0 | no | future_ret20_computable | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 |
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 300}`

## Notes

- legacy analysis env must be `0` for research runs (`MEEMEE_DISABLE_LEGACY_ANALYSIS`).
- standard window min days: `60`
- fallback window min days: `20`
- sample_count=0 or scope_decision != usable sessions must not be used for pruning.
