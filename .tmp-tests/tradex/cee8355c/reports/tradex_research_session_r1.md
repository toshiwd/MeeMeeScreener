# TRADEX Research Session

- session_id: `r1`
- session_scope_id: `r1`
- random_seed: `7`
- manifest_hash: `5547ad1291eda40ad86b7c642bd4df523707eabae63073d570bf5897c15823ed`
- eval_window_mode: `standard`
- eval_window_mode_reason: `standard_windows_available`
- ret20_source_mode: `precomputed`
- ret20_source_mode_reason: `explicit_session_mode`
- eval_window_mode_standard_windows: `3`
- eval_window_mode_fallback_windows: `3`
- evaluation_window_min_days_standard: `60`
- evaluation_window_min_days_used: `20`

## Coverage

| confirmed universe | probe selection | candidate rows | eligible | ret20 computable | compare rows | sample rows | sample count | insufficient |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 20 | 4 | 4 | 0 | 4 | 4 | 4 | 40 | false |
- future_ret20 stage counts: before_guard=`320` / after_guard=`160` / joinable=`160` / compare_emitted=`160` / retained=`160`
- first_zero_stage: `eligibility_passed`
- failure_stage: `eligibility_passed`
- future_ret20: candidate_day_count=`320`, passed_count=`160`, guarded_out_count=`160`
- future_ret20_failure_reason_counts: `{"regime_date_not_in_code_trading_calendar": 160}`
- future_ret20_failure_reason_counts_by_source_mode: `{"precomputed": {"regime_date_not_in_code_trading_calendar": 160}}`
- ret20_source_mode: `precomputed`
- future_ret20_source_coverage: `{"missing_by_code": {}, "missing_by_month": {}, "missing_by_source_table": {}, "missing_examples": [], "missing_join_miss_count": 0, "missing_near_data_end_count": 0, "missing_trade_sequence_shortage_count": 0, "mixed_source_mode": false, "ret20_source_mode": "precomputed"}`
- future_ret20_join_gap_coverage: `{"after_scope_filter_count": 0, "candidate_rows_after_scope_filter": 640, "candidate_rows_before_scope_filter": 640, "examples": [], "future_rows_after_scope_filter": 41280, "future_rows_before_scope_filter": 22400, "joinable_code_date_pairs_after_scope": 640, "joinable_code_date_pairs_before_scope": 640, "reason_counts": {}}`
- candidate_scope_gap_coverage: `{"candidate_in_scope_after_build_count": 0, "candidate_in_scope_before_build_count": 0, "candidate_removed_by_scope_boundary_count": 0, "candidate_scope_gap_count": 0, "candidate_scope_gap_examples": [], "candidate_scope_gap_reason_counts": {}, "candidate_scope_key_mismatch_reason_counts": {}, "key_normalization_mode": "unknown", "scope_filter_applied_stage": "unknown"}`

## Champion

- method_title: `現行ランキング`
- method_thesis: `現行のTRADEX標準順位をそのまま再現する。`
- run_id: `tradex-research-r1-champion-baseline`

## Families

| family | best method | top5 mean | median | monthly capture | promote |
| --- | --- | ---: | ---: | ---: | --- |
| existing-score rescaled | 既存点数の再尺度化 | 0.0000 | 0.0000 | 0.0000 | false |
- 名前: `既存点数の再尺度化`
  - 仮説: `現行スコアを少し強めに再尺度化して、上位の密度を上げる。`
  - 強い局面: `evaluation_summary.windows` を参照
  - 弱い局面: `monthly_capture_not_improved, turnover_too_high`
  - champion との差分: `0.0000`
| penalty-first | 減点優先型 | 0.0000 | 0.0000 | 0.0000 | false |
- 名前: `減点優先型`
  - 仮説: `欠損と未解決を先に強く罰して、上位候補を締める。`
  - 強い局面: `evaluation_summary.windows` を参照
  - 弱い局面: `monthly_capture_not_improved, turnover_too_high, zero_pass_months_not_improved`
  - champion との差分: `0.0000`

## Best Result

- method_title: `既存点数の再尺度化`
- method_id: `existing_score_rescaled_v1`
- promote_ready: `False`
- promote_reasons: `monthly_capture_not_improved, turnover_too_high`

## Phase 4

- status: `skipped`
- reason: `no_promote_ready_winner`

## Notes

- compare artifact が正本で、markdown report は派生物
- MeeMee にはまだ接続しない
- best-result は top-K=5 を主評価にし、同点時は worst regime -> DD -> turnover で選んだ
