# TRADEX Research Session

- session_id: `d1`
- session_scope_id: `d1`
- random_seed: `19`
- manifest_hash: `0721e3fadcd8d44891ccccaaa7eef4a691999416a584a76b358d3d8e89d8c110`
- eval_window_mode: `standard`
- eval_window_mode_reason: `standard_windows_available`
- ret20_source_mode: `derived_from_daily_bars`
- ret20_source_mode_reason: `explicit_session_mode`
- eval_window_mode_standard_windows: `3`
- eval_window_mode_fallback_windows: `3`
- evaluation_window_min_days_standard: `60`
- evaluation_window_min_days_used: `20`

## Coverage

| confirmed universe | probe selection | candidate rows | eligible | ret20 computable | compare rows | sample rows | sample count | insufficient |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 20 | 5 | 5 | 0 | 5 | 5 | 5 | 40 | false |
- future_ret20 stage counts: before_guard=`400` / after_guard=`200` / joinable=`200` / compare_emitted=`200` / retained=`200`
- first_zero_stage: `eligibility_passed`
- failure_stage: `eligibility_passed`
- future_ret20: candidate_day_count=`400`, passed_count=`200`, guarded_out_count=`200`
- future_ret20_failure_reason_counts: `{"regime_date_not_in_code_trading_calendar": 200}`
- future_ret20_failure_reason_counts_by_source_mode: `{"derived_from_daily_bars": {"regime_date_not_in_code_trading_calendar": 200}}`
- ret20_source_mode: `derived_from_daily_bars`
- future_ret20_source_coverage: `{"missing_by_code": {}, "missing_by_month": {}, "missing_by_source_table": {}, "missing_examples": [], "missing_join_miss_count": 0, "missing_near_data_end_count": 0, "missing_trade_sequence_shortage_count": 0, "mixed_source_mode": false, "ret20_source_mode": "derived_from_daily_bars"}`
- future_ret20_join_gap_coverage: `{"after_scope_filter_count": 0, "candidate_rows_after_scope_filter": 800, "candidate_rows_before_scope_filter": 800, "examples": [], "future_rows_after_scope_filter": 51600, "future_rows_before_scope_filter": 28000, "joinable_code_date_pairs_after_scope": 800, "joinable_code_date_pairs_before_scope": 800, "reason_counts": {}}`
- candidate_scope_gap_coverage: `{"candidate_in_scope_after_build_count": 0, "candidate_in_scope_before_build_count": 0, "candidate_removed_by_scope_boundary_count": 0, "candidate_scope_gap_count": 0, "candidate_scope_gap_examples": [], "candidate_scope_gap_reason_counts": {}, "candidate_scope_key_mismatch_reason_counts": {}, "key_normalization_mode": "unknown", "scope_filter_applied_stage": "unknown"}`

## Champion

- method_title: `現行ランキング`
- method_thesis: `現行のTRADEX標準順位をそのまま再現する。`
- run_id: `tradex-research-d1-champion-baseline`

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
| readiness-aware | 準備完了優先型 | 0.0000 | 0.0000 | 0.0000 | false |
- 名前: `準備完了優先型`
  - 仮説: `ready率を少し強めに見て、通過後の安定性を上げる。`
  - 強い局面: `evaluation_summary.windows` を参照
  - 弱い局面: `monthly_capture_not_improved, turnover_too_high`
  - champion との差分: `0.0000`
| liquidity-aware | 流動性ふるい残し | 0.0000 | 0.0000 | 0.0000 | false |
- 名前: `流動性ふるい残し`
  - 仮説: `流動性の弱い候補を上位から外しやすくする。`
  - 強い局面: `evaluation_summary.windows` を参照
  - 弱い局面: `monthly_capture_not_improved, turnover_too_high`
  - champion との差分: `0.0000`
| regime-aware | 逆風回避の順張り | 0.0000 | 0.0000 | 0.0000 | false |
- 名前: `逆風回避の順張り`
  - 仮説: `相場局面を意識して、逆風局面の損失を減らす。`
  - 強い局面: `evaluation_summary.windows` を参照
  - 弱い局面: `monthly_capture_not_improved, turnover_too_high`
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
