# TRADEX Research Session

- session_id: `scope-91a97ac7c2fe-seed-7`
- session_scope_id: `rr_confirmed_20260323_fix5_near_period`
- random_seed: `7`
- manifest_hash: `4339b86fb03fdfe5a8c968f53505cdd8d85ef0cb71b83deae2c157f6f971f8c8`
- eval_window_mode: `fallback`
- eval_window_mode_reason: `fallback_required_standard_windows_unavailable`
- ret20_source_mode: `precomputed`
- ret20_source_mode_reason: `explicit_session_mode`
- eval_window_mode_standard_windows: `0`
- eval_window_mode_fallback_windows: `3`
- evaluation_window_min_days_standard: `60`
- evaluation_window_min_days_used: `20`

## Coverage

| confirmed universe | probe selection | candidate rows | eligible | ret20 computable | compare rows | sample rows | sample count | insufficient |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 30 | 10 | 10 | 10 | 10 | 10 | 10 | 89 | false |
- first_zero_stage: `passed`
- failure_stage: `passed`
- future_ret20: candidate_day_count=`890`, passed_count=`890`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 290}`
- ret20_source_mode: `precomputed`
- future_ret20_source_coverage: `{"missing_by_code": {}, "missing_by_month": {}, "missing_by_source_table": {}, "missing_examples": [], "missing_join_miss_count": 0, "missing_near_data_end_count": 0, "missing_trade_sequence_shortage_count": 0, "mixed_source_mode": false, "ret20_source_mode": "precomputed"}`
- future_ret20_join_gap_coverage: `{"after_scope_filter_count": 580, "candidate_rows_after_scope_filter": 1780, "candidate_rows_before_scope_filter": 20000, "examples": [], "future_rows_after_scope_filter": 164080, "future_rows_before_scope_filter": 2919180, "joinable_code_date_pairs_after_scope": 3560, "joinable_code_date_pairs_before_scope": 40000, "reason_counts": {}}`
- candidate_scope_gap_coverage: `{"candidate_in_scope_after_build_count": 0, "candidate_in_scope_before_build_count": 0, "candidate_removed_by_scope_boundary_count": 0, "candidate_scope_gap_count": 0, "candidate_scope_gap_examples": [], "candidate_scope_gap_reason_counts": {}, "candidate_scope_key_mismatch_reason_counts": {}, "key_normalization_mode": "unknown", "scope_filter_applied_stage": "unknown"}`

## Champion

- method_title: `現行ランキング`
- method_thesis: `現行のTRADEX標準順位をそのまま再現する。`
- run_id: `tradex-research-scope-91a97ac7c2fe-seed-7-champion-baseline`

## Families

| family | best method | top5 mean | median | monthly capture | promote |
| --- | --- | ---: | ---: | ---: | --- |
| existing-score rescaled | 既存点数の再尺度化 | 0.0000 | 0.0000 | 0.0000 | true |
- 名前: `既存点数の再尺度化`
  - 仮説: `現行スコアを少し強めに再尺度化して、上位の密度を上げる。`
  - 強い局面: `evaluation_summary.windows` を参照
  - 弱い局面: `none`
  - champion との差分: `0.0000`
| penalty-first | 減点優先型 | 0.0000 | 0.0000 | 0.0000 | true |
- 名前: `減点優先型`
  - 仮説: `欠損と未解決を先に強く罰して、上位候補を締める。`
  - 強い局面: `evaluation_summary.windows` を参照
  - 弱い局面: `none`
  - champion との差分: `0.0000`
| readiness-aware | 準備完了優先型 | 0.0000 | 0.0000 | 0.0000 | true |
- 名前: `準備完了優先型`
  - 仮説: `ready率を少し強めに見て、通過後の安定性を上げる。`
  - 強い局面: `evaluation_summary.windows` を参照
  - 弱い局面: `none`
  - champion との差分: `0.0000`
| liquidity-aware | 流動性ふるい残し | 0.0000 | 0.0000 | 0.0000 | true |
- 名前: `流動性ふるい残し`
  - 仮説: `流動性の弱い候補を上位から外しやすくする。`
  - 強い局面: `evaluation_summary.windows` を参照
  - 弱い局面: `none`
  - champion との差分: `0.0000`
| regime-aware | 逆風回避の順張り | 0.0000 | 0.0000 | 0.0000 | true |
- 名前: `逆風回避の順張り`
  - 仮説: `相場局面を意識して、逆風局面の損失を減らす。`
  - 強い局面: `evaluation_summary.windows` を参照
  - 弱い局面: `none`
  - champion との差分: `0.0000`

## Best Result

- method_title: `既存点数の再尺度化`
- method_id: `existing_score_rescaled_v1`
- promote_ready: `True`
- promote_reasons: `none`

## Phase 4

- status: `skipped`
- reason: `single_class`

## Notes

- compare artifact が正本で、markdown report は派生物
- MeeMee にはまだ接続しない
- best-result は top-K=5 を主評価にし、同点時は worst regime -> DD -> turnover で選んだ
