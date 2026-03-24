# TRADEX Research Session

- session_id: `scope-2a846c3a2d5f-seed-23`
- session_scope_id: `rr_confirmed_20260323_fix5_regime_shift`
- random_seed: `23`
- manifest_hash: `a9bff34e651a7bbe3c5b4154aa52d35ecc1b9e79ddc074c72707af8c7290c270`
- eval_window_mode: `fallback`
- eval_window_mode_reason: `fallback_required_standard_windows_unavailable`
- ret20_source_mode: `derived_from_daily_bars`
- ret20_source_mode_reason: `explicit_session_mode`
- eval_window_mode_standard_windows: `0`
- eval_window_mode_fallback_windows: `3`
- evaluation_window_min_days_standard: `60`
- evaluation_window_min_days_used: `20`

## Coverage

| confirmed universe | probe selection | candidate rows | eligible | ret20 computable | compare rows | sample rows | sample count | insufficient |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 20 | 5 | 5 | 5 | 0 | 5 | 0 | 0 | true |
- first_zero_stage: `future_ret20_computable`
- failure_stage: `future_ret20_computable`
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
- future_ret20_failure_reason_counts: `{"ret20_source_missing": 100}`
- ret20_source_mode: `derived_from_daily_bars`
- future_ret20_source_coverage: `{"missing_by_code": {}, "missing_by_month": {}, "missing_by_source_table": {}, "missing_examples": [], "missing_join_miss_count": 0, "missing_near_data_end_count": 0, "missing_trade_sequence_shortage_count": 0, "mixed_source_mode": false, "ret20_source_mode": "derived_from_daily_bars"}`

## Validity

- status: `invalid`
- reason: `insufficient_samples`

## Champion

- method_title: `現行ランキング`
- method_thesis: `現行のTRADEX標準順位をそのまま再現する。`
- run_id: `tradex-research-scope-2a846c3a2d5f-seed-23-champion-baseline`

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

- method_title: ``
- method_id: ``
- promote_ready: `False`
- promote_reasons: `none`

## Phase 4

- status: `skipped`
- reason: `insufficient_samples`

## Notes

- compare artifact が正本で、markdown report は派生物
- MeeMee にはまだ接続しない
- best-result は top-K=5 を主評価にし、同点時は worst regime -> DD -> turnover で選んだ
