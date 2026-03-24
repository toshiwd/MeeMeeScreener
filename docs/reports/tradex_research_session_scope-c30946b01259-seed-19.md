# TRADEX Research Session

- session_id: `scope-c30946b01259-seed-19`
- session_scope_id: `rr_confirmed_20260323_fix5`
- random_seed: `19`
- manifest_hash: `ec0285c26d74535bf0ee18cddf5897647b38a5b5ab83aa370b0e93789fb18137`
- eval_window_mode: `fallback`
- eval_window_mode_reason: `fallback_required_standard_windows_unavailable`
- eval_window_mode_standard_windows: `0`
- eval_window_mode_fallback_windows: `3`
- evaluation_window_min_days_standard: `60`
- evaluation_window_min_days_used: `20`

## Coverage

| confirmed universe | probe selection | candidate rows | eligible | ret20 computable | compare rows | sample rows | sample count | insufficient |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 20 | 5 | 5 | 5 | 5 | 5 | 5 | 89 | false |
- first_zero_stage: `passed`
- failure_stage: `passed`

## Champion

- method_title: `現行ランキング`
- method_thesis: `現行のTRADEX標準順位をそのまま再現する。`
- run_id: `tradex-research-scope-c30946b01259-seed-19-champion-baseline`

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
