# TRADEX Family Leaderboard

- session_id: `scope-d749b78ae598-seed-11`
- random_seed: `11`
- generated_at: `2026-03-24T00:49:17.106252+00:00`
- eval_window_mode: `fallback`
- eval_window_mode_reason: `fallback_required_standard_windows_unavailable`
- ret20_source_mode: `precomputed`
- ret20_source_mode_reason: `explicit_session_mode`
- scope_filter_applied_stage: `unknown`
- future_ret20 stage counts: before_guard=`0` / after_guard=`0` / joinable=`0` / compare_emitted=`0` / retained=`0`
- source_compare_path: `C:\work\meemee-screener\.tmp-tradex-fix\research_sessions\scope-d749b78ae598-seed-11\compare.json`
- source_report_path: `C:\work\meemee-screener\docs\reports\tradex_research_session_scope-d749b78ae598-seed-11.md`

## Overview

| families | keep | hold | drop | candidates |
| ---: | ---: | ---: | ---: | ---: |
| 5 | 5 | 0 | 0 | 10 |

- validity: `invalid (insufficient_samples)`

## Family Summary

| family | decision | keep | hold | drop | best method |
| --- | --- | ---: | ---: | ---: | --- |
| existing-score rescaled | keep | 2 | 0 | 0 | 既存点数の再尺度化 |
- `existing-score rescaled` decision_reasons: `[{"code": "candidate_keep_present", "keep_count": 2}]`
| liquidity-aware | keep | 2 | 0 | 0 | 流動性ふるい残し |
- `liquidity-aware` decision_reasons: `[{"code": "candidate_keep_present", "keep_count": 2}]`
| penalty-first | keep | 2 | 0 | 0 | 減点優先型 |
- `penalty-first` decision_reasons: `[{"code": "candidate_keep_present", "keep_count": 2}]`
| readiness-aware | keep | 2 | 0 | 0 | 準備完了優先型 |
- `readiness-aware` decision_reasons: `[{"code": "candidate_keep_present", "keep_count": 2}]`
| regime-aware | keep | 2 | 0 | 0 | 逆風回避の順張り |
- `regime-aware` decision_reasons: `[{"code": "candidate_keep_present", "keep_count": 2}]`

## Candidate Rows

| family | candidate | decision | ret20 mode | top5 | top10 | monthly capture | zero-pass | worst regime | dd | turnover | liquidity fail | reasons |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| existing-score rescaled | 既存点数の再尺度化 | keep | precomputed | 0.0000 | 0.0000 | 0.0000 | 0 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |
| existing-score rescaled | 既存点数の再尺度化強め | keep | precomputed | 0.0000 | 0.0000 | 0.0000 | 0 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |
| liquidity-aware | 流動性ふるい残し | keep | precomputed | 0.0000 | 0.0000 | 0.0000 | 0 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |
| liquidity-aware | 流動性ふるい残し厳しめ | keep | precomputed | 0.0000 | 0.0000 | 0.0000 | 0 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |
| penalty-first | 減点優先型 | keep | precomputed | 0.0000 | 0.0000 | 0.0000 | 0 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |
| penalty-first | 減点優先型厳しめ | keep | precomputed | 0.0000 | 0.0000 | 0.0000 | 0 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |
| readiness-aware | 準備完了優先型 | keep | precomputed | 0.0000 | 0.0000 | 0.0000 | 0 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |
| readiness-aware | 準備完了優先型強め | keep | precomputed | 0.0000 | 0.0000 | 0.0000 | 0 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |
| regime-aware | 逆風回避の順張り | keep | precomputed | 0.0000 | 0.0000 | 0.0000 | 0 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |
| regime-aware | 逆風回避の順張り保守 | keep | precomputed | 0.0000 | 0.0000 | 0.0000 | 0 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |

## Notes

- compare artifact が正本で、markdown report は派生物
- decision は `keep / drop / hold` のみ
- hold は追加 1 候補だけ試す余地を残す暫定状態
- MeeMee にはまだ接続しない
- legacy analysis env must be `0` for research runs (`MEEMEE_DISABLE_LEGACY_ANALYSIS`)
