# TRADEX Family Leaderboard

- session_id: `scope-5f9e9c197a3f-seed-11`
- random_seed: `11`
- generated_at: `2026-03-23T06:46:54.781413+00:00`
- eval_window_mode: `fallback`
- eval_window_mode_reason: `fallback_required_standard_windows_unavailable`
- ret20_source_mode: `derived_from_daily_bars`
- ret20_source_mode_reason: `explicit_session_mode`
- source_compare_path: `C:\work\meemee-screener\.local\meemee\tradex\research_sessions\scope-5f9e9c197a3f-seed-11\compare.json`
- source_report_path: `C:\work\meemee-screener\docs\reports\tradex_research_session_scope-5f9e9c197a3f-seed-11.md`

## Overview

| families | keep | hold | drop | candidates |
| ---: | ---: | ---: | ---: | ---: |
| 5 | 5 | 0 | 0 | 5 |

- validity: `invalid (insufficient_samples)`

## Family Summary

| family | decision | keep | hold | drop | best method |
| --- | --- | ---: | ---: | ---: | --- |
| existing-score rescaled | keep | 1 | 0 | 0 | 既存点数の再尺度化 |
- `existing-score rescaled` decision_reasons: `[{"code": "candidate_keep_present", "keep_count": 1}]`
| liquidity-aware | keep | 1 | 0 | 0 | 流動性ふるい残し |
- `liquidity-aware` decision_reasons: `[{"code": "candidate_keep_present", "keep_count": 1}]`
| penalty-first | keep | 1 | 0 | 0 | 減点優先型 |
- `penalty-first` decision_reasons: `[{"code": "candidate_keep_present", "keep_count": 1}]`
| readiness-aware | keep | 1 | 0 | 0 | 準備完了優先型 |
- `readiness-aware` decision_reasons: `[{"code": "candidate_keep_present", "keep_count": 1}]`
| regime-aware | keep | 1 | 0 | 0 | 逆風回避の順張り |
- `regime-aware` decision_reasons: `[{"code": "candidate_keep_present", "keep_count": 1}]`

## Candidate Rows

| family | candidate | decision | ret20 mode | top5 | top10 | monthly capture | zero-pass | worst regime | dd | turnover | liquidity fail | reasons |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| existing-score rescaled | 既存点数の再尺度化 | keep | derived_from_daily_bars | 0.0000 | 0.0000 | 0.0000 | 0 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |
| liquidity-aware | 流動性ふるい残し | keep | derived_from_daily_bars | 0.0000 | 0.0000 | 0.0000 | 0 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |
| penalty-first | 減点優先型 | keep | derived_from_daily_bars | 0.0000 | 0.0000 | 0.0000 | 0 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |
| readiness-aware | 準備完了優先型 | keep | derived_from_daily_bars | 0.0000 | 0.0000 | 0.0000 | 0 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |
| regime-aware | 逆風回避の順張り | keep | derived_from_daily_bars | 0.0000 | 0.0000 | 0.0000 | 0 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |

## Notes

- compare artifact が正本で、markdown report は派生物
- decision は `keep / drop / hold` のみ
- hold は追加 1 候補だけ試す余地を残す暫定状態
- MeeMee にはまだ接続しない
- legacy analysis env must be `0` for research runs (`MEEMEE_DISABLE_LEGACY_ANALYSIS`)
