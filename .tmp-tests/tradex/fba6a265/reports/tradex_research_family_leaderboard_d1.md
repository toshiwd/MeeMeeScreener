# TRADEX Family Leaderboard

- session_id: `d1`
- random_seed: `19`
- generated_at: `2026-04-03T01:31:29.537018+00:00`
- eval_window_mode: `standard`
- eval_window_mode_reason: `standard_windows_available`
- ret20_source_mode: `derived_from_daily_bars`
- ret20_source_mode_reason: `explicit_session_mode`
- scope_filter_applied_stage: `unknown`
- future_ret20 stage counts: before_guard=`0` / after_guard=`0` / joinable=`0` / compare_emitted=`0` / retained=`0`
- source_compare_path: `C:\work\meemee-screener\.tmp-tests\tradex\fba6a265\research_sessions\d1\compare.json`
- source_report_path: `C:\work\meemee-screener\.tmp-tests\tradex\fba6a265\reports\tradex_research_session_d1.md`

## Overview

| families | keep | hold | drop | candidates |
| ---: | ---: | ---: | ---: | ---: |
| 5 | 0 | 0 | 5 | 5 |

## Family Summary

| family | decision | keep | hold | drop | best method |
| --- | --- | ---: | ---: | ---: | --- |
| existing-score rescaled | drop | 0 | 0 | 1 | 既存点数の再尺度化 |
- `existing-score rescaled` decision_reasons: `[{"code": "all_candidates_drop", "drop_count": 1}]`
| liquidity-aware | drop | 0 | 0 | 1 | 流動性ふるい残し |
- `liquidity-aware` decision_reasons: `[{"code": "all_candidates_drop", "drop_count": 1}]`
| penalty-first | drop | 0 | 0 | 1 | 減点優先型 |
- `penalty-first` decision_reasons: `[{"code": "all_candidates_drop", "drop_count": 1}]`
| readiness-aware | drop | 0 | 0 | 1 | 準備完了優先型 |
- `readiness-aware` decision_reasons: `[{"code": "all_candidates_drop", "drop_count": 1}]`
| regime-aware | drop | 0 | 0 | 1 | 逆風回避の順張り |
- `regime-aware` decision_reasons: `[{"code": "all_candidates_drop", "drop_count": 1}]`

## Candidate Rows

| family | candidate | decision | ret20 mode | top5 | top10 | monthly capture | zero-pass | worst regime | dd | turnover | liquidity fail | reasons |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| existing-score rescaled | 既存点数の再尺度化 | drop | derived_from_daily_bars | 0.0000 | 0.0000 | 0.7000 | 0 | 0.0000 | 0.0000 | 0.3333 | 0.0000 | top5:pass, top10:pass, monthly_capture:fail, zero_pass:pass, worst_regime:pass, dd:pass, turnover:fail, liquidity_fail:pass |
| liquidity-aware | 流動性ふるい残し | drop | derived_from_daily_bars | 0.0000 | 0.0000 | 0.7000 | 0 | 0.0000 | 0.0000 | 0.3333 | 0.0000 | top5:pass, top10:pass, monthly_capture:fail, zero_pass:pass, worst_regime:pass, dd:pass, turnover:fail, liquidity_fail:pass |
| penalty-first | 減点優先型 | drop | derived_from_daily_bars | 0.0000 | 0.0000 | 0.1000 | 1 | 0.0000 | 0.0000 | 0.7500 | 0.0000 | top5:pass, top10:pass, monthly_capture:fail, zero_pass:fail, worst_regime:pass, dd:pass, turnover:fail, liquidity_fail:pass |
| readiness-aware | 準備完了優先型 | drop | derived_from_daily_bars | 0.0000 | 0.0000 | 0.7000 | 0 | 0.0000 | 0.0000 | 0.3333 | 0.0000 | top5:pass, top10:pass, monthly_capture:fail, zero_pass:pass, worst_regime:pass, dd:pass, turnover:fail, liquidity_fail:pass |
| regime-aware | 逆風回避の順張り | drop | derived_from_daily_bars | 0.0000 | 0.0000 | 0.7000 | 0 | 0.0000 | 0.0000 | 0.3333 | 0.0000 | top5:pass, top10:pass, monthly_capture:fail, zero_pass:pass, worst_regime:pass, dd:pass, turnover:fail, liquidity_fail:pass |

## Notes

- compare artifact が正本で、markdown report は派生物
- decision は `keep / drop / hold` のみ
- hold は追加 1 候補だけ試す余地を残す暫定状態
- MeeMee にはまだ接続しない
- legacy analysis env must be `0` for research runs (`MEEMEE_DISABLE_LEGACY_ANALYSIS`)
