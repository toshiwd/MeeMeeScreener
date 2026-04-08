# TRADEX Session Leaderboard Rollup

- generated_at: `2026-04-03T01:47:30.313270+00:00`
- session_count: `1`
- valid_session_count: `1` / invalid_session_count: `0`
- session_ids: `d1`

## Overview

| sessions | families | candidates | keep families | hold families | drop families |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 5 | 5 | 0 | 5 | 0 |

## Family Summary

| family | decision | keep | hold | drop | latest decision |
| --- | --- | ---: | ---: | ---: | --- |
| existing-score rescaled | hold | 0 | 0 | 1 | drop |
| liquidity-aware | hold | 0 | 0 | 1 | drop |
| penalty-first | hold | 0 | 0 | 1 | drop |
| readiness-aware | hold | 0 | 0 | 1 | drop |
| regime-aware | hold | 0 | 0 | 1 | drop |

## Candidate Rows

| family | title | decision | sessions | top5Δ | top10Δ | monthlyΔ | zero-passΔ | worstΔ | ddΔ | turnoverΔ | liquidityΔ | latest reasons |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| existing-score rescaled | 既存点数の再尺度化 | drop | 1 | 0.0000 | 0.0000 | -0.3000 | 0.0000 | 0.0000 | 0.0000 | 0.3333 | 0.0000 | top5:pass, top10:pass, monthly_capture:fail, zero_pass:pass, worst_regime:pass, dd:pass, turnover:fail, liquidity_fail:pass |
| liquidity-aware | 流動性ふるい残し | drop | 1 | 0.0000 | 0.0000 | -0.3000 | 0.0000 | 0.0000 | 0.0000 | 0.3333 | 0.0000 | top5:pass, top10:pass, monthly_capture:fail, zero_pass:pass, worst_regime:pass, dd:pass, turnover:fail, liquidity_fail:pass |
| penalty-first | 減点優先型 | drop | 1 | 0.0000 | 0.0000 | -0.9000 | 1.0000 | 0.0000 | 0.0000 | 0.7500 | 0.0000 | top5:pass, top10:pass, monthly_capture:fail, zero_pass:fail, worst_regime:pass, dd:pass, turnover:fail, liquidity_fail:pass |
| readiness-aware | 準備完了優先型 | drop | 1 | 0.0000 | 0.0000 | -0.3000 | 0.0000 | 0.0000 | 0.0000 | 0.3333 | 0.0000 | top5:pass, top10:pass, monthly_capture:fail, zero_pass:pass, worst_regime:pass, dd:pass, turnover:fail, liquidity_fail:pass |
| regime-aware | 逆風回避の順張り | drop | 1 | 0.0000 | 0.0000 | -0.3000 | 0.0000 | 0.0000 | 0.0000 | 0.3333 | 0.0000 | top5:pass, top10:pass, monthly_capture:fail, zero_pass:pass, worst_regime:pass, dd:pass, turnover:fail, liquidity_fail:pass |

## Notes

- compare artifact と family_leaderboard を正本として集計した rollup です。
- hold は追加 1 候補の余地を示す暫定状態です。
- MeeMee にはまだ接続していません。
