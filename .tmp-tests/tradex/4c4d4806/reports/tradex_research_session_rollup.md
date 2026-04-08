# TRADEX Session Leaderboard Rollup

- generated_at: `2026-04-03T01:31:02.012338+00:00`
- session_count: `2`
- valid_session_count: `2` / invalid_session_count: `0`
- session_ids: `r1, r2`

## Overview

| sessions | families | candidates | keep families | hold families | drop families |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 2 | 2 | 0 | 2 | 0 |

## Family Summary

| family | decision | keep | hold | drop | latest decision |
| --- | --- | ---: | ---: | ---: | --- |
| existing-score rescaled | hold | 0 | 0 | 4 | drop |
| penalty-first | hold | 0 | 0 | 4 | drop |

## Candidate Rows

| family | title | decision | sessions | top5Δ | top10Δ | monthlyΔ | zero-passΔ | worstΔ | ddΔ | turnoverΔ | liquidityΔ | latest reasons |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| existing-score rescaled | 既存点数の再尺度化強め | drop | 4 | 0.0000 | 0.0000 | -0.2500 | 0.0000 | 0.0000 | 0.0000 | 0.5417 | 0.0000 | top5:pass, top10:pass, monthly_capture:fail, zero_pass:pass, worst_regime:pass, dd:pass, turnover:fail, liquidity_fail:pass |
| penalty-first | 減点優先型厳しめ | drop | 4 | 0.0000 | 0.0000 | -0.6500 | 0.5000 | 0.0000 | 0.0000 | 0.6012 | 0.0000 | top5:pass, top10:pass, monthly_capture:fail, zero_pass:pass, worst_regime:pass, dd:pass, turnover:fail, liquidity_fail:pass |

## Notes

- compare artifact と family_leaderboard を正本として集計した rollup です。
- hold は追加 1 候補の余地を示す暫定状態です。
- MeeMee にはまだ接続していません。
