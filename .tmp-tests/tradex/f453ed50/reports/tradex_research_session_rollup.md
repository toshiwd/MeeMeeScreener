# TRADEX Session Leaderboard Rollup

- generated_at: `2026-04-03T01:20:10.005624+00:00`
- session_count: `1`
- valid_session_count: `1` / invalid_session_count: `0`
- session_ids: `rs1`

## Overview

| sessions | families | candidates | keep families | hold families | drop families |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2 | 2 | 0 | 2 | 0 |

## Family Summary

| family | decision | keep | hold | drop | latest decision |
| --- | --- | ---: | ---: | ---: | --- |
| existing-score rescaled | hold | 0 | 0 | 2 | drop |
| penalty-first | hold | 0 | 0 | 2 | drop |

## Candidate Rows

| family | title | decision | sessions | top5Δ | top10Δ | monthlyΔ | zero-passΔ | worstΔ | ddΔ | turnoverΔ | liquidityΔ | latest reasons |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| existing-score rescaled | 既存点数の再尺度化強め | drop | 2 | 0.0000 | 0.0000 | -0.5000 | 0.0000 | 0.0000 | 0.0000 | 0.5714 | 0.0000 | top5:pass, top10:pass, monthly_capture:fail, zero_pass:pass, worst_regime:pass, dd:pass, turnover:fail, liquidity_fail:pass |
| penalty-first | 減点優先型厳しめ | drop | 2 | 0.0000 | 0.0000 | -0.7000 | 0.0000 | 0.0000 | 0.0000 | 0.3333 | 0.0000 | top5:pass, top10:pass, monthly_capture:fail, zero_pass:pass, worst_regime:pass, dd:pass, turnover:fail, liquidity_fail:pass |

## Notes

- compare artifact と family_leaderboard を正本として集計した rollup です。
- hold は追加 1 候補の余地を示す暫定状態です。
- MeeMee にはまだ接続していません。
