# TRADEX Session Leaderboard Rollup

- generated_at: `2026-03-24T01:45:23.766480+00:00`
- session_count: `60`
- valid_session_count: `20` / invalid_session_count: `40`
- session_ids: `scope-0b00d1bcbca5-seed-19, scope-91a97ac7c2fe-seed-7, scope-2169fcf14dc3-seed-29, scope-43d5e1d38840-seed-7, scope-8b63b2f09008-seed-19, scope-e00ca6b7cc50-seed-19, scope-5e18b947ad5d-seed-7, scope-0b782cdf2c61-seed-29, scope-4f40db01d9e1-seed-7, scope-6daa8d56c81b-seed-19, scope-34246233cfe8-seed-19, scope-c850db2f97e6-seed-7, scope-4b4fa6ccb028-seed-29, scope-44c84456e71e-seed-7, scope-b1491dfe2d9c-seed-19, scope-0571205da39d-seed-19, scope-d9dd05484890-seed-7, scope-53206625b424-seed-29, scope-0043cd59bc20-seed-7, scope-2ac97f0e648d-seed-19`
- validity: `invalid (insufficient_samples)`
- note: invalid sessions are excluded from family / candidate aggregation.

## Overview

| sessions | families | candidates | keep families | hold families | drop families |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 60 | 5 | 5 | 0 | 5 | 0 |

## Family Summary

| family | decision | keep | hold | drop | latest decision |
| --- | --- | ---: | ---: | ---: | --- |
| existing-score rescaled | hold | 40 | 0 | 0 | keep |
| liquidity-aware | hold | 39 | 0 | 0 | keep |
| penalty-first | hold | 39 | 0 | 0 | keep |
| readiness-aware | hold | 40 | 0 | 0 | keep |
| regime-aware | hold | 38 | 0 | 1 | keep |

## Candidate Rows

| family | title | decision | sessions | top5Δ | top10Δ | monthlyΔ | zero-passΔ | worstΔ | ddΔ | turnoverΔ | liquidityΔ | latest reasons |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| existing-score rescaled | 既存点数の再尺度化強め | keep | 40 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |
| liquidity-aware | 流動性ふるい残し厳しめ | keep | 39 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |
| penalty-first | 減点優先型厳しめ | keep | 39 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |
| readiness-aware | 準備完了優先型強め | keep | 40 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |
| regime-aware | 逆風回避の順張り保守 | keep | 39 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | 0.0000 | top5:pass, top10:pass, monthly_capture:pass, zero_pass:pass, worst_regime:pass, dd:pass, turnover:pass, liquidity_fail:pass |

## Notes

- compare artifact と family_leaderboard を正本として集計した rollup です。
- hold は追加 1 候補の余地を示す暫定状態です。
- MeeMee にはまだ接続していません。
