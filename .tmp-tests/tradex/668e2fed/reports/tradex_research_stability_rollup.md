# TRADEX Stability Rollup

- generated_at: `2026-04-03T01:31:30.146287+00:00`
- session_count: `2`
- session_ids: `stability-seed-7, stability-seed-19`
- eval_window_mode_counts: standard=`0`, fallback=`2`, unknown=`0`
- first_zero_stage_counts: `{"passed": 2}`

## Overview

| sessions | sample_count_min | sample_count_max | sample_count_mean | best_result_present | insufficient_samples |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 2 | 12 | 14 | 13.00 | 2 | 0 |

## Session Rows

| session | seed | mode | sample_count | best_result | keep | drop | hold | top5Δ | worstΔ | ddΔ | turnoverΔ | liquidityΔ |
| --- | ---: | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| stability-seed-7 | 7 | fallback | 12 | yes | 1 | 0 | 0 | 0.0270 | 0.0100 | -0.0100 | -0.0100 | -0.0100 |
- eval_window_mode_reason: `test`
- ret20_source_mode: `unknown`
- ret20_source_mode_reason: `unknown`
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
- ret20_source_mode: `unknown`
- ret20_source_mode_reason: `unknown`
- family_leaderboard_path: `C:\work\meemee-screener\.tmp-tests\tradex\668e2fed\research_sessions\stability-seed-7\family_leaderboard.json`
- compare_path: `C:\work\meemee-screener\.tmp-tests\tradex\668e2fed\research_sessions\stability-seed-7\compare.json`
| stability-seed-19 | 19 | fallback | 14 | yes | 1 | 0 | 0 | 0.0390 | 0.0100 | -0.0100 | -0.0100 | -0.0100 |
- eval_window_mode_reason: `test`
- ret20_source_mode: `unknown`
- ret20_source_mode_reason: `unknown`
- future_ret20: candidate_day_count=`0`, passed_count=`0`, guarded_out_count=`0`
- ret20_source_mode: `unknown`
- ret20_source_mode_reason: `unknown`
- family_leaderboard_path: `C:\work\meemee-screener\.tmp-tests\tradex\668e2fed\research_sessions\stability-seed-19\family_leaderboard.json`
- compare_path: `C:\work\meemee-screener\.tmp-tests\tradex\668e2fed\research_sessions\stability-seed-19\compare.json`

## Notes

- legacy analysis env: `MEEMEE_DISABLE_LEGACY_ANALYSIS` must be `0` for research runs.
- standard window min days: `60`
- fallback window min days: `20`
- sample_count=0 sessions are invalid and must not be used for pruning.
