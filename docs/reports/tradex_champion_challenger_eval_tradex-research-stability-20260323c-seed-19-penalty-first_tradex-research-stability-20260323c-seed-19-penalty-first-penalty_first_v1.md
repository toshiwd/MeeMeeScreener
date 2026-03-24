# TRADEX Champion / Challenger Evaluation

- family_id: `tradex-research-stability-20260323c-seed-19-penalty-first`
- baseline_run_id: `tradex-research-stability-20260323c-seed-19-penalty-first-baseline`
- candidate_run_id: `tradex-research-stability-20260323c-seed-19-penalty-first-penalty_first_v1`
- report_path: `C:/work/meemee-screener/docs/reports/tradex_champion_challenger_eval_tradex-research-stability-20260323c-seed-19-penalty-first_tradex-research-stability-20260323c-seed-19-penalty-first-penalty_first_v1.md`
- evaluation_window_id: `f41c21f21ed9df6d`
- regime_tag: `multi_regime`
- baseline_method_title: `現行ランキング`
- candidate_method_title: `減点優先型`
- candidate_method_family: `penalty-first`
- promote_ready: `True`
- promote_reasons: `none`

## Definitions

- champion: 現行ランキング
- challenger: readiness / liquidity / regime / missing penalty を加味した 1 案
- selection_summary は proxy 診断であり、独立 backtest ではない
- MeeMee への自動反映はしない。昇格は手動のみ

## Method

- champion_method: `現行ランキング`
- challenger_method: `減点優先型`
- challenger_family: `penalty-first`
- challenger_thesis: `欠損と未解決を先に強く罰して、上位候補を締める。`

## Thresholds

- dd_max_delta: `0.005`
- monthly_capture_min_delta: `0.0`
- monthly_improvement_min_rate: `0.6`
- top10_mean_min_delta: `-0.002`
- top5_liquidity_min_delta: `-0.0`
- top5_mean_min_delta: `0.0`
- top5_median_min_delta: `0.0`
- turnover_max_delta: `0.1`
- worst_regime_min_delta: `-0.005`
- zero_pass_months_max_delta: `0.0`

## Aggregate

| metric | champion | challenger | delta |
| --- | ---: | ---: | ---: |
| top5_ret20_mean | -0.0129 | -0.0129 | 0.0000 |
| top5_ret20_median | -0.0129 | -0.0129 | 0.0000 |
| top10_ret20_mean | -0.0129 | -0.0129 | 0.0000 |
| top10_ret20_median | -0.0129 | -0.0129 | 0.0000 |
| monthly_top5_capture_mean | 1.0000 | 1.0000 | 0.0000 |
| zero_pass_months | 0.0000 | 0.0000 | 0.0000 |
| dd | 0.0905 | 0.0905 | 0.0000 |
| turnover | 0.0000 | 0.0000 | 0.0000 |
| liquidity_fail_rate | 1.0000 | 1.0000 | 0.0000 |
| window_win_rate | 1.0000 | 1.0000 | 0.0000 |

## Windows

| window | regime | days | champion top5 mean | challenger top5 mean | promote |
| --- | --- | ---: | ---: | ---: | --- |
| up:1326758400:1333324800 | up | 54 | -0.0306 | -0.0306 | true |
| down:1225324800:1229644800 | down | 35 | 0.0143 | 0.0143 | true |
| flat:941155200:947030400 | flat | 44 | 0.0000 | 0.0000 | false |

## Notes

- champion / challenger の比較は同一 universe・同一期間・同一約定条件・同一 top-K で行う
- shadow gate は観測専用であり、publish/adopt/compare の判定には干渉しない
- MeeMee にはまだ反映しない
- 残余リスク: window 抽出が market_regime_daily の品質に依存する
