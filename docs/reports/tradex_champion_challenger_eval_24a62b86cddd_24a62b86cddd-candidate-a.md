# TRADEX Champion / Challenger Evaluation

- family_id: `24a62b86cddd`
- baseline_run_id: `24a62b86cddd-baseline`
- candidate_run_id: `24a62b86cddd-candidate-a`
- report_path: `C:/work/meemee-screener/docs/reports/tradex_champion_challenger_eval_24a62b86cddd_24a62b86cddd-candidate-a.md`
- evaluation_window_id: `df57cac1ce9b78d8`
- regime_tag: `multi_regime`
- promote_ready: `False`
- promote_reasons: `evaluation_window_coverage_incomplete, evaluation_windows_unavailable, market_regime_daily_unavailable:CatalogException, no_evaluation_windows, regime_rows_empty`

## Definitions

- champion: 現行ランキング
- challenger: readiness / liquidity / regime / missing penalty を加味した 1 案
- selection_summary は proxy 診断であり、独立 backtest ではない
- MeeMee への自動反映はしない。昇格は手動のみ

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
| top5_ret20_mean | 0.0450 | 0.0450 | 0.0000 |
| top5_ret20_median | 0.0450 | 0.0450 | 0.0000 |
| top10_ret20_mean | 0.0450 | 0.0450 | 0.0000 |
| top10_ret20_median | 0.0450 | 0.0450 | 0.0000 |
| monthly_top5_capture_mean | 1.0000 | 0.7000 | -0.3000 |
| zero_pass_months | 0.0000 | 0.0000 | 0.0000 |
| dd | 0.0000 | 0.0000 | 0.0000 |
| turnover | 0.0000 | 0.6667 | 0.6667 |
| liquidity_fail_rate | 0.0000 | 0.0000 | 0.0000 |
| window_win_rate | 0.0000 | 0.0000 | 0.0000 |

## Windows

| window | regime | days | champion top5 mean | challenger top5 mean | promote |
| --- | --- | ---: | ---: | ---: | --- |

## Notes

- champion / challenger の比較は同一 universe・同一期間・同一約定条件・同一 top-K で行う
- shadow gate は観測専用であり、publish/adopt/compare の判定には干渉しない
- MeeMee にはまだ反映しない
- 残余リスク: window 抽出が market_regime_daily の品質に依存する
