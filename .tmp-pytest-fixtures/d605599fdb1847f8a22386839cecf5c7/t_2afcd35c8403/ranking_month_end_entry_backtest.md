# 月末 仕込み専用選定 バックテスト

- generated_at: 2026-04-03T01:45:20.233812+00:00
- db_path: C:\work\meemee-screener\.tmp-pytest-fixtures\d605599fdb1847f8a22386839cecf5c7\t_2afcd35c8403\stocks.duckdb
- period: 20240131 .. 20240229
- month_end_count: 2
- month_end_mode: rolling_last_n
- direction: up
- round_trip_cost: 0.002

## Verdict
- verdict: watch

## Comparison
- baseline_top10_mean20_net: 0.0962291666666667
- strict_buy_top10_mean20_net: 0.1818333333333334
- strict_buy_state_top10_mean20_net: 0.1818333333333334
- baseline_top10_pf20_net: None
- strict_buy_top10_pf20_net: None
- strict_buy_state_top10_pf20_net: None
- baseline_top10_mean_month_end_net: 0.1449999999999999
- strict_buy_top10_mean_month_end_net: 0.2729999999999999
- strict_buy_state_top10_mean_month_end_net: 0.2729999999999999

## Variant Summary
- baseline: sample=4, days=2, net20_mean=0.096, net20_pf=-, month_end_mean=0.145, watch=2, reject=0, fallback=0
- strict_buy: sample=2, days=2, net20_mean=0.182, net20_pf=-, month_end_mean=0.273, watch=0, reject=0, fallback=0
- strict_buy_state: sample=2, days=2, net20_mean=0.182, net20_pf=-, month_end_mean=0.273, watch=0, reject=0, fallback=0

## Regression Case
- focus_ymd: 20240229
- baseline: sample=2, net20=0.039, month_end=-, watch=1, reject=0, fallback=0
- strict_buy: sample=1, net20=0.076, month_end=-, watch=0, reject=0, fallback=0
- strict_buy_state: sample=1, net20=0.076, month_end=-, watch=0, reject=0, fallback=0

## Monthly Detail
### baseline
- 2024-01-31: n=2, net20=0.153, month_end=0.145, watch=1, reject=0, fallback=0, codes=1001, 1002
- 2024-02-29: n=2, net20=0.039, month_end=-, watch=1, reject=0, fallback=0, codes=2001, 2002

### strict_buy
- 2024-01-31: n=1, net20=0.288, month_end=0.273, watch=0, reject=0, fallback=0, codes=1001
- 2024-02-29: n=1, net20=0.076, month_end=-, watch=0, reject=0, fallback=0, codes=2001

### strict_buy_state
- 2024-01-31: n=1, net20=0.288, month_end=0.273, watch=0, reject=0, fallback=0, codes=1001
- 2024-02-29: n=1, net20=0.076, month_end=-, watch=0, reject=0, fallback=0, codes=2001
