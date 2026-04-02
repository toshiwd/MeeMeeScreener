# Signal Quality Report

- generated_at: 2026-04-02T14:36:52.250926+00:00

## Buy
- qualified_decisions: 11833
- directional_hit_rate_30: 58.2%
- average_directional_return_30: 2.4%
- lift_vs_same_date_universe_30: 0.0%
- median_days_to_max_favorable_30: 17.0
- median_days_to_max_adverse_30: 11.0

## Sell
- qualified_decisions: 1103
- directional_hit_rate_30: 41.3%
- average_directional_return_30: -1.0%
- lift_vs_same_date_universe_30: -0.4%
- median_days_to_max_favorable_30: 9.0
- median_days_to_max_adverse_30: 13.0

## Ranking
- up_average_directional_return_30: 2.4%
- up_directional_win_rate_30: 54.8%
- up_median_days_to_max_favorable_30: 16.0
- down_average_directional_return_30: -0.2%
- down_directional_win_rate_30: 46.8%
- down_median_days_to_max_favorable_30: 12.0

## Buy Failure Top Reasons
- completed_clean: count=1561 avg30=2.0% win=54.8%
- opposite_tone_flip:sell: count=35 avg30=-7.4% win=8.6%
- opposite_signal:sell: count=2 avg30=-20.5% win=0.0%

## Buy By Regime
- risk_on_trend: count=5888 avg30=2.4% lift=0.1%
- neutral_range: count=4050 avg30=2.9% lift=0.4%
- risk_on_range: count=1580 avg30=1.5% lift=-0.6%
- risk_off_trend: count=264 avg30=1.9% lift=-1.5%
- capitulation_rebound: count=48 avg30=5.5% lift=-4.2%

## Profit Timing Patterns
- buy
  - 10d型: count=4405 share=37.3% 10d=-2.6% 20d=-5.5% 30d=-5.3%
  - 20d型: count=2480 share=21.0% 10d=3.2% 20d=4.5% 30d=2.0%
  - 30d型: count=4913 share=41.6% 10d=1.8% 20d=5.6% 30d=9.0%
- sell
  - 10d型: count=597 share=54.2% 10d=-1.4% 20d=-4.1% 30d=-4.6%
  - 20d型: count=248 share=22.5% 10d=1.1% 20d=2.4% 30d=-0.3%
  - 30d型: count=256 share=23.3% 10d=1.1% 20d=3.8% 30d=5.8%

## Sell Subset Comparison
- breakdown only: count=121 hit=37.8% return=-1.8% lift=-1.2% break=27.3%
- repeated only: count=0 hit=-- return=-- lift=-- break=--
- breakdown + repeated: count=0 hit=-- return=-- lift=-- break=--
- weak regime only: count=177 hit=38.3% return=-1.1% lift=-1.2% break=43.5%

## Sell v1 vs v2 (primary)
- primary_horizon: 10
- qualified_decisions: base 1103 -> target 101 (delta -1002.0)
- directional_hit_rate: base 47.4% -> target 50.5% (delta 3.1%)
- average_directional_return: base -0.2% -> target 0.3% (delta 0.5%)
- lift_vs_same_date_universe: base -0.1% -> target 0.4% (delta 0.5%)
- campaign_win_rate: base 45.1% -> target 44.9% (delta -0.2%)
- campaign_average_final_directional_return: base -0.5% -> target -1.3% (delta -0.8%)

## Leakage Audit
- basis_future_source_as_of_count: 0
- basis_future_pred_dt_count: 0
- prohibited_payload_count: 0
- latest_signal_parity_available: True
- latest_signal_parity_mismatch_samples: 0
- label_policy_audit_available: True
- external_replay_audit_available: True
