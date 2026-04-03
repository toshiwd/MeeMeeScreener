# TRADEX Rebound Full Validation

## Summary
- dataset_id: `monthly-event-meemee-registered-sample100-v12`
- decision: `adopt_soft_bonus_only`
- recommended_policy: `soft_bonus_only`
- wall_clock_seconds: `0.02`

## Lock Check
- status: `ok`
- db_path: `C:/db/stocks.duckdb`
- blocking_processes: `0`

## Monitor
- baseline_variant: `baseline_live`
- candidate_variant: `soft_bonus_only`
- baseline_days_with_bonus: `2`
- candidate_days_with_bonus: `4`
- days_with_bonus_delta_vs_baseline: `2`
- max_entry_rank_changed_count: `5`

## Diagnosis
- primary_failure_axis: `turnover`
- base_current.total_return_pct: `-20.0`
- holdings_1.total_return_pct: `-4.0`
- holdings_2.total_return_pct: `-9.0`
- turnover_tight.total_return_pct: `-15.0`
- gate_disabled_for_diagnosis.total_return_pct: `-18.0`

## Decision Reason
- monitor: `soft_bonus_only increased bonus-active days over baseline while keeping rank changes bounded.`
- diagnosis: `primary_failure_axis=turnover does not block ranking policy adoption first.`
