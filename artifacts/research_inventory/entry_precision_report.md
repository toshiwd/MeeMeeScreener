# Entry Precision Audit

- session_id: `entry-precision-20260419-090221`
- baseline_id: `current_rule_trade_gate_baseline`
- challenger_id: `precision_first_stricter_gate_v1`
- decision: `hold`

## Current State
- confirmed: buy_sell_neutral_gate_exists_in_rankings_cache, entry_score_is_distinct_from_entry_qualified, trade_priority_is_stage_b_after_gate, month_end_rolling_oos_is_fixed, json_is_the_authoritative_artifact_layer
- provisional: exact_false_neutral_definition_is_a_research_proxy, early_confirmation_and_mae_family_definitions_are_study heuristics, challenger_is_stricter_but_not_retrained

## Baseline
- long hit rate: `0.6438848920863309`
- short hit rate: `0.4444444444444444`
- top5 changed members: `17`
- top10 changed members: `25`

## Decision
- `hold`
- reasons: top5_branching_observed, bad_pick_removal_observed, sample_or_regime_stability_insufficient_for_keep

## Risks
- monthly_oos_sample_is_small_for_regime_specific_conclusions, bucket_reason_codes_are_heuristic_proxies_for_failure_analysis, coverage_curve_sweeps_entry_score_only_and_does_not_retrain_the_gate, no_model_retraining_was_performed
