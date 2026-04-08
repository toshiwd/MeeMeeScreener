# TRADEX Evaluation

TRADEX evaluation is swing-width evaluation, not only end-point prediction.

The harness must measure whether a candidate can:

- capture trend continuation
- capture range movement on both sides
- react to panic rebound setups
- support partial exits
- support adds after probe entries
- avoid noisy or random setups

## Victory metrics

Persist these metrics in authoritative artifacts:

- `hold_end_return_20d`
- `mfe_20d`
- `mae_20d`
- `win_flag_hold_end`
- `win_flag_mfe`
- `addability_score`
- `trimability_score`
- `opportunity_count`
- `avg_holding_days`
- `max_drawdown`

## Evaluation outputs

Evaluation summaries must also report:

- `top-K uplift`
- `bad-pick removal`
- `changed_top5_members_count`
- `changed_top10_members_count`
- `selection_divergence_reason`
- `opportunity_count`
- `avg_holding_days`
- `max_drawdown`
- `hold_end_return_20d`
- `mfe_20d`
- `mae_20d`

`addability_score` and `trimability_score` are provisional and may be nullable in v1.1.
