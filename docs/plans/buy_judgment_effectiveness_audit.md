# Buy Judgment Effectiveness Audit

## Goal
Audit historical buy judgments using confirmed data only, then classify the buy surface into keep / weaken / strengthen / gate_with_extra_condition / drop decisions.

## Confirmed Sources
- Frozen replay snapshot: `G:\Tradex\data\external_analysis\source_snapshots\historical_replay_replay_full_history_gtradex_20260315_0352_20260315T035608584441Z.duckdb`
- Horizon labels: `G:\Tradex\data\external_analysis\label.duckdb`
- Inventory: `C:\work\meemee-screener\artifacts\research_inventory\research_inventory.json`
- Gate decision: `C:\work\meemee-screener\artifacts\research_inventory\candidate_gate_decision.json`

## Scope
- Exact realized buy surface in the confirmed cohort: `MA20_RECLAIM_INITIAL`
- Provisional contextual buckets used for post-hoc effectiveness: `pullback / rebound`, `MA recovery`, `weak liquidity continuation`, `regime mismatch`, `late/stretched entry`, `early reversal / failed breakdown recovery`, `box breakout`
- 5d and 10d forward returns are derived from the frozen daily bars.
- 20d forward returns come from `label_daily_h20` when available and fall back to frozen daily bars only when the forward window exists in the snapshot.

## Non-Scope
- No overwrite/provenance implementation work unless a new regression is reproduced.
- No boundary-aware compare execution.
- No MeeMee UI, publish, or promote_ready changes.
- No model retraining in this first audit round.
- No redesign of `weak_recovery` or `breakout_failure` outside the audit framing.

## Outputs
- `C:\work\meemee-screener\artifacts\research_inventory\buy_judgment_effectiveness_audit.json`
- `C:\work\meemee-screener\artifacts\research_inventory\buy_judgment_condition_revision_plan.json`

## Verification
- JSON load validation for all new artifacts.
- Confirm the audit corpus is confirmed-data only.
- Confirm 5d, 10d, and 20d fields are populated where horizons are available.
- Confirm the revision plan is evidence-based.
- Confirm no MeeMee UI files were modified.
- Confirm no overwrite/provenance regression was introduced.
- Confirm boundary-aware compare was not executed.

## Decision Rule
Prioritize subtraction before addition.
Use gates and vetoes before broad score inflation.
