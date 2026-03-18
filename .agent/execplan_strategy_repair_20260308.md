# Purpose

MeeMee's current research loop is exploring many strategies that either never trade or consistently lose through excessive drawdown. Users need the worker to spend time on the most plausible strategy family, reduce risk per configuration, and stop generating low-value artifacts that slowly consume SSD space.

After this change, the worker will focus on the only family that is currently near viable (`long_breakout_p2`), remove dead short-only configurations, reduce exposure by default, and cap the size of operational history so long-running research does not keep growing forever. The change is observable in `tmp/walkforward_research/progress_isolated.jsonl`, which should show fewer meaningless full runs and more fast prune decisions, and in the research DB / log files, which should stop unbounded growth.

# Evidence

Saved walkforward runs in `tmp/research_data/stocks.duckdb` showed three concrete issues.

First, `short_failed_high_p1` is a dead strategy. Across the latest 300 stored full runs:

    {"short_failed_high_p1_only": {"count": 53, "zero_trade": 53}}

Second, both-side exploration is worse than long-only breakout exploration. Aggregating recent stored runs:

    both  avg_pnl ~= -7.57, avg_pf ~= 0.92, avg_dd ~= -1.46
    long  avg_pnl ~= -4.41, avg_pf ~= 0.95, avg_dd ~= -1.41

Third, when re-evaluating promising long-breakout variants directly, reducing exposure improved results materially:

    baseline_bestish          pnl=-1.55 pf=1.087 dd=-1.264
    no_pyramid_one_pos        pnl=-1.46 pf=1.153 dd=-0.805
    no_pyramid_one_pos_strict pnl=-1.07 pf=1.148 dd=-0.447
    two_pos_no_pyramid        pnl=-0.38 pf=1.023 dd=-0.739

This does not pass the production gate yet, but it shows the current failure mode is largely over-exposure rather than complete absence of signal.

# Repo Orientation

The strategy simulation logic is in `app/backend/services/strategy_backtest_service.py`. The continuous research worker is in `app/backend/tools/walkforward_research_worker.py`. The worker appends events to `tmp/walkforward_research/progress_isolated.jsonl` and stores the best seen full run in `tmp/walkforward_research/best_isolated.json`.

The worker already uses a two-window fail-fast probe. This plan builds on that by changing the sampled strategy family and adding retention so the same files and tables do not grow forever.

# Progress

- [x] Collected run-level evidence from stored walkforward results.
- [x] Re-ran targeted long-breakout configurations to identify exposure reduction as the most promising correction.
- [ ] Replace the worker sampling space so it no longer explores dead short-only setups and instead samples low-exposure long-breakout variants.
- [ ] Add lightweight retention for JSONL progress history and stored walkforward reports so artifacts stop growing without bound.
- [ ] Restart the isolated worker and confirm it continues under the new strategy family.

# Surprises & Discoveries

The latest persisted full runs that survived the probe often did so because they generated zero trades, not because they were promising. Those runs still created full DB reports and gate rows, which adds noise and storage without improving research quality.

The fastest practical improvement was not a new signal but removing pyramiding and reducing simultaneous positions. That sharply improved profit factor and drawdown while keeping the same breakout family.

# Decision Log

2026-03-08: Decided not to continue broad both-side search for now. Stored evidence shows short exploration is either dead (`short_failed_high_p1`) or net negative, while long breakout is the only family close enough to justify more compute.

2026-03-08: Chose retention over one-off deletion scripts. The user wants long-running research, so the correct fix is to stop unbounded growth during normal operation rather than manually cleaning artifacts repeatedly.

# Milestones

## Milestone 1: Worker strategy repair

Update `app/backend/tools/walkforward_research_worker.py` so sampled configs are centered on `allowed_sides='long'` and `allowed_long_setups=('long_breakout_p2',)`. Default exposure should be lower than today: no add-on entries by default, `max_positions` reduced to `1` or `2`, `max_new_entries_per_day=1`, and stricter long filters such as `require_decision_for_long`, `require_ma_bull_stack_long`, volume confirmation, and distance-to-MA limits sampled around the best evidence-backed values.

Acceptance is a worker smoke run that still produces `run_pruned` and `run_complete` events, but no longer emits full runs for the dead `short_failed_high_p1` family.

## Milestone 2: Artifact retention

Add retention in the worker or service layer so `progress_isolated.jsonl` is capped to a fixed recent window and walkforward report tables stop growing unbounded. Keep the latest history needed for debugging plus the current best payload file. The implementation must be additive and safe to run repeatedly.

Acceptance is a smoke run followed by inspecting the retained file/table counts and confirming that old entries are pruned while recent entries remain readable.

## Milestone 3: Resume research

Restart the isolated worker against `tmp/research_data/stocks.duckdb` and confirm the new iteration stream appears in `tmp/walkforward_research/progress_isolated.jsonl`. Observe at least one fresh event generated under the new strategy family.

# Outcomes & Retrospective

Pending implementation.
