# Purpose

MeeMee's continuous walkforward research is currently too slow to search enough configurations before the gate conditions are met. Users need the research loop to reject obviously bad candidates much earlier while keeping the final production gate unchanged. After this change, the worker will run a short probe first, skip configurations that are already certain to fail the drawdown gate, and reserve full 29-window walkforward runs for the few candidates that survive the probe.

The change is observable in two ways. First, `tmp/walkforward_research/progress_isolated.jsonl` will start recording lightweight prune events before full runs. Second, the elapsed time between worker iterations will drop because most candidates will stop after one or two windows instead of simulating all windows.

# Repo Orientation

The worker lives in `app/backend/tools/walkforward_research_worker.py`. It currently calls `app/backend/services/strategy_backtest_service.py` for every full walkforward run, every gate run, and every daily research snapshot. The service stores full reports in DuckDB via `strategy_walkforward_runs`, while the worker appends operational progress to JSONL files under `tmp/walkforward_research/`.

This speedup keeps the final gate semantics unchanged. Only the exploration flow changes: the worker runs a dry-run probe using the same simulation engine, and only when that probe does not prove certain failure does it execute the full persisted walkforward.

# Assumptions

The research worker continues to use the isolated research database under `tmp/research_data/stocks.duckdb`. The production UI and production DB must remain unaffected. The standard gate stays `OOS損益>=0`, `PF>=1.05`, `勝ち窓比>=0.40`, `WorstDD>=-0.12`.

# Progress

- [x] Measured current hotspots and confirmed `_simulate()` dominates runtime.
- [x] Queried the latest stored walkforward runs and verified that the recent 100 runs all become drawdown-gate failures by window 1 or 2.
- [x] Add optional probe controls to `run_strategy_walkforward()` so the worker can stop after a small number of windows and report why it stopped.
- [x] Switch the worker to `probe -> full run` execution and log prune events separately from full completions.
- [x] Verify on the isolated DB that prune events appear quickly for rejected candidates and that the worker restarts cleanly on the isolated DB.

# Surprises & Discoveries

Recent saved reports show that drawdown is the earliest impossible gate in practice. Evidence from the latest 100 saved `strategy_walkforward_runs`:

    COUNT 100
    Counter({1: 99, 2: 1})

The tuple above means 99 runs crossed below the `WorstDD >= -0.12` gate on the first walkforward window, and the last one crossed it on the second window.

Profiling one representative run with `max_codes=250` showed:

    total_sec ~= 100.7
    sim_sec ~= 90.5 across 58 simulate calls
    build_event_block_sec ~= 1.6
    prepare_features_sec ~= 5.4

This makes early rejection much more valuable than micro-optimizing data loading first.

# Decision Log

2026-03-07: Chose a fail-fast probe instead of a deep `_simulate()` rewrite because the stored evidence shows almost every candidate is already disqualified within the first two windows. This keeps the production gate unchanged and should deliver a large speedup with smaller risk.

2026-03-07: Kept the final full walkforward path unchanged and additive. The worker now treats the 2-window probe as a cheap filter only; any candidate that survives still goes through the original persisted run, gate, and snapshot flow.

# Milestones

## Milestone 1: Probe-capable walkforward service

Extend `run_strategy_walkforward()` in `app/backend/services/strategy_backtest_service.py` with additive optional controls that limit the number of executed windows and optionally stop once the current worst drawdown is already below a provided threshold. The report must expose whether it was truncated and why, so the worker can distinguish a probe rejection from a full evaluation.

Verification is a dry-run command from `C:\work\meemee-screener` that executes a short walkforward and prints a report containing `summary`, `windows`, and a truncation reason.

## Milestone 2: Two-stage worker

Update `app/backend/tools/walkforward_research_worker.py` so each iteration first runs a 2-window dry-run probe. When the probe proves `WorstDD` already fell below `-0.12`, append a `run_pruned` event to the progress JSONL and skip the expensive full run. Otherwise proceed with the current full persisted walkforward, gate, snapshot, and best-run handling.

Verification is a smoke execution with `--max-runs 1` or `2` that produces either a fast `run_pruned` event or a normal `run_complete` event in the temporary JSONL.

## Milestone 3: Restart isolated worker and confirm throughput

Restart the long-running isolated worker against `tmp/research_data/stocks.duckdb` and confirm it writes new events to `tmp/walkforward_research/progress_isolated.jsonl`. Compare elapsed times qualitatively by observing that prune events arrive much faster than previous ~100-second full runs.

# Outcomes & Retrospective

Implemented `max_windows` and `stop_on_oos_worst_max_drawdown_below` in `run_strategy_walkforward()` and switched the research worker to `probe -> full run`. A direct dry-run probe on the isolated research DB finished in about `11.8s`, versus about `100.7s` for the previous full dry-run. After restarting the long-lived isolated worker, the first new event was:

    event=run_pruned
    elapsed_sec=16.814
    reason=oos_worst_max_drawdown_below_threshold

This confirms the worker is now spending seconds, not minutes, on obviously bad configurations. Remaining work, if needed later, is deeper optimization for the smaller number of candidates that survive the probe and still require full walkforward execution.
