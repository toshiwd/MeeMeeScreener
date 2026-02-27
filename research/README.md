# Research Pipeline (Isolated)

`research` is a standalone CLI for monthly Top20 long/short generation.
It does not import `app/` modules.

- Internal outputs: `research_workspace/`
- Public snapshots only: `published/`

## CLI

```powershell
python -m research --help
```

Main commands:

```powershell
python -m research ingest --daily-csv <path> --universe-dir <dir> [--calendar-csv <path>] [--snapshot-id <id>]
python -m research build_features --asof YYYY-MM-DD [--snapshot-id <id>] [--workers N] [--chunk-size M]
python -m research build_labels --asof YYYY-MM-DD [--snapshot-id <id>] [--workers N] [--chunk-size M]
python -m research train --asof YYYY-MM-DD --run_id <id> [--snapshot-id <id>] [--workers N] [--chunk-size M]
python -m research evaluate --run_id <id>
python -m research publish --run_id <id> [--allow-non-pareto] [--publish-phases test,inference]
python -m research loop --asof YYYY-MM-DD [--snapshot-id <id>] [--cycles N] [--workers N] [--chunk-size M]
```

`evaluate` writes:

- `research_workspace/runs/<run_id>/evaluation.json` (overall metrics + Pareto status)
- `research_workspace/runs/<run_id>/evaluation_monthly.csv` (asof-month metrics by side and phase)

Evaluation policy:

- `metrics_by_phase.valid` and `metrics_by_phase.test` are both computed.
- Pareto and publish gating use `selection_phase` metrics (`valid` preferred, fallback to `test`).

## Publish Gate

`publish` requires `evaluation.json` and checks Pareto status by default.

- default: only `is_pareto = true` runs can be published
- override: `--allow-non-pareto`

By default, publish includes monthly Top20 rows from `test` and `inference` phases.
Use `--publish-phases` to change this.

## Scheduler Helper (Windows)

Use:

```powershell
scripts\research_loop_publish.ps1 -AsOf 2026-02-28 -Cycles 5 -Workers 4 -ChunkSize 120 -LowPriority
```

This script runs `loop`, selects Pareto-passing runs, and publishes the best one.

## Parallel Build

`build_features` and `build_labels` support code-chunk parallel processing:

- `--workers`: process count
- `--chunk-size`: codes per worker task

Workers write temporary chunk CSV files.
The main process is the single writer and merges into final CSV once.

## Fixed Defaults

`research/default_config.json`:

- `tp_long = 0.10`
- `tp_short = 0.10`
- `cost.enabled = true`, `cost.rate_per_side = 0.001`
- `stop_loss.enabled = false`, `stop_loss.rate = 0.0`

These values are recorded into each run manifest.

## Label Boundary

`build_labels` resolves the exit horizon using the snapshot's market month-end calendar:

- entry at month-end `t`
- exit path window is `t < date <= next market month-end after t`

This avoids calendar month-end mismatch on weekends/holidays.

## Walkforward Split

Split is fixed-window:

- train: `train_years * 12` months
- valid: `valid_months`
- test: `test_months`

If labeled history is shorter than required total months, `train` fails with a clear error.
