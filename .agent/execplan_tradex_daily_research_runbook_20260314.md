# Tradex daily research runbook

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

If `.agent/PLANS.md` exists in this repository, this ExecPlan must be maintained in accordance with it.

## Purpose / Big Picture

Tradex already has the internal pieces for candidate scoring, similarity research, challenger shadow evaluation, and AI Research review panels. What is still missing is the practical daily entry point that runs those pieces in the correct order and leaves behind a compact report a human can act on the same day.

After this change, a user can run one command and get a complete daily research cycle: candidate publish, similarity publish, challenger shadow, and a summary that matches what AI Research shows. They can observe it working by running the new CLI or PowerShell wrapper and seeing a JSON/text report with the current publish, top action queue items, and promotion review state.

## Progress

- [x] (2026-03-14 16:05Z) Read `.agent/PLANS.md`, `external_analysis/__main__.py`, nightly runtime modules, and the current `analysis_bridge.reader` internals.
- [x] (2026-03-14 16:22Z) Added a new `external_analysis.runtime.daily_research` module that orchestrates candidate, similarity, challenger, and post-run report generation.
- [x] (2026-03-14 16:28Z) Added a new `python -m external_analysis daily-research-run` CLI entry point with optional report outputs.
- [x] (2026-03-14 16:35Z) Added `tools/run_tradex_daily_research.ps1` as the daily operator wrapper that writes log/report files.
- [x] (2026-03-14 16:48Z) Added focused runtime/CLI tests and verified backend import plus frontend build stayed green.

## Surprises & Discoveries

- The internal AI Research summaries already existed in `app/backend/services/analysis_bridge/reader.py`; the gap was not analytics logic but the missing orchestration surface.
- `analysis_bridge.reader` reads DBs through app config defaults, so the runbook needed a temporary environment override layer to make explicit DB path arguments produce matching reports.

## Decision Log

- Decision: reuse `analysis_bridge.reader` for report generation instead of duplicating summary SQL in a second place. Reason: the CLI/runbook should show the same daily summary and action queue that the UI shows.
- Decision: keep the runbook output intentionally compact. Reason: the user explicitly wants summary retention and to avoid wasting storage on bulky artifacts.
- Decision: make `as_of_date` optional and resolve the latest available date from `daily_bars`. Reason: a daily operator command should work without manual date lookup.

## Implementation Notes

Create `external_analysis/runtime/daily_research.py`. This module owns the operational flow and should stay free of frontend code. It should:

1. Resolve `as_of_date` from the source DB when omitted.
2. Run:
   - `run_nightly_candidate_pipeline`
   - `run_nightly_similarity_pipeline`
   - `run_nightly_similarity_challenger_pipeline`
3. Build a compact report after the pipelines finish. The report should include:
   - `publish_id`, `as_of_date`, and per-step statuses
   - `daily_summary`
   - `action_queue`
   - `promotion_review`
   - `trend_watch`
   - `combo_trend_watch`
4. Optionally write JSON and text report files when file paths are supplied.

To keep the report aligned with AI Research, temporarily set environment variables while reading summaries:

- `STOCKS_DB_PATH`
- `MEEMEE_RESULT_DB_PATH`
- `MEEMEE_DATA_DIR` when the DB paths imply a shared `<data>/external_analysis` directory

Update `external_analysis/__main__.py` to register a new `daily-research-run` command. It must accept optional DB paths, optional `as_of_date`, optional `publish_id`, and optional report output paths.

Add `tools/run_tradex_daily_research.ps1` as the user-facing wrapper. It should:

- default to the repo root
- create `tmp/` if missing
- default report paths under `tmp/`
- call `python -m external_analysis daily-research-run`
- tee a short log to disk and stdout

## Verification

Run the focused tests from the repo root:

    python -m pytest tests/test_external_analysis_daily_research.py tests/test_phase2_slice_f_nightly_pipeline.py tests/test_phase3_similarity_nightly_pipeline.py

Then verify imports still succeed:

    python -c "import app.backend.main"

Finally ensure the frontend still builds because AI Research depends on the same report helpers:

    cd app/frontend
    npm run build

Expected signals:

- the new pytest file passes
- backend import exits without traceback
- frontend build completes successfully

## Outcomes & Retrospective

- The repo now has a daily research runbook entry point instead of requiring manual sequencing of multiple nightly commands.
- The report path is intentionally compact and summary-only; deeper retention stays inside existing ops/result DB tables.
- Remaining future work is approval workflow automation, not daily execution plumbing.
