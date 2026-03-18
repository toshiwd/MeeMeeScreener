# Tradex Tag Validation Rollup

## Purpose

This change makes the new Tradex state evaluation behavior inspectable. After this work, a developer can run the nightly candidate pipeline and inspect per-tag validation rollups that show how many observations a tag produced, how often it entered, what the average expectancy and adverse move looked like, and which recent/worst failures belong to that tag. This matters because the current state evaluation logic already depends on `side + holding_band + strategy_tags`, but there is no internal artifact that proves whether those tags are useful or noisy.

The working proof is simple: run the existing candidate baseline flow, then query the new internal rollup endpoint or the ops database and observe rows grouped by `side`, `holding_band`, and `strategy_tag` with metrics and compact failure samples.

## Repo Orientation

The implementation lives in three connected areas. `external_analysis/models/state_eval_baseline.py` builds and persists state-evaluation outputs during nightly candidate generation. `external_analysis/ops/ops_schema.py` and `external_analysis/ops/store.py` define and retain internal ops tables. `app/backend/api/routers/analysis_bridge.py` and `app/backend/services/analysis_bridge/reader.py` expose read-only backend responses. The public publish contract stays unchanged because the new report is internal and reads from the ops database, not the result database.

## Plan

First, add a new ops table for tag validation rollups. The table stores one row per `publish_id + side + holding_band + strategy_tag` and carries the aggregate metrics needed to judge whether a tag is healthy.

Next, extend `persist_state_eval_shadow` so that after shadow rows and failure samples are written, the same labeled data is folded into per-tag aggregates. The aggregate should count observations, labeled observations, enter decisions, and compute expectancy, adverse move, large-loss rate, win rate, teacher-alignment mean, plus compact latest/worst failure examples.

Then, add retention for the new table so the research database does not grow without bound.

Finally, add a read-only internal API that returns the latest published tag rollups for inspection. This is not part of the MeeMee public publish contract and must stay isolated from the existing public tables.

## Implementation Details

Edit `external_analysis/ops/ops_schema.py` to add a new table named `external_state_eval_tag_rollups`. Use a stable primary key such as `rollup_id`. Store at least `publish_id`, `as_of_date`, `side`, `holding_band`, `strategy_tag`, `observation_count`, `labeled_count`, `enter_count`, `wait_count`, `skip_count`, `expectancy_mean`, `adverse_mean`, `large_loss_rate`, `win_rate`, `teacher_alignment_mean`, `failure_count`, `readiness_hint`, `latest_failure_examples`, `worst_failure_examples`, `summary_json`, and `created_at`. Add the table to `OPS_TABLES`.

Edit `external_analysis/ops/store.py` to apply retention to the new rollup table. Keep the default comfortably above one publish cycle history while still bounded.

Edit `external_analysis/models/state_eval_baseline.py` in `persist_state_eval_shadow`. Reuse the already-loaded labels, champion rows, challenger rows, and teacher profile so there is no second pass to other databases. Build one in-memory accumulator per `side + holding_band + strategy_tag`, fold each champion row into all of its tags, then write the aggregate rows after readiness and failure samples are persisted. The aggregation rules are:

- `observation_count` counts all tag appearances from champion rows.
- `labeled_count` counts tag appearances with both `expected_return` and `adverse_move`.
- `enter_count`, `wait_count`, and `skip_count` count champion decisions for that tag.
- `expectancy_mean`, `adverse_mean`, `win_rate`, and `teacher_alignment_mean` are computed only from labeled rows.
- `large_loss_rate` uses the side-aware adverse threshold already defined in this file.
- `failure_count` counts labeled rows where the expectancy is non-positive or the adverse move breaches the side-aware threshold.
- `latest_failure_examples` stores up to the ten newest compact failure objects.
- `worst_failure_examples` stores up to the ten highest-adverse compact failure objects.
- `readiness_hint` should be a simple human-friendly classification such as `promotable`, `needs_samples`, or `risk_heavy` based on sample count, expectancy sign, and large-loss rate.

Edit `app/backend/services/analysis_bridge/reader.py` to add a small read-only helper that opens the ops database, finds the latest published `publish_id`, and returns tag rollups for that publish with optional `side`, `strategy_tag`, and `limit` filters. The payload should mirror the existing reader functions: it must degrade if there is no latest publish, and otherwise return `publish_id`, `as_of_date`, `freshness_state`, and `rows`.

Edit `app/backend/api/routers/analysis_bridge.py` to add a route such as `/api/analysis-bridge/internal/state-eval-tags` that calls the new reader helper.

## Validation

From `C:\work\meemee-screener`, run:

    python -m pytest tests/test_external_analysis_candidate_baseline.py
    python -m pytest tests/test_analysis_bridge_api.py

The candidate baseline test should prove that tag rollup rows are materialized into the ops database with meaningful counts and JSON failure buckets. The API test should prove that the new internal endpoint returns rows for the latest publish and that the existing public endpoints still behave the same.

## Progress

- [x] Investigated the current state-evaluation persistence flow and identified the minimal insertion point.
- [x] Added ops schema and retention for tag validation rollups.
- [x] Persisted per-tag validation rollups from `persist_state_eval_shadow`.
- [x] Added a read-only internal API for inspecting the rollups.
- [x] Verified with targeted pytest runs.

## Surprises & Discoveries

- 2026-03-14: The ops database path resolves from `app.core.config.config.DATA_DIR`, not from a dedicated ops-db environment variable. Tests must either patch `DATA_DIR` or pass explicit paths into ops helpers.

## Decision Log

2026-03-14: Keep the new report internal to the ops database and expose it through a separate read-only endpoint instead of changing the publish contract. This preserves the existing MeeMee boundary while still making tag quality observable.

## Outcomes & Retrospective

Implemented the internal tag validation layer without changing the publish contract. Nightly state-evaluation runs now materialize per-tag rollups into the ops database, including observation counts, expectancy, adverse metrics, readiness hints, and compact latest/worst failure examples. A read-only backend endpoint was added for inspection, and targeted pytest coverage proves both persistence and retrieval.
