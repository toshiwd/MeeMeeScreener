# Tradex state evaluation and shadow comparison ExecPlan

## Purpose

This change makes Tradex produce a real `state_eval_daily` output instead of leaving the table empty, and it ties that output to the user's own trade history. After this change, nightly candidate runs will publish three-way state decisions (`仕込む`, `待つ`, `見送る`), save internal trade-teacher summaries from exported trade events, and persist champion/challenger shadow comparisons that can be reviewed before any promotion.

The user-visible effect is that the external analysis result DB now contains a public state-evaluation feed alongside candidate and similarity feeds, while MeeMee still reads only the public publish tables. The operator-visible effect is that the ops DB contains teacher-profile rows and readiness summaries that show whether a stricter challenger is safe to promote.

## Progress

- [x] Confirmed existing export, label, candidate, similarity, result, and ops boundaries in the current repo.
- [x] Added public `state_eval_daily` contract fields for side, three-way decision, and reason codes.
- [x] Added a state-evaluation baseline that uses exported trade history as a teacher prior.
- [x] Added shadow comparison persistence for champion vs challenger state decisions.
- [x] Wired nightly candidate runs to save state-evaluation outputs and shadow readiness.
- [x] Finished validation with targeted pytest runs and adjusted expectations for the new public and internal rows.

## Orientation

The work touches four connected areas. `external_analysis/exporter/diff_export.py` decides what raw user-trade evidence reaches Tradex. `external_analysis/models/candidate_baseline.py` already produces candidate and regime public rows, so it is the safest place to add `state_eval_daily`. `external_analysis/models/state_eval_baseline.py` is the new internal scoring layer for three-way decisions and shadow comparison. `external_analysis/ops/ops_schema.py` stores internal evidence that MeeMee must not read.

The public read-only path remains `publish_pointer -> publish_manifest -> public result tables`. The new public table is still `state_eval_daily`. The new internal tables live only in the ops DB.

## Milestone 1

Expand the result and bridge contract so `state_eval_daily` can carry three-way decisions in a stable public shape.

Edit `external_analysis/results/result_schema.py`, `app/backend/services/analysis_bridge/contracts.py`, `app/backend/services/analysis_bridge/reader.py`, and `app/backend/api/routers/analysis_bridge.py`.

The schema must preserve the older `state_action` column for compatibility while adding `side`, `decision_3way`, and `reason_codes`. The bridge must expose a new read-only endpoint that returns those rows without exposing any internal tables.

Acceptance signal:

    pytest tests/test_external_analysis_result_schema.py

The expected observation is that the result DB contains the new columns and the bridge contract still lists only public tables.

## Milestone 2

Make the trade export carry usable teacher signals.

Edit `external_analysis/exporter/diff_export.py`. When the source DB has raw `trade_events`, export those first because they preserve long/short entry actions. When only `position_rounds` exists, keep the current fallback behavior so the change is backward compatible.

Acceptance signal:

    pytest tests/test_external_analysis_diff_export.py

The expected observation is that `trade_event_export` is still generated, and when raw trade events exist their action names survive into `event_type`.

## Milestone 3

Add the state-evaluation baseline and shadow comparison.

Create `external_analysis/models/state_eval_baseline.py`. This module computes a trade-teacher profile from `trade_event_export` and `position_snapshot_export`, builds public champion decisions, builds stricter challenger decisions for shadow only, and writes internal comparison rows into the ops DB.

The teacher profile is intentionally simple in this slice. It counts long-entry and short-entry actions per code and combines that with the latest long/short position bias. The champion and challenger both stay end-of-day only. The challenger is stricter and should only pass readiness when expectancy improves and adverse-move risk does not worsen.

Acceptance signal:

    pytest tests/test_external_analysis_candidate_baseline.py

The expected observation is that candidate baseline runs now write `state_eval_daily`, and the returned payload reports shadow readiness fields.

## Milestone 4

Wire nightly candidate runs to save both the public state decisions and the internal readiness evidence.

Edit `external_analysis/models/candidate_baseline.py`, `external_analysis/runtime/nightly_pipeline.py`, and `external_analysis/__main__.py`.

The candidate baseline already owns the publish transaction for candidate and regime rows, so it should also insert `state_eval_daily` before publishing. The nightly pipeline should pass the ops DB path through so teacher profiles and shadow readiness are saved on every automated run.

Acceptance signal:

    pytest tests/test_external_analysis_candidate_baseline.py

The expected observation is that a nightly-capable run reports non-zero `state_eval_count`, sets `state_eval_shadow_saved`, and keeps the publish manifest consistent with the inserted rows.

## Surprises & Discoveries

- `trade_event_export` originally used `position_rounds` only, which is too lossy for long/short teacher alignment because it drops raw action names.
- `state_eval_daily` already existed in the public schema, but it was only a placeholder with no stable public contract beyond `state_action`.
- The existing nightly candidate pipeline was already the best insertion point for automated validation because it passes through export, label, publish, and ops boundaries in one run.

## Decision Log

- 2026-03-14: Chose to keep `state_action` and add new columns instead of replacing it, to avoid breaking existing read-only callers.
- 2026-03-14: Chose to persist trade-teacher and shadow-comparison evidence in the ops DB rather than the result DB, because MeeMee must not read internal evaluation artifacts.
- 2026-03-14: Chose `trade_events` as the preferred teacher source and `position_rounds` as fallback, because the raw action stream contains the long/short entry intent needed for alignment.

## Outcomes & Retrospective

2026-03-14: The first implementation slice adds an actual state-evaluation public feed and internal shadow-readiness evidence without breaking the existing result DB only contract. Targeted tests for result schema, diff export, and candidate baseline passed. Future slices can now improve the teacher features beyond simple entry-count alignment and connect the published state evaluation to richer UI.
