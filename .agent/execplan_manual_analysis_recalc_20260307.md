# Manual Analysis Recalc

## Purpose / Big Picture

Users need a way to refresh analysis results when chart markers look stale or inconsistent with the per-day decision shown in the analysis panel. After this change, a user can recalculate either the currently displayed daily chart range or the recent rolling window without waiting for background prewarm alone, and the chart will reload the refreshed analysis timeline when the job finishes.

The user-visible proof is simple. Open a detail page, start a recalculation from the chart or analysis panel, observe a running progress message, then see markers and panel snapshots reload automatically when the job ends.

## Progress

- [x] Research the current marker path, the analysis timeline cache, and the analysis backfill job payload.
- [x] Extend backend analysis backfill to support explicit date ranges and force recomputation of existing rows.
- [x] Extend the detail view to refresh the timeline cache after analysis jobs finish.
- [x] Add user-triggered recalculation actions for the visible chart range and the recent 130-day window.
- [ ] Build and run the desktop release to verify the end-to-end flow.

## Surprises & Discoveries

- Observation: chart markers are not produced from the exact same payload as the right-side decision panel. The timeline marker path drops additive signals and playbook bonuses, so stale cached rows are not the only reason markers can diverge.
  Evidence: `app/frontend/src/routes/DetailView.tsx` builds timeline markers with `additiveSignals: null` and `playbook...: null`, while `app/backend/api/routers/ticker.py` passes both into `build_analysis_decision(...)`.

- Observation: the existing `analysis_backfill` endpoint only filled missing or stale rows. It could not intentionally recompute a date range that already had current-version rows.
  Evidence: `app/backend/services/analysis_backfill_service.py` previously derived target dates only from coverage gaps.

## Decision Log

- Decision: reuse the existing `analysis_backfill` job type instead of adding a second job type.
  Rationale: job conflict handling, progress reporting, startup prewarm, and txt-update follow-up are already wired to this job type. Adding `force_recompute` and `start_dt/end_dt` keeps the feature small.
  Date/Author: 2026-03-07 / Codex

- Decision: expose two user actions, one for the visible daily chart range and one for the recent 130-day rolling window.
  Rationale: the visible-range action directly addresses the “this chart looks wrong right now” case, while the 130-day action covers the “refresh recent analysis in bulk” case without forcing a full-history recompute.
  Date/Author: 2026-03-07 / Codex

## Outcomes & Retrospective

- The repository now has the core plumbing for user-triggered analysis recomputation, but the final behavior still needs a build-and-run pass to confirm the desktop UI path end to end.
- The marker-vs-panel mismatch may still need a second step in the future: making the timeline marker source use the exact decision pipeline rather than the simplified approximation.
