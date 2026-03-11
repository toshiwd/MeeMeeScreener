# Exact Decision Unification

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

If `.agent/PLANS.md` exists in this repository, this ExecPlan must be maintained in accordance with it.

## Purpose / Big Picture

Users should see one authoritative decision everywhere in the detail chart. After this change, the daily chart markers and the cursor-side analysis panel will be driven by the same exact backend decision logic, so a red/green marker on the chart means the same thing as the decision shown when hovering that date. Approximate timeline-only marker logic will be removed instead of blended with exact results.

## Progress

- [x] (2026-03-07 16:58 JST) Investigated the mismatch and confirmed that chart markers use timeline-based approximation while the cursor panel uses backend exact decisions.
- [x] (2026-03-07 17:00 JST) Confirmed ranking detail displays use `build_analysis_decision()`; only the ranking quality helper still contains an approximate evaluation path.
- [x] (2026-03-07 17:15 JST) Implemented a backend bulk endpoint that returns exact decisions for a requested daily date range.
- [x] (2026-03-07 17:18 JST) Replaced detail chart marker generation to use only exact bulk decisions and removed the timeline-based approximate marker path from the detail screen.
- [x] (2026-03-07 17:20 JST) Reviewed ranking-related approximate paths and confirmed the remaining approximate logic is internal quality analytics, not user-visible ranking decisions.

## Surprises & Discoveries

- Observation: The chart marker path drops `playbookScoreBonus` and `additiveSignals` on purpose.
  Evidence: `app/frontend/src/routes/DetailView.tsx` passes `null` for both fields when converting timeline rows into markers.

- Observation: The cursor panel uses backend `build_analysis_decision()` with richer inputs than the timeline marker path.
  Evidence: `app/backend/api/routers/ticker.py` passes entry policy bonuses, additive signals, and sell context into `build_analysis_decision()`.

- Observation: RankingView does not render a user-visible decision badge from the approximate ranking quality helper.
  Evidence: `app/frontend/src/routes/RankingView.tsx` does not display decision tone fields; the approximate logic remains isolated in `app/backend/services/ranking_analysis_quality.py`.

## Decision Log

- Decision: Treat the cursor-side backend decision as the only authoritative decision for chart markers.
  Rationale: It is the path with the highest-fidelity inputs and already matches user expectations.
  Date/Author: 2026-03-07 / Codex

- Decision: Scope ranking follow-up to user-visible decision outputs, not internal quality analytics.
  Rationale: The current user-facing mismatch is in detail markers; ranking quality backtests can be handled separately unless they directly drive visible decisions.
  Date/Author: 2026-03-07 / Codex

## Outcomes & Retrospective

The exact marker path is implemented. Remaining follow-up, if needed, is performance tuning for the bulk exact endpoint and any later cleanup of internal-only ranking quality analytics.

## Context and Orientation

The detail screen lives in `app/frontend/src/routes/DetailView.tsx`. Today it renders decision markers from `useAnalysisTimeline()` and then overlays per-date exact decisions only for dates the user has already visited with the cursor. That creates mixed behavior where unseen dates still show approximate markers.

The exact decision logic lives in `app/backend/services/analysis_decision.py` and is invoked by `app/backend/api/routers/ticker.py` in the `/api/ticker/analysis` endpoint. That path combines ML probabilities, sell-analysis context, additive signals, and playbook bonuses. Those additional inputs are not present in the timeline payload returned by `app/backend/infra/duckdb/stock_repo.py`.

## Plan of Work

Add a new backend API that accepts `code`, `start_dt`, `end_dt`, and `risk_mode`, enumerates the trading dates in that daily-bar range, and returns one exact `decision` payload per date. Reuse the same backend helper logic as `/api/ticker/analysis` so exact markers and the right panel share the same computation path.

Then update `DetailView.tsx` so daily markers are rendered only from that exact range payload. Remove the old timeline marker derivation and stop using the timeline hook for marker generation. Keep the existing hover-time exact cache only if it still helps avoid refetches, but do not mix approximate markers into the chart.

Finally, inspect ranking-related visible decision outputs. If a ranking screen still shows an approximate tone or badge to the user, switch it to the same exact decision logic or remove it.

## Concrete Steps

From `C:\work\meemee-screener`, inspect the current detail marker flow with `rg` against `app/frontend/src/routes/DetailView.tsx`, `app/backend/api/routers/ticker.py`, and `app/backend/infra/duckdb/stock_repo.py`. Implement the new backend endpoint, then patch the detail screen to consume it. If needed, rebuild the desktop release only after the user explicitly asks for a build.

## Validation and Acceptance

Open a detail chart in analysis mode and compare a date marker with the cursor-side decision on the same date. Acceptance is that the chart marker color and the right-panel `判定` agree for every visited and unvisited date in the visible range. A second acceptance signal is that moving the cursor across the range no longer causes historical markers to flip between approximate and exact states.

## Idempotence and Recovery

The backend endpoint and frontend marker changes are additive until the old timeline marker path is removed. If the new endpoint misbehaves, the previous timeline path can be restored from the working tree diff. No data migration is required for this change.

## Artifacts and Notes

Relevant evidence:

    app/frontend/src/routes/DetailView.tsx:3047 passes null playbook bonuses and null additive signals into computeEnvironmentTone().
    app/backend/api/routers/ticker.py:1026 builds exact decisions with playbook bonuses, additive signals, and sell context.
    app/backend/infra/duckdb/stock_repo.py:704-721 shows the timeline payload does not include those richer fields.

## Interfaces and Dependencies

The new backend interface should live in `app/backend/api/routers/ticker.py` alongside `/analysis` and `/analysis/timeline`. It should return a JSON object with `items`, where each item contains the trading date key and the exact decision payload generated by `build_analysis_decision()`. The frontend detail screen should consume that endpoint directly for marker rendering.
