# MeeMee Screener Phase 1-4 Performance/Stability/UI Refactor

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

If `.agent/PLANS.md` exists in this repository, this ExecPlan must be maintained in accordance with it.

## Purpose / Big Picture

This change set reduces backend latency and steady-state load while making the large frontend routes easier to maintain without changing existing API contracts. After this work, users get faster chart/screener responses, less aggressive idle polling, and the same screen behavior with smaller, more isolated UI modules. Success is observable through API benchmarks, lower polling cadence, and unchanged core workflows (`Grid -> Detail -> back`) during manual use.

## Progress

- [x] (2026-02-28 07:00Z) Phase 1 backend batch bars implemented with repository batch APIs and `/api/batch_bars` compatibility kept (`BATCH_BARS_V2`).
- [x] (2026-02-28 07:45Z) Phase 2 screener monthly fetch limit and cache safety improvements implemented (lock/copy/key-based cache behavior).
- [x] (2026-02-28 08:15Z) Phase 3 health split (`/api/health` light + `/api/health/deep`) and jobs/history limit validation implemented (`HEALTH_LIGHT`).
- [x] (2026-02-28 08:50Z) Frontend polling cadence adjusted (keepalive 15s, terminal/job polling adaptive).
- [x] (2026-02-28 09:20Z) Theme tokens moved to `theme/tokens.css` and base import wiring completed.
- [x] (2026-02-28 10:10Z) Detail data-fetch hooks extracted (`useAnalysisTimeline`, `useAsOfItemFetch`).
- [x] (2026-02-28 10:35Z) Detail drawing state extracted (`useDetailDrawings`).
- [x] (2026-02-28 11:00Z) Detail indicator/debug overlays split into presentational components.
- [x] (2026-02-28 13:19Z) Detail position ledger panel extracted (`DetailPositionLedgerSheet`) and parent wiring completed.
- [x] (2026-02-28 13:19Z) Grid rollout feature flag (`GRID_REFACTOR`) wired with safe fallback polling path.
- [x] (2026-02-28 13:19Z) Grid high-density panel extraction completed (`GridIndicatorOverlay`).
- [x] (2026-02-28 13:19Z) Final implementation notes updated; static verification completed via diff/source inspection.

## Surprises & Discoveries

- Observation: `DetailView.tsx` had multiple large state/effect clusters with repeated request-cache logic and drawing persistence logic, making side-effect regressions likely when adding features.
  Evidence: Prior inline effects and storage logic blocks were >300 lines each before hook extraction.

- Observation: Some newly created files can display mojibake in PowerShell output when Japanese literal text is inserted directly.
  Evidence: `Get-Content` showed garbled Japanese labels; switched to Unicode escape strings in JSX for deterministic source encoding.

- Observation: PowerShell display encoding can show mojibake even when file content itself is valid UTF-8.
  Evidence: `Get-Content -Encoding utf8` confirmed correct Japanese text in `DetailPositionLedgerSheet.tsx`.

## Decision Log

- Decision: Keep all API signatures backward-compatible and gate behavioral replacements with flags where feasible.
  Rationale: Plan requires staged rollout with no destructive API changes.
  Date/Author: 2026-02-28 / Codex

- Decision: Prioritize extracting side-effect heavy clusters (data fetching, drawing state, debug/indicator overlays) before purely visual redesign.
  Rationale: This gives immediate stability/maintainability gains with low behavioral risk.
  Date/Author: 2026-02-28 / Codex

- Decision: Continue refactor in small commits (`1 symptom = 1 fix`).
  Rationale: Easier rollback and safer incremental verification in a large legacy route.
  Date/Author: 2026-02-28 / Codex

## Outcomes & Retrospective

- Current outcome: Backend performance/stability phases are implemented, and frontend decomposition now includes Detail fetch/drawing/panel extraction plus Grid polling flag + panel extraction.
- Remaining work: optional further decomposition of `GridView`/`DetailView` into additional domain panels; no blocking functional gaps remain for this plan scope.
- Lesson learned: introducing small reusable hooks/components in the largest routes yields measurable risk reduction faster than all-at-once route rewrites.

## Implementation Notes

Backend and frontend changes are intentionally additive and compatibility-first. New code paths default to enabled but are designed to allow staged rollback using feature flags. The UI decomposition avoids CSS or behavior rewrites unless required to preserve existing interaction semantics.

## Verification Strategy

For backend, use existing API benchmark tooling in `tools/analytics/benchmark_api.py` to compare p50/p95 before/after for `/api/batch_bars` and `/api/grid/screener`.

For frontend, validate with:

1. Manual workflow checks:
   search ticker -> open detail -> toggle overlays -> return to grid.
2. Polling behavior checks:
   active vs idle interval transitions, no runaway timers.
3. Build/type checks when explicitly requested by the user or release process.
