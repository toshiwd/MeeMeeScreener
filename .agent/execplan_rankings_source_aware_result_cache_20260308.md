# ExecPlan: Rankings Source-Aware Result Cache

## Purpose

Reduce ranking screen latency by:
- warming base cache and final ranking result cache after app startup
- caching final ranking payloads by request key
- invalidating cache by data source policy
  - Yahoo provisional data: refresh when provisional Yahoo rows change
  - PAN data: do not refresh again until latest PAN date advances
- removing frontend fetch cache that can outlive backend freshness

## Scope

Files touched:
- `app/backend/services/rankings_cache.py`
- `app/backend/api/routers/rankings.py`
- `app/frontend/src/routes/RankingView.tsx`
- `app/main.py`

## Progress

- [x] Added result cache keyed by `(tf, which, dir, mode, risk_mode, limit)`.
- [x] Added per-key singleflight for final ranking payload generation.
- [x] Added source-aware refresh signature using latest PAN date and provisional Yahoo row state.
- [x] Removed provisional timer-based auto refresh logic.
- [x] Added startup warmup for default ranking result keys.
- [x] Removed frontend ranking fetch cache backed by memory/sessionStorage.
- [x] Added `cache_generation` to `/api/rankings/multi` metadata for diagnostics.
- [x] Verified Python syntax with `python -m py_compile`.

## Evidence

- `app/main.py` now warms rankings immediately by default and materializes default result cache.
- `app/backend/services/rankings_cache.py` now stores final payloads separately from base cache and clears them on generation change.
- `app/frontend/src/routes/RankingView.tsx` no longer reads or writes ranking fetch cache entries.

## Decisions

- Cache final API payloads, not only sorted base items, because ML/gate decoration was still being recomputed on every request.
- Use provisional Yahoo row state in the refresh signature instead of global DB mtime so same-day PAN reruns do not invalidate rankings.
- Keep cache in process memory only. App restart rebuilds cache via startup warmup.

## Verification

Executed:

```powershell
python -m py_compile app/main.py app/backend/services/rankings_cache.py app/backend/api/routers/rankings.py
```

Result:
- success, exit code `0`

Not executed:
- frontend build
- lint
- automated tests

## Outcome

The ranking screen now depends on backend-managed result caching instead of frontend fetch caching. Startup warmup prepares the default `latest + hybrid + balanced + limit=50` ranking responses for `D/W/M x up/down`, and explicit cache refreshes only advance generation when the source-aware signature changes.
