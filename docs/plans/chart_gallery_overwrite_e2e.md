# Chart Gallery Overwrite E2E

## Current State
- confirmed: the batch chart cache key now includes `runtime_db_path` and `data_version` in `app/backend/api/routers/bars.py`.
- confirmed: frontend persistent chart cache invalidates on `dataVersion` changes and detail prefetch subscribes to chart version changes.
- confirmed: ticker-derived analysis caches now include a runtime DB fingerprint, so confirmed-import changes invalidate cached derived outputs.
- provisional: browser-local persistence outside the reviewed cache layers may still preserve stale payloads until the next invalidation event.

## Problem
- The overwrite contract is now encoded in several layers, but it needs an explicit end-to-end proof path after chart-gallery import.
- The reviewed layers must show the same transition: provisional Yahoo fill before import, confirmed replacement after import, and confirmed judgment becoming authoritative.

## Change Policy
- One axis only: end-to-end overwrite contract validation.
- Do not change ranking logic, retraining, outcome-only flow, boundary-aware compare, publish/promote behavior, or MeeMee-wide UI.
- MeeMee owns display/cache/provenance behavior.
- TRADEX is out of scope for this task.

## Concrete Instructions
- Keep the backend batch overwrite behavior in place.
- Prove the frontend persistent cache invalidates on chart version changes.
- Prove ticker derived judgment caches re-key when the runtime DB fingerprint changes.
- Record a concrete before/after overlap trace and publish a machine-readable status artifact.
- Explicitly keep `confirmed` and `provisional` judgment lanes separate.
- Do not add any UI redesign or ranking/model changes.
