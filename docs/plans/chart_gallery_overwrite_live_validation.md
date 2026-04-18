# Chart Gallery Overwrite Live Validation

## Current State
- confirmed = backend batch chart cache keying now includes runtime DB identity and the derived judgment cache layers are fingerprinted by runtime DB changes.
- confirmed = test-layer overwrite artifacts already exist and the reviewed cache layers pass regression tests.
- provisional = browser-local persistence and the live detail-view session were not yet directly observed in this turn.

## Problem
- The overwrite contract must be proven in a real app session, not only in API tests or artifact summaries.
- The user requirement is that provisional Yahoo fill can appear before import, and the confirmed chart-gallery import must replace that provisional basis in the visible detail session.

## Change Policy
- Scope = one live-session overwrite proof path for a single symbol/date scenario.
- Non-scope = ranking logic, retraining, boundary-aware compare execution, weak_recovery redesign, breakout_failure redesign, publish/promote flow changes, MeeMee-wide visual redesign.
- Boundary check = MeeMee owns the visible chart, cache provenance, and overwrite observability; TRADEX logic remains untouched.
- Risk = browser-local persistence may still conceal stale state unless the live browser session is refreshed through the post-import cache path.

## Concrete Instructions
- Run one deterministic before/after scenario for `7203` with a provisional Yahoo-backed current-day row visible before import.
- Apply a confirmed chart-gallery import by updating the runtime stock DB so the same overlapping date becomes confirmed.
- Verify the live detail view shows:
  - chart basis classification before import = provisional or mixed
  - chart basis classification after import = confirmed
  - judgment basis before import = provisional or dual
  - judgment basis after import = confirmed
  - cache provenance and overwrite status change across the import boundary
- Record the observed state in `artifacts/research_inventory/chart_gallery_overwrite_live_validation.json`.
- Update the authoritative overwrite contract artifacts only after the live browser session proves the switch.
- Do not execute boundary-aware compare.
- Do not change ranking/model logic.

