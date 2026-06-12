# Nikkei225 Environment Monitoring Plan

## Goal

Build a review-oriented environment-change monitor that helps the operator detect when the monitored market regime, especially the Nikkei 225 leadership environment, is changing fast enough to invalidate the previous playbook.

This plan does not change production ranking logic.
It does not move TRADEX research logic into MeeMee.

## Boundary

- MeeMee owns:
  - display
  - operator confirmation
  - watchlist and market dashboard surfaces
- TRADEX owns:
  - research-side environment scoring
  - threshold calibration
  - typed state definitions
  - validation of whether a state transition was useful

## Current Reusable Pieces

- `GET /api/market/heatmap?period=1d|1w|1m`
  - sector heatmap surface already exists
- current buy review boards and block reasons
  - current breadth and entry-fit compression already exist in artifacts
- `market_regime_daily`
  - concept exists but freshness is currently insufficient for latest-day operator use
- watchlist infrastructure
  - watchlist read/write path already exists in backend

## Problem To Solve

The current system can show:

- candidate boards
- sector heatmaps
- stock-level conditions

But it does not yet produce one explicit, operator-readable answer to this question:

> Did the Nikkei 225 environment materially change today or over the last few sessions, and if so, what kind of change was it?

Without that answer, the operator has to infer environment change indirectly from many partial screens.

## Minimum Useful Product

Create one environment monitor response with four sections:

1. Index breadth state
2. Leadership deterioration state
3. Sector rotation state
4. Watchlist damage state

The response should be review-only and typed.

## Proposed Output Contract

The monitor should emit one JSON response with:

- `as_of_confirmed_close`
- `index_scope`
  - `nikkei225_available`
  - `nikkei225_member_count_used`
- `environment_change_state`
  - `stable`
  - `soft_deterioration`
  - `leadership_break`
  - `broad_risk_off`
  - `rotation_without_break`
- `signals`
  - `breadth_above_ma20`
  - `advancers_ratio_1d`
  - `advancers_ratio_3d`
  - `mean_return_1d`
  - `mean_return_3d`
  - `leader_strong_close_count`
  - `leader_ma20_break_count`
  - `leader_weak_close_count`
  - `top_sector_rotation_share`
  - `watchlist_ma20_break_count`
  - `watchlist_strong_close_count`
- `operator_summary`
  - short Japanese text
- `evidence_rows`
  - small sample for manual check

## Nikkei225-Specific Scope

The monitor should prefer Nikkei 225 members when membership is available.

If the repo does not yet have a trusted Nikkei 225 constituent source, the contract should expose:

- `nikkei225_available = false`
- `fallback_scope = broad_market_confirmed_pan`

That fallback must be explicit.

## State Definitions

Initial operator states should be simple and typed:

- `stable`
  - breadth normal
  - leader damage contained
- `soft_deterioration`
  - breadth weakening or weak-close expansion
  - but strong leaders still present
- `leadership_break`
  - strong-leader count falling
  - MA20-break leaders increasing
- `broad_risk_off`
  - breadth collapse plus leader damage
- `rotation_without_break`
  - market internals mixed, but capital rotating instead of fully exiting

These are display states only until TRADEX validates them.

## Implementation Order

1. Backend read-only contract
   - add one API response builder for environment change
   - no ranking mutation
2. Nikkei 225 membership source
   - add explicit source if available
   - otherwise expose broad-market fallback clearly
3. Signal assembly
   - breadth
   - leader pattern counts
   - sector rotation concentration
   - watchlist damage counts
4. MeeMee operator panel
   - compact summary card
   - no research logic in frontend
5. TRADEX validation
   - test whether typed state transitions actually improved operator decisions

## Validation Rules

Before trusting the monitor:

- verify confirmed-bar freshness
- verify heatmap source is not fallback
- verify Nikkei 225 scope is explicit
- verify last 5-session transitions are reproducible from confirmed bars
- verify the typed state changes when breadth and leader counts genuinely change

## Non-Scope

- no auto-trading
- no live alerting policy
- no production ranking rewrite
- no silent use of provisional-only bars for confirmed state
- no MeeMee-only heuristic that is not reproducible in TRADEX research

## First Concrete Build

The first implementation should be narrow:

- one backend JSON endpoint
- one review-only panel
- one broad-market fallback
- one typed state machine with no more than five states

Do not start with:

- intraday prediction
- macro/news classification
- dozens of indicators
- multi-model scoring
