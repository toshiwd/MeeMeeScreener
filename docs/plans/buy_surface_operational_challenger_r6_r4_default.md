# TRADEX Buy Surface Operational Challenger R6

## Current State
- confirmed default source: `buy_judgment_revision_r4_reclaim_quality_gate`
- confirmed references:
  - `buy_judgment_revision_r1_weak_liquidity_gate` = prior keep
  - `buy_judgment_revision_r2_regime_mismatch_gate` = prior keep
  - `buy_judgment_revision_r3_liquidity_plus_regime_gate` = dropped_closed
- compare unit: confirmed audit event instance
- compare contract:
  - same universe
  - same period
  - same top-K = 10
  - same regime framing
  - same cost/slippage
  - same artifact detail level

## Problem
- The next unresolved question is operational, not design-level:
  - does the selected R4 default surface improve compare-engine top-K quality when promoted into a compare-ready challenger?
- We need one real compare-ready challenger and one compare-engine validation, not another gate design round.

## Change Policy
- TRADEX only.
- One axis only: promote R4 into a compare-ready operational challenger.
- No new gate family.
- No gate combination.
- No threshold retuning.
- No overwrite/provenance work unless a new regression appears.
- No boundary-aware compare execution.
- No model retraining.
- No MeeMee UI / publish / promote_ready changes.

## Concrete Instructions
- Source default surface: `buy_judgment_revision_r4_reclaim_quality_gate`
- Challenger: `buy_surface_operational_challenger_r6_r4_default`
- Operational integration point: compare-ready selection over confirmed audit events.
- Excluded changes:
  - no new gate
  - no new threshold
  - no liquidity/regime retuning
  - no late-entry or breakout redesign
- Keep / hold / drop rules:
  - keep if top-K branching is real and bad_pick_removal is positive without obvious regime collapse
  - hold if evidence is positive but still ambiguous
  - drop if branching is absent or the trade-off collapses

## Verification
- JSON load validation
- confirm exactly one challenger
- confirm confirmed-data-only basis
- confirm top-10 branching is reported
- confirm no MeeMee UI files were modified
- confirm no overwrite/provenance regression was introduced
