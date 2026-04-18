# TRADEX Buy Surface R9 - Regime-Scoped Operational Challenger

## Objective
Promote the already-kept R6 challenger into a regime-scoped operational overlay:
- active in `risk_off` and `risk_on`
- fallback to baseline behavior in `neutral`

This is policy-only operationalization. It does not introduce a new gate, threshold, or feature family.

## Source of Truth
- Baseline: `buy_judgment_revision_r4_reclaim_quality_gate`
- Challenger source: `buy_surface_operational_challenger_r6_r4_default`
- Diagnosis basis: `buy_surface_operational_diagnosis_r8_r6_by_regime`
- Confirmed regimes from R8:
  - allowed: `risk_off`, `risk_on`
  - blocked: `neutral`

## Compare Contract
- compare unit: `symbol`
- selection unit: `symbol`
- same universe / same period / same top-K / same regime / same cost / same artifact detail level
- confirmed data only
- boundary-aware compare not executed
- confirmed symbol universe count: 671

## Policy Overlay
- enabled regimes: `risk_off`, `risk_on`
- blocked regime: `neutral`
- fallback policy: `baseline_in_blocked_regime`
- operational effect:
  - use the R6 challenger where R8 showed positive regime deltas
  - suppress the neutral-regime regression by falling back to baseline in neutral

## Decision Rules
- keep: scoping preserves non-negative branching behavior and the neutral fallback removes the blocker
- hold: raw compare remains flat and the policy overlay has not been independently re-run by a scope-aware engine
- drop: the scoped overlay still fails to produce a usable operational policy

## Non-Scope
- no new gate family
- no threshold retuning
- no regime-definition redesign
- no combination with R1 or R2
- no liquidity or late-entry logic addition
- no boundary-aware compare execution
- no MeeMee UI / publish / promote_ready changes

## Verification
- JSON load validation for the challenger artifact, the decision artifact, the inventory, and the candidate gate decision
- confirm `compare_unit = symbol`
- confirm allowed / blocked regimes are explicit
- confirm fallback behavior is machine-readable
- confirm `primary_next_axis` is updated to this axis
- confirm no MeeMee UI files were modified
- confirm no overwrite / provenance regression was introduced
