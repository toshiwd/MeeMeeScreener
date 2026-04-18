# TRADEX Buy Judgment Policy Selection R5 - Default Surface Choice

## Current State
- confirmed: `buy_judgment_revision_r1_weak_liquidity_gate` remains a kept prior result.
- confirmed: `buy_judgment_revision_r2_regime_mismatch_gate` remains a kept prior result.
- confirmed: `buy_judgment_revision_r3_liquidity_plus_regime_gate` is `dropped_closed`.
- confirmed: `buy_judgment_revision_r4_reclaim_quality_gate` is `keep`.
- confirmed: `bp_liquidity_trap_penalty/v1` remains the active deep-dive.
- confirmed: overwrite/live status remains `verified_live`.
- confirmed: `boundary_aware_compare_executed = false`.

## Problem
- The next question is policy selection, not new gate design.
- R1, R2, and R4 are all valid keeps, but they serve different operational roles.
- The repo needs one default operational challenger and explicit secondary roles so the surface is not ambiguous.

## Change Policy
- TRADEX only.
- One axis only: default buy-surface selection.
- No new gate family.
- No gate combination.
- No threshold retuning.
- Preserve R1, R2, R3, and R4 as fixed evaluated references.
- No overwrite/provenance work.
- No boundary-aware compare execution.
- No model retraining.
- No MeeMee UI / publish / promote_ready changes.
- Use confirmed-data-only inputs only.

## Concrete Instructions
- Compare the existing buy surfaces as operational profiles, not as redesign candidates.
- Active review paths: baseline, R1, R4.
- Reference-only path: R2.
- Closed path: R3.
- Required artifacts: [`buy_judgment_policy_selection_r5_default_surface_choice.json`](C:\work\meemee-screener\artifacts\research_inventory\buy_judgment_policy_selection_r5_default_surface_choice.json), [`buy_judgment_policy_selection_r5_gate_decision.json`](C:\work\meemee-screener\artifacts\research_inventory\buy_judgment_policy_selection_r5_gate_decision.json).
- Expected decision split: R4 default operational challenger, R1 secondary defensive reference, R2 secondary reference-only, R3 dropped_closed.
- Explicitly not changing: any gate thresholds, gate composition, or the active deep-dive.

## Evidence Basis
- R4 is the best current balance between downside control and upside preservation.
- R1 is the strongest defensive filter, but it removes all >=20% winners in this cohort.
- R2 remains useful as a reference regime check, but it is superseded by R4 for default operation.
- R3 stays closed because it is cumulative over-pruning.

## Verify
- JSON load validation for the new policy-selection artifacts and the updated inventory artifacts.
- confirm no new gate candidate was created.
- confirm one default operational challenger is selected explicitly.
- confirm R3 remains `dropped_closed`.
- confirm R1, R4, and R2 roles are machine-readable.
- confirm `boundary_aware_compare_executed = false` remains intact.
- confirm no overwrite/provenance regression is introduced.
