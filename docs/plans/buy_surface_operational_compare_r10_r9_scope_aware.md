# TRADEX Buy Surface R10 - Scope-Aware Compare Rerun for R9

## Objective
- Re-run the compare layer directly for the already-kept regime-scoped challenger.
- Make the compare contract explicit about scope-aware execution instead of inheriting proxy metrics.
- Keep the same confirmed-data contract, same universe, same period, same top-K, same regime framing, same cost/slippage.

## Source Surface
- Source challenger: `buy_surface_operational_challenger_r9_r6_regime_scoped`
- Source policy: allowed regimes `risk_off`, `risk_on`; blocked regime `neutral`; fallback `baseline_in_blocked_regime`
- Baseline: `buy_judgment_revision_r4_reclaim_quality_gate`

## Comparison Contract
- compare unit: `symbol`
- selection unit: `symbol`
- confirmed data only: `true`
- scope-aware compare executed: `true`
- scope-aware metric source: `direct_rerun`
- scope application mode: `regime_scoped_operational_overlay`

## What Changes
- The scope-aware compare is materialized directly, not inherited as a proxy summary.
- The blocked `neutral` regime explicitly falls back to baseline behavior.
- No new gate family, no threshold retuning, no regime redesign.

## What Does Not Change
- R4 remains the default operational challenger source.
- R1 remains the defensive reference.
- R2 remains reference-only.
- R3 remains dropped closed.
- R9 remains the kept scoped policy source.
- boundary-aware compare remains not executed.
- overwrite/live remains verified.
- MeeMee UI remains untouched.

## Decision Rules
- `promote_ready`: only if the direct rerun shows real branching, non-negative top-K uplift, non-negative bad-pick removal, and no blocking regime regression.
- `keep`: if the direct rerun is useful but still lacks enough operational confidence.
- `hold`: if the rerun still leaves meaningful ambiguity or liquidity-quality concern.
- `drop`: if the scoped challenger fails under direct recomputation.

## Expected Outputs
- `artifacts/research_inventory/buy_surface_operational_compare_r10_r9_scope_aware.json`
- `artifacts/research_inventory/buy_surface_operational_compare_r10_gate_decision.json`
- inventory pointer updates for the next axis
