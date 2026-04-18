# Outcome-Only Core Track

## Current State
- confirmed: `bp_liquidity_trap_penalty/v1` remains the active TRADEX deep-dive candidate.
- confirmed: `boundary_aware_compare_run_status = not_executed`.
- confirmed: historical judgment is `explainability_only` and is not part of core scoring.
- research-fallback: `judgment_signal_trade_value_summary.json` still reports `complete_horizon_count = 0`, so judgment-history trade validation remains blocked.

## Problem
- The policy correction is complete, but the next main execution lane still needs to be concrete and compare-ready.
- Core ranking must now move forward without any dependence on historical buy/sell judgment features.

## Change Policy
- TRADEX only.
- One axis only: outcome-only core-track execution planning.
- No boundary-aware compare execution.
- No MeeMee UI, publish, or promote-ready changes.
- No weak_recovery or breakout_failure redesign.
- Keep the liquidity deep-dive active.
- Keep the judgment-validation blocker visible, but separate from the core track.

## Concrete Instructions
- Make the next main compare-ready lane an outcome-only ranking track.
- Define the target as direct future-outcome ranking for the existing future-horizon label surface.
- Allow only market-derived feature families in the core model.
- Explicitly exclude historical buy/sell judgment from the core model.
- Keep judgment outputs only as explanation, operator review, or optional future auxiliary-only ablation.
- Preserve the blocked judgment-validation lane as a separate research-fallback track.
- Do not claim trade-value proof until the labeled corpus is repaired.

## Core Track Contract
- target_label: `future_outcome`
- main_objective: `direct_future_outcome_ranking`
- core_model_scope: `outcome_only_baseline`
- allowed_core_feature_families:
  - OHLCV-derived features
  - price/volume-derived features
  - regime-aware market features
  - liquidity-related market features
- excluded_core_feature_families:
  - historical buy/sell judgment
- compare_contract:
  - same universe
  - same period
  - same top-K
  - same regime
  - same cost
  - leakage-safe out-of-sample
  - time-block split with embargo
  - no random shuffle
- success_metrics:
  - top-K uplift
  - bad-pick removal
  - changed_top10_members_count
  - changed_top20_members_count when available
  - boundary score gap when available
- keep_criteria:
  - positive same-condition top-K movement
  - no obvious worst-regime collapse
  - no evidence of leakage dependence
- hold_criteria:
  - compare-ready but incomplete evidence
  - missing corpus still blocks judgment validation only
- fail_criteria:
  - no-op movement
  - unstable sign across comparable scopes
  - dependence on historical judgment history
- dependency_status_on_missing_trade_value_corpus: blocked for judgment-history validation only, not for the outcome-only core track
