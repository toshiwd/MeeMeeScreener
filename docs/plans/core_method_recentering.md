# Core Method Recentering

## Current State
- confirmed: `bp_liquidity_trap_penalty/v1` remains the active TRADEX deep-dive.
- confirmed: `boundary_aware_compare_run_status` remains `not_executed`.
- confirmed: historical judgment signals are defined as TRADEX Research OS `strategy_judgement.json` and are currently classified as `explainability_only`.
- research-fallback: the authoritative trade-value corpus still has `complete_horizon_count = 0`, so trade-value confirmation is blocked.

## Problem
- The current core method still risks carrying historical judgment history as if it were a core scorer, even though no leakage-safe same-condition evidence exists.
- That keeps the product question blurred: the core model should be judged by direct future-outcome ranking and top-K practical quality, not by historical judgment inertia.

## Change Policy
- TRADEX only.
- One axis only: core-method recentering.
- Do not execute boundary-aware compare.
- Do not change MeeMee UI, publish flow, or promote-ready behavior.
- Do not redesign weak_recovery or breakout_failure.
- Do not move research logic into MeeMee.
- Preserve same-condition comparison discipline.

## Concrete Instructions
- Freeze historical judgment signals out of core scoring.
- Keep judgment outputs only for explanation, operator review, or weak auxiliary review if needed.
- Re-center the next compare-ready modeling track on direct future-outcome ranking.
- Use an outcome-only baseline as the main path.
- Keep the trade-value study in `research_fallback` until the labeled corpus is complete.
- Add a blocker artifact that explains why `complete_horizon_count = 0` prevents honest trade-value validation.
