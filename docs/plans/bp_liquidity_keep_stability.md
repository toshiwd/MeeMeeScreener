# Liquidity Keep Stability

## Contract

- Authoritative keep: `bp_liquidity_trap_penalty/v1`
- Compare-engine local decision: `hold`
- Local reason: `turnover_too_high`
- Authoritative override: `keep`
- Research contract status: `provisional_keep_with_soft_warning`

## What Stayed Fixed

- TRADEX only
- same universe / same period / same top-K / same regime / same cost / same artifact detail level
- MeeMee remains read-only source input only
- boundary-aware remains backlog
- weighting_hygiene remains backlog

## New Envelope

- The keep envelope is now explicit in `bp_liquidity_keep_envelope.json`.
- High-turnover months remain positive, so `turnover_too_high` stays a soft warning.
- The first fragile boundary is still top10, not the keep itself.

## Pairwise Decomposition Result

- `liq_adv20_plus_turnover_penalty_v1` dropped as no-op
- `liq_turnover_plus_followthrough_penalty_v1` dropped as no-op
- `liq_adv20_plus_followthrough_penalty_v1` dropped as no-op
- No pairwise branch changed top-K membership
- The monolithic liquidity keep remains the only stable keep candidate in this round

## Turnover Policy

- `turnover_too_high` is explicitly modeled as a soft warning here
- blocker promotion is reserved for turnover combined with value collapse or incomplete coverage
- shadow blocker evaluation is complete, but it remains shadow-only in this round
