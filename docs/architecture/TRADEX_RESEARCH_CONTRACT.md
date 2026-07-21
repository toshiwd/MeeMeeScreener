# TRADEX Research Contract

TRADEX is a multi-layer market-environment research system. It models:

- long-horizon environment
- mid-horizon regime transition
- short-horizon execution opportunity

This document is the authoritative contract for compare, leaderboard, rollup, and run manifest artifacts.

## Authoritative artifacts

- `compare.json`
- `family_leaderboard.json`
- `session_leaderboard_rollup.json`
- `scope_stability_rollup.json`
- `run_manifest.json`

Markdown reports are derived material only. The JSON artifacts are the source of truth.

## Decision names

The following decision fields are explicit and required:

- `candidate_local_decision`
- `session_aggregate_decision`
- `authoritative_rollup_decision`

`latest_decision` may exist for compatibility, but it must not be the authoritative source.

## Same-condition compare contract

Same-condition compare must enforce:

- same universe
- same period
- same top-K
- same regime
- same cost policy
- same artifact detail level

If any of those differ, compare must fail fast.

For short-selling research, the default cost policy is `ignored_by_user_rule`: trading costs, fees, slippage, and borrow costs are not modeled unless the user explicitly asks to evaluate them. The same-condition compare requirement is therefore that every compared short-side candidate uses the same zero/ignored cost policy, not that a realistic cost model is applied.

Short-side research assumes candidates are margin-shortable / loanable by default. Loanability is not a ranking, filter, penalty, or comparison axis unless explicitly requested.

## Required environment states

- `trend_long`
- `trend_short`
- `range_buy`
- `range_sell`
- `panic_rebound`
- `bottom_building`
- `top_warning`
- `break_risk`
- `avoid`

## Required execution-support states

- `probe_entry`
- `add_ok`
- `concern_trim`
- `decisive_exit`

## Victory metrics

Authoritative artifacts must persist these metrics:

- `hold_end_return_20d`
- `mfe_20d`
- `mae_20d`
- `win_flag_hold_end`
- `win_flag_mfe`
- `addability_score`
- `trimability_score`
- `opportunity_count`
- `avg_holding_days`
- `max_drawdown`

`addability_score` and `trimability_score` are provisional in this phase and may be nullable.

## Feature families

Every challenger entering compare must declare `feature_family`.

Allowed values:

- `environment_recognition`
- `common_pattern`
- `regime_adjustment`
- `boundary_feature`
- `bad_pick_removal`
- `symbol_specific_adjustment`
- `image_context_support`

Candidates without `feature_family` must not enter compare.

## Fallback handling

If a lighter path is used, it must be explicitly marked as `research-fallback`.
Research fallback must never silently become authoritative.

## Image branch

The numeric branch remains primary.
The image branch is auxiliary only and may rerank, veto, or boost.
Image-only ranking semantics are forbidden.
Chart screenshots used by the image branch must be captured through MeeMee functionality, and only after the weekly and monthly charts are visibly rendered. Every research screenshot, regardless of capture timing or research phase, must use the MeeMee detail screenshot format that visibly includes: ticker code and name; basis date (with confirmed/provisional status when available); latest daily OHLC; 7/20/60MA; volume relative to its 20-day average; and the next earnings date. External ad hoc browser/window screenshots may be used only as explicitly labeled `research-fallback` material and must not become authoritative.
