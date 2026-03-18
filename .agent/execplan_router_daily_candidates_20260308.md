# Purpose

MeeMee already stores market-regime labels, future-pattern labels, and strategy-conditioned performance buckets, but it still cannot answer the next operational question: which strategy should be preferred for today's chart state. This change adds the first lightweight daily router table so the research DB can emit ranked strategy recommendations per stock without training a new model first.

After this change, a developer can run one CLI against the lightweight DuckDB and inspect `router_daily_candidates` for the latest trading day or a bounded date range. The result is observable via the CLI JSON summary and by querying the new table for top-ranked codes and recommended strategy IDs.

# Evidence

`strategy_conditional_stats` already summarizes how each seeded strategy performs inside `(regime_id, pattern_id)` buckets. `market_regime_daily` already labels each day, and `_prepare_feature_frame()` already derives enough present-state information to define a coarse state bucket from current candles, moving-average structure, and volatility. The missing piece is a compact lookup layer that maps current state to future-pattern priors and then scores strategies against those priors.

This is deliberately a lookup router, not an ML predictor. The goal is to start producing actionable daily research output while keeping storage bounded and avoiding another large training artifact.

# Repo Orientation

`app/backend/services/strategy_backtest_service.py` owns the regime/pattern builders and the conditional-strategy aggregates, so the router builder belongs there as well. It can reuse `_load_market_frame()` and `_prepare_feature_frame()` without adding a new service layer.

`app/backend/tools/build_router_daily_candidates.py` is the CLI entry point. It should be the only new executable file because the user asked to keep disk growth under control.

# Progress

- [x] (2026-03-08 13:20 JST) Confirmed that present-state features already include breakout/pullback cues, trend persistence, and ATR/volume inputs needed for a coarse router bucket.
- [x] (2026-03-08 13:33 JST) Added `router_daily_candidates` schema plus lookup-based router build logic in `strategy_backtest_service.py`.
- [x] (2026-03-08 13:35 JST) Added CLI wrapper `app/backend/tools/build_router_daily_candidates.py`.
- [x] (2026-03-08 19:43 JST) Ran the first router build against `tmp/research_data/stocks.duckdb` with the research worker paused. The initial score was too optimistic and produced `25` `long` rows for 2026-03-05.
- [x] (2026-03-08 19:45 JST) Tightened DD and fallback penalties, rebuilt the router, and reduced the result to `7` `watch` rows with no `long` recommendations for 2026-03-05.
- [x] (2026-03-08 19:46 JST) Restarted the research worker after the router build completed.
- [x] (2026-03-08 20:49 JST) Rebuilt `strategy_conditional_stats` after expanding the registry with setup-specific challengers, then re-ran the router against the richer strategy set.
- [x] (2026-03-08 20:54 JST) Added minimum-support rules for exact/regime/strategy fallbacks and a daily watchlist fallback so the router no longer overreacts to single-trade buckets while still emitting compact research output.

# Surprises & Discoveries

- Observation: The first scoring formula over-weighted profit-factor/support and under-weighted drawdown, so it recommended `lb_p2_no_regime_v1` broadly even when expected drawdown was about `-2.36`.
  Evidence: First run summary returned `rows=25`, all `action=long`, with top row `best_strategy_id=lb_p2_no_regime_v1` and `expected_worst_dd=-2.3639`.

- Observation: After adding DD and fallback penalties, the latest day no longer clears the action gate. This is a useful negative result: the router now says "watch only" instead of manufacturing false longs.
  Evidence: Second run summary returned `rows=7`, all `action=watch`, top row `best_strategy_id=lb_p2_decision_only_v1`, `best_score=0.0435`.

- Observation: The latest target date in the lightweight DB is stored as a UNIX timestamp, not `YYYYMMDD`.
  Evidence: `target_start_dt=1772668800`, which resolves to `2026-03-05T09:00:00+09:00`.

- Observation: once setup-diverse strategies were added, the `decision_only` recommendation disappeared after minimum-support thresholds were enforced. The previous top pick had been riding on a one-trade exact bucket.
  Evidence: before support thresholds, `lb_p2_decision_only_v1` was selected from `high_vol_chaos / mixed` with `trades=1`; after thresholds, the best watch candidate shifted to `lb_p2_no_regime_v1` with score around `-1.01`.

- Observation: the pullback-specific strategy is correctly not selected for the 2026-03-05 `long_pullback_p3` watchlist because its historical bucket is materially worse than the breakout fallback in the same environment.
  Evidence: `lp_p3_pullback_v1` scored about `-25.16` versus `lb_p2_no_regime_v1` at about `-1.01` on the latest day, despite receiving a positive setup-alignment bonus.

# Decision Log

2026-03-08: Decided to score only candidate rows and keep only the daily top-N recommendations. This keeps the router table compact and avoids the SSD growth problem that broad full-history artifacts caused earlier.

2026-03-08: Decided to use historical priors from dates strictly before the target date range. This is a simple causal cutoff that avoids obvious lookahead when scoring the current day.

2026-03-08: Decided not to persist per-strategy-per-stock score grids. The table stores only the best strategy recommendation plus a small `reason_json` payload with top patterns and top strategies.

2026-03-08: Decided to penalize non-exact fallback rows and expected drawdown below `-1.2`. The first run showed the lookup router would otherwise recommend aggressive longs in a `high_vol_chaos` regime.

2026-03-08: Decided to add explicit minimum trade support for exact/regime/strategy buckets (`5/15/40`). The router should prefer broader evidence over one-off wins.

2026-03-08: Decided to keep a fallback watchlist even when no row clears the normal score threshold. Research users still need to see the least-bad candidates for the day, but they must remain marked `watch`.

# Milestones

## Milestone 1: Durable router table

Add `router_daily_candidates` to `app/backend/services/strategy_backtest_service.py`. Each row stores the date, code, regime, coarse state bucket, dominant future-pattern prior, recommended strategy, expected metrics, router score, and a small explanation payload.

Acceptance is that the schema can be created lazily and re-used safely across multiple reruns for the same `(scope_key, label_version)`.

## Milestone 2: Lookup router materialization

Implement `build_router_daily_candidates()` in `app/backend/services/strategy_backtest_service.py`. The function must:

1. load historical market data and derive present-state buckets
2. build future-pattern priors from dates before the target window
3. combine those priors with `strategy_conditional_stats`
4. score strategies per candidate row
5. keep only the top-N rows per day
6. write the compact result table

Acceptance is a non-empty `router_daily_candidates` table when run against the research DB with existing `v1/top500` labels and conditional stats.

## Milestone 3: Run on the research DB

Run the new CLI against `tmp/research_data/stocks.duckdb` while the research worker is stopped, then inspect the resulting rows.

Acceptance is:

    cd C:\work\meemee-screener
    $env:MEEMEE_DATA_DIR='C:\work\meemee-screener\tmp\research_data'
    python -m app.backend.tools.build_router_daily_candidates --label-version v1 --horizon 20 --max-codes 500 --top-n-per-day 25

The output must report a non-zero row count and show at least one `best_code` / `best_strategy_id` pair in the summary.

# Outcomes & Retrospective

The lookup router is now implemented and runnable. `app/backend/services/strategy_backtest_service.py` contains the durable `router_daily_candidates` table and the `build_router_daily_candidates()` materializer, and `app/backend/tools/build_router_daily_candidates.py` exposes it as a CLI.

The research DB now holds the latest router output for `top500 / v1 / horizon=20`. After adding setup-aware challenger strategies and support-aware scoring, the latest day still does not produce actionable longs. The router keeps a fallback watchlist of five rows for 2026-03-05, all marked `watch`, with the least-bad candidate at roughly `-1.01`. That is the correct outcome for the current evidence set: the system now prefers "no trade, but here are the closest candidates" over fabricating a buy signal.

What remains is the next refinement step: split regime-level priors with a stronger state bucket, add setup-specific conditional stats if the registry broadens further, or introduce a learned future-pattern model so the router is not forced to fall back to coarse regime priors so often.
