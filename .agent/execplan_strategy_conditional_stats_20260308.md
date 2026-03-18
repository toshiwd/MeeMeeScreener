# Purpose

MeeMee now has daily market-regime labels and daily future-pattern labels, but nothing in the database yet answers the next practical question: which strategy variant performs well inside each regime/pattern bucket. This change adds a lightweight strategy registry and a conditional-statistics table that aggregates trade outcomes by strategy, market regime, future pattern, and side.

After this change, a developer can seed a small set of strategy variants, run one batch job, and inspect a compact table showing where each strategy wins or fails. The result is observable by querying `strategy_conditional_stats` or by running the CLI tool and reading its JSON summary.

# Evidence

`strategy_backtest_service._simulate()` already constructs full trade events internally. Each trade event includes `code`, `entry_dt`, `exit_dt`, `side`, `setup_id`, `ret_net`, `qty`, and sector metadata. That is sufficient to join each trade back to `market_regime_daily` and `future_pattern_daily` at the entry date.

The missing piece is persistence. Existing backtests only keep aggregate metrics and at most a small `sample_trades` tail. Without a dedicated aggregate table, the system cannot learn which regime/pattern combinations favor one strategy over another.

# Repo Orientation

`app/backend/services/strategy_backtest_service.py` is still the correct place to implement this because it owns both simulation and strategy-oriented table helpers.

The new CLI belongs in `app/backend/tools/` so the research DB can be updated without touching the API layer yet.

# Progress

- [x] (2026-03-08 14:10 JST) Confirmed that `_simulate()` exposes enough trade metadata internally to compute regime/pattern-conditioned aggregates.
- [x] (2026-03-08 15:05 JST) Added `strategy_registry` and `strategy_conditional_stats` schema helpers.
- [x] (2026-03-08 15:20 JST) Extended simulation output so internal batch code can access the full trade list without persisting it permanently.
- [x] (2026-03-08 15:50 JST) Added batch functions to seed default strategies and build conditional stats from the research DB.
- [x] (2026-03-08 16:05 JST) Ran the first batch against `tmp/research_data/stocks.duckdb`; it produced `226` rows across five breakout variants.
- [x] (2026-03-08 20:45 JST) Expanded the seeded registry from five breakout-only variants to eight setup-aware challengers (`p1/p2/p3/decision`) and rebuilt the table.
- [x] (2026-03-08 20:49 JST) Verified the rebuilt research DB now holds `319` aggregate rows across eight registered strategies, with seven strategies producing trades.
- [x] (2026-03-08 21:03 JST) Identified and fixed the zero-trade bug in `ld_decision_up_v1`: decision-only longs had been blocked by the shared `entry_long` gate.
- [x] (2026-03-08 21:11 JST) Rebuilt the research DB after the fix; `strategy_conditional_stats` now holds `366` rows and all eight seeded strategies have non-zero trade buckets.
- [x] (2026-03-08 21:28 JST) Tightened `lp_p3_pullback_v1` to require `decision_up` confirmation and a stricter ATR cap, then rebuilt the research DB again.
- [x] (2026-03-08 21:29 JST) Verified that the stricter `lp_p3_pullback_v1` reduced trade count and improved aggregate quality, though it still does not win the latest-day router contest.

# Surprises & Discoveries

- Observation: the initial registry was too narrow for routing research because every active strategy only allowed `long_breakout_p2`.
  Evidence: the first seeded `strategy_registry` contained five rows, all with `allowed_long_setups=["long_breakout_p2"]`.

- Observation: adding setup-diverse challengers materially widened the aggregate table without blowing up storage.
  Evidence: `strategy_conditional_stats` grew from `226` to `319` rows after adding `lr_p1_reversal_v1`, `lp_p3_pullback_v1`, and `ld_decision_up_v1`.

- Observation: `ld_decision_up_v1` currently produces zero aggregate rows under the present filters.
  Evidence: before the fix, `strategy_registry` contained `8` rows but `strategy_conditional_stats` showed trade rows for only `7` distinct `strategy_id` values.

- Observation: the root cause of the zero-trade bug was structural, not statistical. There are many `decision_up` rows that are not `entry_long`.
  Evidence: on the top-500 research universe, `decision_up_total=366,092` and `decision_only_rows=236,933`, so a strategy that only allows `long_decision_up` was being filtered out before setup matching.

- Observation: after the gate fix, `ld_decision_up_v1` becomes active immediately but its quality is still poor.
  Evidence: a direct replay produced `3,193` trades with `profit_factor=0.6606` and `total_realized_unit_pnl=-11.4585`; after the DB rebuild it materialized `47` aggregate rows.

- Observation: a stricter pullback configuration improves aggregate quality but does not change the latest-day recommendation.
  Evidence: `lp_p3_pullback_v1` moved from about `avg_ret=-0.0057 / trades=3608 / rows=46` to `avg_ret=+0.0001 / trades=3223 / rows=43`, yet the 2026-03-05 router still ranks `lb_p2_no_regime_v1` above it.

# Decision Log

2026-03-08: Decided to join regime and future-pattern labels at `entry_dt`, not `exit_dt`. The router will decide whether to enter based on the market state and chart condition at entry time, so conditioning on entry is the correct causal framing.

2026-03-08: Decided not to store per-trade rows in DuckDB for now. The batch will aggregate trades in memory and persist only the compact conditional table to avoid disk growth.

2026-03-08: Decided to expand the registry with setup-specific long challengers before introducing a learned router. The original breakout-only registry could never answer which setup family should be chosen for a `p1` or `p3` chart.

2026-03-08: Decided to add an explicit `allow_decision_only_long_entries` flag instead of weakening the global long entry rule. This keeps the fix local to decision-only strategies and avoids changing behavior for breakout/pullback/reversal variants.

2026-03-08: Decided to tighten `lp_p3_pullback_v1` with `require_decision_for_long=True` and `max_atr_pct_long=0.06`. A direct replay showed a measurable DD/PF improvement without introducing more schema or storage.

# Milestones

## Milestone 1: Strategy registry

Add `strategy_registry` with a tiny default seed set of long-breakout variants that reflect the current research focus. These rows store `strategy_id`, `family`, `side`, `status`, `config_json`, and timestamps.

Acceptance is that the seed function can populate an empty DB with a few challenger strategies and can be re-run safely.

## Milestone 2: Conditional statistics materialization

Extend simulation output with an opt-in full trade payload, then implement `build_strategy_conditional_stats()` that:

1. loads active strategies from `strategy_registry`
2. replays each strategy on the chosen universe
3. joins each trade to `market_regime_daily` and `future_pattern_daily`
4. writes grouped metrics into `strategy_conditional_stats`

Acceptance is a non-empty `strategy_conditional_stats` table for the default strategy set.

## Milestone 3: Run on the research DB

Add a CLI wrapper and execute it against `tmp/research_data/stocks.duckdb` with the worker paused.

Acceptance is:

    cd C:\work\meemee-screener
    $env:MEEMEE_DATA_DIR='C:\work\meemee-screener\tmp\research_data'
    python -m app.backend.tools.build_strategy_conditional_stats --label-version v1 --horizon 20 --max-codes 500

The output must report seeded strategies, non-zero aggregate rows, and at least one per-strategy count summary.

# Outcomes & Retrospective

The conditional-statistics layer is implemented and now supports broader routing research than the first breakout-only pass. `app/backend/services/strategy_backtest_service.py` owns the registry seed, trade-event-enabled replay, and aggregate materializer, while `app/backend/tools/build_strategy_conditional_stats.py` exposes the batch as a CLI.

On the lightweight research DB, the latest rebuild produced `319` rows for `top500 / v1 / horizon=20`. The registry now contains eight challenger strategies, and seven of them have non-zero conditional buckets. This is enough to let the router compare breakout, reversal, and pullback families without creating heavy per-trade artifacts.

After the decision-only gate fix, the table grew to `366` rows and all eight challengers became active. Tightening `lp_p3_pullback_v1` then reduced the table to `363` rows, with better pullback aggregates and fewer trades. The plumbing issues are resolved; the remaining work is quality. `ld_decision_up_v1` is now measurable but weak, and even the improved `lp_p3_pullback_v1` still loses the latest-day router comparison. The next step is to keep refining setup selection and exit logic, not to add more storage-heavy raw outputs.
