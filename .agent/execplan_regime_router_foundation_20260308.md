# Purpose

MeeMee's current research loop searches for one strategy that works everywhere. Stored results already show that this is not sufficient: the same setup behaves differently across market states, and wide searches waste both compute and disk. This change adds the first durable foundation for an environment-aware router by materializing two daily label tables: one table that says what kind of market the day was, and one table that says what kind of 20-day future path each stock actually produced.

After this change, a developer can generate `market_regime_daily` and `future_pattern_daily` inside the research database, inspect their distributions, and use them as inputs for later strategy-routing work. The change is observable by running the new builder tool and then querying the new tables for row counts and label breakdowns.

# Evidence

Recent walkforward work has established two facts.

First, broad multi-side search keeps exploring dead or low-value regions. The worker had to be constrained to long-breakout variants because short-only and both-side candidates were repeatedly producing either zero trades or early drawdown failures.

Second, the current evaluation model still lacks any notion of market state or future path type. Full runs store aggregate performance, but they do not answer the question "which setup works in which environment." That missing conditioning is the main obstacle to building a router that can choose different playbooks for different states.

# Repo Orientation

`app/backend/services/strategy_backtest_service.py` already owns strategy-oriented schema helpers, backtest logic, and walkforward report persistence. This is the lowest-risk place to add new daily label builders because it already depends on the same market data tables and can create its own strategy-related tables lazily.

`app/backend/tools/` contains one-off or recurring backend scripts. A small CLI wrapper here is the safest way to make the new builders runnable without wiring new API surfaces first.

# Progress

- [x] Collected the existing design constraints and confirmed that `ml_feature_daily` is empty in the lightweight research DB, so the new builders must work directly from `daily_bars`.
- [x] Added `market_regime_daily` and `future_pattern_daily` schema helpers to `strategy_backtest_service.py`.
- [x] Added build functions that populate both tables and return compact summaries.
- [x] Added a CLI tool to execute both builders against the active DuckDB.
- [x] Ran the builder against `tmp/research_data/stocks.duckdb` and captured counts / label distributions.

# Surprises & Discoveries

The lightweight research DB intentionally omits `daily_ma`, and `ml_feature_daily` is empty. That means any durable label builder must be self-sufficient and compute its own moving averages / ATR metrics from raw `daily_bars`.

The research worker was still running during this task. Because DuckDB is sensitive to concurrent writers, the safe operational path is to stop the worker, materialize the new tables, and then restart the worker if needed.

The full build was fast enough to run routinely on the lightweight research DB. On 2026-03-08, building both tables for the full history completed in about 37 seconds and produced `7,782` market-regime rows plus `4,011,546` future-pattern rows.

The first label distribution is usable but not yet balanced. `panic_down` is the largest future-pattern bucket, so the thresholds are directionally sensible but will likely need calibration before the router consumes these labels as-is.

# Decision Log

2026-03-08: Decided to implement v1 labels with deterministic rules rather than clustering. This keeps the first pass interpretable, easier to validate, and lighter on storage.

2026-03-08: Decided not to depend on `ml_feature_daily` because the research DB currently has zero rows in that table.

2026-03-08: Decided to keep the new schema local to `strategy_backtest_service.py` instead of moving strategy-specific tables into global schema bootstrap. Existing walkforward tables in this repo already follow that pattern.

2026-03-08: Decided to keep the first build offline from the worker. The worker itself still does not consume the new labels, so the safest path was to materialize the tables first, then restart the worker unchanged.

# Milestones

## Milestone 1: Add daily regime and future-pattern tables

Add two schema helpers in `app/backend/services/strategy_backtest_service.py`.

`market_regime_daily` stores one row per date with breadth, advancers ratio, proxy/index trend distance, market ATR percentage, sector dispersion, and the resulting `regime_id`.

`future_pattern_daily` stores one row per `(code, dt, horizon)` with normalized forward returns, maximum favorable excursion (best move after entry), maximum adverse excursion (worst move after entry), forward drawdown, realized volatility, and the resulting `pattern_id`.

Acceptance is that the new builder code can create both tables in an empty research DB without manual DDL steps.

## Milestone 2: Materialize labels from real market data

Implement `build_market_regime_daily()` and `build_future_pattern_daily()` using `daily_bars` as the source of truth. The regime builder may mix SQL aggregation with small pandas post-processing because it only emits one row per day. The future-pattern builder should stay mostly in DuckDB SQL to avoid loading the entire market history into Python memory.

Acceptance is that both functions return summaries that include inserted row counts and label distributions.

## Milestone 3: Provide a repeatable CLI and run it

Add a tool under `app/backend/tools/` that runs both builders and prints compact JSON. Then run it against `tmp/research_data/stocks.duckdb` with the worker paused.

Acceptance is:

    cd C:\work\meemee-screener
    $env:MEEMEE_DATA_DIR='C:\work\meemee-screener\tmp\research_data'
    python -m app.backend.tools.build_regime_router_foundation --label-version v1 --horizon 20

The output must report non-zero rows in both new tables and list at least a few label categories for each.

# Outcomes & Retrospective

Implemented the first router foundation successfully.

`app/backend/services/strategy_backtest_service.py` now owns two new schema helpers and two new materialization functions: `build_market_regime_daily()` and `build_future_pattern_daily()`. `app/backend/tools/build_regime_router_foundation.py` provides a repeatable CLI wrapper.

The research DB now contains populated `market_regime_daily` and `future_pattern_daily` tables under label version `v1` and horizon `20`. The worker was restarted afterward and resumed normal prune/full-run behavior on the lightweight research DB.

What remains is the actual consumer layer: `strategy_registry`, `strategy_conditional_stats`, and a router that uses these new labels to choose setups rather than only testing fixed configurations.
