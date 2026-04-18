# TRADEX Runtime DB Contract

## Purpose
- Fail closed when the research runner would otherwise bind to an empty or insufficient stocks DuckDB.
- Keep the outcome-only compare reproducible across environments by making the DB path choice explicit.

## Resolution Order
1. `STOCKS_DB_PATH` explicit runtime override.
2. `TRADEX_LIVE_STOCKS_DB_PATH` legacy live override.
3. `G:\Tradex\db\stocks.duckdb` default TRADEX runtime store.
4. `%LOCALAPPDATA%\MeeMeeScreener-dev\data\stocks.duckdb` populated evidence-bearing app-data store.
5. `%LOCALAPPDATA%\MeeMeeScreener\data\stocks.duckdb` legacy app-data store.

## Required Checks
- `market_regime_daily` must exist.
- `market_regime_daily` must have rows.
- `label_version = v1` rows must exist.
- The selected regime windows must cover `up`, `down`, and `flat`.
- The compare must keep `time_block_split`, `embargo`, and `no_shuffle` on.

## Failure Mode
- If the contract is not satisfied, the runner must raise `RuntimeError` before compare execution.
- Empty, missing, or incomplete regime stores are not allowed to silently fallback.

## Evidence-Bearing Path
- The compare that established the first outcome-only no-op used `C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb`.

## Enforcement Point
- The runner validates the contract via `app.backend.services.tradex_research_environment_readiness.evaluate_environment_readiness()`.
- The compare runner blocks before session execution when the readiness report is not ready.

## Scope
- TRADEX only.
- No MeeMee UI or publish flow changes.
- No boundary-aware compare execution.
