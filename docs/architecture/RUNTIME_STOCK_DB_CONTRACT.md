# Runtime Stock DB Contract

This contract fixes the shared stock database source used by MeeMee and TRADEX.

## Authoritative runtime path

- MeeMee and TRADEX runtime now resolve the stock DB to the freshest valid local snapshot among the allowed local appdata candidates when no explicit override is present.
- Explicit overrides still win:
  - `STOCKS_DB_PATH`
  - `TRADEX_LIVE_STOCKS_DB_PATH`
- The selected local snapshot must be the newest valid local `stocks.duckdb`, not the first valid one encountered.

## Resolution order

1. `STOCKS_DB_PATH`
2. `TRADEX_LIVE_STOCKS_DB_PATH`
3. Freshest valid local snapshot among:
   - `MEEMEE_DATA_DIR\stocks.duckdb`
   - `%LOCALAPPDATA%\MeeMeeScreener-dev\data\stocks.duckdb`
   - `%LOCALAPPDATA%\MeeMeeScreener\data\stocks.duckdb`
4. `G:\Tradex\db\stocks.duckdb`

## Allowed fallback paths

- `MEEMEE_DATA_DIR\stocks.duckdb`
- `%LOCALAPPDATA%\MeeMeeScreener-dev\data\stocks.duckdb`
- `%LOCALAPPDATA%\MeeMeeScreener\data\stocks.duckdb`
- `G:\Tradex\db\stocks.duckdb`

## Forbidden fallback behavior

- Do not silently continue when the resolved source is stale or incomplete.
- Do not treat a lagged source as a current-date judgment.
- Do not let TRADEX continue into comparison when the selected DB fails table, row-count, or freshness checks.

## Freshness requirements

- `daily_bars` must exist and contain rows.
- `market_regime_daily` must exist and contain rows.
- Latest trading date must be machine-readable.
- Requested symbol coverage must be checked against the requested evaluation date.
- When the source is older than the requested chart/evaluation date, the report must be marked `lagged_provisional` or `blocked`, never current.

## Failure mode

- If freshness is insufficient, TRADEX must fail closed.
- Required machine-readable stale-source fields:
  - `requested_chart_date`
  - `artifact_source_last_date`
  - `date_gap_days`
  - `date_match_status`
  - `judgment_validity_status`
  - `runtime_db_path`
  - `resolution_reason`
  - `source_freshness_status`

## Verified evidence-bearing run

- Update this section only after the fresh-path regression is re-run on a real machine.
