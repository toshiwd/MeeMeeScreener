# Chart Data Provenance Contract

## Purpose
This contract separates confirmed historical candles from provisional current-day chart overlays so MeeMee and TRADEX never treat a provisional candle as confirmed analysis basis.

## Source Layers
- `daily`: confirmed rows from `runtime_stock_db.daily_bars`, with optional provisional Yahoo chart overlay for the current JST day.
- `weekly`: derived from the daily frame; it inherits the same confirmed/provisional separation.
- `monthly`: direct monthly bars plus daily consolidation; if provisional daily overlay is present, the frame is mixed and must be labeled.

## Rules
- Confirmed analysis must use confirmed rows only.
- Provisional display is allowed only when it is explicitly labeled as provisional or mixed.
- Mixing is allowed for display, but not for confirmed judgment.
- If a requested judgment date exceeds confirmed coverage, the analysis path must fail closed.
- Cached frames must expose cache source, cache timestamp, upstream source class, and cache freshness.

## MeeMee Runtime Access
- MeeMee resolves only the allowlisted chart provenance / overwrite artifacts through `app/backend/services/meemee_artifact_boundary.py` and `GET /api/system/meemee/artifacts/{artifact_name}`.
- Any artifact not on the allowlist is denied by default, including TRADEX-only and blocked / hold research artifacts.

## Required Payload Fields
- `chart_source_provider`
- `chart_source_type`
- `chart_source_path_or_identifier`
- `chart_requested_date`
- `chart_last_confirmed_date`
- `chart_last_provisional_date`
- `chart_date_match_status`
- `chart_source_freshness_status`
- `chart_data_classification`
- `chart_aggregation_source`
- `chart_cache_source`
- `chart_cache_generated_at`
- `chart_cache_upstream_source_class`
- `chart_cache_freshness_status`

## Verified Trace
- Current verified trace artifact: `artifacts/research_inventory/2531_chart_source_trace_20260416.json`
- Current runtime DB contract artifact: `artifacts/research_inventory/runtime_stock_db_contract.json`

