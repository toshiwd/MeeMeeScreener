# Chart Gallery Authoritative Overwrite Contract

## Scope
- MeeMee detail charts and TRADEX chart-consistency reports.
- Confirmed historical chart data is owned by the chart gallery confirmed store.
- Yahoo is a provisional lane for intraday or not-yet-imported dates only.

## Contract
- Confirmed data priority: chart gallery confirmed source.
- Provisional data priority: Yahoo intraday/unconfirmed source.
- Overlap rule: confirmed import replaces overlapping provisional coverage.
- Display rule: confirmed and provisional states stay separate in payloads.
- Judgment rule: confirmed judgment uses chart gallery confirmed data only.
- Provisional judgment rule: Yahoo data may power provisional judgment only when it is explicitly labeled provisional.

## Overwrite Semantics
- Trigger: a chart gallery import lands for a date or range that overlaps provisional Yahoo coverage.
- Scope: full overlap replacement for the confirmed-covered interval.
- Cache rule: overlapping provisional chart/judgment cache entries must be invalidated or recomputed from confirmed data.
- Partial-import rule: if confirmed coverage is incomplete, the system must fail closed for confirmed judgment and keep provisional labeling explicit.

## Machine-Readable Status
- See `artifacts/research_inventory/chart_gallery_authoritative_overwrite_contract.json`.
- See `artifacts/research_inventory/chart_gallery_authoritative_adoption.json`.
- See `artifacts/research_inventory/chart_data_provenance_contract.json`.
