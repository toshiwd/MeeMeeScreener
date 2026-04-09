# TRADEX Environment Readiness

This document defines the preflight readiness checks used before unshimmed single-session TRADEX execution.
It is intentionally narrow:

- it does not change compare truth
- it does not change decision policy
- it does not auto-repair missing upstream data
- it only classifies whether the local execution environment is ready enough to continue

## Scope

Environment readiness answers one question:

> Can the current unshimmed single-session TRADEX run proceed past preflight with the data and local runtime state that are currently available?

The readiness layer checks the following before runner execution:

- legacy analysis enablement
- configured DuckDB availability
- required upstream table presence
- expected upstream table schema
- required upstream row presence
- expected label-version coverage
- evaluation-window availability

## Cause Classification

The typed preflight failure codes remain the top-level result contract.
Readiness adds a second layer of cause classification inside `preflight_report.json` so failures are more actionable.

Possible readiness causes:

- `environment_not_ready`
- `database_dependency_missing`
- `required_table_missing`
- `required_table_empty`
- `schema_mismatch`
- `genuine_data_unavailable`

These cause classes are descriptive only.
They do not replace the existing typed preflight failure codes.

## Failure Interpretation

- `environment_not_ready` means the local execution environment is not configured for unshimmed TRADEX.
- `database_dependency_missing` means the configured DuckDB file is missing or unreadable.
- `required_table_missing` means the required upstream table does not exist.
- `required_table_empty` means the required table exists but has no usable rows.
- `schema_mismatch` means the expected upstream schema shape is not present.
- `genuine_data_unavailable` means the required upstream inputs exist, but there is still not enough usable data to form the required evaluation windows.

## Relationship to Preflight

Preflight still decides pass or fail using the existing typed failure codes.
Readiness only adds audit detail to the preflight report:

- `cause_class`
- `cause_source`
- `remediation_hint`
- `readiness_checks`
- `readiness_summary`

On failure, `preflight_report.json` is still the only artifact written.
No judge artifacts, no authoritative decision artifact, and no research memory update are produced for failed preflight runs.

