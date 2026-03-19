# Runtime Selection

## Purpose

MeeMee runtime resolves which logic artifact to use without owning publish governance.

The runtime consumes:

- local override state
- publish registry state
- local last-known-good artifact
- safe fallback

## Resolution Order

The resolution order is fixed:

1. `selected_logic_override`
2. `default_logic_pointer`
3. `last_known_good`
4. `safe fallback`

The resolver must not silently skip validation.

## Storage Locations

- `selected_logic_override`: MeeMee local config in `config/logic_selection.json`
- `default_logic_pointer`: publish registry metadata, mirrored locally
- `last_known_good`: MeeMee local cached artifact plus metadata
- `safe fallback`: builtin bundled fallback

`last_known_good` is a real local artifact, not just a pointer.

## Runtime Selection Snapshot

`GET /api/system/runtime-selection` must expose at least:

- `resolved_source`
- `selected_logic_id`
- `selected_logic_version`
- `logic_key`
- `artifact_uri`
- `snapshot_created_at`
- `override_present`
- `last_known_good_present`
- `validation_state`
- `source_of_truth`
- `registry_sync_state`
- `degraded`
- `last_sync_time`
- `bootstrap_rule`
- `champion_logic_key`
- `challenger_logic_keys`

During an active operator mutation, the runtime selection endpoint may return the last stable in-memory snapshot instead of forcing a fresh DB read. This is a stability measure for operator console refreshes, not a change to the resolution order.
The endpoint may also expose a minimal operator mutation observability block so operators can tell whether a refresh was served from a stable snapshot during a mutation.

## Publish Registry Read Path

Publish registry data is read in this order:

1. `external_analysis` source of truth
2. local mirror in `config/publish_registry.json`
3. empty safe state

The read order for publish registry does not change the runtime resolution order.

## Candidate Bundle Relation

`published_logic_artifact` and `published_logic_manifest` are prepared as a candidate bundle in `external_analysis` before manual approve/promote.
The candidate bundle's validation summary is authored in `external_analysis` only.
Legacy `ops_fallback_*` publish-maintenance observations are removed from the runtime snapshot; runtime now exposes maintenance state, last run timestamps, and live non-promotable legacy counts instead.

Runtime selection does not read the candidate bundle directly for choice resolution.
It still resolves from:

1. `selected_logic_override`
2. `default_logic_pointer`
3. `last_known_good`
4. `safe fallback`

The candidate bundle only influences which logic becomes promotable in publish governance.
`published_ranking_snapshot` is captured at bundle creation time when available and is never the runtime source of truth.

## Selection Semantics

- `selected_logic_override` is local and user-controlled.
- `default_logic_pointer` comes from the publish registry and identifies the current default champion path.
- `last_known_good` is local recovery state.
- `safe fallback` is only for continuity and must not be treated as a publish source of truth.

## Observability

The runtime snapshot must make the resolution path explicit:

- which source won
- whether the runtime is degraded
- whether the registry came from `external_analysis` or the local mirror
- which champion and challengers are currently visible
- which bootstrap rule produced the current champion
- which registry version is external and which is local
- whether the local mirror is normalized
- whether the mirror is stale or legacy
- operator mutation observability

Rollback target resolution itself belongs to publish governance, not runtime selection.
The operator console must not rely on a stale client-side rollback key; the backend resolves the latest valid rollback target from registry state.

## Skeleton / Extension Points

Current implementation is intentionally minimal:

- override UI is not included
- pure-function migration is not included
- full publish promotion automation is not included
- mirror normalize / resync is internal only and not a UI flow
- publish/maintenance cleanup is internal only and not a UI flow

Those can be added later without changing the resolution order.
