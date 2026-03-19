# Publish Promotion / Rollback

## Purpose

TradeX owns publish registry governance. MeeMee consumes the registry and exposes only the runtime view.

The registry is stored in `external_analysis` as the source of truth. MeeMee keeps `config/publish_registry.json` as a mirror and fallback copy only.

Promotion and rollback must never mutate the artifact file itself. Artifact files are immutable.

## State Model

The publish registry tracks these roles:

- `champion`: the single active logic entry
- `challenger`: one or more queued candidate entries
- `retired`: historical entries that are no longer active
- `demoted`: entries removed from active use
- `blocked`: entries that failed validation or checksum checks

Identity is `logic_id:logic_version`.
`artifact_uri` is only a locator.

## Multi-Challenger Queue

The registry supports a queue of challengers:

- one champion
- zero or more challengers
- queue order is stable and explicit
- retired and demoted entries remain in history

Each challenger record should carry at least:

- `logic_key`
- `logic_id`
- `logic_version`
- `logic_family`
- `artifact_uri`
- `checksum`
- `queued_at`
- `promotion_state`
- `queue_order`
- `validation_state`

## Bootstrap Champion Rule

Bootstrap champion selection must not depend on seed order.

The selection order is:

1. explicit `bootstrap_champion` flag
2. explicit `default_logic_pointer`
3. last stable promoted entry
4. empty safe state

The chosen rule must be stored in registry metadata as `bootstrap_rule`.

## State Transitions

Supported transitions:

- `enqueue challenger`
- `promote challenger -> champion`
- `demote champion -> demoted`
- `retire challenger`
- `rollback champion -> previous stable champion`

Previous champion entries are kept as rollback candidates in registry metadata.

## Candidate Bundle

`external_analysis` prepares a candidate bundle before manual review.

The bundle is the source of truth for candidate review data and contains:

- `published_logic_artifact`
- `published_logic_manifest`
- `validation_summary`
- optional `published_ranking_snapshot`

The bundle is assembled from `external_analysis` results only.
`ops_db` readiness fallback has been removed.
Readiness and validation summary are authored from `external_analysis` shadow/result data and are not read from ops.

`published_ranking_snapshot` is captured at bundle creation time when candidate rows exist and is not regenerated on approve/promote.
It remains a cache / audit artifact only.
Retention rules:

- `approved` / `promoted`: keep for 90 days by default
- `rejected` / `retired`: keep for 14 days by default
- orphaned or stale snapshots are sweep targets

## Promotion Gate

Promotion uses an approved candidate bundle plus validation and research evidence from TradeX. Minimum checks include:

- readiness passed
- sample count is sufficient
- expectancy is non-negative
- expectancy improved
- MAE is not worse
- adverse move is not worse
- stable window is true
- alignment is true

The candidate bundle must already exist in `external_analysis` and must be in `approved` state before promote is allowed.

Legacy candidates without a complete validation summary remain non-promotable.
They may be backfilled later, but promotion must still reject them until the summary is complete.
Backfill never auto-promotes and never consults `ops_db`.
If cleanup or migration is needed, use the maintenance helper / CLI to normalize legacy state instead of reintroducing fallback reads.

`approve` means "promotion may proceed". It does not mutate champion state.
`promote` means "candidate becomes champion".

Promotion failure must remain a failure. A local-only success must not be reported if `external_analysis` write fails.

## Rollback Rule

Rollback restores the previous stable champion from registry metadata.

Rollback must:

- preserve artifact immutability
- preserve audit history
- update `default_logic_pointer`
- keep the previous champion available as history or rollback candidate

## API Surface

Read APIs:

- `GET /api/system/publish/state`
- `GET /api/system/publish/queue`

Write APIs:

- `POST /api/system/publish/promote`
- `POST /api/system/publish/demote`
- `POST /api/system/publish/rollback`
- `POST /api/system/publish/challenger/enqueue`
- `POST /api/system/publish/challenger/retire`
- `POST /api/system/publish/candidates/{logic_key}/approve`
- `POST /api/system/publish/candidates/{logic_key}/reject`

Candidate read APIs:

- `GET /api/system/publish/candidates`
- `GET /api/system/publish/candidates/{logic_key}`

## Audit Trail

Audit records are stored in `external_analysis` as the primary trail.

Minimum fields:

- `action`
- `previous_logic_key`
- `new_logic_key`
- `changed_at`
- `source`
- `reason`
- `queue_order_before`
- `queue_order_after`

MeeMee may keep a mirror audit for continuity, but the authoritative audit trail lives in `external_analysis`.

## Read Path

MeeMee reads publish state in this order:

1. `external_analysis` registry
2. local mirror
3. empty safe state

The runtime view must expose:

- `source_of_truth`
- `champion logic_key`
- `challenger list`
- `default_logic_pointer`
- `registry_sync_state`
- `degraded`
- `bootstrap_rule`
- `last_sync_time`

## Sync / Repair

The sync state should make it obvious which side is authoritative and whether the mirror is stale.

Suggested values:

- `in_sync`
- `mirror_stale`
- `mirror_legacy`
- `external_unreachable`
- `external_invalid`

Repair is one-way:

- `POST /api/system/publish/mirror/normalize`
- `POST /api/system/publish/mirror/resync`

Repair must copy `external_analysis` to the local mirror. It must not overwrite the external source of truth from the mirror.

## Transitional Fallback Removal

`ops_db` readiness fallback has been removed.
The publish maintenance snapshot should expose:

- `maintenance_state`
- `candidate_backfill_last_run`
- `snapshot_sweep_last_run`
- `non_promotable_legacy_count`
- `maintenance_degraded`
- `updated_at`

`non_promotable_legacy_count` is a live aggregate derived from candidate bundle state.
It may be cached in maintenance state for reporting, but the source of truth is the current bundle table.

## Maintenance Runbook

Backfill and snapshot sweep are maintenance tasks, not auto-promotions.
They may be executed manually from CLI or internal API.
They may also run on a lightweight scheduler in MeeMee runtime when the env flag is enabled.

Maintenance commands:

- `python -m external_analysis publish-maintenance-backfill`
- `python -m external_analysis publish-maintenance-sweep`
- `python -m external_analysis publish-maintenance-cycle`
- `python -m external_analysis publish-maintenance-cleanup`

Both commands support `--dry-run`.
The same operations may also be triggered from MeeMee internal API or the lightweight scheduler.

Backfill rules:

- never auto-promote
- incomplete legacy candidates remain non-promotable
- only repair what can be reconstructed from `external_analysis` data
- do not use backfill to bypass validation summary gates
- do not consult `ops_db`

Cleanup rules:

- normalize legacy maintenance state
- remove deprecated `ops_fallback_*` columns when the engine supports it
- strip deprecated `ops_fallback_*` JSON residue from maintenance details
- keep startup tolerant of older files until cleanup has been run

Maintenance cleanup rules:

- legacy `ops_fallback_*` columns or JSON residue may exist in older DuckDB files
- cleanup helpers may drop or strip them in place
- startup must remain tolerant of old files and must not require manual intervention

Scheduler note:

- `MEEMEE_PUBLISH_CANDIDATE_MAINTENANCE_ENABLED=1` enables the internal background scheduler
- `MEEMEE_PUBLISH_CANDIDATE_MAINTENANCE_DRY_RUN=1` runs the scheduler in dry-run mode
- the scheduler is optional and must not be the only recovery path

Snapshot retention rules remain:

- `approved` / `promoted`: keep for 90 days by default
- `rejected` / `retired`: keep for 14 days by default
- orphaned or stale snapshots are sweep targets

## Notes

`published_ranking_snapshot` is a cache / audit artifact only.
It is not the source of truth for runtime selection.

`published_logic_artifact`, `published_logic_manifest`, `validation_summary`, and optional `published_ranking_snapshot` are stored together as a candidate bundle before manual promote.
Snapshot capture happens once at bundle creation time when rows are available.
Snapshot cleanup is a maintenance task and never changes source of truth state.

TODO for removal phase:

- once the last 7-day and 30-day fallback aggregates stay at zero across a sustained window, remove the ops fallback code path entirely
