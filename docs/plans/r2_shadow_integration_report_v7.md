# R2 Shadow Integration Report v7

## Current State
- confirmed: `R2 failed_rebound_reshort_chain` is accepted for shadow integration only.
- confirmed: production wiring remains false.
- confirmed: `outside_top20_locked = true`.
- confirmed: HMB-family alternatives remain non-adopted.

## Shadow Boundary
- The accepted candidate is `R2 failed_rebound_reshort_chain`.
- The fixed compare method is `boundary_local_rerank`.
- The shadow integration boundary is config-driven and read-only for production ranking.
- MeeMee reflection, publish candidate inclusion, and production auto-wire are not allowed.

## Monitoring Contract
- Track changed top-5 / top-10 membership counts.
- Track rank change count.
- Track top-5 / top-10 mean and median return metrics.
- Track support-influenced picks.
- Track bad-pick removal.
- Confirm the `outside_top20_locked` boundary on every shadow run.

## Rollback Boundary
- Shadow-only wiring can be disabled without changing the production ranking path.
- The boundary must remain isolated from publish flow and MeeMee surfaces.
- The accepted state is persisted in `config/tradex` JSON artifacts and mirrored in runtime selection snapshots.

## Authoritative JSON
- [r2_shadow_integration_state_v7.json](../../config/tradex/r2_shadow_integration_state_v7.json)
- [r2_shadow_monitoring_contract_v7.json](../../config/tradex/r2_shadow_monitoring_contract_v7.json)
- [r2_shadow_rollout_boundary_v7.json](../../config/tradex/r2_shadow_rollout_boundary_v7.json)
- [r2_shadow_verify_v7.json](../../config/tradex/r2_shadow_verify_v7.json)

