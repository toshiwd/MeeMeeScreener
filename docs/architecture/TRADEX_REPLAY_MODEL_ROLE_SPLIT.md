# TRADEX Replay Model Role Split

`docs/contracts/tradex_replay_model_role_contract.json` is the authoritative source for this policy.
This markdown file is explanatory only.

## Purpose

This contract fixes the default operating split for TRADEX replay research so planning, implementation, verification gathering, and final judgment do not collapse into one unmarked lane.

This policy applies only to replay research:

- fixed-period replay research
- exact ledger reconstruction in `sell-buy` notation
- replay artifact generation
- postmortem interpretation after replay runs

## Fixed Roles

- `GPT-5.4 Thinking` is the planner/judge.
  - owns requirements, scope, non-scope, boundary judgment, compare-condition freeze, result interpretation, and final keep/drop/hold judgment
- `GPT-5.4 mini high` is the default Step 2 executor.
  - owns repo inspection, narrow implementation, artifact generation, test updates, fixed command runs, and raw verification gathering under the frozen contract

## Workflow

1. `GPT-5.4 Thinking` freezes the contract:
   - `Current State`
   - `Problem`
   - `Change Policy`
   - `Concrete Instructions`
   - non-scope
   - acceptance criteria
   - remaining risks
2. Step 2 is delegated by default to a separate `GPT-5.4 mini high` sub-agent.
3. `GPT-5.4 Thinking` reviews the returned evidence against the frozen contract, rejects drift if present, and issues the final structured judgment.

## Delegation Default

For replay-research work, the normal path is delegated Step 2 execution.

Default delegation covers:

- replay-research implementation work
- repo inspection
- artifact generation
- test updates
- raw verification gathering

`GPT-5.4 Thinking` must not silently absorb Step 2 executor work when that work is meaningful.

## Trivial Inline Exception

Inline Step 2 work is allowed only when both conditions hold:

- no real implementation is needed
- the work is limited to very small documentation or registry/pointer edits

This exception is narrow. It does not permit silent single-lane execution for meaningful implementation or verification work.

## Fallback Rule

If delegation is unavailable, that lane must be marked explicitly as `research-fallback`.

Under `research-fallback`:

- the planner/judge may perform Step 2 work directly
- the frozen 3-step workflow must still remain explicit
- single-lane execution must not be presented as normal split-role execution

## What This Does Not Change

This policy does not change:

- MeeMee behavior
- TRADEX replay logic
- TRADEX compare or evaluation conditions
- keep/drop/hold semantics
- broader non-replay TRADEX research unless a separate policy adopts the same split
