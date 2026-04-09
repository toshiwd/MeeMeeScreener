# TRADEX Preflight Policy

This document fixes the Phase 2A.1 preflight and normalization layer for TRADEX Research OS.

## Scope

- The preflight layer runs before the existing single-session TRADEX research runner.
- Its only job is to decide whether unshimmed single-session execution is runnable.
- It does not change compare truth.
- It does not change the authoritative decision policy.
- It does not generate judge or authoritative decision artifacts when it fails.

## Source Of Truth

- Family-level `compare.json` remains the only compare-contract truth for the research OS.
- Session-level `compare.json` remains non-authoritative session state.
- Preflight only checks execution readiness and artifact availability.
- Preflight must not be used to reinterpret compare contracts or decision policy rules.

## Preflight Order

Checks are evaluated in this order:

1. hypothesis validation
2. `execution.runner` is `tradex_research_session`
3. legacy analysis execution requirement
4. required inputs and required artifact presence
5. evaluation windows availability
6. regime rows existence
7. minimum window count
8. recognized artifact and input shape

If any check fails, the runner body must not execute.

## Typed Failure Codes

The machine-readable policy fixes these failure codes:

- `preflight_failed`
- `legacy_analysis_disabled`
- `evaluation_windows_unavailable`
- `regime_rows_empty`
- `insufficient_evaluation_windows`
- `missing_required_inputs`
- `artifact_shape_unrecognized`

## Normalization Boundary

- Preflight may normalize known upstream failure shapes into typed failure codes.
- Preflight may inspect existing TRADEX helpers that expose evaluation-regime rows and evaluation windows.
- Preflight must not repair upstream business logic.
- Preflight must not modify compare generation or decision policy logic.

## Artifact Behavior

- When preflight fails, the runner must persist `preflight_report.json`.
- When preflight fails, it must not create `judge_input.json`, `judge_decision.json`, or `authoritative_decision.json`.
- When preflight fails, it must not update `research_memory.json`.
- When preflight passes, the existing single-session runner path continues unchanged.

## Output Contract

`preflight_report.json` records:

- the experiment identity used by the research OS
- the hypothesis identity
- the runner name
- the typed status
- the typed failure code if any
- the normalized checked inputs
- the normalization steps applied

This layer is intentionally narrow. Phase 2A.1 only standardizes execution readiness and upstream failure classification.
