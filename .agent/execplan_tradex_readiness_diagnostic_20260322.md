# TRADEX readiness / compare diagnostics

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

## Purpose / Big Picture

The current TRADEX research family can run end to end, but the last operational check showed that the candidate plans were collapsing to the same result: `signal_count=0` and `ready_rate=0.0` across baseline and A/B/C. The purpose of this change is not to change trading logic. It is to make the existing diagnostics tell us where plan differences disappear.

After this change, a TRADex family run should record the effective plan configuration, a readiness summary, a small per-row diff dump for baseline versus each candidate, and compare output that makes it obvious whether the plan difference reached the engine, got filtered before readiness, or was simply invisible to the compare metrics.

## Progress

- [x] (2026-03-22) Inspect the existing TRADEX experiment flow, identify where run artifacts and compare payloads are built, and confirm which outputs are already available for reuse.
- [x] (2026-03-22) Add effective config to each run artifact, including plan hash and readiness config hash.
- [x] (2026-03-22) Add readiness intermediate aggregation to each run and expose it in compare output.
- [x] (2026-03-22) Add a compact per-row diff dump for a small shared sample set across baseline and candidates.
- [x] (2026-03-22) Update or extend tests so the new diagnostic fields are covered with deterministic fake analysis data.
- [x] (2026-03-22) Run the smallest relevant backend tests and verify the new fields appear in the stored artifacts.
- [x] (2026-03-22) Re-run the real-data family with the new diagnostics and record the cut result.

## Surprises & Discoveries

- Observation: the current real-data family produced identical values for baseline and A/B/C because `publish_readiness.ready` never became true in the sampled inputs.
  Evidence: the latest operational run showed `signal_count=0`, `ready_rate=0.0`, and identical compare rows for all three candidates.

- Observation: the per-row diff dump initially came back empty because the shared trace samples are stored under `metrics.samples`, not `analysis.samples`.
  Evidence: the failing test showed `len(row_diffs) == 0`; the fix was to fall back to `metrics.samples`.

- Observation: the real-data diagnostic family showed different plan hashes but identical raw traces and zero readiness across baseline and candidates.
  Evidence: `diagnostic_row_diffs` reported `signal_changed=false`, `publish_ready_changed=false`, and `trace_hash_changed=false` for the sampled rows while `ready_pre_gate_rate=0.0` and `ready_post_gate_rate=0.0`.

## Decision Log

- Decision: treat this work as diagnostic instrumentation rather than a trading-logic change.
  Rationale: the problem to solve is not which threshold to tune, but whether candidate differences reach the engine and survive the readiness/filter path.
  Date/Author: 2026-03-22 / Codex

- Decision: store the effective config on the run artifact and include a hash of the readiness config used for the run.
  Rationale: this makes it possible to tell whether two runs were genuinely comparable and whether a cache or config drift caused the same output.
  Date/Author: 2026-03-22 / Codex

- Decision: compute compare row diffs from `metrics.samples` when `analysis.samples` is absent.
  Rationale: the run artifact already stores the per-sample traces in metrics, so reusing that source avoids duplicating large payloads and keeps the diff dump populated.
  Date/Author: 2026-03-22 / Codex

- Decision: classify the current real-data result as plan-difference-not-reaching-engine rather than a threshold problem.
  Rationale: the diagnostics show different effective configs, but the sampled raw traces do not change and readiness never becomes true, so lowering thresholds would not explain the missing variation.
  Date/Author: 2026-03-22 / Codex

## Outcomes & Retrospective

The diagnostic layer is now in place and backed by tests. Each run records the effective plan configuration, a readiness summary, and compare rows now include a compact per-row diff dump. The test family passes with the new fields present.

The real-data diagnostic family now gives a clearer cut result. The effective plan hashes differ, but the sampled raw traces do not: `trace_hash_changed=false` across the sampled rows, and both `ready_pre_gate_rate` and `ready_post_gate_rate` are zero. That means the candidate differences are not reaching the engine output path, so this is not a threshold-tuning problem.

## Context and Orientation

The TRADEX family runner lives in `app/backend/services/tradex_experiment_service.py`. It currently builds the run artifact, aggregates metrics, builds `compare.json`, and serves run detail through lazy cache files.

The file-backed storage helpers live in `app/backend/services/tradex_experiment_store.py`. The family and run artifacts are stored under `.local/meemee/tradex/families/<family_id>/`.

The current compare shape already includes baseline absolute metrics, candidate absolute metrics, metric deltas, winning examples, losing examples, and a review focus list. What is missing is the diagnostic layer that explains why candidates collapse to the same result.

In this plan, “effective config” means the normalized plan parameters that were actually used for the run, together with hashes that make drift visible. “Readiness summary” means a small set of aggregate counts and percentiles that describe how many samples became ready before and after the signal filter.

## Plan of Work

First, extend the run artifact builder in `app/backend/services/tradex_experiment_service.py` so every run records its effective plan configuration and a deterministic hash for that configuration. Add the readiness config hash from the current gate configuration file so the artifact shows which gate rules were active.

Second, add a readiness diagnostic summary on the run. This summary should be based on the existing per-sample traces and should report ready rate before filtering, ready rate after filtering, raw readiness score percentiles, pre- and post-filter counts, and counts of the reasons rows failed the gate.

Third, extend compare generation so each candidate entry carries the same diagnostic summary and a small per-row diff dump against the baseline. The dump should use a few shared code/date samples and show what changed, if anything, in the raw traces.

Fourth, extend the fake-data test in `tests/test_tradex_experiment_family_api.py` so the new fields are asserted directly. The test should prove that the compare payload contains the new diagnostics and that a repeated detail read reuses the same cache file.

## Concrete Steps

Work from `C:\work\meemee-screener`.

1. Update the run builder and compare generator in `app/backend/services/tradex_experiment_service.py`.
2. If needed, add small helper functions near the existing hash and aggregation helpers in the same file.
3. Extend `tests/test_tradex_experiment_family_api.py` with assertions for the new fields.
4. Run:

       python -m py_compile app/backend/services/tradex_experiment_service.py app/backend/services/tradex_experiment_store.py app/backend/api/routers/tradex.py
       python -m pytest tests/test_tradex_experiment_family_api.py -q

5. If the new test fails, inspect the stored compare JSON and run JSON in `.local/meemee/tradex/families/<family_id>/` to see whether the failure is in artifact generation or in the test expectations.

## Validation and Acceptance

The change is acceptable when a test family run stores:

- an `effective_config` block on each run with `plan_id`, `plan_version`, `plan_hash`, `effective_parameters`, and `readiness_config_hash`
- a readiness summary with the requested counts and percentiles
- compare rows that include the same diagnostic summaries and a small per-row diff dump
- a deterministic test that proves the new fields exist

The feature should be considered incomplete if compare output still only shows the old deltas and examples, because that would not answer why candidate differences disappear.

## Idempotence and Recovery

This work is additive. Re-running the test family should overwrite only the artifact files for that family. If a partial run fails, remove that family directory and rerun the test or create a new family id. Do not edit historical artifacts by hand.

## Artifacts and Notes

The most useful artifacts after implementation will be:

    .local/meemee/tradex/families/<family_id>/family.json
    .local/meemee/tradex/families/<family_id>/compare.json
    .local/meemee/tradex/families/<family_id>/runs/<run_id>/run.json
    .local/meemee/tradex/families/<family_id>/runs/<run_id>/detail/<code>.json

## Interfaces and Dependencies

The main implementation target is `app/backend/services/tradex_experiment_service.py`.

The compare payload returned by the backend must continue to expose the existing candidate fields and also include:

    effective_config
    readiness_summary
    row_diffs

The run artifact must continue to carry the existing family/run metadata and also include:

    effective_config
    readiness_summary
