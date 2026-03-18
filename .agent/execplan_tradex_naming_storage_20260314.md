# Tradex Naming and G Drive Storage ExecPlan

## Purpose

This change makes the external validation and research stack use the official name `Tradex`, meaning "Trade + CODEX". After this change, project documentation and default development storage point to `Tradex` instead of `Toredex`, and newly generated heavy development artifacts default to `G:\Tradex` so they do not pressure the system drive.

Users of MeeMee should still see MeeMee as the distributed product. Developers should see a clear boundary: MeeMee is the viewer and decision-update app, while Tradex is the external validation and research stack. The change is observable by reading the updated Markdown docs and the default runs path in `toredex_config.json`.

## Scope

This plan only changes naming policy, storage policy, and the default configuration for newly generated development artifacts. It does not rename existing Python packages, API routes, database table names, or historical artifacts in one pass. Those internal names still contain `toredex` and remain as compatibility identifiers until a later migration.

## Orientation

The current repository already documents that heavy validation stays outside MeeMee. The relevant files are `docs/MEEMEE_PRINCIPLES.md`, `docs/ARCHITECTURE_EXTERNAL_ANALYSIS.md`, and `docs/CANDIDATE_ENGINE_SPEC.md`. The current default runs location for the development validation stack is `toredex_config.json`, which still points to `G:/meemee_toredex_runs`.

The intent of this plan is to align these pieces under one public name and one storage root.

## Decisions

The official product-facing and documentation-facing name for the external validation stack is `Tradex`.

MeeMee remains the distributed user application. Tradex is the development-only external validation, replay, walk-forward, training, and optimization stack.

All newly generated heavy Tradex artifacts should default to `G:\Tradex`. The preferred child layout is:

    G:\Tradex\runs
    G:\Tradex\data
    G:\Tradex\artifacts

`G:\meemee_toredex_runs` and similar historical locations are legacy paths. They should not be deleted or bulk-moved automatically by this plan because they may contain large historical runs.

Internal compatibility names such as Python modules, route paths, and DuckDB table names may remain `toredex_*` during this phase. That is an explicit compatibility decision, not the desired end state.

## Milestone 1

Update the living product and architecture documents so they define Tradex as the official external-validation name and explain that MeeMee should not store heavy Tradex artifacts on `C:`.

Edit:

    docs/MEEMEE_PRINCIPLES.md
    docs/ARCHITECTURE_EXTERNAL_ANALYSIS.md
    docs/CANDIDATE_ENGINE_SPEC.md

Add plain-language statements that:

- Tradex is the official name of the external validation stack.
- MeeMee keeps only viewer and decision-update responsibilities.
- Heavy validation databases, replay outputs, model artifacts, and optimization logs belong under `G:\Tradex`, not under MeeMee user data on `C:`.
- Existing internal `toredex` identifiers are temporary compatibility names.

Acceptance:

- `rg -n "Tradex|G:\\\\Tradex|互換|compat" docs/MEEMEE_PRINCIPLES.md docs/ARCHITECTURE_EXTERNAL_ANALYSIS.md docs/CANDIDATE_ENGINE_SPEC.md` returns the new policy lines.

## Milestone 2

Update the default development configuration so newly created validation runs use the new storage root.

Edit:

    toredex_config.json

Change:

- `policyVersion` from the old `toredex.*` string to a `tradex.*` string.
- `runsDir` to `G:/Tradex/runs`.

This file name may remain unchanged in this phase to avoid breaking existing code that imports or opens it by name.

Acceptance:

- `Get-Content toredex_config.json | Select-String "tradex|G:/Tradex/runs"` shows the new defaults.

## Milestone 3

Record the migration boundary so future work does not accidentally break compatibility.

The documentation must state that a later migration may rename:

- Python package names
- CLI entry points
- API route names
- DuckDB table names
- existing run directories

That later migration must include compatibility shims and database migration steps. This plan does not perform those risky changes.

Acceptance:

- The decision is written into the documentation and this ExecPlan.

## Progress

- [x] Found existing `toredex` naming in code, docs, and config.
- [x] Found the current default G-drive runs location in `toredex_config.json`.
- [x] Updated the three policy documents to use `Tradex` and `G:\Tradex`.
- [x] Updated the default config path to `G:/Tradex/runs`.
- [x] Verified the new policy strings by search.

## Surprises & Discoveries

- The project already has a clean conceptual split between MeeMee and the external stack, but the docs still use `external_analysis` and `Toredex` in parallel.
- The current runs path already targets `G:`. The real gap is naming consistency and a single canonical root.
- A full internal rename would be large because `toredex` appears in modules, routes, scripts, tests, and DuckDB table names.

## Decision Log

- 2026-03-14: Chose a phased rename. Public and documentation naming moves to `Tradex` now, while internal `toredex` identifiers remain temporarily for compatibility.
- 2026-03-14: Chose `G:\Tradex` as the canonical heavy-artifact root because the user explicitly wants large development outputs off `C:`.
- 2026-03-14: Chose not to auto-move historical `G:\meemee_toredex_runs` content in this change because the dataset may be large and migration safety must be handled separately.

## Outcomes & Retrospective

2026-03-14: Completed the documentation and default-path phase. The repo now defines `Tradex` as the official external-validation name, new heavy outputs default to `G:\Tradex`, and the compatibility boundary is documented. A later phase is still required if we want to rename Python modules, routes, table names, and legacy G-drive folders without breaking existing data.
