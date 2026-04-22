# Tools

Tooling for MeeMee build, release, self-test, and repo hygiene lives here.

## Build and release

- `build_release.cmd`
  - Builds the packaged application.
  - The normal build keeps only the onedir output under `release/MeeMeeScreener/`.
  - The default DuckDB source is `%LOCALAPPDATA%\\MeeMeeScreener\\data\\stocks.duckdb`.
  - Set `MEEMEE_RELEASE_DB_PATH` to point at a different DB for release packaging.
  - `-PackageZip` creates `release/MeeMeeScreener-portable.zip`.
  - `-SmokeRun` launches the built executable for a smoke check.
- `build_release.ps1`
  - PowerShell wrapper for `build_release.cmd`.

## Portable launcher

- `portable_bootstrap.cmd`
- `portable_bootstrap.ps1`
  - Validates the extracted ZIP runtime and starts `MeeMeeScreener.exe`.
  - Does not build artifacts.

## Self-test

- `selftest.ps1`
  - `-Mode dev`: run the development self-test flow.
  - `-Mode release`: run self-test against `release/MeeMeeScreener/`.

## Repo hygiene

- `check_repo_hygiene.ps1`
  - Run before heavy Codex or VS Code work.
  - Fails when repo-root scratch trees such as `.pytest_cache` or `.tmp-*` remain.
  - Fails when unpacked build or release residue stays in the repo.
  - Heavy TRADEX outputs must live under `G:\\Tradex`, not inside the repo.
  - Repo-root temp or cache directories are treated as reproducible residue and should be cleaned up.

## Supporting files

- `export_pan.vbs`
  - PAN export helper.
- `code.txt`
  - Reference code list input.
- `setup/`
  - DB, seed, and import setup helpers.
- `debug/`
  - Debugging scripts.
