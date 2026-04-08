# Repo Hygiene Baseline 2026-04-08

## Purpose

This note records the repo cleanup baseline after the TRADEX storage boundary fix, release canonicalization, Git lightweighting, and repo-root scratch removal.

## Canonical Contract

- Heavy TRADEX outputs must go to `G:\Tradex`.
- Do not create repo-root temp/test/cache trees.
- In-repo release canonical artifact is the portable zip only.
- Do not keep build or unpacked release trees resident in the repo.

## Preflight

Run `powershell -ExecutionPolicy Bypass -File tools/check_repo_hygiene.ps1` before starting heavy Codex or VS Code work.

The check should stay lightweight. It reports:

- any repo-root temp/test/cache trees that have reappeared
- how many entries `git status` is currently surfacing
- how long `git status` took

## Recovery Rule

If the check reports root scratch, delete only reproducible residue.

If the check reports a slow or noisy `git status`, investigate new temp output or unexpected file churn before continuing.

## Baseline

- Repo size: 5.873 GB
- File count: 48,798
- Repo-root temp/test residue: 0
- Canonical release artifact: `release/MeeMeeScreener-portable.zip`
