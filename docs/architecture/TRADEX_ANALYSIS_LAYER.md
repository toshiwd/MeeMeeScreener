# TRADEX Analysis Layer

## Purpose

TRADEX owns analysis responsibilities, not UI responsibilities.
This boundary exists so MeeMee can stay the operational product while TRADEX becomes the research, validation, and comparison layer.

The carve-out starts from responsibilities:

- define analysis input contracts
- separate analysis execution
- define analysis output contracts

## Analysis Input Contract

TRADEX analysis consumes only declared inputs.

Minimum input families:

- `confirmed_market_bars`
- `financial_facts`
- `trade_history_normalized`
- `published_logic_artifact`
- `published_logic_manifest`

In code, the typed entry boundary for this slice is `external_analysis/contracts/analysis_input.py`. It is the single object TRADEX should accept before analysis execution.

Optional inputs may include already-approved candidate metadata, but the input contract must not depend on MeeMee UI state.

The input contract must preserve source semantics:

- confirmed market data is the analysis baseline
- provisional intraday overlay is display-only and not a baseline input
- financial facts are shared reference data
- trade history is normalized before analysis

## Analysis Execution Layer

TRADEX execution is the isolated layer that turns inputs into evidence.

Responsibilities:

- feature evaluation
- backtest
- replay
- walk-forward
- comparison across candidate logic
- validation summary generation

Execution must be deterministic with respect to declared inputs and manifest versioning.
It must not own publish governance, runtime selection, or operator UI behavior.

For this slice, the single entrypoint is `external_analysis/runtime/orchestrator.py::run_tradex_analysis`. It receives the typed input contract, calls the existing decision builder behind the orchestrator, and returns the typed output contract.

## Analysis Output Contract

TRADEX outputs evidence objects that publish governance can consume.

Minimum output families:

- `validation_summary`
- `published_ranking_snapshot`
- candidate-ready manifest metadata

Output rules:

- `validation_summary` is the review gate for publish promotion
- `published_ranking_snapshot` is cache / audit output, not source of truth
- output artifacts must be immutable once published

The typed output boundary for this slice is `external_analysis/contracts/analysis_output.py`. The orchestrator maps the existing decision payload into that contract end to end.

## Boundary Rules

- MeeMee consumes TRADEX output but does not own analysis execution
- TRADEX prepares evidence but does not mutate MeeMee runtime state directly
- publish registry state remains governed by publish promotion rules, not analysis execution
- UI is outside this boundary

## Next Cut Line

The next carve-out should move analysis code toward these three contracts without changing publish / runtime operator behavior:

1. input contract validation
2. execution runner isolation
3. output artifact normalization
