# TRADEX Analysis Layer Carve-out ExecPlan

## Purpose

This change gives TRADEX a single typed analysis entrypoint so analysis can be run from declared inputs and returned as a declared output object. The user-visible effect is not a new screen. The benefit is that TRADEX analysis can be called in a stable way without depending on UI state or ad-hoc dict payloads, and the existing runtime decision logic stays callable behind one orchestrator.

After this change, a caller can pass one typed analysis input object into TRADEX, receive one typed analysis output object back, and verify that the mapping from the existing decision payload remains stable through tests. MeeMee runtime and operator behavior are not changed by this slice.

## Progress

- [x] Confirmed the existing analysis decision builder and the typed TRADEX output contract already present in the repo.
- [x] Added the typed TRADEX analysis input contract.
- [x] Added the single TRADEX analysis orchestrator entrypoint.
- [x] Connected the orchestrator to the existing decision logic and typed output mapping.
- [x] Added tests proving the input and output mapping stability end to end.
- [x] Ran targeted verification.
- [x] Split the scoring core boundary into axis-based pure scorers and a final aggregator without changing the entrypoint contract.
- [ ] Decide whether any further internal scoring decomposition is worth the added maintenance cost.

## Orientation

This slice touches three connected boundaries.

`external_analysis/contracts/analysis_input.py` will define the typed input boundary. It must hold only declared values that the analysis runner needs, such as the symbol, as-of date, model-side probabilities, scenario rows, publish readiness, and override state. It should not know anything about MeeMee UI state.

`app/backend/services/analysis/analysis_decision.py` already contains the existing runtime decision logic. The orchestrator should keep this callable behind the new boundary instead of reimplementing it.

`external_analysis/contracts/analysis_output.py` already defines the typed output boundary. The orchestrator should map the existing result payload into that typed output contract and return it end to end.

## Milestone 1: Typed input boundary

Create `external_analysis/contracts/analysis_input.py` as the single typed input boundary for TRADEX analysis.

The input object must hold the declared fields used by the existing analysis decision builder:

- `symbol`
- `asof`
- `analysis_p_up`
- `analysis_p_down`
- `analysis_p_turn_up`
- `analysis_p_turn_down`
- `analysis_ev_net`
- `playbook_up_score_bonus`
- `playbook_down_score_bonus`
- `additive_signals`
- `sell_analysis`
- `scenarios`
- `publish_readiness`
- `override_state`

The object must provide stable serialization for tests and a helper that converts the typed input into the keyword arguments expected by the existing decision builder.

Acceptance signal:

    python -m pytest tests/test_tradex_analysis_input_contract.py -q

The expected observation is that the input object round-trips to a dict with declared fields and can be converted into runtime kwargs without depending on UI state.

## Milestone 2: Single orchestrator entrypoint

Create `external_analysis/runtime/orchestrator.py` with one public entrypoint, `run_tradex_analysis(input_contract)`.

The orchestrator must do three things only:

1. convert the typed input into the arguments expected by the existing decision builder
2. call the existing decision builder behind the orchestrator
3. map the resulting payload into the typed analysis output contract

The orchestrator must not split into multiple small services yet. It is the only entrypoint for this carve-out slice.

Acceptance signal:

    python -m pytest tests/test_tradex_analysis_orchestrator.py -q

The expected observation is that one typed input object produces one typed output object and that the existing decision logic still runs behind the orchestrator.

## Milestone 3: Typed output mapping stability

Extend the existing TRADEX output mapping so the orchestrator can return a typed output object end to end while preserving the existing mapping behavior.

The mapping must keep these output families stable:

- side ratios
- confidence
- reasons
- candidate comparisons
- publish readiness
- override state

The output object is evidence, not runtime state. It may be consumed later by publish governance, but it does not mutate runtime state directly.

Acceptance signal:

    python -m pytest tests/test_tradex_analysis_output_contract.py tests/test_tradex_analysis_orchestrator.py -q

The expected observation is that the output contract still maps the existing decision payload deterministically and the orchestrator returns the same typed boundary object every time for the same input.

## Surprises & Discoveries

- The existing decision builder already produces a stable analysis payload and is suitable to keep as the runtime logic behind the orchestrator.
- The typed TRADEX output contract already exists, so this slice is mostly a boundary and wiring change rather than a new scoring implementation.
- The TRADEX runtime package needed to remain lazily loaded to avoid a circular import between `analysis_decision.py` and the runtime adapter while the new pure scoring helpers were being extracted.

## Decision Log

- 2026-03-20: Chose to start the TRADEX carve-out from the analysis input/output boundaries rather than from UI or service decomposition, because the user asked for a responsibility-first split and the current runtime logic can stay callable behind one orchestrator.
- 2026-03-20: Chose to keep the existing analysis decision builder callable behind the orchestrator instead of rewriting it, because the goal of this slice is boundary stabilization, not analytical logic replacement.
- 2026-03-20: Chose to split the scoring core into axis-based pure scorers plus one aggregator instead of reworking the buy/sell formulas themselves, because the user explicitly asked for a fast-lane batch with stable behavior and no contract widening.

## Outcomes & Retrospective

The TRADEX analysis layer now has a single typed entrypoint, a typed output boundary, and a scoring core that is split into axis-based pure scorers plus one final aggregator. The public contracts stayed unchanged, and regression tests now pin the output shape for representative bullish and range-leaning inputs.

The main hidden dependency discovered during implementation was a circular import between the analysis decision module and the runtime package. The fix was to keep the runtime package lazily loaded instead of eager-importing the new helpers.
