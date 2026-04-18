# Buy Surface R8 Regime Diagnosis

## Objective
Diagnose the already-kept R6 operational challenger on the same confirmed symbol-level compare contract, but segmented by regime.

## Compare Contract
- Compare unit: `symbol`
- Selection unit: `symbol`
- Same universe, same period, same top-K, same regime, same cost, same artifact detail level
- Confirmed-data only
- Boundary-aware compare remains not executed

## Regime Segmentation
- `risk_off` -> down regime bucket
- `neutral` -> flat regime bucket
- `risk_on` -> up regime bucket
- Regime mapping follows the existing TRADEX compare contract over `market_regime_daily`

## Decision Rules
- `keep_global`: no regime blocks
- `keep_regime_scoped`: some regimes help, some block
- `hold`: segmentation still ambiguous
- `drop`: most regimes fail or overall uplift stays flat everywhere

## Current Diagnosis
- `risk_off` and `risk_on` are favorable
- `neutral` is the blocking regime
- The expected action is a regime-scoped keep, not a global keep

## Non-scope
- No new gate design
- No threshold retuning
- No gate recombination
- No policy reselection
- No overwrite/provenance work unless a new regression is reproduced
- No boundary-aware compare execution
- No MeeMee UI changes
- No model retraining
