# TRADEX Weighting and Label Hygiene

## Status
Provisional. Verified code does not currently expose a TRADEX proxy evaluation path with `sample_weight = time_decay * regime_similarity * data_quality`.
This is backlog, not this round's execution axis.

## What Was Verified
- The repo contains research search-space weight sampling in `research/study_search_space.py`.
- That is not the same thing as evaluation weighting.
- No current TRADEX proxy compare path was verified to use the requested sample-weight formula.

## Proposed Formula
- `sample_weight = time_decay * regime_similarity * data_quality`
- `time_decay`: down-weight stale samples without hiding them.
- `regime_similarity`: keep weight high only when the sample regime matches the evaluation regime.
- `data_quality`: down-weight ambiguous labels near the boundary.

## Boundary Hygiene
- Add ambiguous-case handling near label thresholds only if it is traceable.
- Do not hide samples with silent fallback.
- Do not change live inference semantics casually.

## Ablation Axes
- Time decay
- Regime similarity
- Data quality / boundary-noise handling

## Acceptance
- Keep the comparison contract fixed.
- Measure top10 uplift, bad-pick removal, changed_top10_count, worst-regime delta, and overlap against the champion.
- Treat unchanged outputs as non-evidence.

## Backlog Position
- Do not implement weighting hygiene in the current round.
- Revisit only after the first comparable `bad-pick-removal` run lands.
- Keep `verified=false` until a proxy path is explicitly confirmed.

## Source
- `C:\work\meemee-screener\research\study_search_space.py`
- `C:\work\meemee-screener\docs\analysis\market_structure_ledger.json`
