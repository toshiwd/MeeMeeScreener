# Judgment Signal Value Test

## Current State
- confirmed: `strategy_judgement.json` is produced by TRADEX Research OS from `observation_snapshot.json` and mirrors the primary adapter output.
- confirmed: the current signal surface is `machine_action_state`, `human_readable_judgement`, `buy_score`, score subcomponents, `reason_codes`, and `adapter_outputs`.
- confirmed: `teacher_evaluation_row.json` labels the future outcome using a fixed 20-bar horizon.
- provisional: there is no verified evidence yet that historical buy/sell judgments improve same-condition ranking or realized trade outcomes.

## Problem
- The current benchmark corpus in `G:\Tradex\keep\research_os\trader_benchmark\v1` has zero complete-horizon rows, so realized trade-value evaluation cannot be completed from that corpus alone.
- Because of that, historical judgment history cannot yet be justified as a core model feature on evidence rather than narrative.

## Change Policy
- TRADEX only.
- One axis only: judgment-history value.
- No MeeMee UI changes.
- No boundary-aware compare execution.
- No publish or promote flow changes.
- No live inference semantic changes.

## Concrete Instructions
- Define historical buy/sell judgment as the TRADEX-generated `strategy_judgement.json` lineage plus the derived `human_readable_judgement` / `buy_score` outputs used for research comparison.
- Treat the signal as a research-only candidate until a complete-horizon, leakage-safe ablation produces stable out-of-sample value.
- Keep the current liquidity deep-dive branch intact.
- If future data allows a full ablation, use the same-condition contract, time-block split, embargo, and no-shuffle evaluation.
- For now, keep the signal available for explanation, not core scoring.
