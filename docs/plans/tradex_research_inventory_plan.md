# TRADEX Research Inventory Plan

## Purpose
- Build one authoritative inventory for the current TRADEX research surface.
- Keep the comparison contract fixed: same universe, same period, same top-K, same regime, same cost, same artifact detail level.
- Separate proxy compare units from MeeMee executable pipelines and from noncomparable or stale units.

## Current Symptoms
- Search breadth is wider than decision clarity.
- Proxy success, backtest success, and mere branching are easy to mix.
- Near-duplicate candidates are still being treated as independent branches.

## Files To Change
- `artifacts/research_inventory/research_inventory.json`
- `artifacts/research_inventory/pruning_decision.json`
- `artifacts/research_inventory/champion_bad_pick_ledger.json`
- `artifacts/research_inventory/candidate_gate_decision.json`
- `artifacts/research_inventory/weighting_ablation.json`
- `docs/plans/tradex_research_inventory_plan.md`
- `docs/plans/champion_bad_pick_ledger.md`
- `docs/plans/tradex_candidate_design_round_next.md`
- `docs/plans/tradex_weighting_and_label_hygiene.md`

## Non-Goals
- No MeeMee UI changes.
- No publish behavior changes.
- No auto-promotion into MeeMee.
- No multi-axis experiments in one pass.
- No silent fallback.

## Risks
- Historical artifacts may be incomplete or inconsistent.
- Some legacy variants may not have a directly comparable scope.
- The failure taxonomy is provisional and may need one refinement pass.

## Verification
- Inventory JSON is machine-readable.
- Comparable and noncomparable units are separated.
- Duplicate or no-op branches are marked explicitly.
- Bad-pick ledger contains typed reasons.
- At most three next candidates are proposed.
- No MeeMee files are modified.

## Sources
- `C:\work\meemee-screener\docs\reports\tradex_champion_challenger_eval.md`
- `C:\work\meemee-screener\docs\reports\tradex_research_session_rollup.md`
- `C:\work\meemee-screener\docs\reports\tradex_research_family_leaderboard_rr_confirmed_20260323.md`
- `C:\work\meemee-screener\docs\analysis\market_structure_ledger.json`
- `C:\work\meemee-screener\app\backend\tools\tradex_research_runner.py`
