# TOREDEX Narrative 2024-02-05

- season_id: stage2_candidate8d1_a60_rank2_20260218
- mode: BACKTEST
- policy_version: toredex.v8

## Buy Top 3
1. 9211 ev=0.03426791277258567 upProb=1.0 revRisk=0.0 gate=True:ENTRY_OK
2. 3099 ev=0.06639118457300275 upProb=0.8572745490981963 revRisk=0.382744854788943 gate=True:ENTRY_OK
3. 6702 ev=0.050134288272157566 upProb=0.8457825179689311 revRisk=0.2924066774866682 gate=True:ENTRY_OK

## Sell Top 3
1. 4506 ev=-0.028735632183908046 upProb=1.0 revRisk=1.0 gate=True:ENTRY_OK
2. 4005 ev=-0.07033361297841731 upProb=0.7283956739891796 revRisk=0.7283956739891796 gate=True:ENTRY_OK
3. 5727 ev=-0.016078838174273857 upProb=0.459154771431226 revRisk=0.459154771431226 gate=False:SETUP_reject

## Actions
1. 1711 LONG delta=-2 reason=R_CUT_LOSS_WARN
2. 9552 LONG delta=-2 reason=X_EXIT_GATE_NG
3. 9211 LONG delta=2 reason=E_NEW_TOP1_GATE_OK
4. 3099 LONG delta=2 reason=E_NEW_TOPK_GATE_OK

## Metrics
- equity: 13287476.079952
- cum_return_pct: 32.874761
- max_drawdown_pct: -6.671514
- holdings_count: 2
- game_over: False
