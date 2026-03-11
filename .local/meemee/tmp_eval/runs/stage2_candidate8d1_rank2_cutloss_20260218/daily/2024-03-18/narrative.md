# TOREDEX Narrative 2024-03-18

- season_id: stage2_candidate8d1_rank2_cutloss_20260218
- mode: BACKTEST
- policy_version: toredex.v8

## Buy Top 3
1. 6240 ev=0.05733944954128441 upProb=0.9059903381642511 revRisk=0.2252549651100376 gate=True:ENTRY_OK
2. 9418 ev=0.05573083534601893 upProb=0.7459532986410492 revRisk=0.18359231980510934 gate=True:ENTRY_OK
3. 9616 ev=0.048482952768220205 upProb=0.7597051940340215 revRisk=0.4034178630060428 gate=True:ENTRY_OK

## Sell Top 3
1. 6619 ev=-0.04844290657439446 upProb=1.0 revRisk=1.0 gate=True:ENTRY_OK
2. 7033 ev=-0.012366737739872069 upProb=0.8998139158576052 revRisk=0.8998139158576052 gate=True:ENTRY_OK
3. 9090 ev=-0.007251631617113851 upProb=0.7774179743223966 revRisk=0.7774179743223966 gate=True:ENTRY_OK

## Actions
1. 9501 LONG delta=-2 reason=R_CUT_LOSS_HARD
2. 5021 LONG delta=-2 reason=X_EXIT_GATE_NG
3. 6240 LONG delta=2 reason=E_NEW_TOP1_GATE_OK
4. 9418 LONG delta=2 reason=E_NEW_TOPK_GATE_OK

## Metrics
- equity: 13499162.763212
- cum_return_pct: 34.991628
- max_drawdown_pct: -6.671514
- holdings_count: 2
- game_over: False
