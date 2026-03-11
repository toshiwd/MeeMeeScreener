# TOREDEX Narrative 2023-06-22

- season_id: stage2_rank2_topkgate_u005e000_20260220
- mode: BACKTEST
- policy_version: toredex.v8

## Buy Top 3
1. 8002 ev=0.04088485087892554 upProb=1.0 revRisk=0.21796708930402384 gate=True:ENTRY_OK
2. 3993 ev=0.02865761689291101 upProb=1.0 revRisk=0.1028563371202965 gate=True:ENTRY_OK
3. 6201 ev=0.028627838104639685 upProb=1.0 revRisk=0.21839136215012037 gate=True:ENTRY_OK

## Sell Top 3
1. 5246 ev=-0.053445850914205346 upProb=1.0 revRisk=1.0 gate=True:ENTRY_OK
2. 7717 ev=-0.02816342721142404 upProb=0.5706324943569059 revRisk=0.5706324943569059 gate=True:ENTRY_OK
3. 7383 ev=-0.07446808510638298 upProb=0.3321046314677297 revRisk=0.3321046314677297 gate=False:SETUP_reject

## Actions
1. 5445 LONG delta=-2 reason=X_EXIT_GATE_NG
2. 9201 LONG delta=-2 reason=X_EXIT_GATE_NG
3. 8002 LONG delta=2 reason=E_NEW_TOP1_GATE_OK
4. 3993 LONG delta=2 reason=E_NEW_TOPK_GATE_OK

## Metrics
- equity: 10741682.436717
- cum_return_pct: 7.416824
- max_drawdown_pct: -6.614346
- holdings_count: 2
- game_over: False
