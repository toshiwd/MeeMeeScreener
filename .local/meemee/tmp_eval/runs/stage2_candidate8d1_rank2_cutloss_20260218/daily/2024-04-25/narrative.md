# TOREDEX Narrative 2024-04-25

- season_id: stage2_candidate8d1_rank2_cutloss_20260218
- mode: BACKTEST
- policy_version: toredex.v8

## Buy Top 3
1. 9842 ev=0.029411764705882353 upProb=0.9199999999999999 revRisk=0.4548241518829755 gate=True:ENTRY_OK
2. 9324 ev=0.007215007215007215 upProb=0.6555364736061946 revRisk=0.371630894207053 gate=True:ENTRY_OK
3. 4205 ev=0.010718113612004287 upProb=0.7682634079633721 revRisk=0.47165323847184676 gate=True:ENTRY_OK

## Sell Top 3
1. 3697 ev=-0.06137299780181239 upProb=1.0 revRisk=1.0 gate=True:ENTRY_OK
2. 5253 ev=-0.044407894736842105 upProb=1.0 revRisk=1.0 gate=True:ENTRY_OK
3. 4180 ev=-0.07122507122507123 upProb=0.8435294117647059 revRisk=0.8435294117647059 gate=True:ENTRY_OK

## Actions
1. 4316 LONG delta=-2 reason=R_CUT_LOSS_HARD
2. 7240 LONG delta=-2 reason=X_EXIT_GATE_NG
3. 9842 LONG delta=2 reason=E_NEW_TOP1_GATE_OK
4. 6997 LONG delta=2 reason=E_NEW_TOPK_GATE_OK

## Metrics
- equity: 17092494.64398
- cum_return_pct: 70.924946
- max_drawdown_pct: -9.049643
- holdings_count: 2
- game_over: False
