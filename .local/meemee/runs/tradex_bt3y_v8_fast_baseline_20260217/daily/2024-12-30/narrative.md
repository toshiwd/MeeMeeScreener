# TOREDEX Narrative 2024-12-30

- season_id: tradex_bt3y_v8_fast_baseline_20260217
- mode: BACKTEST
- policy_version: toredex.v8

## Buy Top 3
1. 7205 ev=0.026105805697757178 upProb=1.0 revRisk=0.23603465851172276 gate=True:ENTRY_OK
2. 4825 ev=0.16390977443609023 upProb=0.7572340425531915 revRisk=0.31409996622762576 gate=True:ENTRY_OK
3. 7383 ev=0.017341040462427744 upProb=0.8385258964143426 revRisk=0.0 gate=True:ENTRY_OK

## Sell Top 3
1. 2695 ev=-0.028292354328056584 upProb=0.7904620462046205 revRisk=0.7904620462046205 gate=True:ENTRY_OK
2. 4527 ev=-0.02622673434856176 upProb=0.4965873519084346 revRisk=0.4965873519084346 gate=False:SETUP_reject
3. 4091 ev=-0.019068736141906874 upProb=0.5818264358838598 revRisk=0.5818264358838598 gate=True:ENTRY_OK

## Actions
1. 2432 LONG delta=-2 reason=X_EXIT_GATE_NG
2. 4784 LONG delta=-2 reason=X_EXIT_GATE_NG
3. 7205 LONG delta=2 reason=E_NEW_TOP1_GATE_OK

## Metrics
- equity: 17566614.915696
- cum_return_pct: 75.666149
- max_drawdown_pct: -6.512792
- holdings_count: 1
- game_over: False
