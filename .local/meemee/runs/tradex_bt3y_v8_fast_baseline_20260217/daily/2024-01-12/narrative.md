# TOREDEX Narrative 2024-01-12

- season_id: tradex_bt3y_v8_fast_baseline_20260217
- mode: BACKTEST
- policy_version: toredex.v8

## Buy Top 3
1. 1514 ev=0.09841269841269841 upProb=1.0 revRisk=0.0 gate=True:ENTRY_OK
2. 3905 ev=0.19753086419753085 upProb=1.0 revRisk=0.18663663663663663 gate=True:ENTRY_OK
3. 9418 ev=0.10178120154461069 upProb=0.9852172690692089 revRisk=0.27063486344492504 gate=True:ENTRY_OK

## Sell Top 3
1. 1357 ev=-0.026595744680851064 upProb=1.0 revRisk=1.0 gate=True:ENTRY_OK
2. 6474 ev=-0.07105263157894737 upProb=0.7832823365785814 revRisk=0.7832823365785814 gate=True:ENTRY_OK
3. 3659 ev=-0.028409090909090908 upProb=0.5100660912260757 revRisk=0.5100660912260757 gate=False:SETUP_reject

## Actions
1. 1514 LONG delta=-2 reason=T_TP_PARTIAL_3_TO_2
2. 3905 LONG delta=2 reason=E_NEW_TOP1_GATE_OK

## Metrics
- equity: 14477785.81719
- cum_return_pct: 44.777858
- max_drawdown_pct: -6.512792
- holdings_count: 2
- game_over: False
