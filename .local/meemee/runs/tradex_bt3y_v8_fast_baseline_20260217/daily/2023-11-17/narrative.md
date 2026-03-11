# TOREDEX Narrative 2023-11-17

- season_id: tradex_bt3y_v8_fast_baseline_20260217
- mode: BACKTEST
- policy_version: toredex.v8

## Buy Top 3
1. 2160 ev=0.11875249900039984 upProb=1.0 revRisk=0.011067366579177625 gate=True:ENTRY_OK
2. 2168 ev=0.08686440677966102 upProb=1.0 revRisk=0.28089977585654824 gate=True:ENTRY_OK
3. 6315 ev=0.07814999813098086 upProb=1.0 revRisk=0.0 gate=True:ENTRY_OK

## Sell Top 3
1. 3474 ev=-0.05217391304347826 upProb=1.0 revRisk=1.0 gate=True:ENTRY_OK
2. 4911 ev=-0.02228860294117647 upProb=1.0 revRisk=1.0 gate=True:ENTRY_OK
3. 9467 ev=-0.016176470588235296 upProb=1.0 revRisk=1.0 gate=True:ENTRY_OK

## Actions
1. 6254 LONG delta=-2 reason=X_EXIT_GATE_NG
2. 6871 LONG delta=-2 reason=T_TP_PARTIAL_3_TO_2
3. 2160 LONG delta=2 reason=E_NEW_TOP1_GATE_OK
4. 6871 LONG delta=-3 reason=S_SWITCH_EV_GAP
5. 2168 LONG delta=2 reason=E_NEW_SWITCH_IN

## Metrics
- equity: 13247849.015842
- cum_return_pct: 32.47849
- max_drawdown_pct: -6.512792
- holdings_count: 3
- game_over: False
