# TOREDEX Narrative 2024-02-19

- season_id: tradex_bt3y_v8_fast_baseline_20260217
- mode: BACKTEST
- policy_version: toredex.v8

## Buy Top 3
1. 3905 ev=0.12884333821376281 upProb=1.0 revRisk=0.0 gate=True:ENTRY_OK
2. 7003 ev=0.1220614828209765 upProb=1.0 revRisk=0.0 gate=True:ENTRY_OK
3. 3993 ev=0.05745062836624776 upProb=1.0 revRisk=0.06668667466986795 gate=True:ENTRY_OK

## Sell Top 3
1. 3481 ev=-0.02228416511646217 upProb=0.8552867254348423 revRisk=0.8552867254348423 gate=True:ENTRY_OK
2. 4523 ev=-0.0202271666407344 upProb=0.8148380260369361 revRisk=0.8148380260369361 gate=True:ENTRY_OK
3. 8984 ev=-0.016474464579901153 upProb=0.8310393943063306 revRisk=0.8310393943063306 gate=True:ENTRY_OK

## Actions
1. 3905 LONG delta=2 reason=E_NEW_TOP1_GATE_OK
2. 9211 LONG delta=-2 reason=S_SWITCH_EV_GAP
3. 7003 LONG delta=2 reason=E_NEW_SWITCH_IN
4. 3993 LONG delta=5 reason=A_ADD_STAGE2_STRICT_OK

## Metrics
- equity: 16132907.487294
- cum_return_pct: 61.329075
- max_drawdown_pct: -6.512792
- holdings_count: 3
- game_over: False
