# TOREDEX Narrative 2023-03-13

- season_id: tradex_bt_speed_probe2_20260217
- mode: BACKTEST
- policy_version: toredex.v8

## Buy Top 3
1. 7776 ev=0.16260162601626016 upProb=1.0 revRisk=0.0 gate=True:ENTRY_OK
2. 2585 ev=0.00790106492614222 upProb=0.8062372404083069 revRisk=0.08709570182643044 gate=True:ENTRY_OK
3. 3097 ev=0.011430678466076696 upProb=0.8018867924528301 revRisk=0.2917190775681342 gate=True:ENTRY_OK

## Sell Top 3
1. 3064 ev=-0.04120879120879121 upProb=0.7692957746478873 revRisk=0.7692957746478873 gate=True:ENTRY_OK
2. 6330 ev=-0.037800687285223365 upProb=0.5148843581683938 revRisk=0.5148843581683938 gate=False:SETUP_reject
3. 7744 ev=-0.04800984576915185 upProb=0.4342534036191228 revRisk=0.4342534036191228 gate=False:SETUP_reject

## Actions
1. 7911 LONG delta=-2 reason=X_EXIT_GATE_NG
2. 7776 LONG delta=2 reason=E_NEW_TOP1_GATE_OK

## Metrics
- equity: 9860193.440192
- cum_return_pct: -1.398066
- max_drawdown_pct: -2.522354
- holdings_count: 1
- game_over: False
