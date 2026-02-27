# TOREDEX Narrative 2023-10-11

- season_id: rank2_dd_v3_cutloss35_badregime_20260219
- mode: BACKTEST
- policy_version: toredex.v8

## Buy Top 3
1. 6315 ev=0.045454545454545456 upProb=0.9769195799018628 revRisk=0.01845836800071471 gate=True:ENTRY_OK
2. 6920 ev=0.06326488280439743 upProb=0.698733575549218 revRisk=0.36994569676133926 gate=True:ENTRY_OK
3. 5108 ev=0.014196383302349163 upProb=0.6638217994356247 revRisk=0.5688403867962195 gate=True:ENTRY_OK

## Sell Top 3
1. 8783 ev=-0.09090909090909091 upProb=1.0 revRisk=1.0 gate=True:ENTRY_OK
2. 2980 ev=-0.053597650513950074 upProb=1.0 revRisk=1.0 gate=True:ENTRY_OK
3. 3064 ev=-0.13014386082301774 upProb=0.8956713554987212 revRisk=0.8956713554987212 gate=True:ENTRY_OK

## Actions
1. 1514 LONG delta=-2 reason=R_CUT_LOSS_HARD
2. 9107 LONG delta=-2 reason=R_CUT_LOSS_HARD
3. 6315 LONG delta=2 reason=E_NEW_TOP1_GATE_OK
4. 6920 LONG delta=2 reason=E_NEW_TOPK_GATE_OK

## Metrics
- equity: 9301389.15485
- cum_return_pct: -6.986108
- max_drawdown_pct: -8.354225
- holdings_count: 2
- game_over: False
