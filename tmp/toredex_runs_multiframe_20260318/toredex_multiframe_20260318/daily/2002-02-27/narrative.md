# TOREDEX Narrative 2002-02-27

- season_id: toredex_multiframe_20260318
- mode: BACKTEST
- policy_version: toredex.v8

## Buy Top 3
1. 1001 ev=0.036365418545383256 upProb=0.6146612149305997 revRisk=0.5688806242133 frame=MIXED Wup=0.6146612149305997 Mup=None Wdn=0.5688806242133 Mdn=None gate=True:ENTRY_OK
2. 1306 ev=0.02030456852791878 upProb=0.4680874811463047 revRisk=0.6500874811463047 frame=BEARISH Wup=0.4680874811463047 Mup=None Wdn=0.6500874811463047 Mdn=None gate=False:SETUP_reject
3. 1308 ev=0.015228426395939087 upProb=0.3608686868686869 revRisk=None frame=UNKNOWN Wup=None Mup=None Wdn=None Mdn=None gate=False:SETUP_reject

## Sell Top 3
1. 1306 ev=0.02030456852791878 upProb=0.6500874811463047 revRisk=0.6500874811463047 frame=BEARISH Wup=0.4680874811463047 Mup=None Wdn=0.6500874811463047 Mdn=None gate=False:SETUP_reject
2. 1001 ev=0.036365418545383256 upProb=0.5688806242133 revRisk=0.5688806242133 frame=MIXED Wup=0.6146612149305997 Mup=None Wdn=0.5688806242133 Mdn=None gate=False:SETUP_reject
3. 1308 ev=0.015228426395939087 upProb=0.4066043699838262 revRisk=None frame=UNKNOWN Wup=None Mup=None Wdn=None Mdn=None gate=False:SETUP_reject

## Actions
1. 1001 LONG delta=2 reason=E_NEW_TOP1_GATE_OK

## Metrics
- equity: 9952045.431673
- cum_return_pct: -0.479546
- max_drawdown_pct: -0.520618
- holdings_count: 1
- game_over: False
