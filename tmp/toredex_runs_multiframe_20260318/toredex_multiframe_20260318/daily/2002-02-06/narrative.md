# TOREDEX Narrative 2002-02-06

- season_id: toredex_multiframe_20260318
- mode: BACKTEST
- policy_version: toredex.v8

## Buy Top 3
1. 1001 ev=-0.005804749340369393 upProb=0.4649142665484025 revRisk=0.6889461572089948 frame=BEARISH Wup=0.4649142665484025 Mup=None Wdn=0.6889461572089948 Mdn=None gate=False:SETUP_reject
2. 1306 ev=-0.004310344827586207 upProb=0.4183760683760684 revRisk=0.819391480730223 frame=BEARISH Wup=0.4183760683760684 Mup=None Wdn=0.819391480730223 Mdn=None gate=False:SETUP_reject
3. 1308 ev=-0.003243243243243243 upProb=0.6549279646043978 revRisk=None frame=UNKNOWN Wup=None Mup=None Wdn=None Mdn=None gate=False:SETUP_reject

## Sell Top 3
1. 1306 ev=-0.004310344827586207 upProb=0.819391480730223 revRisk=0.819391480730223 frame=BEARISH Wup=0.4183760683760684 Mup=None Wdn=0.819391480730223 Mdn=None gate=True:ENTRY_OK
2. 1001 ev=-0.005804749340369393 upProb=0.6889461572089948 revRisk=0.6889461572089948 frame=BEARISH Wup=0.4649142665484025 Mup=None Wdn=0.6889461572089948 Mdn=None gate=True:ENTRY_OK
3. 1308 ev=-0.003243243243243243 upProb=0.3425641025641026 revRisk=None frame=UNKNOWN Wup=None Mup=None Wdn=None Mdn=None gate=False:SETUP_reject

## Actions
1. 1001 SHORT delta=2 reason=E_NEW_TOP1_GATE_OK

## Metrics
- equity: 10000000.0
- cum_return_pct: 0.0
- max_drawdown_pct: 0.0
- holdings_count: 1
- game_over: False
