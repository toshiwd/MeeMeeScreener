# TOREDEX Narrative 2002-02-20

- season_id: toredex_multiframe_20260318
- mode: BACKTEST
- policy_version: toredex.v8

## Buy Top 3
1. 1001 ev=-0.0013201990453945364 upProb=0.5236907507955786 revRisk=0.6598510883483211 frame=BEARISH Wup=0.5236907507955786 Mup=None Wdn=0.6598510883483211 Mdn=None gate=False:SETUP_reject
2. 1306 ev=-0.002079002079002079 upProb=0.449420814479638 revRisk=0.6687541478129714 frame=BEARISH Wup=0.449420814479638 Mup=None Wdn=0.6687541478129714 Mdn=None gate=False:SETUP_reject
3. 1308 ev=0.0010471204188481676 upProb=0.5761254730030307 revRisk=None frame=UNKNOWN Wup=None Mup=None Wdn=None Mdn=None gate=False:SETUP_reject

## Sell Top 3
1. 1306 ev=-0.002079002079002079 upProb=0.6687541478129714 revRisk=0.6687541478129714 frame=BEARISH Wup=0.449420814479638 Mup=None Wdn=0.6687541478129714 Mdn=None gate=True:ENTRY_OK
2. 1001 ev=-0.0013201990453945364 upProb=0.6598510883483211 revRisk=0.6598510883483211 frame=BEARISH Wup=0.5236907507955786 Mup=None Wdn=0.6598510883483211 Mdn=None gate=False:SETUP_reject
3. 1308 ev=0.0010471204188481676 upProb=0.2856666666666667 revRisk=None frame=UNKNOWN Wup=None Mup=None Wdn=None Mdn=None gate=False:SETUP_reject

## Actions
1. 1306 SHORT delta=2 reason=E_NEW_TOP1_GATE_OK

## Metrics
- equity: 10004128.765006
- cum_return_pct: 0.041288
- max_drawdown_pct: -0.346072
- holdings_count: 1
- game_over: False
