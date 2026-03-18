# TOREDEX Narrative 2002-02-19

- season_id: toredex_multiframe_20260318
- mode: BACKTEST
- policy_version: toredex.v8

## Buy Top 3
1. 1001 ev=-0.02437332804914297 upProb=0.5236907507955786 revRisk=0.6598510883483211 frame=BEARISH Wup=0.5236907507955786 Mup=None Wdn=0.6598510883483211 Mdn=None gate=False:SETUP_reject
2. 1306 ev=-0.019367991845056064 upProb=0.449420814479638 revRisk=0.6687541478129714 frame=BEARISH Wup=0.449420814479638 Mup=None Wdn=0.6687541478129714 Mdn=None gate=False:SETUP_reject
3. 1308 ev=-0.026503567787971458 upProb=0.5850996475535801 revRisk=None frame=UNKNOWN Wup=None Mup=None Wdn=None Mdn=None gate=False:SETUP_reject

## Sell Top 3
1. 1001 ev=-0.02437332804914297 upProb=0.6598510883483211 revRisk=0.6598510883483211 frame=BEARISH Wup=0.5236907507955786 Mup=None Wdn=0.6598510883483211 Mdn=None gate=True:ENTRY_OK
2. 1306 ev=-0.019367991845056064 upProb=0.6687541478129714 revRisk=0.6687541478129714 frame=BEARISH Wup=0.449420814479638 Mup=None Wdn=0.6687541478129714 Mdn=None gate=True:ENTRY_OK
3. 1308 ev=-0.026503567787971458 upProb=0.1784144144144144 revRisk=None frame=UNKNOWN Wup=None Mup=None Wdn=None Mdn=None gate=False:SETUP_reject

## Actions
1. 1306 SHORT delta=-2 reason=X_EXIT_EV_DROP

## Metrics
- equity: 10004128.765006
- cum_return_pct: 0.041288
- max_drawdown_pct: -0.346072
- holdings_count: 0
- game_over: False
