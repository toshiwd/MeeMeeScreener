# TOREDEX Narrative 2002-03-18

- season_id: toredex_multiframe_20260318
- mode: BACKTEST
- policy_version: toredex.v8

## Buy Top 3
1. 1306 ev=0.0 upProb=0.6801975654944332 revRisk=0.4171540872335636 frame=BULLISH Wup=0.6801975654944332 Mup=None Wdn=0.4171540872335636 Mdn=None gate=True:ENTRY_OK
2. 1001 ev=-0.012877747252747252 upProb=0.6361389169322577 revRisk=0.3615823002883286 frame=BULLISH Wup=0.6361389169322577 Mup=None Wdn=0.3615823002883286 Mdn=None gate=False:SETUP_reject
3. 1308 ev=0.0045871559633027525 upProb=0.4023695518723143 revRisk=None frame=UNKNOWN Wup=None Mup=None Wdn=None Mdn=None gate=False:SETUP_reject

## Sell Top 3
1. 1306 ev=0.0 upProb=0.4171540872335636 revRisk=0.4171540872335636 frame=BULLISH Wup=0.6801975654944332 Mup=None Wdn=0.4171540872335636 Mdn=None gate=False:SETUP_reject
2. 1001 ev=-0.012877747252747252 upProb=0.3615823002883286 revRisk=0.3615823002883286 frame=BULLISH Wup=0.6361389169322577 Mup=None Wdn=0.3615823002883286 Mdn=None gate=False:SETUP_reject
3. 1308 ev=0.0045871559633027525 upProb=0.5764514889342687 revRisk=None frame=UNKNOWN Wup=None Mup=None Wdn=None Mdn=None gate=False:SETUP_reject

## Actions
1. 1001 LONG delta=-2 reason=X_EXIT_GATE_NG
2. 1306 LONG delta=2 reason=E_NEW_TOP1_GATE_OK

## Metrics
- equity: 10135535.627413
- cum_return_pct: 1.355356
- max_drawdown_pct: -0.578233
- holdings_count: 1
- game_over: False
