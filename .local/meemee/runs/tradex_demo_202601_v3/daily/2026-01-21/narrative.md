# TOREDEX Narrative 2026-01-21

- season_id: tradex_demo_202601_v3
- mode: BACKTEST
- policy_version: toredex.v2

## Buy Top 3
1. 6323 ev=0.12275165667402967 upProb=1.0 revRisk=0.13825077297546215 gate=True:ENTRY_OK_FALLBACK
2. 6525 ev=0.082312591866732 upProb=0.8441633199464524 revRisk=0.0 gate=True:ENTRY_OK_FALLBACK
3. 5801 ev=0.078125 upProb=0.5582364933741081 revRisk=0.40731906218144753 gate=True:ENTRY_OK_FALLBACK

## Sell Top 3
1. 5721 ev=-0.2818181818181818 upProb=0.175 revRisk=0.175 gate=True:ENTRY_OK_FALLBACK
2. 6330 ev=-0.07815275310834814 upProb=1.0 revRisk=0.0 gate=True:ENTRY_OK_FALLBACK
3. 6532 ev=-0.06636771300448431 upProb=0.24363495613656588 revRisk=0.5564641829844843 gate=True:ENTRY_OK_FALLBACK

## Actions
1. 2801 LONG delta=-2 reason=X_EXIT_GATE_NG
2. 4366 LONG delta=-5 reason=X_EXIT_GATE_NG
3. 8368 LONG delta=-2 reason=X_EXIT_GATE_NG
4. 6323 LONG delta=2 reason=E_NEW_TOP1_GATE_OK
5. 6525 LONG delta=2 reason=E_NEW_TOPK_GATE_OK
6. 6361 LONG delta=2 reason=E_NEW_TOPK_GATE_OK

## Metrics
- equity: 12009028.63863
- cum_return_pct: 20.090286
- max_drawdown_pct: -4.262253
- holdings_count: 3
- game_over: False
