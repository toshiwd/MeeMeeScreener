# TRADEX Close-Based Buy Judgement v1

Authoritative goal:

> Judge whether buying at today’s close is eligible, using the completed candlestick and recent confirmed candle context up to that close.

## Core rules

- Close-based means execution timing is the day close.
- Close-based does not mean close-only judgement.
- The model must be candle-aware.
- Trigger and risk inputs must include confirmed OHLC candle structure, not close-derived features alone.
- The model may use volume, gap, breakout, recent high/low context, moving averages, and recent multi-bar candle sequence.
- Decision outputs must remain artifact-traceable.

## Required outputs

- `machine_action_state`: `enter | wait | skip`
- `human_readable_judgement`: `buy | hold | reject`
- `buy_score`
- `environment_score`
- `trend_score`
- `trigger_score`
- `risk_score`
- `invalidation_price`
- `invalidation_reason_code`
- `reason_codes`

## Candle feature minimum

- OHLC, not close-only
- candle body size
- upper wick size
- lower wick size
- body-to-range ratio
- gap up / gap down state
- breakout / failed-breakout relation to recent highs and lows
- multi-bar candle context
- volume expansion / contraction around the setup
- relationship between candle shape and key moving averages / range boundaries

## Design consequence

- `trigger_features` and `risk_features` must explicitly contain candle-structure features.
- A close-only model is not acceptable for v1.
- Reason codes should be explainable in candle terms where applicable, such as:
  - `breakout_failure`
  - `pullback_low_break`
  - `daily_swing_low_break`

