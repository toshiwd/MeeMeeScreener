# TRADEX Environment Model

TRADEX separates three layers of market interpretation:

1. long-horizon environment
2. mid-horizon regime transition
3. short-horizon execution opportunity

The environment labels are not a single opaque score. They are explicit states that must remain visible in artifacts.

## Environment states

- `trend_long`
- `trend_short`
- `range_buy`
- `range_sell`
- `panic_rebound`
- `bottom_building`
- `top_warning`
- `break_risk`
- `avoid`

## Execution support states

- `probe_entry`
- `add_ok`
- `concern_trim`
- `decisive_exit`

## Reporting rule

Authoritative artifacts must persist both:

- `long_horizon_regime_score`
- `recent_adaptation_score`

These must stay separate so the harness can detect candidates that only work in the latest theme-heavy market.
