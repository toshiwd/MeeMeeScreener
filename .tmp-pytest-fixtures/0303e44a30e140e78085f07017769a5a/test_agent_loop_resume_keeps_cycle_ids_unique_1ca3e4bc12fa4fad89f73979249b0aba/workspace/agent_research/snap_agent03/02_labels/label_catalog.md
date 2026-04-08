# Label Catalog

- `buy_candidate`: long-side signal candidate evaluated by walk-forward expectancy.
- `sell_candidate`: short-side signal candidate evaluated by walk-forward expectancy.
- `takeprofit_candidate`: close-based exit overlay relative to 60-day baseline exit.
- `stop_candidate`: close-based stop overlay relative to 60-day baseline exit.
- `skip_candidate`: avoid-entry state where residual expectancy improves after exclusion.
- `failure_reason`: condition that appears meaningfully more often in losing cases than winners.
