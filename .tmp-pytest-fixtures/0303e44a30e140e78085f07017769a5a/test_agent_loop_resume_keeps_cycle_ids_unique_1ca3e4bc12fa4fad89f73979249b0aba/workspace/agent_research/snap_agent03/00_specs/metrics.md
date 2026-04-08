# Metrics

## Buy / Sell Adoption

- samples >= 80
- pooled expectancy > 0
- median fold expectancy > 0
- positive-fold ratio >= 0.55
- p90 close-MAE <= 0.08
- median hold <= 30

## Skip Adoption

- residual expectancy improvement >= 20 bp
- long/short both non-advantage, or move potential is too thin

## Failure Reason Adoption

- loser occurrence / winner occurrence >= 1.50
- reproduced in >= 3 folds
- loser share >= 0.25

## Takeprofit / Stop Adoption

- improvement versus 60-day close exit in expectancy, or
- p90 close-MAE improves by at least 15%
