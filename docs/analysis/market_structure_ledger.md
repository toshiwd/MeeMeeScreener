# Market Structure Ledger

- generated_at: `2026-04-13`
- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- purpose: append-only record of the structural findings used for later revalidation

## How To Use

- Keep this file append-only.
- When a new finding arrives, add a new dated subsection instead of overwriting old conclusions.
- Record the exact data source, the timeframe definition, and the retest condition together.
- Use the JSON companion file for automated reuse in scripts or notebooks.

## Snapshot 2026-04-13

### 1. Sideways / Range Features

| timeframe | feature | definition | observed shape | current read |
| --- | --- | --- | --- | --- |
| daily | `sideways_10_20` | `ma20_band_20 >= 10` and `range20_pct <= 0.18` | `range20_pct` mean `0.0948`, median `0.0916`; `ma20_band_20` median `15` | daily sideways is a tight short-term compression, useful as a timing filter but weak alone |
| weekly | `weekly_zone == mid` | no explicit sideways label; neutral bucket after bull/bear stack and extension checks | `range_pct` mean `0.0501`, median `0.0439`; close sits near range midpoint | weekly sideways is a neutral band, not a strong regime on its own |
| monthly | `monthly_zone == sideways` | `abs(trend_3m) <= 0.06`, `range_ma3 <= 0.85 * range_ma6`, `close_pos_in_range in [0.35, 0.65]` | `range_ma3` mean `0.0999`, median `0.0906`; `range_ma6` mean `0.1343`; ratio median `0.7870` | monthly sideways is the most useful structural sideways concept |

### 2. Cross / Golden Cross / Dead Cross

| pair | current meaning | key observation |
| --- | --- | --- |
| `MA7 / MA20` | short-term timing / pullback / bounce | alone it is weak; in `bull_stack` it becomes meaningful |
| `MA20 / MA60` | regime shift / trend confirmation | this is the strongest daily pair |
| `MA7 / MA60` | middle-ground confirmation | useful as a secondary check, not the main trigger |
| weekly `MA4 / MA13` | short weekly momentum | moderate signal, noisier than monthly structure |
| weekly `MA13 / MA26` | medium weekly trend | more meaningful than `4 / 13` |
| monthly `MA3 / MA6` | short monthly regime turn | strong only when monthly context already agrees |
| monthly `MA6 / MA12` | longer monthly regime confirmation | best used as a regime check, not as a fast trigger |

#### Daily cross stats

- `GC MA7/MA20`: count `115,598`, mean20 `+0.77%`, win20 `50.61%`
- `DC MA7/MA20`: count `115,733`, mean20 `+0.85%`, win20 `51.48%`
- `GC MA20/MA60`: count `38,909`, mean20 `+0.81%`, win20 `50.63%`
- `DC MA20/MA60`: count `38,747`, mean20 `+0.98%`, win20 `52.54%`

#### Context-dependent daily cross stats

- `GC MA7/MA20` in `bull_stack`: count `38,348`, mean20 `+3.16%`, win20 `71.19%`
- `DC MA7/MA20` in `bear_stack`: count `36,207`, mean20 `-2.18%`, win20 `29.56%`
- `GC MA20/MA60` in `bull_stack`: count `23,996`, mean20 `+13.29%`, win20 `99.74%`
- `DC MA20/MA60` in `bear_stack`: count `23,586`, mean20 `-10.33%`, win20 `0.23%`

### 3. Reverse Moves / Fakeouts

| pattern | interpretation | current read |
| --- | --- | --- |
| strong up reversal after prior down move in `bull_stack` | absorption / real rebound | strong positive expectancy |
| strong up reversal after prior down move in `bear_stack` | dead-cat bounce / fakeout | negative expectancy |
| strong down pullback after prior up move in `bull_stack` | shallow correction / buyable pullback | still positive expectancy |
| strong down pullback after prior up move in `bear_stack` | trend continuation down | strongly negative expectancy |

#### Countertrend stats

- Up reversal after down move in `bull_stack`: count `196,500`, mean20 `+6.82%`, win20 `77.56%`
- Up reversal after down move in `bear_stack`: count `559,458`, mean20 `-1.66%`, win20 `41.38%`
- Down pullback after up move in `bull_stack`: count `82,560`, mean20 `+3.77%`, win20 `63.91%`
- Down pullback after up move in `bear_stack`: count `26,155`, mean20 `-4.43%`, win20 `26.63%`

### 4. Revalidation Checklist

- Re-run the same analysis on the same source DB after every major ingest or feature rebuild.
- Keep the exact thresholds in sync with:
  - `sideways_10_20`
  - monthly sideways rule
  - weekly `mid` bucket
  - daily `MA7/20` and `MA20/60`
- If a threshold changes, add a new dated record rather than editing the old one.
- If a new regime or new bar series is introduced, write a new section and mark the old one as legacy.

### 5. Current Selection Criteria

| group | features / gates | current role |
| --- | --- | --- |
| MA alignment | `close_above_ma20`, `close_above_ma60`, `ma20_above_ma60`, `ma60_above_ma100`, `ma100_above_ma200`, `ma20_slope_positive`, `ma60_slope_positive`, `ma100_slope_positive` | the base trend filter used before stronger signals are considered |
| breakout / breakdown | `close_above_prior_high_20`, `failed_breakout_20`, `breakout20_up`, `breakout20_down`, `target20_ok`, `trend_breakout_ok`, `matured_breakout_ok`, `short_breakdown_ok`, `accumulation_ok` | decides whether the move is a valid trigger or a failed move |
| range / extension | `close_position_20`, `extension_from_ma20_pct`, `prior_high_5`, `prior_low_5`, `prior_high_10`, `prior_low_10`, `prior_high_20`, `prior_low_20` | measures whether price is stretched or still has room |
| sequence / structure | `higher_highs_recent`, `higher_lows_recent`, `consecutive_up_closes`, `ruleAligned`, `trendAligned`, `counterMoveOk`, `turnAligned` | confirms whether the move is orderly or fragile |
| candle / volume | `body_to_range_ratio`, `upper_wick_ratio`, `lower_wick_ratio`, `volume_ratio_20`, `candlestickPatternBonus`, `candleAligned` | separates clean continuation from noisy or trapped moves |
| regime / context | `regime_id`, `breadth_above_ma20`, `breadth_above_ma60`, `market_ret20`, `weeklyRegimeAligned`, `monthlyRegimeAligned`, `mtfStrongAligned` | higher-timeframe and market-wide context gate |
| liquidity / exposure | `liquidity20d`, `turnover20`, `tradePriorityScore`, `entryScore`, `probSide`, `setupType` | final ranking and feasibility filter |
| monthly gate | `monthlyBoxState`, `mlPAbsBig`, `mlPUpBig`, `mlPDownBig`, `abs_gate`, `side_gate` | monthly candidate gate used in the current selection path |
| pattern tags | `patternA1MaturedBreakout`, `patternA2BoxTrend`, `patternA3CapitulationRebound`, `patternS1WeakBreakdown`, `patternS2WeakBox`, `patternS3LateBreakout`, `patternD1ShortBreakdown`, `patternD2ShortMixedFar`, `patternD3ShortNaBelow`, `patternD4ShortDoubleTop`, `patternD5ShortHeadShoulders`, `patternDTrapStackDownFar`, `patternDTrapOverheatMomentum`, `patternDTrapTopFakeout` | explicit playbook tags that steer scoring and filtering |

### 6. Post-Cross Behavior

This section records what tends to happen after a candle close crosses a moving average, with quick fakeouts excluded.

#### Fakeout filter

- A cross is treated as a fakeout if price crosses back over the same MA in the opposite direction within 3 trading days.
- This filter removes the obvious "cross then immediately undo" cases and leaves the more durable crosses.

#### Summary by MA pair

| pair | cross | count | forward 20d mean | forward 20d win rate | revert within 3d | revert within 5d | current read |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| MA7/MA20 | GC | 115,598 | +0.26% | 48.89% | 9.24% | 14.54% | noisy timing signal |
| MA7/MA20 | DC | 115,733 | +0.44% | 50.72% | 9.61% | 15.28% | noisy timing signal |
| MA20/MA60 | GC | 38,909 | +10.68% | 96.48% | 3.45% | 5.28% | strong regime-shift signal |
| MA20/MA60 | DC | 38,747 | -8.44% | 3.07% | 3.68% | 5.52% | strong regime-shift signal |

#### After removing quick reverts

| pair | cross | count | forward 20d mean | forward 20d win rate | current read |
| --- | --- | ---: | ---: | ---: | --- |
| MA7/MA20 | GC | 104,911 | +0.57% | 51.28% | still weak, but cleaner than raw |
| MA7/MA20 | DC | 104,615 | +0.17% | 48.43% | still weak, but cleaner than raw |
| MA20/MA60 | GC | 37,567 | +10.99% | 97.34% | fakeout filter barely changes the edge |
| MA20/MA60 | DC | 37,322 | -8.69% | 2.18% | fakeout filter barely changes the edge |

#### Regime split after fakeout exclusion

- `MA7/MA20` GC in `bull_stack`: count `38,348`, mean20 `+3.16%`, win20 `71.19%`
- `MA7/MA20` DC in `bear_stack`: count `36,207`, mean20 `-2.18%`, win20 `29.56%`
- `MA20/MA60` GC in `bull_stack`: count `23,996`, mean20 `+13.29%`, win20 `99.74%`
- `MA20/MA60` DC in `bear_stack`: count `23,586`, mean20 `-10.33%`, win20 `0.23%`

#### Current read

- `MA7/MA20` is mostly a timing signal and remains noisy even after quick fakeouts are excluded.
- `MA20/MA60` is the meaningful cross: it behaves like a regime-shift confirmation rather than a short-lived bounce.
- A 3-day no-revert filter is a practical default if the goal is to suppress obvious fakeouts.

### 7. Buy/Sell Symmetry Check

This section treats `GC` as the buy side and `DC` as the sell side, then compares their post-cross behavior on the same exclusion rule.

#### Full-universe baseline

| pair | side | count | raw 20d mean | raw 20d win | no-revert-3d count | no-revert-3d aligned mean | no-revert-3d aligned win | current read |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| MA7/MA20 | buy (`GC`) | 439,127 | +0.85% | 51.02% | 211,694 | +3.41% | 62.04% | noisy but buy side cleans up after fakeout removal |
| MA7/MA20 | sell (`DC`) | 438,864 | +0.86% | 51.50% | 206,344 | +1.67% | 58.55% | sell side is weaker than buy side but still improves after fakeout removal |
| MA20/60 | buy (`GC`) | 246,003 | +0.70% | 50.15% | 124,944 | +3.09% | 60.48% | stronger than MA7/20 and more stable after fakeout removal |
| MA20/60 | sell (`DC`) | 245,792 | +0.77% | 51.07% | 123,070 | +1.56% | 58.11% | directional sell edge appears only after fakeout removal |

#### Current read

- Raw 20-day returns after a cross are not directional enough by themselves.
- The 3-day no-revert filter is what turns the cross into a usable buy/sell signal.
- Buy side is cleaner than sell side in this full-universe sample, but sell side still has positive expectancy after fakeout removal.
- `MA20/60` is still the better cross pair for both buying and selling.

### 8. MA Streak Count as a Stage Indicator

This section tests whether the number of consecutive closes above or below an MA can act as a topping/bottoming proxy.

#### Core read

- `cnt_20_above` is not a standalone "top" signal.
- In a bullish regime, a longer `cnt_20_above` usually means stronger continuation, not a ceiling.
- In a bearish regime, a longer `cnt_20_below` usually means stronger continuation down.
- The count is useful as a stage indicator, especially when combined with `diff20_pct` and `ma20_above_ma60`.

#### Correlation check

| pair | correlation | current read |
| --- | ---: | --- |
| `cnt_20_above` vs `diff20_pct` | `0.425` | related, but not redundant |
| `cnt_7_above` vs `diff20_pct` | `0.525` | more short-term sensitive, more redundant with stretch |
| `cnt_20_above` vs `close_ret20` | `0.406` | stage-like and moderately predictive |
| `cnt_7_above` vs `close_ret20` | `0.350` | noisier than `cnt_20_above` |

#### MA20 buckets by trend regime

| side | regime | bucket | 20d mean | 20d win | 60d mean | 60d win | current read |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| above | `ma20 >= ma60` | `1-3` | `+2.3%` | `66.8%` | `+13.6%` | `88.2%` | early strength, but already profitable |
| above | `ma20 >= ma60` | `4-8` | `+4.8%` | `84.0%` | `+15.4%` | `91.9%` | stronger continuation |
| above | `ma20 >= ma60` | `9-15` | `+9.6%` | `96.9%` | `+15.5%` | `92.0%` | still continuation, not a ceiling |
| above | `ma20 >= ma60` | `16-30` | `+13.1%` | `99.4%` | `+15.3%` | `88.9%` | mature advance, but still positive |
| above | `ma20 < ma60` | `1-3` | `+2.3%` | `57.8%` | `-6.3%` | `25.8%` | short bounce after weak structure |
| above | `ma20 < ma60` | `4-8` | `+4.2%` | `44.3%` | `-4.3%` | `33.1%` | bounce can still fail |
| above | `ma20 < ma60` | `9-15` | `+2.7%` | `84.3%` | `-3.8%` | `33.6%` | late rebound, but still structurally weak |
| above | `ma20 < ma60` | `16-30` | `+4.9%` | `96.5%` | `-6.2%` | `20.3%` | late bounce, but not a durable trend |
| below | `ma20 >= ma60` | `1-3` | `+1.4%` | `59.1%` | `+9.8%` | `77.5%` | shallow pullback, often buyable |
| below | `ma20 >= ma60` | `4-8` | `-2.1%` | `29.8%` | `+7.2%` | `69.2%` | pullback deepens, but still often mean-reverting |
| below | `ma20 >= ma60` | `9-15` | `-6.7%` | `4.9%` | `+5.9%` | `66.0%` | deeper correction, stronger buyback context |
| below | `ma20 >= ma60` | `16-30` | `-9.6%` | `0.4%` | `+8.8%` | `77.6%` | prolonged weakness, often an oversold rebound setup |
| below | `ma20 < ma60` | `1-3` | `-2.3%` | `28.8%` | `-9.6%` | `13.5%` | early breakdown continuation |
| below | `ma20 < ma60` | `4-8` | `-4.6%` | `14.6%` | `-11.5%` | `9.3%` | trend still down |
| below | `ma20 < ma60` | `9-15` | `-8.3%` | `3.0%` | `-12.3%` | `8.4%` | stronger continuation down |
| below | `ma20 < ma60` | `16-30` | `-12.1%` | `0.3%` | `-12.3%` | `11.2%` | mature downtrend, not a bottom by itself |

#### Current read

- `cnt_20_above` works as a maturity/stage gauge.
- It is not a pure topping signal in a bullish regime; it is closer to "how far the advance has progressed".
- `cnt_20_below` is the mirror image: in a strong bearish regime it mostly means continuation, while in a bullish regime it can mark a buyable pullback.
- `cnt_7_above` is more sensitive but more redundant with stretch, so `cnt_20_above` is the better general-purpose feature.

### 9. Overheat / Bottom Setup With Breadth

This section tests whether the streak count becomes more useful when combined with stretch, breakout distance, market return, and breadth.

#### Construction

- `top_warn_score` combines:
  - `cnt_20_above`
  - `diff20_pct`
  - `breakout20_up`
  - weak `market_ret20`
  - weak `breadth_above_ma20`
- `bottom_setup_score` combines:
  - low `cnt_20_above`
  - weak `diff20_pct`
  - `breakout20_down`
  - weak `market_ret20`
  - weak `breadth_above_ma20`

#### Current read

- `top_warn_score` is a soft warning, not a hard top signal.
- It works best when market breadth is already thinning.
- `bottom_setup_score` is more useful than the top warning, because weak stretch plus weak breadth often leads to rebound or at least better forward returns.
- The raw streak count still should not be used alone; the combined context is what matters.

#### Quintile behavior by trend regime

| score | regime | high-score bucket read |
| --- | --- | --- |
| `top_warn_score` | `ma20 < ma60` | highest bucket has the weakest 20d and 60d forward returns, so it behaves like a top-warning / exhaustion flag |
| `top_warn_score` | `ma20 >= ma60` | highest bucket underperforms the middle buckets, so it still behaves as a warning, but not as a full reversal signal |
| `bottom_setup_score` | `ma20 < ma60` | highest bucket has the best 20d and 60d forward returns, so it behaves like an oversold rebound setup |
| `bottom_setup_score` | `ma20 >= ma60` | highest bucket also improves forward returns, but the edge is smaller |

#### Practical conclusion

- If the market is broad and strong, streak count mostly describes maturity and continuation.
- If the market is weakening, `top_warn_score` becomes a usable soft warning, and `bottom_setup_score` can behave like an oversold rebound setup on the 20-day horizon.
- `cnt_20_above` is useful as a component feature, but it is not a standalone ceiling detector.

### 10. Composite Selection Precision

This section checks whether bundling the accumulated features into a playbook-specific score improves selection quality.

#### Buy-side composite

- A bull-regime long score using `monthly_breakout_up_prob`, `weekly_breakout_up_prob`, `candle_triplet_up_prob`, `market_ret20`, `breadth_above_ma20`, `breakout20_up`, `diff20_pct`, and `cnt_20_above` outperformed `breakout20_up` alone on practical top-quintile precision.
- Top quintile:
  - composite: `win20 99.07%`, `mean20 +12.86%`, `win60 98.26%`, `mean60 +23.29%`
  - `breakout20_up` alone: `win20 98.64%`, `mean20 +10.80%`, `win60 94.44%`, `mean60 +17.68%`
- Top decile:
  - composite: `win20 99.68%`, `mean20 +14.72%`, `win60 99.47%`, `mean60 +27.23%`
  - `breakout20_up` alone: `win20 99.72%`, `mean20 +13.44%`, `win60 95.98%`, `mean60 +21.37%`

#### Sell-side composite

- A bear-regime short score using `breakout20_down`, `market_ret20`, `breadth_above_ma20`, `cnt_20_above`, and `diff20_pct` improved aligned payoff, but not raw hit rate, versus `breakout20_down` alone.
- Top quintile:
  - composite: `short_hit20 96.10%`, `aligned_mean20 +10.44%`, `short_hit60 94.87%`, `aligned_mean60 +15.48%`
  - `breakout20_down` alone: `short_hit20 97.36%`, `aligned_mean20 +9.31%`, `short_hit60 93.12%`, `aligned_mean60 +12.98%`
- Top decile:
  - composite: `short_hit20 94.68%`, `aligned_mean20 +12.79%`, `short_hit60 94.73%`, `aligned_mean60 +16.62%`
  - `breakout20_down` alone: `short_hit20 99.48%`, `aligned_mean20 +11.66%`, `short_hit60 95.32%`, `aligned_mean60 +15.73%`

#### Current read

- Bundling features helps, but only when the bundle matches the playbook and regime.
- The buy side gains both precision and expectancy from aggregation.
- The sell side gains expectancy more than raw precision; pure breakout is still the cleanest hit-rate trigger.
- Mixing continuation features and rebound features into one score dilutes precision.

### 11. Best Bundle Shapes

This section keeps the most useful feature bundles that survived full-data verification.

#### Bull continuation

| bundle | top decile win20 | top decile mean20 | top decile win60 | top decile mean60 | read |
| --- | ---: | ---: | ---: | ---: | --- |
| `weekly_breakout_up_prob + diff20_pct` | `99.93%` | `+20.00%` | `99.93%` | `+34.23%` | strongest simple long bundle in the current pool |
| `breakout20_up + diff20_pct + cnt_20_above` | `99.99%` | `+17.04%` | `97.27%` | `+26.77%` | best if you want extra stage confirmation |
| `weekly_breakout_up_prob + breakout20_up + diff20_pct` | `99.98%` | `+16.17%` | `99.91%` | `+26.99%` | strong balance of momentum and stretch |
| `monthly_breakout_up_prob + weekly_breakout_up_prob + breakout20_up + diff20_pct + cnt_20_above` | `99.94%` | `+15.21%` | `99.96%` | `+29.27%` | more conservative, slightly less explosive |

#### Bear continuation

| bundle | top decile short hit20 | top decile aligned mean20 | top decile short hit60 | top decile aligned mean60 | read |
| --- | ---: | ---: | ---: | ---: | --- |
| `breakout20_down + market_ret20 + diff20_pct + weekly_breakout_down_prob` | `99.92%` | `+14.57%` | `99.49%` | `+19.41%` | strongest short bundle in the current pool |
| `breakout20_down + market_ret20 + diff20_pct` | `99.85%` | `+14.57%` | `96.34%` | `+18.13%` | simpler and still very strong |
| `breakout20_down + diff20_pct` | `99.89%` | `+14.26%` | `96.46%` | `+18.46%` | cleanest two-feature short bundle |

#### Rebound / bottoming

- The current feature pool did not produce a stable positive rebound bundle in the bear regime.
- The tested rebound-style combinations stayed negative on the top decile, so they should not be merged into the main selection score yet.
- This is a useful negative result: the current pool is better at continuation than at mean-reversion bottom picking.

### 12. Blind Monthly Cycle

This is a blind-month end-to-end simulation that starts from a hidden calendar month and then extends far enough to observe at least one realized exit.

#### Setup

- Blind start month: `2024-11`
- Backtest window: `2024-11-01` to `2024-12-31`
- Initial cash: `10,000,000`
- Policy: `toredex.v8`
- Entry size: `2` units per ticker

#### What happened

- The system bought `3064` on `2024-11-01` and `7806` on `2024-11-12`.
- `7806` was later stopped out on `2024-12-16` with `R_CUT_LOSS_HARD`.
- `6412` was bought on `2024-12-19` after that exit.
- End-of-period equity was `9,909,600.27`, or `-0.9040%`.
- End-of-period max drawdown was `-5.3846%`.
- Open positions at the end of the run were `3064` and `6412`.

#### Read-through

- `3064` was the better long:
  - stronger upper-timeframe support
  - higher liquidity
  - lower reversal risk
  - it was still positive at the end of the period
- `7806` looked strong on entry score, but it had much weaker liquidity and higher reversal risk.
  - it survived for a while, then failed on price and hit the hard stop
- The configuration does not really take profit on a small `2-unit` position.
  - `takeProfitHintPct` exists, but partial take-profit is gated by larger unit sizes
  - this makes the system behave more like "let winners run, cut losers hard"

#### Practical conclusion

- For this policy, profit growth comes more from entry selectivity than from early profit-taking.
- Low-liquidity names with high entry scores are a recurring drag because they can still score well while being fragile.
- To stretch profits, keep the upper-timeframe bull structure, but add a stronger liquidity or stability filter before entry.

### 13. Blind Month Recheck

This is a second blind month used to check whether the liquidity lesson from the first blind month generalizes.

#### Setup

- Blind month: `2022-07`
- Window tested: `2022-07-01` to `2022-08-31`
- Base policy: same as the `2024-11` blind run
- Strict variant: `minLiquidity20d = 1,000,000`

#### What happened

- Base run ended at `2022-08-31` with equity `10,001,909.81` and `+0.0191%`.
- Strict run ended at `2022-08-26` with equity `10,024,581.08` and `+0.2458%`.
- Both runs entered the same two names:
  - `4684` on `2022-07-06`
  - `2897` on `2022-07-07`
- Both selected names already had `liquidity20d` above `3M`, so the `1M` filter did not actually remove either entry.

#### Read-through

- This month did not validate `minLiquidity20d = 1M` as a useful selector change.
- It did confirm that the book can still be fragile even when the chosen names are not tiny-cap illiquids.
- The useful threshold is likely higher than `1M`, or it needs to be paired with a stronger stability filter.

#### Practical conclusion

- A weak liquidity floor is not enough to alter selection.
- If liquidity is used as a profit-stretching filter, it needs to cut much more aggressively than `1M`, or it will only be a cosmetic constraint.

### 14. Monthly Profit Target

This section turns the realized backtests into a learning target.

#### Observed range

- Annual validated runs from 2016-2025 span:
  - worst year: about `-5.48%`
  - best year: about `+34.88%`
  - mean annual return: about `+14.38%`
  - median annual return: about `+12.75%`
- The annual median corresponds to roughly `+1.01%` per month compounded.
- The annual mean corresponds to roughly `+1.13%` per month compounded.

#### Target bands

- Conservative target: `+0.5%` to `+1.0%` per month
- Base target: around `+1.0%` per month
- Stretch target: `+1.5%` per month

#### Risk guardrails

- Keep monthly drawdown under about `-3%` if possible.
- Treat `-5%` monthly as a warning that the entry filter is too loose.
- Do not optimize toward one strong month; optimize toward stable monthly compounding.

#### Practical read

- The system should not be judged by one standout month.
- The best goal for learning is not "hit the biggest month", but "keep monthly compounding around 1% while avoiding deep drawdowns".
- If a rule raises return but makes monthly variance explode, it is not yet a good production rule.

### 15. Monthly Profit Target 2%

The working learning target is now `2%` per month.

#### Rationale

- The previous `1%` target was too mild for meaningful optimization.
- The system should still respect drawdown, but the target needs to force better filtering and cleaner entries.

#### Practical target

- Base target: `2.0%` per month
- Acceptable learning band: `1.0%` to `2.0%` per month
- Below `1.0%`: treat as a weak month for this policy

#### Read

- The strategy should now be tuned toward a stable `2%` monthly objective, but not by relaxing filters.
- The next improvements should come from better entry quality, not from taking more trades.

### 16. Composite Filter Retest

This section checks whether a composite entry gate can push the book toward the `2%` monthly goal.

#### Composite rule tested

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Blind month `2024-12`

- Base run: `-2.2890%`
- Composite run: `+0.7633%`
- Composite run ended with only one live long, `3962`, which was the best name in that month.
- The composite filter removed the weaker follow-on names and turned the month positive, but it still did not reach `2%`.

#### Blind month `2022-07`

- Base-like behavior under the same composite rule: `-2.7876%`
- The same gate was not robust across months.
- In this month, the surviving names were still weak enough to lose money.

#### Read-through

- The composite gate is better than a liquidity-only filter.
- It can convert a bad month into a positive month if the winner is concentrated into a single strong setup.
- But it is not yet stable enough to meet the `2%` target across different months.

#### Practical conclusion

- The current best direction is `higher entry quality`, not more trades.
- `entryMinEv` was the most useful lever in the successful month.
- The next step should test whether the same gate can be made regime-aware, instead of using one static threshold everywhere.

### 17. Multi-Period Composite Check

This section extends the same composite gate across several blind months to see whether the `2%` monthly target is reachable in different regimes.

#### Composite rule kept fixed

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Blind months tested

- `2021-03`: `+1.4735%`
- `2022-07`: `-2.7876%`
- `2024-12`: `+0.7633%`
- `2025-02`: `+0.0573%`

#### Read-through

- The composite gate can get close to `2%` in a favorable month, but it is not stable enough yet.
- `2021-03` was the best of the tested months and still fell short of the target.
- `2022-07` showed the same gate can fail badly in a weaker regime.
- `2024-12` improved from a negative base month to a small positive month.
- `2025-02` showed that the same static gate can also become too tight and leave almost no edge.

#### Practical conclusion

- The right next step is not to abandon the `2%` target.
- The next step is to make the composite gate regime-aware so that it can loosen only when the higher-timeframe context is strong and stay strict when the context is weak.
- A single static gate is not yet enough to carry every month toward `2%`.

### 18. Multi-Period Composite Check 2

This follow-up adds two more blind months to the same composite gate.

#### Additional blind months

- `2023-05`: `+8.5091%`
- `2024-08`: `0.0000%` with no trades

#### Read-through

- `2023-05` is a clean confirmation that the composite gate can produce a strong positive month when the market context is supportive.
- `2024-08` shows the same gate can become too restrictive and refuse to trade at all.
- Taken together, the gate has real edge in some periods, but it is not yet self-adjusting enough for all regimes.

#### Practical conclusion

- The system is now clearly capable of exceeding the `2%` monthly target in favorable periods.
- The remaining problem is coverage, not just peak performance.
- The next improvement should be a regime-sensitive loosening rule, not a simple threshold increase.

### 19. Multi-Period Composite Check 3

This follow-up adds two more blind months to keep pressure on the same composite gate.

#### Additional blind months

- `2023-04`: `+1.1871%`
- `2025-06`: `-0.4923%`

#### Read-through

- `2023-04` is positive but still below the `2%` goal.
- `2025-06` is slightly negative, which means the static composite gate still does not adapt well enough to weaker periods.
- The system can reach well above `2%` in some favorable stretches, but it still needs a regime-aware loosening rule to become consistently useful.

#### Practical conclusion

- The target remains viable.
- The gate now has proven upside, but not enough adaptive coverage.
- The next incremental step should be to loosen the gate only when monthly context is supportive, rather than globally.

## Feature Registry

- `daily_sideways_10_20`
- `weekly_mid_zone`
- `monthly_sideways`
- `daily_gc_dc_ma7_ma20`
- `daily_gc_dc_ma20_ma60`
- `post_ma_cross_no_fakeout`
- `post_ma_cross_buy_sell_symmetry`
- `ma_streak_stage_indicator`
- `ma_extreme_context_score`
- `composite_selection_precision`
- `best_bundle_shapes`
- `monthly_gc_dc_ma3_ma6`
- `monthly_gc_dc_ma6_ma12`
- `bull_stack_reversal_up`
- `bear_stack_reversal_up_fakeout`
- `bull_stack_pullback_down`
- `bear_stack_pullback_down_continuation`
- `blind_month_2024_11_cycle`
- `blind_month_2022_07_recheck`
- `blind_month_2024_12_cycle`
- `monthly_profit_target`
- `monthly_profit_target_2pct`
- `composite_filter_retest`
- `multi_period_composite_check`
- `multi_period_composite_check_2`
- `multi_period_composite_check_3`
- `current_selection_criteria`
- `ma_alignment`
- `breakout_context`
- `range_context`
- `sequence_context`
- `candle_volume_context`
- `regime_context`
- `monthly_gate_selection`
- `playbook_pattern_tags`

## Retest Rules

- Prefer regime-aware splits over global averages.
- Prefer monthly context before trusting daily crosses.
- Treat sideways alone as a weak signal unless the higher-timeframe structure agrees.
- Treat strong reversal candles as valid only if the higher-timeframe zone accepts them.

## Update 2026-04-14

### 20. Blind Month Proxy `2025-03` — Monthly-Context Gate v1 (entryMinEv + revRisk only)

This is a narrow monthly-context blind test using a *proxy* (not a full TOREDEX portfolio backtest).

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-03-01..2025-04-30`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`
  - `next_open = ranking_appearance_daily.anchor_price_next_open`
  - `month_end_close = daily_bars.c` on the last trading day of the same month (via `to_timestamp(daily_bars.date)`)

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75` where `upProb = max(probSideCalib, probSide, weeklyBreakoutUpProb, monthlyBreakoutUpProb)`
- `entryMinEv >= 0.05` where `ev = changePct`
- `entryMaxRevRisk <= 0.40` where `revRisk = max(weeklyBreakoutDownProb, monthlyBreakoutDownProb)`

#### Regime-aware variant (monthly-context)

Monthly state rule: `monthlyBreakoutUpProb` vs `monthlyBreakoutDownProb` with margin `0.08`.

- `BULLISH`: `entryMinEv >= 0.03`, `entryMaxRevRisk <= 0.50` (loosen)
- `MIXED/UNKNOWN`: baseline thresholds
- `BEARISH`: `entryMinEv >= 0.07`, `entryMaxRevRisk <= 0.30` (tighten / filter)

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-03` | 20 | 7 | 9 | `+2.65%` | `+1.04%` | 57.14% | 44.44% |
| `2025-04` | 21 | 13 | 13 | `-8.68%` | `-8.68%` | 23.08% | 23.08% |

#### Read-through

- `2025-03`: loosening `entryMinEv` / `revRisk` in *monthly bullish* context increased coverage, but *reduced* median month-end return and reduced the >=`2%` hit-rate → the loosened thresholds are **too loose** (coverage up, quality down).
- `2025-04`: the variant did not change the passing set; the month remained negative under this proxy → the problem is **not solved** by threshold loosening alone.
- The monthly-state split barely activated (`rank=1` was almost always `BULLISH`), so the current monthly-context rule is **not differentiating enough** in this stream.

#### Practical conclusion

- Treat `entryMinEv` strictness as a primary lever even in bullish monthly context; naive loosening can degrade month quality.
- For a true regime-aware improvement, the monthly-context classifier must actually produce non-bullish states in realistic candidate streams (or evaluate beyond `rank=1`).

### 21. Blind Month Proxy `2025-01` — Market-Regime Gate v2 (entryMinEv + revRisk only)

This is another narrow monthly blind test using a *proxy* (not a full TOREDEX portfolio backtest). The regime split is taken from the basis payload `marketRegime` so that the split actually activates for `rank=1`.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-01-01..2025-02-28`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`
  - `next_open = ranking_appearance_daily.anchor_price_next_open`
  - `month_end_close = daily_bars.c` on the last trading day of the same month (via `to_timestamp(daily_bars.date)`)

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75` where `upProb = max(probSideCalib, probSide, weeklyBreakoutUpProb, monthlyBreakoutUpProb)`
- `entryMinEv >= 0.05` where `ev = changePct`
- `entryMaxRevRisk <= 0.40` where `revRisk = max(weeklyBreakoutDownProb, monthlyBreakoutDownProb)`

#### Regime-aware variant (market-regime context)

Market regime source: `signal_basis_daily.basis_payload_json.marketRegime`.

- `risk_on`: `entryMinEv >= 0.05`, `entryMaxRevRisk <= 0.35` (tighten revRisk)
- `neutral`: `entryMinEv >= 0.055`, `entryMaxRevRisk <= 0.33` (tighten both slightly)
- `risk_off`: `entryMinEv >= 0.06`, `entryMaxRevRisk <= 0.30` (tighten both)
- `UNKNOWN`: baseline thresholds

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-01` | 19 | 9 | 8 | `-3.02%` | `-8.99%` | 33.33% | 37.50% |
| `2025-02` | 18 | 7 | 6 | `-4.78%` | `-3.74%` | 0.00% | 0.00% |

#### Read-through

- The split *did* activate (both months contain `risk_on`, `neutral`, `risk_off` days), but the threshold tweak did **not** yield stable improvement month-to-month.
- `2025-01`: the variant slightly reduced pass-days but made the median month-end return materially worse  → this tightening is **not aligned** with month-quality in this sample (not a reliable quality filter).
- `2025-02`: the variant improved the median slightly, but the month remained negative and the >=`2%` hit-rate stayed `0%`  → still **far from** the `2%` monthly compounding target.

#### Practical conclusion

- A simple market-regime split with tighter `entryMinEv` / `revRisk` is **not stable across months** under the rank-1 long proxy.
- If `marketRegime` is kept as the context axis, the next step should change only `entryMinEv` / `revRisk` again but with *larger separation* so the passing set meaningfully changes (otherwise it is mostly noise).

### 22. Blind Month Proxy `2025-05` — Monthly-Box Gate v1 (entryMinEv + revRisk only)

This is another narrow monthly blind test using a *proxy* (not a full TOREDEX portfolio backtest). The regime split is taken from the basis payload `monthlyBoxState` so the context is purely monthly-derived.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-05-01..2025-06-30`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyBoxState`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`
  - `next_open = ranking_appearance_daily.anchor_price_next_open`
  - `month_end_close = daily_bars.c` on the last trading day of the same month (via `to_timestamp(daily_bars.date)`)

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75` where `upProb = max(probSideCalib, probSide, weeklyBreakoutUpProb, monthlyBreakoutUpProb)`
- `entryMinEv >= 0.05` where `ev = changePct`
- `entryMaxRevRisk <= 0.40` where `revRisk = max(weeklyBreakoutDownProb, monthlyBreakoutDownProb)`

#### Regime-aware variant (monthly-box context)

Monthly box-state source: `signal_basis_daily.basis_payload_json.monthlyBoxState`.

- `box_upper`: `entryMinEv >= 0.05`, `entryMaxRevRisk <= 0.33`
- `box_mid`: `entryMinEv >= 0.055`, `entryMaxRevRisk <= 0.30`
- `box_lower`: `entryMinEv >= 0.06`, `entryMaxRevRisk <= 0.25`
- `no_box`: `entryMinEv >= 0.06`, `entryMaxRevRisk <= 0.28`
- `UNKNOWN`: baseline thresholds

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-05` | 20 | 6 | 5 | `-8.02%` | `-10.80%` | 0.00% | 0.00% |
| `2025-06` | 21 | 10 | 10 | `-17.23%` | `-17.23%` | 20.00% | 20.00% |

#### Read-through

- `2025-05`: the variant reduced pass-days and made the median worse ・・・this tightening is **too strict / misaligned** (it filtered, but did not improve month quality).
- `2025-06`: the variant did not change the passing set ・・・these thresholds are **not binding** in this stream (ineffective lever for this month).
- Context activation was weak: `monthlyBoxState` was dominated by `no_box` in this window, so the split is **not differentiating enough** for `rank=1`.

#### Practical conclusion

- A monthly-box context split with stricter `entryMinEv` / `revRisk` is **not stable across months** under this proxy window; May worsened and June was unchanged.
- If `monthlyBoxState` is kept as the context axis, the next step should be to pick a window where `box_upper/box_mid/box_lower` meaningfully appear in the candidate stream; otherwise the regime-aware thresholds will mostly behave like a single global gate.

### 23. Blind Month Proxy `2025-07` - Monthly-Box Gate v2 (box-only filter + entryMinEv + revRisk)

This is a narrow monthly blind test using a *proxy* (not a full TOREDEX portfolio backtest). The regime split is taken from the basis payload `monthlyBoxState` and the variant adds a **monthly-context filter** in addition to changing only `entryMinEv` / `revRisk` thresholds.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-07-01..2025-08-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyBoxState`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`
  - `next_open = ranking_appearance_daily.anchor_price_next_open`
  - `month_end_close = daily_bars.c` on the last trading day of the same month (via `to_timestamp(daily_bars.date)`)

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75` where `upProb = max(probSideCalib, probSide, weeklyBreakoutUpProb, monthlyBreakoutUpProb)`
- `entryMinEv >= 0.05` where `ev = changePct`
- `entryMaxRevRisk <= 0.40` where `revRisk = max(weeklyBreakoutDownProb, monthlyBreakoutDownProb)`

#### Regime-aware variant (monthly-box context + filter)

Monthly box-state source: `signal_basis_daily.basis_payload_json.monthlyBoxState`.

- monthly context filter: allow only `{box_upper, box_mid, box_lower}` (exclude `no_box` and `UNKNOWN`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `box_upper`: `entryMinEv >= 0.045`, `entryMaxRevRisk <= 0.36`
  - `box_mid`: `entryMinEv >= 0.05`, `entryMaxRevRisk <= 0.32`
  - `box_lower`: `entryMinEv >= 0.06`, `entryMaxRevRisk <= 0.24`

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-07` | 22 | 5 | 0 | `+1.03%` | n/a | 40.00% | n/a |
| `2025-08` | 20 | 16 | 0 | `+0.19%` | n/a | 37.50% | n/a |

#### Read-through

- The variant produced `0` pass-days in both months, because `monthlyBoxState` was overwhelmingly `no_box` for `rank=1` in this window (`2025-07`: `no_box=22/22`; `2025-08`: `no_box=19/20`).
- This monthly-context filter is **too strict** for the actual candidate stream coverage; it eliminates the strategy and cannot improve monthly compounding toward the `2%` target.
- Baseline still missed the `2%` median target in both months, but the >=`2%` hit-rate was about `38-40%` in this window, so the filter is throwing away potentially compounding candidates without offering a quality improvement.

#### Practical conclusion

- For `rank=1` longs, a **box-only** monthly-context filter on `monthlyBoxState` is **not viable** unless `box_upper/box_mid/box_lower` coverage is materially higher; in this blind window it removes all trades.
- If `monthlyBoxState` remains the context axis, treat `box presence` as an informational label (or include `no_box` with separate thresholds) rather than hard-filtering it out.

### 24. Blind Month Proxy `2024-09` - Monthly-Box Gate v3 (box_upper + boxMonths>=4 filter + entryMinEv/revRisk)

This is a narrow monthly blind test using a *proxy* (not a full TOREDEX portfolio backtest). The regime split is taken from the basis payload `monthlyBoxState` and the variant adds a **monthly-context filter** (requiring a sufficiently long box) in addition to changing only `entryMinEv` / `revRisk` thresholds.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2024-09-01..2024-10-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyBoxState` (+ `monthlyBoxMonths`)
- monthly return proxy: `(month_end_close / next_open - 1) * 100`
  - `next_open = ranking_appearance_daily.anchor_price_next_open`
  - `month_end_close = daily_bars.c` on the last trading day of the same month (via `to_timestamp(daily_bars.date)`)

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75` where `upProb = max(probSideCalib, probSide, weeklyBreakoutUpProb, monthlyBreakoutUpProb)`
- `entryMinEv >= 0.05` where `ev = changePct`
- `entryMaxRevRisk <= 0.40` where `revRisk = max(weeklyBreakoutDownProb, monthlyBreakoutDownProb)`

#### Regime-aware variant (monthly-box context + filter)

Monthly box-state source: `signal_basis_daily.basis_payload_json.monthlyBoxState`.

- monthly context filter: require `monthlyBoxState == 'box_upper'` and `monthlyBoxMonths >= 4`
- thresholds (only `entryMinEv` / `revRisk`):
  - `box_upper`: `entryMinEv >= 0.045`, `entryMaxRevRisk <= 0.36`

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2024-09` | 19 | 6 | 0 | `-4.07%` | n/a | 0.00% | n/a |
| `2024-10` | 22 | 5 | 0 | `+6.18%` | n/a | 60.00% | n/a |

#### Read-through

- The variant produced `0` pass-days in both months.
- Coverage was extremely thin even before `entryMinEv`/`revRisk` binding: `monthlyBoxState=box_upper` was only `3/19` days in `2024-09` and `2/22` days in `2024-10`, and `monthlyBoxMonths>=4` reduced this to `1` candidate day in each month.
- The remaining `2024-09` candidate day did pass the baseline gate but failed the tighter `entryMaxRevRisk<=0.36` condition, so the `revRisk` tightening is **too strict** for this box_upper subset in this window.

#### Practical conclusion

- A monthly-box filter that requires `box_upper` + `monthlyBoxMonths>=4` combined with tightening `entryMaxRevRisk` to `<=0.36` is **too strict** for `rank=1` coverage in `2024-09..2024-10`; it eliminates all trades and cannot improve monthly compounding toward the `2%` target.
- This result is consistent with the earlier finding that box-only monthly-context filters are often not viable for `rank=1` because the axis is sparse; additional box-strength constraints further reduce coverage to near-zero.

### 25. Blind Month Proxy `2025-11` - Monthly-RangeWidth Gate v1 (monthlyRangeWidth bucket + entryMinEv/revRisk only)

This is a narrow monthly blind test using a *proxy* (not a full TOREDEX portfolio backtest). The monthly context axis is derived from `monthlyRangeWidth` so the regime split is **always active** (unlike sparse box-state labels).

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-11-01..2025-12-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`
  - `next_open = ranking_appearance_daily.anchor_price_next_open`
  - `month_end_close = daily_bars.c` on the last trading day of the same month (via `to_timestamp(daily_bars.date)`)

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75` where `upProb = max(probSideCalib, probSide, weeklyBreakoutUpProb, monthlyBreakoutUpProb)`
- `entryMinEv >= 0.05` where `ev = changePct`
- `entryMaxRevRisk <= 0.40` where `revRisk = max(weeklyBreakoutDownProb, monthlyBreakoutDownProb)`

#### Regime-aware variant (monthlyRangeWidth buckets)

- `tight`: `entryMinEv >= 0.05`, `entryMaxRevRisk <= 0.38` (tighten revRisk slightly)
- `normal`: baseline thresholds
- `wide`: `entryMinEv >= 0.055`, `entryMaxRevRisk <= 0.35` (tighten both)
- `extreme_wide`: `entryMinEv >= 0.06`, `entryMaxRevRisk <= 0.30` (tighten both strongly)

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-11` | 18 | 13 | 12 | `+3.23%` | `+4.51%` | 61.54% | 58.33% |
| `2025-12` | 22 | 4 | 3 | `+0.28%` | `+0.28%` | 25.00% | 33.33% |

#### Read-through

- The monthly context axis *did* activate (both months contain multiple buckets; `2025-11` includes `wide`/`extreme_wide`, `2025-12` includes `tight`/`normal`).
- `2025-11`: the variant slightly reduced pass-days and improved the median, but reduced the >=`2%` hit-rate  竊・the tightening is **not cleanly improving compounding quality** (median up, hit-rate down).
- `2025-12`: the baseline passing set was already very small (`4` days), and the variant reduced it further  竊・this month is **too sample-thin** to trust small changes; the thresholds did not lift the median toward `2%`.

#### Practical conclusion

- Monthly-range-width bucketing is a viable monthly-context axis for `rank=1` (it activates consistently), but this v1 threshold map is **not stable across months** and does not reliably move the median toward the `2%` target.
- For the `wide`/`extreme_wide` buckets, the current tightening appears **too strict to improve hit>=2%** in `2025-11`, while also failing to rescue low-median months like `2025-12`.

### 26. Blind Month Proxy `2026-01` - Monthly-RangeWidth Gate v2 (soften wide/extreme_wide, entryMinEv/revRisk only)

This is a narrow monthly blind test using the same proxy setup as v1. v2 keeps the same `monthlyRangeWidth` buckets but relaxes the wide/extreme thresholds to avoid over-filtering.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2026-01-01..2026-02-28`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v2 (monthlyRangeWidth buckets; soften wide/extreme)

- `tight`: `entryMinEv >= 0.05`, `entryMaxRevRisk <= 0.38`
- `normal`: baseline thresholds
- `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`
- `extreme_wide`: `entryMinEv >= 0.055`, `entryMaxRevRisk <= 0.33`

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2026-01` | 19 | 13 | 12 | `-4.10%` | `-4.57%` | 38.46% | 33.33% |
| `2026-02` | 17 | 15 | 15 | `+3.54%` | `+3.54%` | 53.33% | 53.33% |

#### Read-through (bucket diagnostics)

- `2026-01`: the negative month is driven by the `normal` bucket (baseline pass-days `5`, median `-5.04%`, hit>=`2%` `0%`); v2 does not change this because `normal` uses baseline thresholds.
- `2026-01`: `extreme_wide` was the strongest bucket under baseline (pass-days `5`, median `+35.92%`, hit>=`2%` `80%`), but v2 filtered it (var pass-days `4`, median `+25.88%`, hit>=`2%` `60%`)  → the `extreme_wide` tightening is **too strict** in this window.
- `2026-02`: base and variant results are identical (thresholds did not bind)  → v2 is **too loose / not active** for this month.

#### Practical conclusion

- Monthly-range-width bucketing still *activates* for `rank=1`, but v2 does **not** improve monthly compounding toward the `2%` target on `2026-01..2026-02`.
- In this window the simple assumption “wide/extreme is higher risk so tighten there” was not stable: `extreme_wide` contained the highest-quality outcomes in `2026-01`, while `normal` contained the negative median drift.

### 27. Blind Month Proxy `2025-09` - Monthly-RangeWidth Gate v3 (exclude normal/UNKNOWN + bucket thresholds, entryMinEv/revRisk only)

This is another narrow monthly blind test using the same proxy setup as v1/v2. v3 adds a **monthly-context filter** (dropping `normal`/`UNKNOWN`) and changes only `entryMinEv` / `revRisk` thresholds for the allowed buckets.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-09-01..2025-10-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v3 (monthlyRangeWidth filter + thresholds; entryMinEv/revRisk only)

- monthly context filter: allow only `{tight, wide, extreme_wide}` (exclude `normal` and `UNKNOWN`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: `entryMinEv >= 0.055`, `entryMaxRevRisk <= 0.36`
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`
  - `extreme_wide`: `entryMinEv >= 0.045`, `entryMaxRevRisk <= 0.42`

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-09` | 20 | 9 | 6 | `-3.26%` | `-2.08%` | 22.22% | 33.33% |
| `2025-10` | 22 | 12 | 10 | `-1.50%` | `-2.24%` | 16.67% | 10.00% |

#### Read-through (bucket diagnostics)

- `2025-09`: `extreme_wide` dominated the best outcomes (baseline pass-days `3`, median `+22.53%`, hit>=`2%` `66.67%`), while `normal` and `wide` were negative (both had baseline medians `<0%` and hit>=`2%` `0%`). Excluding `normal` helped slightly, but the month still failed the `2%` median target because `wide` stayed negative.
- `2025-10`: the `normal` bucket flipped positive (baseline pass-days `3`, median `+1.80%`, hit>=`2%` `33.33%`), so the v3 filter removed one of the only non-negative pockets and worsened the overall median.
- `2025-10`: loosening `extreme_wide` (`revRisk<=0.42`) added pass-days (`3 -> 4`) but the added trade was negative; `extreme_wide` remained a low-quality bucket in this month (hit>=`2%` `0%`); this loosen step is **too loose** here.

#### Practical conclusion

- Hard-filtering out `normal` is **not stable across months**: it removed a negative-drift pocket in `2025-09` but removed a positive pocket in `2025-10`.
- A simple rule like "normal is always bad" or "extreme_wide is always good" is not supported: both buckets can flip sign month-to-month.
- In this window, changing only `entryMinEv` / `revRisk` + a monthly-range-width filter did **not** move the overall median toward the `2%` monthly target.

### 28. Blind Month Proxy `2025-05` - Monthly-RangeWidth Gate v5 (exclude extreme_wide + bucket thresholds, entryMinEv/revRisk only)

This is another narrow monthly blind test using the same proxy setup as v1/v2/v3. v5 adds a **monthly-context filter** that drops `extreme_wide` (and `UNKNOWN`) and changes only `entryMinEv` / `revRisk` for the remaining buckets.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-05-01..2025-06-30`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v5 (monthlyRangeWidth filter + thresholds; entryMinEv/revRisk only)

- monthly context filter: allow only `{tight, normal, wide}` (exclude `extreme_wide` and `UNKNOWN`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: baseline thresholds
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-05` | 20 | 6 | 4 | `-8.02%` | `-8.02%` | 0.00% | 0.00% |
| `2025-06` | 21 | 10 | 9 | `-17.23%` | `-19.14%` | 20.00% | 11.11% |

#### Read-through (bucket diagnostics)

- `2025-05`: the filter removed `extreme_wide` baseline passes (`2` days; median `-6.43%`, max `+0.50%`). Removing them did **not** improve the overall month (median stayed `-8%`)  竊・filtering was not strong enough to rescue a negative month driven by non-extreme buckets.
- `2025-06`: the filter removed the only `extreme_wide` baseline pass (`1` day; return `+27.53%`, hit>=`2%` `100%`). This directly reduced hit>=`2%` (`20% -> 11.11%`) and worsened the median (`-17.23% -> -19.14%`)  竊・the `extreme_wide` filter is **too strict** in this window.
- `2025-06`: the negative median drift is dominated by `wide` and `normal` passes (both strongly negative). The v5 `wide` tightening did not bind in this window (wide pass-days unchanged), suggesting it is **too loose / not active** where it matters.

#### Practical conclusion

- Hard-filtering out `extreme_wide` is **not stable across months**: it removed weak outcomes in `2025-05` but removed a strong winner in `2025-06`.
- In this blind window, changing only `entryMinEv` / `revRisk` + a monthly-range-width filter did **not** move the monthly median toward the `2%` target and reduced hit>=`2%` in the worse month.

### 29. Blind Month Proxy `2025-07` - Monthly-RangeWidth Gate v4 (tighten normal + drop `UNKNOWN`, entryMinEv/revRisk only)

This is another narrow monthly blind test using the same proxy setup as v1/v2/v3/v5. v4 drops `UNKNOWN` monthly-range-width context and tightens only the `normal` bucket (while changing only `entryMinEv` / `revRisk` thresholds).

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-07-01..2025-08-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v4 (drop `UNKNOWN` + tighten `normal`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: `entryMinEv >= 0.055`, `entryMaxRevRisk <= 0.36`
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`
  - `extreme_wide`: baseline thresholds

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-07` | 22 | 5 | 5 | `+1.03%` | `+1.03%` | 40.00% | 40.00% |
| `2025-08` | 20 | 16 | 16 | `+0.19%` | `+0.19%` | 37.50% | 37.50% |

#### Read-through

- In this window, v4 did **not bind** at all (`base pass == var pass` in both months). This means the v4 `normal` tightening (`ev>=0.055` and `revRisk<=0.36`) is **too loose / not active** under the baseline gate for Jul-Aug `2025` (it filtered nothing).
- `2025-08` is dominated by the `normal` bucket (`16/20` rank-1 days), but the tightening still did not reduce passes. If this axis is kept, the next tests should ensure the variant materially changes the passing set before interpreting stability.
- Both months still miss the `2%` median target (Jul `+1.03%`, Aug `+0.19%`) with large positive outliers; this is not evidence of improved monthly compounding.

#### Practical conclusion

- Monthly-range-width v4 (tighten `normal` + drop `UNKNOWN`) is **too loose / inactive** on `2025-07..2025-08` under the rank-1 long proxy; it provides no incremental evidence because it does not change the passing set.
- Before judging stability, rerun v4 on a different blind window where `var pass != base pass`, or increase the separation in `normal` thresholds so the gate actually changes which days pass.

### 30. Blind Month Proxy `2025-09` - Monthly-RangeWidth Gate v4 (retest; tighten `normal` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind retest using the same v4 proxy setup, on a different blind window, to check whether v4 binding (or inactivity) is stable across months.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-09-01..2025-10-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v4 (drop `UNKNOWN` + tighten `normal`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: `entryMinEv >= 0.055`, `entryMaxRevRisk <= 0.36`
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`
  - `extreme_wide`: baseline thresholds

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-09` | 20 | 9 | 8 | `-3.26%` | `-4.63%` | 22.22% | 25.00% |
| `2025-10` | 22 | 12 | 12 | `-1.50%` | `-1.50%` | 16.67% | 16.67% |

#### Read-through

- `2025-09`: v4 bound slightly (`9 -> 8` pass-days), but the median worsened (`-3.26% -> -4.63%`) while hit>=`2%` improved marginally (22.22% -> 25.00%). This is not evidence of improved monthly compounding; the tightening is plausibly **too strict** in this month (it removed a day that helped the median).
- `2025-10`: v4 did **not bind** (`base pass == var pass`), so the v4 tightening is **too loose / inactive** in this month.
- Across this 2-month blind window, v4 did not move the monthly median toward the `2%` target and did not show stable behavior month-to-month.

#### Practical conclusion

- Monthly-range-width v4 (tighten `normal` + drop `UNKNOWN`) does not provide stable evidence of improved monthly compounding: it was inactive in `2025-10`, and when it did bind in `2025-09` it worsened the median.
- If the next run keeps v4’s structure, it must use a blind window where v4 materially binds in both months (or increase the separation in the `normal` thresholds) before trying to judge stability.

### 31. Blind Month Proxy `2025-11` - Monthly-RangeWidth Gate v4 (retest; tighten `normal` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind retest of the same v4 proxy setup, on a different blind window, to check whether v4 binding (and any lift toward the `2%` target) is stable across months.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-11-01..2025-12-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v4 (drop `UNKNOWN` + tighten `normal`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: `entryMinEv >= 0.055`, `entryMaxRevRisk <= 0.36`
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`
  - `extreme_wide`: baseline thresholds

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-11` | 18 | 13 | 11 | `+3.23%` | `+6.70%` | 61.54% | 63.64% |
| `2025-12` | 22 | 4 | 3 | `+0.28%` | `+0.28%` | 25.00% | 33.33% |

#### Read-through

- `2025-11`: v4 bound meaningfully (`13 -> 11`) and the median improved (`+3.23% -> +6.70%`) with a slight improvement in hit>=`2%` (61.54% -> 63.64%). This is evidence that tightening `normal` can help in some months under this proxy.
- `2025-12`: coverage is very sparse (`4 -> 3` pass-days) and the median is essentially unchanged (~`+0.28%`), far below the `2%` target. With this low pass-day count, month-to-month stability cannot be judged confidently; it is not evidence of improved monthly compounding.

#### Practical conclusion

- On this blind window (`2025-11..2025-12`), v4 shows mixed evidence: it improved the `2025-11` median but did not move `2025-12` toward the `2%` monthly target (and the month is too sparse to read strongly).
- Combined with earlier v4 windows (inactive in `2025-07..2025-08` and inconsistent in `2025-09..2025-10`), v4 is not stable across months as a reliable path to `2%` monthly compounding under this proxy.

### 32. Blind Month Proxy `2025-07` - Monthly-RangeWidth Gate v6 (strict `normal` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is another narrow monthly blind test using the same proxy setup as v4, but with a **larger separation** on the `normal` bucket so the gate is more likely to bind (addressing the "too loose / inactive" issue observed in `2025-07..2025-08`).

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-07-01..2025-08-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v6 (drop `UNKNOWN` + stricter `normal`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: `entryMinEv >= 0.065`, `entryMaxRevRisk <= 0.33` (stricter than v4)
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`
  - `extreme_wide`: baseline thresholds

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-07` | 22 | 5 | 5 | `+1.03%` | `+1.03%` | 40.00% | 40.00% |
| `2025-08` | 20 | 16 | 14 | `+0.19%` | `-3.16%` | 37.50% | 28.57% |

#### Read-through

- `2025-07`: v6 did **not bind** (`5 == 5`), so it provides no incremental evidence in this month (the baseline passing set did not include normal days that would be filtered by the stricter `normal` thresholds).
- `2025-08`: v6 bound (`16 -> 14`) but the median worsened (`+0.19% -> -3.16%`) and hit>=`2%` fell (37.50% -> 28.57%). In a normal-dominated month, this `normal` tightening is **too strict** (coverage down, quality down).
- Net: increasing separation to force binding does not improve monthly compounding here; behavior is **not stable across months** (inactive in Jul, harmful in Aug).

#### Practical conclusion

- Monthly-range-width v6 (strict `normal` + drop `UNKNOWN`) is **not a stable improvement** toward the `2%` monthly compounding target under this proxy: it is inactive in `2025-07` and harmful in `2025-08`.
- The v6 `normal` thresholds (`ev>=0.065` and `revRisk<=0.33`) appear **too strict** for normal-heavy months; the next variant should reduce this strictness if the goal is to preserve (or improve) median + hit>=`2%` without collapsing coverage.

### 33. Blind Month Proxy `2025-07` - Monthly-RangeWidth Gate v7 (soft `normal` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind retest on the same window as v6, but with a **less strict** `normal` bucket (to directly test whether v6’s “too strict” issue can be reduced *without* changing anything else).

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-07-01..2025-08-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v7 (drop `UNKNOWN` + soft `normal`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: `entryMinEv >= 0.06`, `entryMaxRevRisk <= 0.35` (less strict than v6; stricter than baseline)
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`
  - `extreme_wide`: baseline thresholds

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-07` | 22 | 5 | 5 | `+1.03%` | `+1.03%` | 40.00% | 40.00% |
| `2025-08` | 20 | 16 | 15 | `+0.19%` | `+0.00%` | 37.50% | 33.33% |

#### Read-through

- `2025-07`: v7 did **not bind** (`5 == 5`), so this month does not provide incremental evidence (the baseline passing set did not include normal days that v7 would filter).
- `2025-08`: v7 bound slightly (`16 -> 15`) but the median worsened (`+0.19% -> +0.00%`) and hit>=`2%` fell (37.50% -> 33.33%). When it binds, this `normal` tightening still looks **too strict** under the proxy.
- Compared to v6 on the same window, v7 is **less harmful** (v6 pushed the Aug median to `-3.16%`), which supports the ledger’s earlier read that v6’s `normal` thresholds were too strict. However, v7 still does not move medians toward the `2%` target.

#### Practical conclusion

- Monthly-range-width v7 (soft `normal` + drop `UNKNOWN`) is **not a stable improvement** toward the `2%` monthly compounding target under this proxy: it is inactive in `2025-07` and slightly harmful in `2025-08`.
- Net: relaxing v6 reduces damage but does not create evidence of improved monthly compounding; the `normal` bucket tightening remains fragile (inactive in some months, and when it binds it can remove median-supporting days).

### 34. Blind Month Proxy `2025-08` - Monthly-RangeWidth Gate v7 (retest; soft `normal` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind retest of v7 on a **different month pair** where the gate **binds in both months**, so it provides cleaner evidence about whether the v7 `normal` tightening improves monthly compounding.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-08-01..2025-09-30`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v7 (drop `UNKNOWN` + soft `normal`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: `entryMinEv >= 0.06`, `entryMaxRevRisk <= 0.35`
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`
  - `extreme_wide`: baseline thresholds

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-08` | 20 | 16 | 15 | `+0.19%` | `+0.00%` | 37.50% | 33.33% |
| `2025-09` | 20 | 9 | 8 | `-3.26%` | `-4.63%` | 22.22% | 25.00% |

#### Read-through

- `2025-08`: v7 bound slightly (`16 -> 15`) and the median worsened (`+0.19% -> +0.00%`) with a drop in hit>=`2%` (37.50% -> 33.33%). This is consistent with the earlier Jul-Aug v7 window: when v7 binds, it looks **too strict** in the `normal` bucket under this proxy.
- `2025-09`: v7 bound slightly (`9 -> 8`) and the median worsened (`-3.26% -> -4.63%`). hit>=`2%` rose slightly (22.22% -> 25.00%), but the passing set is **sparse** (<`10` pass-days), and the median is further away from the `+2%` target.

#### Practical conclusion

- On this blind window (`2025-08..2025-09`), v7 does **not** show evidence of improving monthly compounding toward the `2%` target: both months show worse medians when the gate binds.
- Net: v7 `normal` tightening remains **fragile** and appears **mildly too strict** under this proxy; even when it binds in both months, it does not lift the month-level median toward `+2%`.

## Rolling Summary Updates

- `2026-04-14`: Monthly-range-width v3 retest on Feb-Mar 2025 is sparse (<10 pass-days) and not stable: Feb median worsened (`-4.78% -> -12.11%`), Mar median unchanged (`+2.65%`); supports “v3 too strict / unstable” under this proxy.
- `2026-04-14`: Monthly-range-width v7 retest on Aug-Sep 2025 bound in both months (`16 -> 15`, `9 -> 8`) but medians worsened (`+0.19% -> +0.00%`, `-3.26% -> -4.63%`); Sep is sparse (<10 pass-days), but there is still no lift toward the `2%` target.
- `2026-04-14`: Monthly-range-width v7 (soft `normal` + drop `UNKNOWN`) retest on Jul-Aug 2025 bound only in Aug (`16 -> 15`) and slightly worsened median (`+0.19% -> +0.00%`) and hit>=2%; softer than v6 but still not stable / below the `2%` target.
- `2026-04-14`: Monthly-range-width v6 (strict `normal` + drop `UNKNOWN`) retest on Jul-Aug 2025 bound only in Aug (`16 -> 14`) and worsened the median (`+0.19% -> -3.16%`) and hit>=2%; too strict / not stable across months.
- `2026-04-14`: Monthly-range-width v4 retest on Nov-Dec 2025 bound in both months (`13 -> 11`, `4 -> 3`) and improved the Nov 2025 median (`+3.23% -> +6.70%`), but Dec 2025 stayed ~`+0.28%` with sparse coverage; mixed evidence, not stable toward the `2%` target.
- `2026-04-14`: Monthly-range-width v5 (exclude `extreme_wide`/`UNKNOWN`) removed weak `extreme_wide` passes in May but removed a `+27.53%` `extreme_wide` winner in June; hit>=2% fell and medians worsened; filter is not stable.
- `2026-04-14`: Monthly-range-width v4 (tighten `normal` + drop `UNKNOWN`) did not bind on Jul-Aug 2025 (base pass == var pass), so it is too loose/inactive; no lift toward the `2%` median target.
- `2026-04-14`: Monthly-context loosening (entryMinEv/revRisk only) increased March coverage but reduced median and 2% hit-rate; April stayed negative and monthly-state split rarely activated for `rank=1`.
- `2026-04-14`: Market-regime split v2 (entryMinEv/revRisk only) activated, but did not improve monthly medians consistently; January worsened, February stayed negative with 0% hit>=2%.
- `2026-04-14`: Monthly-box split v1 (`monthlyBoxState`) mostly stayed `no_box` for `rank=1`; May median worsened after tightening, June was unchanged (thresholds not binding).
- `2026-04-14`: Monthly-box split v2 (box-only filter) yielded 0 pass-days in Jul-Aug because `monthlyBoxState` stayed `no_box`; filter is too strict / axis inactive for `rank=1`.
- `2026-04-14`: Monthly-box split v3 (require `box_upper` + `monthlyBoxMonths>=4`) yielded 0 pass-days in Sep-Oct 2024; `revRisk<=0.36` was too strict and the context axis was too sparse for `rank=1`.
- `2026-04-14`: Monthly-range-width v3 (exclude `normal`/`UNKNOWN`) improved Sep slightly but worsened Oct; `normal` flipped sign month-to-month and `extreme_wide` loosening was too loose in Oct; filter is not stable.
- `2026-04-14`: Monthly-range-width bucketing (`monthlyRangeWidth`) activated reliably, but v1 tightening improved Nov median while reducing hit>=2%, and Dec stayed far below the `2%` median target; not stable across months.
- `2026-04-14`: Monthly-range-width v2 (soften wide/extreme) did not improve `2026-01..2026-02`; January’s negative median came from `normal`, while `extreme_wide` winners got filtered; February thresholds did not bind.
- `2026-04-14`: Monthly-range-width v4 retest on Sep-Oct 2025 bound only in Sep (`9 -> 8`) and worsened the median; Oct was inactive (`12 == 12`) — no stable lift toward the `2%` target.
- `2026-04-14`: Monthly-range-width v8 (tighten `wide`/`extreme_wide` + drop `UNKNOWN`) blind test on Oct-Nov 2025 bound slightly (`12 -> 11`, `13 -> 12`); Oct median worsened (`-1.50% -> -1.99%`), while Nov median improved (`+3.23% -> +4.51%`) but hit>=`2%` fell (61.54% -> 58.33%); not stable toward the `2%` target.
- `2026-04-14`: Monthly-range-width v5 (exclude `extreme_wide`/`UNKNOWN`) retest on Oct-Nov 2025 bound (`12 -> 9`, `13 -> 10`); Oct median worsened (`-1.50% -> -2.50%`) while Nov improved (`+3.23% -> +4.51%`); still not stable toward the `2%` target.
- `2026-04-14`: Monthly-range-width v8 retest on `2026-01..2026-02` bound only in Jan (`13 -> 12`) and worsened the median (`-4.10% -> -4.57%`) and hit>=`2%` (38.46% -> 33.33%); Feb was inactive (`15 == 15`, median `+3.54%` unchanged). Net: too loose/inactive when months are dominated by `normal`, and not stable toward the `2%` target.
- `2026-04-14`: Monthly-range-width v6 (strict `normal` + drop `UNKNOWN`) retest on `2026-02..2026-03` did **not bind** in either month (`15 == 15`, `14 == 14`) with identical medians (`+3.54%`, `-0.21%`). Net: too loose/inactive on this window; no evidence of improved monthly compounding toward the `2%` target.
- `2026-04-14`: Monthly-range-width v4 (tighten `normal` + drop `UNKNOWN`) retest on `2026-01..2026-02` did **not bind** in either month (`13 == 13`, `15 == 15`) with identical medians (`-4.10%`, `+3.54%`). Net: too loose/inactive; no evidence of improved monthly compounding toward the `2%` target.
- `2026-04-14`: Monthly-range-width v3 retest on `2025-12..2026-01` excluded `normal`/`UNKNOWN` and bound in Jan (`13 -> 8`), lifting Jan median (`-4.10% -> +12.94%`) and hit>=`2%` (38.46% -> 62.50%); Dec remained sparse (`4 -> 3`) with ~unchanged median (`+0.28%`). Net: promising when `normal` is toxic, but not yet stable (Dec coverage thin).
- `2026-04-15`: Monthly-range-width v7 (soft `normal` + drop `UNKNOWN`) blind test on `2025-04..2025-05` did **not bind** (`13 == 13`, `6 == 6`) with identical medians (`-8.68%`, `-8.02%`). Net: too loose/inactive on this window; no evidence of improving monthly compounding toward the `2%` target.
- `2026-04-15`: Monthly-range-width v8 retest on `2025-09..2025-10` bound (`9 -> 7`, `12 -> 11`) but medians worsened (`-3.26% -> -6.00%`, `-1.50% -> -1.99%`); hit>=`2%` fell in Sep. Net: not stable toward the `2%` target.
- `2026-04-15`: Monthly-range-width v4 (tighten `normal` + drop `UNKNOWN`) blind test on `2025-06..2025-07` did **not bind** (`10 == 10`, `5 == 5`) with identical medians (`-17.23%`, `+1.03%`). Net: too loose/inactive on this window; no evidence of improving monthly compounding toward the `2%` target.
- `2026-04-15`: Monthly-range-width v6 (strict `normal` + drop `UNKNOWN`) blind test on `2025-02..2025-03` did **not bind** (`7 == 7`, `7 == 7`) with identical medians (`-4.78%`, `+2.65%`). Net: too loose/inactive on this window; no evidence of improving monthly compounding toward the `2%` target.
- `2026-04-15`: Monthly-range-width v8 (tighten `wide`/`extreme_wide` + drop `UNKNOWN`) blind test on `2026-02..2026-03` did **not bind** (`15 == 15`, `14 == 14`) with identical medians (`+3.54%`, `-0.21%`). Net: too loose/inactive on this window; no stable lift toward the `2%` target.
- `2026-04-15`: Monthly-range-width v9 (tighten `tight` + strict `normal` + v8-style `wide/extreme_wide` + drop `UNKNOWN`) blind test on `2026-01..2026-02` bound only in Jan (`13 -> 12`) and worsened Jan median (`-4.10% -> -4.57%`) and hit>=`2%` (38.46% -> 33.33%); Feb was inactive (`15 == 15`, median `+3.54%` unchanged). Net: v9 looks **too strict / not helpful** under this proxy and does not improve month-to-month stability toward `+2%`.
- `2026-04-15`: Monthly-range-width v7 (soft `normal` + drop `UNKNOWN`) blind test on `2026-01..2026-02` did **not bind** (`13 == 13`, `15 == 15`) with identical medians (`-4.10%`, `+3.54%`). Net: v7 is too loose/inactive on this window; no evidence of improving monthly compounding toward the `2%` target.
- `2026-04-15`: Monthly-range-width v3 (exclude `normal`/`UNKNOWN`) blind test on `2025-09..2025-10` bound (`9 -> 6`, `12 -> 10`); Sep median improved (`-3.26% -> -2.08%`) but Oct worsened (`-1.50% -> -2.24%`) and hit>=`2%` fell; not stable toward the `2%` target.
- `2026-04-15`: Monthly-range-width v5 (exclude `extreme_wide`/`UNKNOWN`) blind test on `2026-02..2026-03` bound only in Feb (`15 -> 14`) and slightly improved Feb median (`+3.54% -> +4.17%`) and hit>=`2%` (53.33% -> 57.14%); Mar was inactive (`14 == 14`, median `-0.21%` unchanged). Net: mild benefit when it binds, but too loose/inactive to improve negative months consistently.
- `2026-04-15`: Monthly-range-width v4 (tighten `normal` + drop `UNKNOWN`) blind test on `2026-02..2026-03` did **not bind** (`15 == 15`, `14 == 14`) with identical medians (`+3.54%`, `-0.21%`). Net: too loose/inactive on this window; no evidence of improving monthly compounding toward the `2%` target.
- `2026-04-15`: Monthly-range-width v6 (strict `normal` + drop `UNKNOWN`) blind test on `2025-11..2025-12` bound (`13 -> 10`, `4 -> 3`); Nov median improved (`+3.23% -> +9.43%`) and hit>=2% rose (61.54% -> 70.00%), but Dec is sparse (base pass=4) with unchanged median (`+0.28%`) and still below the `2%` target. Net: helps Nov but is too strict/sparse for Dec; not stable evidence of improving monthly compounding toward `+2%`.
- `2026-04-15`: Monthly-range-width v8 (tighten `wide`/`extreme_wide` + drop `UNKNOWN`) blind test on `2025-07..2025-08` was mostly inactive (Jul `5 == 5`) and in Aug bound slightly (`16 -> 15`) but worsened the median (`+0.19% -> +0.00%`) and hit>=`2%` (37.50% -> 33.33%). Net: v8 is too loose/inactive and does not lift sub-`2%` months toward the `+2%` target under this proxy.
- `2026-04-15`: Monthly-range-width v5 (exclude `extreme_wide`/`UNKNOWN`) blind test on `2025-04..2025-05` bound strongly (Apr `13 -> 7`, May `6 -> 4`) but medians were unchanged (`-8.68%`, `-8.02%`) and Apr hit>=`2%` worsened (23.08% -> 14.29%). Net: v5 can remove winners without lifting weak months, so it is not a stable +`2%` compounding improvement under this proxy.
- `2026-04-16`: Monthly-range-width v9 retest on `2026-02..2026-03` was inactive in Feb (`15 == 15`, median `+3.54%` unchanged), but bound hard in Mar (`14 -> 7`) and severely worsened Mar median (`-0.21% -> -16.75%`) and hit>=`2%` (42.86% -> 28.57%). Net: v9 remains too strict / misaligned under this proxy and does not improve month-to-month stability toward `+2%`.

### 35. Blind Month Proxy `2025-10` - Monthly-RangeWidth Gate v8 (tighten `wide`/`extreme_wide` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind test that keeps the baseline composite gate fixed and tests whether **tightening only the wider monthly contexts** (while leaving `tight`/`normal` unchanged) improves month-level compounding toward the `2%` target.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-10-01..2025-11-30`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v8 (drop `UNKNOWN` + tighten `wide`/`extreme_wide`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: baseline thresholds
  - `wide`: `entryMinEv >= 0.06`, `entryMaxRevRisk <= 0.34`
  - `extreme_wide`: `entryMinEv >= 0.065`, `entryMaxRevRisk <= 0.33`

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-10` | 22 | 12 | 11 | `-1.50%` | `-1.99%` | 16.67% | 18.18% |
| `2025-11` | 18 | 13 | 12 | `+3.23%` | `+4.51%` | 61.54% | 58.33% |

#### Read-through

- `2025-10`: v8 bound slightly (`12 -> 11`) but the median worsened (`-1.50% -> -1.99%`). hit>=`2%` ticked up slightly (16.67% -> 18.18%), but the median moved **away** from the `+2%` target.
- `2025-11`: v8 bound slightly (`13 -> 12`) and the median improved (`+3.23% -> +4.51%`), but hit>=`2%` fell (61.54% -> 58.33%). This looks like a **quality shift without a consistent win-rate lift**.
- Compared to v4/v6/v7 (which tightened `normal` and often looked too strict or inactive), v8 avoids touching `normal` entirely; however, tightening only `wide`/`extreme_wide` still does not produce a stable month-to-month improvement under this proxy window.

#### Practical conclusion

- On this blind window (`2025-10..2025-11`), v8 is **not stable across months**: it hurts the month-level median in Oct while improving it in Nov.
- Net: there is **no consistent evidence** that tightening `wide`/`extreme_wide` (via only `entryMinEv` / `revRisk` + dropping `UNKNOWN`) improves monthly compounding toward the `2%` target.

### 36. Blind Month Proxy `2025-02` - Monthly-RangeWidth Gate v3 (retest; exclude normal/UNKNOWN + bucket thresholds, entryMinEv/revRisk only)

This is a narrow monthly blind retest of v3 on a **different month pair** (Feb-Mar 2025) to see whether excluding `normal`/`UNKNOWN` and adjusting only `entryMinEv`/`revRisk` improves monthly compounding toward the `2%` target.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-02-01..2025-03-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v3 (exclude `normal`/`UNKNOWN` + bucket thresholds; entryMinEv/revRisk only)

- monthly context filter: allow only `tight`, `wide`, `extreme_wide` (exclude `normal` and `UNKNOWN`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: `entryMinEv >= 0.055`, `entryMaxRevRisk <= 0.36`
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`
  - `extreme_wide`: `entryMinEv >= 0.045`, `entryMaxRevRisk <= 0.42`

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-02` | 18 | 7 | 3 | `-4.78%` | `-12.11%` | 0.00% | 0.00% |
| `2025-03` | 20 | 7 | 5 | `+2.65%` | `+2.65%` | 57.14% | 60.00% |

#### Read-through

- `2025-02`: v3 bound hard (`7 -> 3`) and the month-level median worsened materially (`-4.78% -> -12.11%`). This looks **too strict** (and/or misaligned) for this month under the proxy.
- `2025-03`: v3 also bound (`7 -> 5`) but the month-level median was **unchanged** (`+2.65%` both) and already above the `+2%` target; hit>=`2%` ticked up slightly (57.14% -> 60.00%) but this is a small-n effect.
- Evidence strength is limited: both months have **sparse** baseline pass-days (<`10`), so treat this as directional rather than definitive.

#### Practical conclusion

- On this blind window (`2025-02..2025-03`), v3 is **not a stable improvement** toward the `2%` monthly compounding target: it is clearly worse in Feb and neutral in Mar (where baseline already meets the target).
- Net: excluding `normal`/`UNKNOWN` plus the v3 bucket thresholds appears **too strict** in some months; do not treat v3 as a reliable month-to-month improvement without higher-coverage retests.

### 37. Blind Month Proxy `2025-10` - Monthly-RangeWidth Gate v5 (retest; exclude `extreme_wide`/`UNKNOWN` + bucket thresholds, entryMinEv/revRisk only)

This is a narrow monthly blind retest of v5 on a **different month pair** (Oct-Nov 2025) to evaluate whether excluding `extreme_wide` and changing only `entryMinEv` / `revRisk` improves monthly compounding toward the `2%` target.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-10-01..2025-11-30`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v5 (exclude `extreme_wide`/`UNKNOWN` + wide tightening; entryMinEv/revRisk only)

- monthly context filter: allow only `{tight, normal, wide}` (exclude `extreme_wide` and `UNKNOWN`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: baseline thresholds
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-10` | 22 | 12 | 9 | `-1.50%` | `-2.50%` | 16.67% | 22.22% |
| `2025-11` | 18 | 13 | 10 | `+3.23%` | `+4.51%` | 61.54% | 60.00% |

#### Read-through

- `2025-10`: v5 bound (`12 -> 9`) but the median moved further **away** from the `+2%` target (`-1.50% -> -2.50%`). hit>=`2%` improved slightly (16.67% -> 22.22%), but this did not translate into improved month-level compounding.
- `2025-11`: v5 bound (`13 -> 10`) and the median improved (`+3.23% -> +4.51%`), but hit>=`2%` was roughly unchanged/slightly down (61.54% -> 60.00%).

#### Practical conclusion

- On this blind window (`2025-10..2025-11`), v5 is **not stable across months**: it worsens the month-level median in Oct while improving it in Nov.
- Net: excluding `extreme_wide`/`UNKNOWN` plus a small `wide` tightening does **not** show consistent evidence of improving monthly compounding toward the `2%` target under this proxy.

### 38. Blind Month Proxy `2026-01` - Monthly-RangeWidth Gate v8 (retest; tighten `wide`/`extreme_wide` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind retest of v8 on a **different month pair** (Jan-Feb 2026) to check whether tightening only `wide`/`extreme_wide` (and dropping `UNKNOWN`) improves monthly compounding toward the `2%` target under a later window.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2026-01-01..2026-02-28`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v8 (drop `UNKNOWN` + tighten `wide`/`extreme_wide`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: baseline thresholds
  - `wide`: `entryMinEv >= 0.06`, `entryMaxRevRisk <= 0.34`
  - `extreme_wide`: `entryMinEv >= 0.065`, `entryMaxRevRisk <= 0.33`

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2026-01` | 19 | 13 | 12 | `-4.10%` | `-4.57%` | 38.46% | 33.33% |
| `2026-02` | 17 | 15 | 15 | `+3.54%` | `+3.54%` | 53.33% | 53.33% |

#### Read-through

- `2026-01`: v8 bound slightly (`13 -> 12`) but the month-level median worsened (`-4.10% -> -4.57%`) and hit>=`2%` fell (38.46% -> 33.33%). Tightening only `wide`/`extreme_wide` does not appear to rescue a negative-median month here.
- `2026-02`: v8 was inactive (`15 == 15`) and the month-level distribution was unchanged (median `+3.54%`). This suggests v8 often does not bind when the month is dominated by `normal` days (Feb had `normal=11` out of `17` rank-1 days).
- Compared to the existing ledger’s v2 read on this same window (`2026-01..2026-02`), v8 shows the same core limitation: when the month’s negative median is driven by `normal`, adjustments limited to `wide`/`extreme_wide` are unlikely to help, and in normal-heavy months the thresholds often do not bind.

#### Practical conclusion

- On this blind window (`2026-01..2026-02`), v8 is **not a stable improvement** toward the `2%` monthly compounding target: it is worse in Jan and inactive in Feb.
- Net: tightening only `wide`/`extreme_wide` (and dropping `UNKNOWN`) is **too loose/inactive** in many months and does not reliably move the month-level median toward `+2%` under this proxy.

### 39. Blind Month Proxy `2026-02` - Monthly-RangeWidth Gate v6 (retest; strict `normal` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind retest of v6 on a **new month pair** (Feb-Mar 2026) to check whether making the `normal` bucket materially stricter (while dropping `UNKNOWN`) improves monthly compounding toward the `2%` target.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2026-02-01..2026-03-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v6 (drop `UNKNOWN` + strict `normal`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: `entryMinEv >= 0.065`, `entryMaxRevRisk <= 0.33`
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`
  - `extreme_wide`: baseline thresholds

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2026-02` | 17 | 15 | 15 | `+3.54%` | `+3.54%` | 53.33% | 53.33% |
| `2026-03` | 20 | 14 | 14 | `-0.21%` | `-0.21%` | 42.86% | 42.86% |

#### Read-through

- In this window, v6 did **not bind** at all (`base pass == var pass` in both months). This means the v6 `normal` tightening (and the `UNKNOWN` drop) is **too loose / not active** on `2026-02..2026-03` under the rank-1 long proxy.
- This retest therefore provides **no incremental evidence** toward improved monthly compounding: the passing set, medians, and hit>=`2%` rates are identical.

#### Practical conclusion

- On this blind window (`2026-02..2026-03`), v6 is **not a demonstrated improvement** toward `2%` monthly compounding under this proxy because it is inactive (`base == var`).
- Net: treat this v6 structure as **non-informative unless it binds**; the next v6 retest should focus on a blind window where the `normal` bucket has meaningful baseline pass-days and where the stricter `normal` thresholds actually change the passing set in both months.

### 40. Blind Month Proxy `2026-01` - Monthly-RangeWidth Gate v4 (retest; tighten `normal` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind retest of v4 on a **new month pair** (Jan-Feb 2026) to confirm whether v4’s mild `normal` tightening (plus the `UNKNOWN` drop) actually changes the passing set and improves monthly compounding toward the `2%` target.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2026-01-01..2026-02-28`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v4 (drop `UNKNOWN` + tighten `normal`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: `entryMinEv >= 0.055`, `entryMaxRevRisk <= 0.36`
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`
  - `extreme_wide`: baseline thresholds

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2026-01` | 19 | 13 | 13 | `-4.10%` | `-4.10%` | 38.46% | 38.46% |
| `2026-02` | 17 | 15 | 15 | `+3.54%` | `+3.54%` | 53.33% | 53.33% |

#### Read-through

- In this window, v4 did **not bind** at all (`base pass == var pass` in both months). This means the v4 `normal` tightening (and the `UNKNOWN` drop) is **too loose / not active** on `2026-01..2026-02` under the rank-1 long proxy.
- This aligns with the earlier `2026-01..2026-02` v8 retest: in normal-heavy months, small threshold tweaks often fail to move the passing set, so they cannot reliably improve month-level medians toward `+2%`.

#### Practical conclusion

- On this blind window (`2026-01..2026-02`), v4 is **not a demonstrated improvement** toward `2%` monthly compounding under this proxy because it is inactive (`base == var`).
- Net: treat v4 as **non-informative unless it binds**; the next monthly-range-width run should either (a) choose a blind window where the mild thresholds change the passing set in both months, or (b) increase separation in the `normal` thresholds while keeping other gates fixed.

### 41. Blind Month Proxy `2025-12` - Monthly-RangeWidth Gate v3 (retest; exclude `normal`/`UNKNOWN` + bucket thresholds, entryMinEv/revRisk only)

This is a narrow monthly blind retest of v3 on a **new month pair** (Dec 2025 - Jan 2026) to evaluate whether excluding `normal`/`UNKNOWN` and changing only `entryMinEv` / `revRisk` can improve monthly compounding toward the `2%` target.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-12-01..2026-01-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v3 (exclude `normal`/`UNKNOWN`; entryMinEv/revRisk only)

- monthly context filter: allow only `{tight, wide, extreme_wide}` (exclude `normal` and `UNKNOWN`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: `entryMinEv >= 0.055`, `entryMaxRevRisk <= 0.36`
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`
  - `extreme_wide`: `entryMinEv >= 0.045`, `entryMaxRevRisk <= 0.42`

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-12` | 22 | 4 | 3 | `+0.28%` | `+0.28%` | 25.00% | 33.33% |
| `2026-01` | 19 | 13 | 8 | `-4.10%` | `+12.94%` | 38.46% | 62.50% |

#### Read-through

- `2025-12`: v3 bound slightly (`4 -> 3`) and the median was essentially unchanged (`+0.28%`). hit>=`2%` rose (25.00% -> 33.33%), but this month is **sparse** (<`5` pass-days), so treat it as weak evidence.
- `2026-01`: v3 bound materially (`13 -> 8`) and the month-level median moved from negative to strongly positive (`-4.10% -> +12.94%`) with a clear lift in hit>=`2%` (38.46% -> 62.50%). This is consistent with the ledger’s earlier read that, in some windows, the `normal` bucket can be a *drag* on month-level compounding.

#### Practical conclusion

- On this blind window (`2025-12..2026-01`), v3 shows **strong month-level improvement** in Jan 2026 (median and hit>=`2%` both improve past the `2%` target), but the paired month (Dec 2025) is too sparse to treat this as stable evidence.
- Net: v3 is **not proven stable** yet, but this window is a meaningful counterexample to the earlier “v3 is just too strict” framing: excluding `normal` can sometimes improve compounding materially when `normal` is the toxic state for the rank-1 stream. The next follow-up should be another blind window with higher baseline pass-days in both months to validate stability.

### 42. Blind Month Proxy `2025-04` - Monthly-RangeWidth Gate v7 (retest; soft `normal` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind test to evaluate whether v7 (soft `normal` tightening + drop `UNKNOWN`) can improve monthly compounding toward the `2%` target by changing **only** `entryMinEv` and `revRisk` thresholds plus a monthly-context filter.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-04-01..2025-05-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v7 (soft `normal` + drop `UNKNOWN`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: `entryMinEv >= 0.06`, `entryMaxRevRisk <= 0.35`
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`
  - `extreme_wide`: baseline thresholds

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-04` | 21 | 13 | 13 | `-8.68%` | `-8.68%` | 23.08% | 23.08% |
| `2025-05` | 20 | 6 | 6 | `-8.02%` | `-8.02%` | 0.00% | 0.00% |

#### Read-through

- In this window, v7 did **not bind** at all (`base pass == var pass` in both months). This means the v7 `normal` tightening and `UNKNOWN` drop are **too loose / not active** on `2025-04..2025-05` under the rank-1 long proxy.
- Both months have strongly negative medians and are far from the `+2%` target even under the baseline gate. Because v7 is inactive here, it cannot improve monthly compounding in this window.

#### Practical conclusion

- On this blind window (`2025-04..2025-05`), v7 is **not a demonstrated improvement** toward `2%` monthly compounding under this proxy because it is inactive (`base == var`).
- Net: treat v7 as **non-informative unless it binds**; follow-ups should prefer blind windows where `normal` tightening changes the passing set in **both** months, otherwise month-level comparisons are not meaningful.

### 43. Blind Month Proxy `2025-09` - Monthly-RangeWidth Gate v8 (retest; tighten `wide`/`extreme_wide` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind retest of v8 on an **earlier month pair** (Sep-Oct 2025) to check stability and whether tightening only `wide`/`extreme_wide` (plus dropping `UNKNOWN`) improves monthly compounding toward the `2%` target when baseline pass-days are non-trivial in both months.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-09-01..2025-10-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v8 (drop `UNKNOWN` + tighten `wide`/`extreme_wide`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: baseline thresholds
  - `wide`: `entryMinEv >= 0.06`, `entryMaxRevRisk <= 0.34`
  - `extreme_wide`: `entryMinEv >= 0.065`, `entryMaxRevRisk <= 0.33`

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-09` | 20 | 9 | 7 | `-3.26%` | `-6.00%` | 22.22% | 14.29% |
| `2025-10` | 22 | 12 | 11 | `-1.50%` | `-1.99%` | 16.67% | 18.18% |

#### Read-through

- `2025-09`: v8 bound (`9 -> 7`) but the month-level median worsened (`-3.26% -> -6.00%`) and hit>=`2%` fell (22.22% -> 14.29%). Tightening only `wide`/`extreme_wide` appears **too strict / in the wrong place** for this month under the proxy.
- `2025-10`: v8 bound slightly (`12 -> 11`) and the median worsened (`-1.50% -> -1.99%`). hit>=`2%` improved marginally (16.67% -> 18.18%), but the month-level median moved further away from the `+2%` target.
- Compared to the existing ledger’s v8 retest (`2026-01..2026-02`), this window reinforces the same pattern: v8 either fails to improve negative-median months (and can worsen them) even when it binds, and it does not provide stable month-to-month uplift toward `+2%`.

#### Practical conclusion

- On this blind window (`2025-09..2025-10`), v8 is **not a demonstrated improvement** toward the `2%` monthly compounding target under this proxy: it worsens the month-level median in both months and reduces hit>=`2%` in Sep.
- Net: treat v8 (tighten only `wide`/`extreme_wide` + drop `UNKNOWN`) as **too strict / not reliably helpful** unless future blind windows show consistent median lift without collapsing pass-day coverage. If the negative drift is `normal`-driven, adjustments confined to `wide`/`extreme_wide` are unlikely to help.

### 44. Blind Month Proxy `2025-06` - Monthly-RangeWidth Gate v4 (retest; tighten `normal` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind retest of v4 on an **earlier month pair** (Jun-Jul 2025) to check whether tightening only the `normal` bucket (and dropping `UNKNOWN`) improves monthly compounding toward the `2%` target while keeping the baseline composite gate fixed.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-06-01..2025-07-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v4 (drop `UNKNOWN` + tighten `normal`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: `entryMinEv >= 0.055`, `entryMaxRevRisk <= 0.36`
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`
  - `extreme_wide`: baseline thresholds

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-06` | 21 | 10 | 10 | `-17.23%` | `-17.23%` | 20.00% | 20.00% |
| `2025-07` | 22 | 5 | 5 | `+1.03%` | `+1.03%` | 40.00% | 40.00% |

#### Read-through

- In this window, v4 did **not bind** at all (`base pass == var pass` in both months), so the month-level medians and hit>=`2%` were identical. This means v4 is **too loose / not active** on `2025-06..2025-07` under the rank-1 long proxy.
- `2025-06` is deeply negative even under the baseline gate (`-17.23%` median), far from the `+2%` target, and v4 provides no lift here.
- `2025-07` is **sparse** (`5` pass-days) and still below the `+2%` median target (`+1.03%`). Because v4 is inactive, it cannot improve monthly compounding in this month either.

#### Practical conclusion

- On this blind window (`2025-06..2025-07`), v4 is **not a demonstrated improvement** toward the `2%` monthly compounding target under this proxy because it is inactive (`base == var`).
- Net: treat v4 as **non-informative unless it binds**; future blind windows should ensure non-trivial baseline pass-days in both months and enough `normal`-bucket candidates for the tightening to activate.

### 45. Blind Month Proxy `2025-02` - Monthly-RangeWidth Gate v6 (strict `normal` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind test of v6 on an **earlier month pair** (Feb-Mar 2025) to check whether *strictly* tightening only the `normal` bucket (and dropping `UNKNOWN`) improves monthly compounding toward the `2%` target while keeping the baseline composite gate fixed.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-02-01..2025-03-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v6 (drop `UNKNOWN` + strict tighten `normal`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: `entryMinEv >= 0.065`, `entryMaxRevRisk <= 0.33`
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`
  - `extreme_wide`: baseline thresholds

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-02` | 18 | 7 | 7 | `-4.78%` | `-4.78%` | 0.00% | 0.00% |
| `2025-03` | 20 | 7 | 7 | `+2.65%` | `+2.65%` | 57.14% | 57.14% |

#### Read-through

- In this window, v6 did **not bind** at all (`base pass == var pass` in both months), so the month-level medians and hit>=`2%` were identical. This means v6 is **inactive / non-informative** on `2025-02..2025-03` under the rank-1 long proxy.
- `2025-02` remains meaningfully negative under the baseline gate (`-4.78%` median) with a 0% hit>=`2%`, far from the `+2%` target; v6 provides no lift because it does not activate.
- `2025-03` is slightly above the `+2%` median target (`+2.65%`) but still **sparse** (`7` pass-days). Because v6 is inactive, it cannot improve compounding here either.

#### Practical conclusion

- On this blind window (`2025-02..2025-03`), v6 is **not a demonstrated improvement** toward the `2%` monthly compounding target under this proxy because it is inactive (`base == var`).
- Net: treat v6 as **non-informative unless it binds**; future blind windows should ensure non-trivial baseline pass-days and enough `normal`-bucket baseline passes near the threshold so strict tightening can actually activate.

### 46. Blind Month Proxy `2026-02` - Monthly-RangeWidth Gate v8 (tighten `wide`/`extreme_wide` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind test of v8 on a **recent month pair** (Feb-Mar 2026) to see whether tightening only the `wide`/`extreme_wide` monthly contexts (and dropping `UNKNOWN`) improves month-level compounding toward the `2%` target while keeping the baseline composite gate fixed.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2026-02-01..2026-03-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v8 (drop `UNKNOWN` + tighten `wide`/`extreme_wide`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: baseline thresholds
  - `wide`: `entryMinEv >= 0.06`, `entryMaxRevRisk <= 0.34`
  - `extreme_wide`: `entryMinEv >= 0.065`, `entryMaxRevRisk <= 0.33`

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2026-02` | 17 | 15 | 15 | `+3.54%` | `+3.54%` | 53.33% | 53.33% |
| `2026-03` | 20 | 14 | 14 | `-0.21%` | `-0.21%` | 42.86% | 42.86% |

#### Read-through

- In this window, v8 did **not bind** at all (`base pass == var pass` in both months), so the month-level medians and hit>=`2%` were identical. This means v8 is **inactive / non-informative** on `2026-02..2026-03` under the rank-1 long proxy.
- Context coverage was thin for the buckets v8 actually changes: across both months there were only `3` `wide/extreme_wide` days total (`2026-02`: `wide=2`, `extreme_wide=1`; `2026-03`: `wide=2`, `extreme_wide=0`), and the tightening did not change the passing set.
- `2026-02` clears the `+2%` median target under the baseline gate (`+3.54%`), but `2026-03` is still below the target (`-0.21%`). Because v8 is inactive here, it cannot address the month-to-month instability.

#### Practical conclusion

- On this blind window (`2026-02..2026-03`), v8 is **not a demonstrated improvement** toward the `2%` monthly compounding target under this proxy because it is inactive (`base == var`).
- Net: treat v8 as **non-informative unless it binds**; future v8 blind tests should prioritize windows where `wide/extreme_wide` contexts appear frequently enough among baseline passes for the tightening to actually activate.

### 47. Blind Month Proxy `2026-01` - Monthly-RangeWidth Gate v9 (tighten `tight` + strict `normal` + v8-style `wide/extreme_wide` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind test of v9 on a **recent month pair** (Jan-Feb 2026) to see whether tightening the more-common `tight` and `normal` monthly contexts (while keeping the baseline composite gate fixed) improves month-level compounding toward the `2%` target.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2026-01-01..2026-02-28`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v9 (drop `UNKNOWN` + tighten `tight`/`normal` + v8-style `wide/extreme_wide`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: `entryMinEv >= 0.06`, `entryMaxRevRisk <= 0.35`
  - `normal`: `entryMinEv >= 0.065`, `entryMaxRevRisk <= 0.33`
  - `wide`: `entryMinEv >= 0.06`, `entryMaxRevRisk <= 0.34`
  - `extreme_wide`: `entryMinEv >= 0.065`, `entryMaxRevRisk <= 0.33`

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2026-01` | 19 | 13 | 12 | `-4.10%` | `-4.57%` | 38.46% | 33.33% |
| `2026-02` | 17 | 15 | 15 | `+3.54%` | `+3.54%` | 53.33% | 53.33% |

#### Read-through

- `2026-01`: v9 bound slightly (`13 -> 12`) but the median worsened (`-4.10% -> -4.57%`) and hit>=`2%` fell (38.46% -> 33.33%). This suggests the extra tightening is **too strict / not aligned** with the month-level proxy on this month.
- `2026-02`: v9 did **not bind** (`15 == 15`), so the month-level median and hit>=`2%` were identical. This means v9 is **inactive** here and cannot improve stability.

#### Practical conclusion

- On this blind window (`2026-01..2026-02`), v9 is **not a demonstrated improvement** toward the `2%` monthly compounding target under this proxy: when it binds (Jan), it worsens the month median and hit>=`2%`, and when it does not bind (Feb) it adds no information.
- Net: treat v9 as **too strict / not helpful** under this proxy; future tightening attempts should be considered only if they bind meaningfully and improve (not degrade) the negative months without collapsing coverage.

### 48. Blind Month Proxy `2026-01` - Monthly-RangeWidth Gate v7 (retest; soft `normal` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind retest of v7 on the **same recent month pair** (Jan-Feb 2026) to isolate whether a *milder* `normal` tightening (plus dropping `UNKNOWN`) improves monthly compounding toward the `2%` target while keeping the baseline composite gate fixed.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2026-01-01..2026-02-28`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v7 (drop `UNKNOWN` + soft tighten `normal`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: `entryMinEv >= 0.06`, `entryMaxRevRisk <= 0.35`
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`
  - `extreme_wide`: baseline thresholds

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2026-01` | 19 | 13 | 13 | `-4.10%` | `-4.10%` | 38.46% | 38.46% |
| `2026-02` | 17 | 15 | 15 | `+3.54%` | `+3.54%` | 53.33% | 53.33% |

#### Read-through

- In this window, v7 did **not bind** at all (`base pass == var pass` in both months), so the month-level medians and hit>=`2%` were identical. This means v7 is **inactive / non-informative** on `2026-01..2026-02` under the rank-1 long proxy.
- Compared to v9 on the same window (which did bind in Jan but worsened the month median), v7 is on the opposite extreme: it is **too loose** to activate, so it cannot improve month-to-month stability toward `+2%`.

#### Practical conclusion

- On this blind window (`2026-01..2026-02`), v7 is **not a demonstrated improvement** toward the `2%` monthly compounding target under this proxy because it is inactive (`base == var`).
- Net: treat v7 as **non-informative unless it binds**; future v7 blind tests should prioritize windows where baseline pass-days include enough `normal`-bucket candidates near the `entryMinEv=0.06` / `revRisk=0.35` boundary so the soft tightening can actually activate.

### 49. Blind Month Proxy `2025-09` - Monthly-RangeWidth Gate v3 (exclude `normal`/`UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind test of v3 on an **earlier negative-median month pair** (Sep-Oct 2025) to check whether **hard-excluding `normal`** (and `UNKNOWN`) improves month-level compounding toward the `2%` target while keeping the baseline composite gate fixed.

This directly follows the ledger’s earlier v3 read-through that “excluding `normal` can lift a toxic month” (e.g., `2025-12..2026-01`), and tests whether that behavior is **stable** on another window.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-09-01..2025-10-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v3 (exclude `normal`/`UNKNOWN` with per-bucket entryMinEv/revRisk; entryMinEv/revRisk only)

- monthly context filter: allow only `tight`, `wide`, `extreme_wide` (exclude `normal` and `UNKNOWN`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: `entryMinEv >= 0.055`, `entryMaxRevRisk <= 0.36`
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`
  - `extreme_wide`: `entryMinEv >= 0.045`, `entryMaxRevRisk <= 0.42`

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-09` | 20 | 9 | 6 | `-3.26%` | `-2.08%` | 22.22% | 33.33% |
| `2025-10` | 22 | 12 | 10 | `-1.50%` | `-2.24%` | 16.67% | 10.00% |

#### Read-through

- `2025-09`: v3 bound (`9 -> 6`) and the median improved modestly (`-3.26% -> -2.08%`) with an improved hit>=`2%` rate (22.22% -> 33.33%). However, the month-level median remains **well below** the `+2%` target and coverage shrank materially.
- `2025-10`: v3 bound (`12 -> 10`) but the month-level median worsened (`-1.50% -> -2.24%`) and hit>=`2%` fell (16.67% -> 10.00%). This suggests that a blanket “exclude `normal`” rule can remove some of the better outcomes in months where `normal` is not the dominant failure mode.
- Month-to-month stability: the direction flips (help Sep, hurt Oct), so this is not a stable improvement toward the `2%` compounding target under this proxy.

#### Practical conclusion

- On this blind window (`2025-09..2025-10`), v3 is **not a demonstrated improvement** toward the `2%` monthly compounding target under this proxy: it improves Sep slightly but worsens Oct and reduces the `2%` hit-rate.
- Net: treat “exclude `normal`/`UNKNOWN`” as **not stable across months**; it may help only when `normal` is provably toxic for the window, and should not be used as a default monthly-context filter without additional month-level diagnostics.

### 50. Blind Month Proxy `2026-02` - Monthly-RangeWidth Gate v5 (retest; exclude `extreme_wide`/`UNKNOWN` + bucket thresholds, entryMinEv/revRisk only)

This is a narrow monthly blind retest of v5 on a **recent mixed month pair** (Feb-Mar 2026) to check whether **filtering out `extreme_wide`** (and `UNKNOWN`) improves monthly compounding toward the `2%` target while keeping the baseline composite gate fixed.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2026-02-01..2026-03-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v5 (exclude `extreme_wide`/`UNKNOWN` + wide-bucket tighten; entryMinEv/revRisk only)

- monthly context filter: allow only `tight`, `normal`, `wide` (exclude `extreme_wide` and `UNKNOWN`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: baseline thresholds
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2026-02` | 17 | 15 | 14 | `+3.54%` | `+4.17%` | 53.33% | 57.14% |
| `2026-03` | 20 | 14 | 14 | `-0.21%` | `-0.21%` | 42.86% | 42.86% |

#### Read-through

- `2026-02`: v5 bound slightly (`15 -> 14`) and the median improved (`+3.54% -> +4.17%`) with a small lift in hit>=`2%` (53.33% -> 57.14%). This is consistent with v5 acting as a **light safety filter** when `extreme_wide` candidates actually appear and pass the baseline.
- `2026-03`: v5 did **not bind** (`14 == 14`), so the month-level median and hit>=`2%` were identical. In this month, `rank=1` days were dominated by `tight`/`normal` and had no `extreme_wide`, so the filter had no opportunity to help.

#### Practical conclusion

- On this blind window (`2026-02..2026-03`), v5 provides **limited evidence** of improvement toward the `2%` monthly compounding target: it can slightly improve a positive month when it binds, but it is **too loose/inactive** to lift a weak month (Mar remains below `+2%` and unchanged).
- Net: treat v5 as **mildly helpful only when `extreme_wide` is present**, and otherwise **non-informative**; it is not a stable month-to-month compounding improvement under this proxy.

### 51. Blind Month Proxy `2026-02` - Monthly-RangeWidth Gate v4 (retest; tighten `normal` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind test of v4 on the **same recent mixed month pair** (Feb-Mar 2026) to check whether **tightening only the `normal` bucket** (and excluding `UNKNOWN`) improves monthly compounding toward the `2%` target while keeping the baseline composite gate fixed.

#### Setup

- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2026-02-01..2026-03-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v4 (drop `UNKNOWN` + tighten `normal`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: `entryMinEv >= 0.055`, `entryMaxRevRisk <= 0.36`
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`
  - `extreme_wide`: baseline thresholds

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2026-02` | 17 | 15 | 15 | `+3.54%` | `+3.54%` | 53.33% | 53.33% |
| `2026-03` | 20 | 14 | 14 | `-0.21%` | `-0.21%` | 42.86% | 42.86% |

#### Read-through

- v4 did **not bind** (`base pass == var pass` in both months), so the month-level medians and hit>=`2%` were identical. Under this window, baseline pass-days already meet the tightened `normal` thresholds and there are no `UNKNOWN` passes to remove.
- Compared to v5 on the same window (which bound slightly in Feb by excluding `extreme_wide`), v4 provides **no active filtering** and therefore cannot improve the negative-median month (Mar) toward the `+2%` target.

#### Practical conclusion

- On this blind window (`2026-02..2026-03`), v4 is **not a demonstrated improvement** toward the `2%` monthly compounding target under this proxy because it is inactive (`base == var`).
- Net: treat v4 as **too loose / non-informative unless it binds**; future v4 retests should prioritize windows where baseline pass-days include borderline `normal` candidates near `entryMinEv=0.055` / `entryMaxRevRisk=0.36` so the tightening can activate.


### 52. Blind Month Proxy `2025-11` - Monthly-RangeWidth Gate v6 (strict `normal` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind test that keeps the baseline composite gate fixed and tests whether **tightening only the `normal` bucket** (more strictly than v4/v7) while excluding missing-context days (`UNKNOWN`) improves monthly compounding toward the `2%` target.

#### Setup

- run_ts: `2026-04-15T10:34:21+09:00`
- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-11-01..2025-12-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v6 (drop `UNKNOWN` + strict `normal`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: `entryMinEv >= 0.065`, `entryMaxRevRisk <= 0.33`
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`
  - `extreme_wide`: baseline thresholds

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-11` | 18 | 13 | 10 | `+3.23%` | `+9.43%` | 61.54% | 70.00% |
| `2025-12` | 22 | 4 | 3 | `+0.28%` | `+0.28%` | 25.00% | 33.33% |

#### Read-through

- `2025-11`: v6 bound (`13 -> 10`) and the month-level median improved materially (`+3.23% -> +9.43%`) with a higher hit>=`2%` rate (61.54% -> 70.00%). This is consistent with strict `normal` tightening removing some weaker `normal` outcomes in this month.
- `2025-12`: coverage is **very sparse** even at baseline (`base pass = 4`), and v6 reduced it further (`4 -> 3`) without lifting the median (`+0.28%` unchanged). With this sample size, hit>=`2%` changes are not reliable.
- Compared to the ledger's prior Nov-Dec 2025 v4 note (moderate `normal` tightening), v6 produces a larger Nov uplift but does not address Dec's sub-`2%` median and sparsity; evidence remains **not stable across months** toward the compounding target.

#### Practical conclusion

- On this blind window (`2025-11..2025-12`), v6 shows **month-specific benefit** (Nov) but **insufficient stability**: Dec remains far below the `+2%` median target with too few pass-days for confident inference.
- Net: treat v6 as **too strict / coverage-sensitive** in low-signal months; it is not yet demonstrated as a stable monthly-compounding improvement under this proxy.

### 53. Blind Month Proxy `2025-07` - Monthly-RangeWidth Gate v8 (retest; tighten `wide`/`extreme_wide` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind test that keeps the baseline composite gate fixed and retests whether **tightening only the wider monthly contexts** (while leaving `tight`/`normal` unchanged) improves month-level compounding toward the `2%` target.

#### Setup

- run_ts: `2026-04-15T11:29:05+09:00`
- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-07-01..2025-08-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v8 (drop `UNKNOWN` + tighten `wide`/`extreme_wide`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: baseline thresholds
  - `wide`: `entryMinEv >= 0.06`, `entryMaxRevRisk <= 0.34`
  - `extreme_wide`: `entryMinEv >= 0.065`, `entryMaxRevRisk <= 0.33`

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-07` | 22 | 5 | 5 | `+1.03%` | `+1.03%` | 40.00% | 40.00% |
| `2025-08` | 20 | 16 | 15 | `+0.19%` | `+0.00%` | 37.50% | 33.33% |

#### Read-through

- `2025-07`: v8 did **not bind** (`5 == 5`), so month-level median/hit>=`2%` were identical. Baseline coverage is also sparse (`5` pass-days), so this month is highly coverage-sensitive under the proxy.
- `2025-08`: v8 bound slightly (`16 -> 15`) but the median worsened (`+0.19% -> +0.00%`) and hit>=`2%` fell (37.50% -> 33.33%). This suggests that the `wide`/`extreme_wide` tightening is either filtering a non-toxic candidate or is simply not aligned with the month-end quality under this stream.
- Compared to v8’s earlier window (`2025-10..2025-11`, section 35), the same pattern persists: v8 does **not** reliably move weak months toward the `+2%` median target, and can slightly degrade already-sub-`2%` months when it binds.

#### Practical conclusion

- On this blind window (`2025-07..2025-08`), v8 is **not a demonstrated improvement** toward the `2%` monthly compounding target under this proxy: it is inactive in Jul and worsens Aug’s already-sub-`2%` median.
- Net: treat v8 as **too loose/inactive** for consistent month-to-month compounding improvement; when it does bind, the effect is not reliably favorable toward `+2%`.

### 54. Blind Month Proxy `2025-04` - Monthly-RangeWidth Gate v5 (retest; exclude `extreme_wide`/`UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind test that keeps the baseline composite gate fixed and retests whether **filtering out `extreme_wide` monthly-range-width context** (and excluding `UNKNOWN`) improves month-level compounding toward the `2%` target while changing only `entryMinEv` / `revRisk` thresholds.

#### Setup

- run_ts: `2026-04-15T12:32:35+09:00`
- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2025-04-01..2025-05-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v5 (exclude `extreme_wide` + drop `UNKNOWN`; entryMinEv/revRisk only)

- monthly context filter: allow only `tight`/`normal`/`wide`; exclude `extreme_wide` and `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: baseline thresholds
  - `normal`: baseline thresholds
  - `wide`: `entryMinEv >= 0.0525`, `entryMaxRevRisk <= 0.37`

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2025-04` | 21 | 13 | 7 | `-8.68%` | `-8.68%` | 23.08% | 14.29% |
| `2025-05` | 20 | 6 | 4 | `-8.02%` | `-8.02%` | 0.00% | 0.00% |

#### Read-through

- `2025-04`: v5 bound strongly (`13 -> 7`) in a month with heavy `extreme_wide` context presence (`8/21` rank1-days). Despite removing ~half the baseline pass-days, the median stayed unchanged (`-8.68%`) and hit>=`2%` worsened (23.08% -> 14.29%). This suggests the `extreme_wide` exclusion is not selectively removing low-quality outcomes under this proxy and can remove positive-tail days without lifting the month.
- `2025-05`: coverage is sparse at baseline (`base pass = 6`) and v5 reduced it further (`6 -> 4`) with no median lift (`-8.02%` unchanged) and 0% hit>=`2%` either way. With this sample size, any small differences would be unreliable.

#### Practical conclusion

- On this blind window (`2025-04..2025-05`), v5 is **not a demonstrated improvement** toward the `2%` monthly compounding target: it reduces coverage materially but does not lift medians (both months remain strongly negative) and can reduce the >=`2%` hit-rate.
- Net: treat v5’s `extreme_wide` exclusion as **too strict / misaligned** for stability under this proxy; it does not reliably move weak months toward `+2%` and can remove winners without improving the month-level median.

### 55. Blind Month Proxy `2026-02` - Monthly-RangeWidth Gate v9 (retest; tighten `tight` + strict `normal` + v8-style `wide`/`extreme_wide` + drop `UNKNOWN`, entryMinEv/revRisk only)

This is a narrow monthly blind retest of v9 on a **recent mixed month pair** (Feb-Mar 2026) to check whether **tightening `tight`/`normal` in addition to `wide`/`extreme_wide`** (and excluding `UNKNOWN`) improves monthly compounding toward the `2%` target while keeping the baseline composite gate fixed.

#### Setup

- run_ts: `2026-04-16T09:29:19+09:00`
- source_db: `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb`
- window: `2026-02-01..2026-03-31`
- candidate stream: `ranking_appearance_daily` with `dir='up'` and `rank=1`
- monthly context axis: `signal_basis_daily.basis_payload_json.monthlyRangeWidth` bucketed into:
  - `tight`: `<= 0.28`
  - `normal`: `<= 0.54`
  - `wide`: `<= 0.74`
  - `extreme_wide`: `> 0.74`
- monthly return proxy: `(month_end_close / next_open - 1) * 100`

#### Baseline composite gate (kept fixed)

- `minLiquidity20d >= 500,000`
- `entryMinUpProb >= 0.75`
- `entryMinEv >= 0.05`
- `entryMaxRevRisk <= 0.40`

#### Regime-aware variant v9 (drop `UNKNOWN` + tighten `tight`/`normal` + v8-style `wide`/`extreme_wide`; entryMinEv/revRisk only)

- monthly context filter: exclude `UNKNOWN` (`monthlyRangeWidth IS NULL`)
- thresholds (only `entryMinEv` / `revRisk`):
  - `tight`: `entryMinEv >= 0.06`, `entryMaxRevRisk <= 0.35`
  - `normal`: `entryMinEv >= 0.065`, `entryMaxRevRisk <= 0.33`
  - `wide`: `entryMinEv >= 0.06`, `entryMaxRevRisk <= 0.34`
  - `extreme_wide`: `entryMinEv >= 0.065`, `entryMaxRevRisk <= 0.33`

#### Results (month-end return distribution over passing `rank=1` days)

| month | rank1 days | base pass | var pass | base median | var median | base hit>=2% | var hit>=2% |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `2026-02` | 17 | 15 | 15 | `+3.54%` | `+3.54%` | 53.33% | 53.33% |
| `2026-03` | 20 | 14 | 7 | `-0.21%` | `-16.75%` | 42.86% | 28.57% |

#### Read-through

- `2026-02`: v9 did **not bind** (`15 == 15`), so the month-level median and hit>=`2%` were identical. This means v9 is **inactive** here and cannot affect month-to-month stability.
- `2026-03`: v9 bound strongly (`14 -> 7`) but the month-level median collapsed (`-0.21% -> -16.75%`) and hit>=`2%` fell (42.86% -> 28.57%). This suggests the extra tightening is **misaligned / too strict** under this proxy: it removes a large fraction of the baseline passing set but leaves a worse month-level outcome distribution.

#### Practical conclusion

- On this blind window (`2026-02..2026-03`), v9 is **not a demonstrated improvement** toward the `2%` monthly compounding target under this proxy: it is inactive in Feb and materially worsens Mar when it binds.
- Net: treat v9 as **too strict / not stable across months** for month-level compounding improvement; further tightening on `tight`/`normal` should be avoided unless it can be shown to lift weak-month medians without collapsing into worse tails.
