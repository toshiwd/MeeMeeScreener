# Discretionary Chart Pattern Notes

This memo collects user-reviewed chart patterns for discretionary trade review.
It is not an automated TRADEX rule, ranking input, or MeeMee display contract.

## Boundary

- Owner: discretionary review / TRADEX research notes.
- MeeMee role: chart, MA, volume, and position-history confirmation only.
- TRADEX role: possible later research validation only after enough cases exist.
- Do not convert these notes into production ranking, candidate generation, or buy/sell claims without a separate fixed-condition validation artifact.

## Breakout Success Pattern

Confirmed examples from screenshot review:

- `1605 INPEX`: range breakout, range high becomes support, price holds above 7MA/20MA, trend accelerates.
- `2531 Takara HD`: long box breakout, early full exit missed the later continuation; 2026-05-14 full return above 7MA/20MA was a re-entry candidate.
- `6963 ROHM`: long box breakout, price stays above the breakout range and rides 7MA/20MA upward.

Conditions:

- Breakout closes above a meaningful range high or prior ceiling.
- After breakout, price does not quickly return inside the box.
- 7MA stays under price and works as short-term support.
- 20MA slopes upward or starts turning upward.
- Pullbacks hold 7MA or 20MA.
- Higher MAs are below price or no longer acting as overhead resistance.
- Volume expands on the breakout or price holds after volume expansion.

Trade-management notes:

- Initial breakout can be a starter entry, not necessarily full size.
- Add only after support confirmation: range-high hold, 7MA/20MA hold, or small candles above support.
- Do not exit fully while price is above 7MA/20MA and the breakout line is holding.
- If an early full exit happens, a full return above 7MA/20MA can be a re-entry trigger.

## Breakout Failure Pattern

Confirmed examples from screenshot review:

- `1963 JGC HD`: attempted ceiling breakout failed quickly; gap-down, 20MA/60MA breaks, then further MA deterioration.
- `5938 LIXIL G`: breakout attempt advanced first, then failed to hold the upper range; 7MA/20MA broke and price returned toward the range.

Conditions:

- Breakout fails to hold the prior range high.
- Price closes back inside the box.
- 7MA breaks first, then 20MA breaks.
- 20MA turns flat to down.
- Higher MAs shift from support to overhead resistance.
- Rebound attempts fail under 7MA/20MA.

Trade-management notes:

- A breakout entry is invalidated when price quickly loses 20MA and cannot reclaim it.
- 20MA break plus range re-entry is not a normal pullback; treat it as breakout failure.
- 60MA break after 20MA failure ends the breakout scenario.
- If buying remains under consideration after failure, reclassify the setup as range-bottom/reversal, not breakout continuation.

## 20MA Failed Reclaim As Sell Candidate

Working hypothesis from user review:

- A close below 20MA is a warning.
- If price cannot reclaim 20MA quickly, the setup can shift from long-exit to short/hedge candidate.
- This is especially useful after a failed breakout, because the failed reclaim means the breakout line and short MAs are becoming overhead supply.

Candidate sell conditions:

- Breakout or high-zone continuation attempt fails.
- Price closes below 20MA.
- Within the next 1-3 sessions, price cannot close back above 20MA.
- 7MA is below or curling down into price.
- Rebound candles stall under 7MA/20MA.
- Price is back inside the prior box or below the breakout line.

Risk notes:

- A single 20MA break is not enough by itself; fast reclaim can still be a buy-back pattern.
- Do not use this as a broad short rule without fixed-condition validation.
- Separate "exit long" from "open short"; short/hedge requires the failed reclaim and overhead-resistance context.
- First fixed-condition check (`G:\Tradex\discretionary_pattern_validation_v1\20260606T135031Z-failed-20ma-reclaim-axis\failed_20ma_reclaim_compare.json`) did not support a standalone delayed short entry after 3-day failed reclaim. It supports long invalidation more clearly than short entry.

## Current Rule Summary

- Breakout success: breakout line holds, 7MA/20MA support, add or hold.
- Breakout failure: breakout line lost, 20MA lost, failed reclaim, exit long.
- Sell/hedge candidate: 20MA lost and not reclaimed within 1-3 sessions after a failed breakout.
- Re-entry candidate after early exit: full return above 7MA/20MA with price holding above the breakout support.
- Resistance-zone battle is the core context: the useful split is not just "breakout happened", but whether the prior resistance zone becomes support over the next few sessions.

## Resistance-Zone Battle Pattern

First fixed-condition check:

- Artifact: `G:\Tradex\discretionary_pattern_validation_v1\20260606T160228Z-resistance-zone-battle-axis\resistance_zone_battle_compare.json`
- Decision: `keep`
- Result: resistance breaks that held the zone for the next 3 sessions materially outperformed breaks that lost the zone within 3 sessions.

Working definition used in the first check:

- Resistance proxy: prior 120-session high with at least 2 prior touches near that high.
- Break: high reaches the zone and close finishes at least 0.5% above the resistance proxy.
- Support hold: next 3 closes remain at or above 99% of the resistance proxy.
- Zone lost: any next 3 close falls below 99% of the resistance proxy.

Observed result:

- All resistance breaks: ret20 mean about +1.1%, ret20 positive-rate about 52.0%.
- Break and support-hold 3d: ret20 mean about +2.6%, ret20 positive-rate about 58.2%.
- Break but zone-lost within 3d: ret20 mean about -3.0%, ret20 positive-rate about 34.3%.

Trade-management notes:

- The entry question should be framed as a battle around a known resistance zone.
- Initial break is only a candidate event.
- Add or hold only when the resistance zone turns into support.
- If the zone is lost within 3 sessions, treat the breakout as failed even if the chart still looks strong by MA distance.
- This pattern explains why MA-only and candle-only checks were weak: they missed the support/rejection boundary.
- Timing check (`G:\Tradex\discretionary_pattern_validation_v1\20260607T010821Z-resistance-support-hold-timing-axis\resistance_support_hold_timing_compare.json`) returned `hold`: waiting 1/2/3 days before new entry reduced much of the edge, while losing the zone was already bad from day 1. Practical use is starter on break, then exit quickly if the zone is lost; support-hold is more useful for hold/add confirmation than for delayed first entry.

## Candidate Samples To Review

Source: rough DB screen on runtime `daily_bars` / `daily_ma`, 2025-01-01 to 2026-06-06.
These are not validated TRADEX artifacts.

Breakout success-like candidates:

- `6330`, 2025-07-01: likely trigger was not the late 2025-12-29 screen hit, but the earlier recovery after spending 200+ bars below 200MA. The useful pattern may be "long 200MA-under base, then 20MA reclaim" as an accumulation/re-entry setup before the later breakout.
- `4784`, 2025-04-15 area: not the scary 2025-04-07 surge itself. The practical entry pattern is "strong up candle -> full-return down candle -> negation breakout up candle" after the initial surge.
- `3905`, 2025-06-03: breakout candidate with large volume expansion and follow-through.
- Low-priced outliers such as `4889`, `5721`, `2134`, and `6740` showed extreme returns but should be reviewed separately because low price / event-like moves may not generalize.
- `6976`, 2026-05-19 to 2026-05-21: remove from clean range-breakout samples. It showed strong continuation, but the user review says it is not really a range breakout pattern.

## 200MA-Under Long Base To 20MA Reclaim Pattern

User-reviewed example:

- `6330`, around 2025-07-01: price had spent 200+ bars below 200MA. After enough time under 200MA, reclaiming 20MA may be an early accumulation/re-entry clue before the obvious later breakout.

Working conditions:

- Price has spent a very long period below 200MA, roughly 200 bars or more.
- Selling pressure has decayed into a base rather than continuing as a clean downtrend.
- Price reclaims 20MA after the long 200MA-under period.
- 20MA turns flat to up, or price repeatedly holds around 20MA after reclaim.
- The setup is not yet a clean range breakout; it is a pre-breakout accumulation candidate.

Trade-management notes:

- Treat this as early accumulation, not confirmation.
- Initial size should be smaller than a confirmed breakout entry.
- Add only if 20MA reclaim holds or the later range-high breakout confirms.
- If 20MA is lost again and cannot be reclaimed, the base attempt failed.

## Surge Pullback Negation Entry Pattern

User-reviewed example:

- `4784`, around 2025-04-15: the first surge on 2025-04-07 is difficult to enter in real time. A more usable entry appears after the sequence: up candle, full-return down candle, then a negation up candle that breaks back above the failed down move.

Working conditions:

- A sharp first surge occurs, but it is too extended or too scary for a clean entry.
- The next move fully returns or nearly fully returns the surge candle.
- Instead of continuing down, price prints a strong negation up candle.
- The negation candle closes back above the short-term damage area.
- 7MA/20MA context remains supportive or quickly recovers.

Trade-management notes:

- Treat the first surge as discovery, not mandatory entry.
- The negation up candle after the full-return down candle is the practical entry trigger.
- Initial size can be smaller than a clean base breakout because volatility is high.
- Add only if the negation candle holds and price does not immediately fall back under 7MA/20MA.
- First surge chase-ban check (`G:\Tradex\discretionary_pattern_validation_v1\20260606T135956Z-surge-chase-ban-axis\surge_chase_ban_compare.json`) returned `hold`: highly extended surge events were not bad on average, but had weaker median/win-rate and higher next-day rejection risk than less-extended surges.
- First three-candle negation check (`G:\Tradex\discretionary_pattern_validation_v1\20260606T144812Z-three-candle-negation-entry-axis\three_candle_negation_compare.json`) returned `drop`: the pattern captured `4784` on 2025-04-15, but broad same-condition results were worse than the raw context, likely because many negation candles were already too extended above 20MA.

Breakout failure / 20MA failed-reclaim candidates:

- `6526`, 2025-10-31 to 2025-11-05 area: 2025-10-31 surge is too risky to chase, and the next large down candle is also difficult to enter because it has already fallen too far. Practical entry review should start around 2025-11-05, after the failed surge and failed recovery context is visible.
- `5449`, 2026-02-05: breakout attempt followed by failed 20MA reclaim and sharp decline.
- `5016`, 2026-05-11: breakout attempt followed by failed 20MA reclaim and large 5-session decline.
- `5253`, 2025-02-12: breakout attempt followed by failed 20MA reclaim.
- `4063`, 2025-07-24: large-cap failure sample; useful for checking whether 20MA failed reclaim works outside small/mid caps.
- `4689`, 2026-05-11: large/liquid failure sample; smaller decline than the worst cases but useful for robustness.

## Failed Surge Delayed Entry Pattern

User-reviewed example:

- `6526`, around 2025-10-31 to 2025-11-05: the first surge is not a practical entry because chasing it is high risk. The next large down candle may be directionally strong, but it has already dropped too far for a clean entry. The usable review window starts after that, around 2025-11-05, when the failed surge and failed recovery structure is clearer.

Working conditions:

- A sudden high-volume surge appears.
- Chasing the surge is rejected because the candle is too extended.
- A large next down candle rejects the surge.
- Do not enter immediately after the large down candle if the move is already too stretched.
- Wait for the post-rejection structure: weak rebound, failed reclaim, or stall under 7MA/20MA.
- Entry review begins after the failed recovery becomes visible, not at the first emotional candle.

Trade-management notes:

- First surge is a warning/discovery event, not an automatic long entry.
- Next large down candle confirms rejection but may be too late for immediate short/hedge entry.
- Prefer delayed entry from the weak rebound or failed reclaim area.
- If price quickly reclaims 20MA and holds, the failed-surge short idea is invalidated.
- First fixed-condition check (`G:\Tradex\discretionary_pattern_validation_v1\20260606T135518Z-failed-surge-delayed-entry-axis\failed_surge_delayed_entry_compare.json`) did not support a standalone t+4 delayed short entry. It supports "do not chase the first surge" more clearly than "open short after weak rebound."
