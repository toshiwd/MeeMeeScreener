# 5541 One-Month Replay

## Purpose

Fixed-symbol learning for `5541` (大平洋金属) starting from the first buy signal in the live trade history.

This note is meant to answer four questions:

- where the entry was early
- where confirmation arrived
- when to add vs. when to hold
- whether a first-month exit was actually justified

## Setup

- symbol: `5541`
- entry anchor: `2025-10-10`
- initial entry price: `1933`
- replay window: `2025-10-10` to `2025-11-10`
- month-end close: `2178`
- first-month mark-to-market return: `+12.67%`
- related position round: opened `2025-10-10`, closed `2026-01-19`

## What Happened

The first month was not a quick flip. It was a build-and-hold phase.

### Key checkpoints

| Date | Close | `diff20_pct` | `cnt_20_above` | `cnt_7_above` | `weeklyBreakoutUpProb` | `monthlyBreakoutUpProb` | `weeklyBreakoutDownProb` | `monthlyBreakoutDownProb` | Read |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 2025-10-10 | 1933 | -3.55% | 2 | 3 | 0.502 | 0.421 | 0.467 | 0.177 | Probe only. Still below MA20, but not a broken chart. |
| 2025-10-15 | 2037 | +1.85% | 1 | 1 | 0.518 | 0.421 | 0.463 | 0.177 | First clean reclaim above MA20. This is the first real confirmation. |
| 2025-10-22 | 2050 | +2.89% | 2 | 3 | 0.566 | 0.421 | 0.436 | 0.177 | Follow-through started. Breadth improved and the move looked more durable. |
| 2025-10-27 | 2073 | +4.08% | 5 | 6 | 0.622 | 0.421 | 0.381 | 0.177 | Trend continuation. This is the kind of day where adding is reasonable. |
| 2025-10-30 | 2016 | +0.98% | 8 | 0 | 0.622 | 0.421 | 0.381 | 0.177 | Pause day. Not a stop by itself because the higher-timeframe structure was still intact. |
| 2025-11-04 | 2188 | +8.52% | 10 | 2 | 0.627 | 0.448 | 0.375 | 0.150 | Extension day. Strong, but also the first day that looked trim-worthy. |
| 2025-11-10 | 2178 | +6.41% | 14 | 5 | 0.675 | 0.448 | 0.353 | 0.150 | Month-end: still bullish, no structural exit signal yet. |

## Supporting Context

- `market_ret20` was already positive at entry and stayed supportive through the first month.
- `breadth_above_ma20` improved from `0.306` on 2025-10-10 to `0.575` on 2025-11-10.
- `sector_ret20` was weak for much of the month, so the move was driven more by the name itself and the broad market than by sector leadership.
- `breakout20_up` turned positive on 2025-11-04, which was the first clear sign that the move had escaped the earlier box.

## Lessons

### 1. The first entry was too early for full size

On `2025-10-10`, the stock was still below MA20 and `diff20_pct` was negative.
That is a probe, not a conviction entry.

### 2. The real confirmation was the MA20 reclaim

The decisive change came on `2025-10-15`.
That was the first day when the stock reclaimed MA20 and started to prove that the move was not just a bounce.

### 3. The best add zone was the follow-through window

The period from `2025-10-22` to `2025-10-27` was the cleanest add window.
`cnt_20_above` kept rising and breadth improved at the same time.

### 4. A pause is not a stop if the higher frame still holds

`2025-10-30` was noisy, but it did not break the structure.
Stopping there would have been premature.

### 5. Extension days are trim candidates, not automatic exits

`2025-11-04` was strong enough to justify taking some profit off the table.
It was not strong enough to justify abandoning the core trend.

### 6. One-month learning outcome

For this symbol, the right mindset is:

- probe early
- confirm on MA20 reclaim
- add on follow-through
- trim on extension
- stop only when the structure weakens, not just because the trade has been open for a month

## Practical Rule Candidates

- Probe when the first buy signal appears but the chart is still below MA20.
- Add when the close reclaims MA20 and `cnt_20_above` starts rising.
- Hold the core while `cnt_20_above` and `breadth_above_ma20` keep improving.
- Trim when `diff20_pct` extends hard and the candle shape starts showing exhaustion.
- Cut when price loses MA20 for multiple days and breadth weakens at the same time.

## Monthly Carry Check

The same fixed symbol is useful because it shows whether the trade can be managed across full calendar months, not only across the first confirmation week.

### Month-by-month

| Month end | Close | Change vs prior month end | Long units | Short units | Gross notional at close | Gross / 10M | Read |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 2025-10-31 | 2,049 | `+6.0%` from entry | 1,700 | 300 | about `4.10M` | about `41%` | Still bullish. Add was justified. |
| 2025-11-28 | 2,530 | `+23.5%` vs Oct end | 1,700 | 1,200 | about `7.34M` | about `73%` | Strong month, but hedge had to get heavier. |
| 2025-12-30 | 2,266 | `-10.4%` vs Nov end | 100 | 100 | about `0.45M` | about `4.5%` | Chop month. De-risking was correct. |
| 2026-01-30 | 2,938 | `+29.7%` vs Dec end | 0 | 0 | `0` | `0%` | Final expansion finished cleanly. |

### What this means for a 10M account

- The observed gross exposure never got close to exhausting 10M.
- The peak month-end gross in this replay was about `7.34M`, so the account still had roughly `2.66M` of headroom.
- That means the symbol was large enough to support:
  - staged adds
  - a real hedge leg
  - partial trims without becoming forced

## Position Transition Log

This is the part that makes the month-by-month trade path visible.

### 2025-10

- Start: `100 long / 200 short`
- 2025-10-14: added `100 long / 200 short`
- 2025-10-15: covered `200 short` after the MA20 reclaim
- 2025-10-17 to 2025-10-30: long side kept building while the hedge was reduced
- Month-end: `1700 long / 300 short`
- Read: still a probe at the start, then a valid expansion once confirmation arrived

### 2025-11

- Start: `1700 long / 300 short`
- 2025-11-07: added `100 long / 200 short`
- 2025-11-10 to 2025-11-13: added `300 long` total
- 2025-11-18: hedge increased by `200 short`
- 2025-11-25: hedge increased by `500 short` while `500 long` was trimmed
- Month-end: `1700 long / 1200 short`
- Read: the book became a capped long with a meaningful hedge; this is not weird, it is controlled expansion

### 2025-12

- Start: `1700 long / 1200 short`
- 2025-12-02 to 2025-12-08: small adds and covers, but the book stayed hedged
- 2025-12-09: large long reduction
- 2025-12-11: another large long reduction
- 2025-12-15 to 2025-12-26: short leg was gradually covered while the long core was mostly flattened
- Month-end: `100 long / 100 short`
- Read: this was the de-risk month; if anything looks odd, this is the month to challenge

### 2026-01

- Start: `100 long / 100 short`
- 2026-01-16: long was fully closed
- 2026-01-19: short was fully closed
- Month-end: flat
- Read: clean exit, no sign of forced liquidation

## Daily Transition Log

This is the daily version. The point is to see whether each add or hedge had a chart reason.

| Date | Close | MA20 | MA60 | `diff20_pct` | Long / Short after actions | Actions | Read |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| 2025-10-10 | 1,933 | 2,004 | 1,950 | `-3.55%` | `100 / 200` | `open long 100`, `open short 200` | Probe only. |
| 2025-10-14 | 1,962 | 2,001 | 1,954 | `-1.95%` | `200 / 400` | `open long 100`, `open short 200` | Still probe. |
| 2025-10-15 | 2,037 | 2,000 | 1,959 | `+1.85%` | `400 / 200` | `open long 200`, `close short 200` | MA20 reclaim. First real confirmation. |
| 2025-10-17 | 1,982 | 1,995 | 1,967 | `-0.64%` | `500 / 400` | `open long 100`, `open short 200` | Pullback, but not broken. |
| 2025-10-20 | 1,992 | 1,993 | 1,969 | `-0.06%` | `800 / 200` | `open long 300`, `close short 200` | Strength improved, hedge lightened. |
| 2025-10-22 | 2,050 | 1,992 | 1,974 | `+2.89%` | `1,100 / 200` | `open long 300` | Follow-through. |
| 2025-10-23 | 2,041 | 1,992 | 1,977 | `+2.43%` | `1,300 / 200` | `open long 200` | Add continued. |
| 2025-10-27 | 2,073 | 1,992 | 1,984 | `+4.08%` | `1,500 / 300` | `open long 200`, `open short 100` | Extension, but still healthy. |
| 2025-10-28 | 1,990 | 1,990 | 1,986 | `+0.01%` | `1,500 / 500` | `open short 200` | Hedge up on a pause day. |
| 2025-10-30 | 2,016 | 1,996 | 1,991 | `+0.98%` | `1,700 / 300` | `open long 200`, `close short 200` | Trend intact, hedge trimmed. |
| 2025-11-07 | 2,088 | 2,039 | 2,011 | `+2.40%` | `1,800 / 500` | `open long 100`, `open short 200` | Re-acceleration with some hedge. |
| 2025-11-10 | 2,178 | 2,047 | 2,017 | `+6.41%` | `2,000 / 500` | `open long 200` | Strong continuation. |
| 2025-11-11 | 2,124 | 2,056 | 2,022 | `+3.29%` | `2,000 / 500` | none | Hold, not exit. |

## Sizing Read-Through

- The gross book never exceeded the rough `10M` capital line in this replay.
- Peak month-end gross was about `7.34M`, so there was still about `2.66M` of room.
- The hedge leg acted as a pressure valve, not as a full directional flip.

## What To Challenge

If you want to point at a weird trade, the best places are:

- whether November was hedged too early
- whether December was de-risked too hard
- whether January was flattened too completely before the final continuation

Those are the three places where a human review is most likely to disagree with the automated path.

## Keep / Drop / Next

This is the compressed version of the learning so far.

### Keep

- Use `5541` as a fixed-symbol study target because the trade history is dense enough to learn from.
- Treat the first entry as a probe when the chart is still below MA20.
- Wait for MA20 reclaim before calling it a real conviction entry.
- Add only when follow-through and breadth improve together.
- Keep a small hedge around one-fifth of gross exposure when the structure is healthy but not clean.
- Trim on extension days instead of forcing a full exit too early.
- Keep a small core through noisy pullbacks if MA20/MA60 structure is still intact.

### Drop

- Do not use half or full hedge as the default response to every pause.
- Do not flatten the whole book just because the trade has been open for a month.
- Do not treat the first candle that turns green as a full-size entry.
- Do not keep replaying blind monthly filter tests when the fixed-symbol daily replay already explains the path better.

### Next minimal check

- Replay one more month on `5541` and focus only on:
  - the first day of trim
  - the first day of real de-risk
  - the final exit day
- Compare those three points against the actual candle structure and MA state.
- If the same pattern repeats, stop expanding the test scope and turn it into a rule candidate.

### Monthly reflection

- October: the first probe was early, but it became valid once MA20 was reclaimed.
- November: this was the best expansion month. The mistake would have been to over-hedge too early and miss the continuation.
- December: this was the month where a long-only hold would have given back a lot. The hedge and de-risking were justified.
- January: the final leg showed why leaving a small core can matter. If the book is flattened too hard in a correction month, the next rally has to be re-entered from scratch.

## Conclusion

`5541` is a good fixed-symbol study target.
The first month shows that the edge was not in a single entry candle.
The edge was in the transition from probe to confirmation to follow-through.

The month finished at `+12.67%`, and the position was still structurally alive at the end of the window.
That is the main lesson: for this kind of trade, the goal is not to guess the exact top, but to avoid sizing the first probe as if it were already the final confirmation.
