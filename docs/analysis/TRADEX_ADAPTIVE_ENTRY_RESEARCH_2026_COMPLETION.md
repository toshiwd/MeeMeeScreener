# TRADEX 2026 adaptive entry research completion audit

## Outcome

The research objective is implemented as a review-only, point-in-time entry selection workflow. It does not place orders and does not use holdings or a capital assumption.

Latest authoritative refresh:

- confirmed data date: 2026-07-10
- refresh status: pass (17/17 steps)
- current regime: broad_up
- current directional bias: buy_priority
- current actionable entry review: 3479 / leaf9 / buy
- current provisional short: none (no newer Yahoo provisional row after the confirmed sync)

## Requirement audit

| Requirement | Status | Evidence |
|---|---|---|
| Adapt to the current 2026 regime | proven | `adaptive_rule_router_v1` evaluates lagged recent-20, recent-60 and same-regime histories at every decision date. The selected recent guard rejects negative recent-20 expectancy or PF below 1.0. |
| Reduce overfitting | proven within the tested rule surface | Outcomes become available only after the declared delay; regime permissions use 2019-2025 or rolling point-in-time history; 2026 is reported separately; train/validation/test and annual breakdowns exist for shape axes. |
| Support multiple buy shapes | proven | Momentum leader re-entry, leaf9/14/20, MA60 weekly reversal, GU first pullback, volatility contraction, base breakout, clean breakout and MA20 support reclaim are retained independently rather than collapsed into one permanent rule. |
| Support sell selection | proven | Support-break with breadth>=40% has a confirmed-close historical ledger and an intraday Yahoo preview contract. 2019-2025 daily PF=1.779; 2026 daily PF=1.294, but only 0.464 events/week, so it is not forced into the current broad-up board. |
| Select actual symbols | proven | `integrated_entry_board_v1` emits ranked actionable and watch rows. Latest actionable row is 3479 leaf9; watch rows are 2120/7803 clean breakout and 3479/6141/6988 MA20 reclaim. |
| Make a pre-close judgment | proven when a newer Yahoo bar exists | The preview calculates provisional MA20, market breadth, support break, volume, close position and MA20 distance. It remains explicitly provisional and requires official-close confirmation plus a next-session signal-low break. Tests cover both Yahoo-present and Yahoo-absent states. |
| Compare with existing rules | proven | The adaptive policy variants report development and 2026 metrics side by side. The current selected policy is active_recent_guard_top3; fixed MA20 reclaim and clean breakout were measured and rejected as current executable rules. |
| Reproduce through the daily workflow | proven | `tradex_adaptive_rule_refresh_v1.py` regenerates source events, current family scans, adaptive router, intraday short preview, integrated entry board and stress audit. Latest full run passed all 17 steps. |

## Latest measured performance

Selected buy router (`active_recent_guard_top3`):

- 2019-2025 daily PF: 1.284
- 2019-2025 daily expectancy: +0.436%
- 2026 daily PF: 1.631
- 2026 daily expectancy: +0.973%
- 2026 average events/calendar week: 1.929
- 2026 weeks with an event: 12/28
- maximum empty run: 8 weeks

Momentum-removal stress:

- 2026 daily PF: 1.250
- 2026 daily expectancy: +0.460%
- average events/calendar week: 1.036
- This passes the declared daily aggregation gate, so the result is not solely dependent on the momentum family. Event-level expectancy is negative and remains a disclosed weakness.

Conditional support-break short:

- 2019-2025 daily PF: 1.779
- 2019-2025 daily expectancy: +1.549%
- 2026 daily PF: 1.294
- 2026 daily expectancy: +0.739%
- 2026 average events/calendar week: 0.464

## Current family decisions

- Active: momentum leader re-entry, leaf9
- Secondary/watch: clean breakout
- Dormant/watch: MA20 support reclaim
- Conditional sell: support-break only when the breadth and shape gates pass
- No-entry days remain valid; frequency is evaluated over time and is not manufactured by weakening the gate.

## Boundaries and limitations

- These are historical/research estimates, not a promise of future profit.
- Costs are excluded in the adaptive-router comparison; short borrow availability and borrow fees are not present in the DB contract.
- Same-bar stop/target ambiguity uses the conservative stop-first convention where declared.
- Yahoo rows are intentionally removed when the confirmed production DB is synchronized. Pre-close selection therefore requires the intraday Yahoo ingestion process to have produced a newer row after that sync.
- The broad `negative_guard_matched` surface showed high winner capture in the prior missed-winner audit, but it is not promoted as a standalone executable family because a calibrated point-in-time positive-selection score contract is absent. MA20 reclaim and clean breakout provide executable independent candidate surfaces without treating recall alone as an entry edge.
- The workflow is review-only. It does not submit orders or mutate the production ranking.
