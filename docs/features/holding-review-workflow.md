# Holding Review Workflow

## Purpose

Use `meemee_holding_review_bundle_v1` as the standard input for daily position review.
The bundle is read-only and supports operational judgment for current holdings.

## Boundary

- MeeMee owns structured holding, chart, signal, ranking, provisional, and event inputs.
- CODEX reads the bundle first and produces the daily action summary.
- TRADEX remains research-only and is not changed by this workflow.
- External fundamentals are supplemental until dedicated adapters are implemented.

## Required Input

Call one of:

- `GET /api/positions/holding-review`
- `GET /api/positions/holding-review/{code}`

Use these payload sections:

- `position`
- `entry_reason_snapshot`
- `current_hold_reason`
- `chart_context`
- `confirmed_bar`
- `provisional_bar`
- `event_gate`
- `decision`
- `data_quality`

## Review Rules

- Do not treat a past ranking appearance as current support.
- Do not merge Yahoo provisional data into confirmed data.
- Do not call a holding safe when `event_gate.event_risk_level` is `high`.
- Do not recommend `buy_add` when current hold reason is weak and event risk is high.
- If `data_quality.missing_fields` is non-empty, label the missing inputs explicitly.
- Use external fundamentals only as supplemental P2 checks.

## Supplemental P2 Checks

Use external sources only when needed for:

- earnings progress
- consensus gap
- revision possibility
- dividend or shareholder benefit
- credit balance, lending, and reverse stock loan fees

These checks do not override confirmed/provisional separation.

## Output

For each holding, report:

- bought reason
- whether current hold reason remains alive
- chart structure
- provisional intraday bar
- position P/L
- event risk
- supplemental fundamentals
- final action: `buy_add`, `hold`, `reduce`, `sell_close`, `hedge_increase`, or `hedge_maintain`
- position operation proposal
