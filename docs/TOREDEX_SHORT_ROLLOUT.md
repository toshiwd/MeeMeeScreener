# TOREDEX Short Rollout Runbook

Last updated: 2026-03-04

## 1) Fixed rollout configuration

Use this config for the first production short rollout:

```json
{
  "rankingMode": "hybrid",
  "sides": {
    "longEnabled": true,
    "shortEnabled": true
  },
  "thresholds": {
    "entryMinUpProb": 0.56,
    "entryMinEv": -0.01,
    "entryMaxRevRisk": 0.70,
    "maxNewEntriesPerDay": 2.0,
    "newEntryMaxRank": 10.0
  }
}
```

## 2) Season IDs (shadow -> production)

- Shadow season: `toredex_live_short_hybrid_shadow_20260304`
- Production season: `toredex_live_short_hybrid_prod_20260304`

Do not reuse existing live season IDs. Keep rollout history isolated.

## 3) Submit live job via existing API

Endpoint: `POST /api/jobs/toredex/live`

Example payload:

```json
{
  "season_id": "toredex_live_short_hybrid_shadow_20260304",
  "asOf": "2026-03-04",
  "dry_run": false,
  "operating_mode": "champion",
  "config_override": {
    "rankingMode": "hybrid",
    "sides": { "longEnabled": true, "shortEnabled": true },
    "thresholds": {
      "entryMinUpProb": 0.56,
      "entryMinEv": -0.01,
      "entryMaxRevRisk": 0.70,
      "maxNewEntriesPerDay": 2.0,
      "newEntryMaxRank": 10.0
    }
  }
}
```

## 4) Shadow promotion gate (10 trading days)

Promote shadow to production only when all conditions are met:

1. `short_entries >= 1` for the shadow window.
2. `risk_gate_pass = true` every day.
3. `max_drawdown_pct >= -8.0` (champion limit).

## 5) Rollback conditions

Immediately stop production short rollout if either condition is met:

1. Any day with `risk_gate_pass = false`.
2. Any day with `max_drawdown_pct < -8.0`.

Then rerun in shadow mode after threshold review.

## 6) Monitoring query (daily check)

```sql
WITH days AS (
  SELECT
    m.season_id,
    m."asOf",
    m.net_cum_return_pct,
    m.max_drawdown_pct,
    m.risk_gate_pass,
    m.short_units,
    (
      SELECT COUNT(*)
      FROM toredex_trades t
      WHERE t.season_id = m.season_id
        AND t."asOf" = m."asOf"
        AND UPPER(t.side) = 'SHORT'
        AND t.delta_units > 0
    ) AS short_entries
  FROM toredex_daily_metrics m
  WHERE m.season_id = ?
)
SELECT *
FROM days
ORDER BY "asOf";
```

## 7) Replay scan before rollout

Use scan script before enabling in production:

```bash
python scripts/toredex_short_rollout_scan.py \
  --start-date 2026-02-27 \
  --end-date 2026-03-04 \
  --include-daily
```

## 8) Daily automation (production)

Run live decision and monitor rollback gate in one command:

```bash
python scripts/toredex_short_rollout_daily.py \
  --season-id toredex_live_short_hybrid_prod_20260304 \
  --operating-mode champion \
  --config-override-json tmp/toredex_short_hybrid_shadow_config_20260304.json \
  --window-days 10 \
  --rollback-max-dd -8.0 \
  --fail-on-rollback
```

PowerShell wrapper:

```powershell
./scripts/toredex_short_rollout_daily.ps1 -FailOnRollback
```

Monitor only (no run-live):

```bash
python scripts/toredex_short_rollout_daily.py \
  --season-id toredex_live_short_hybrid_prod_20260304 \
  --monitor-only \
  --window-days 10
```

Append daily observation log (JSONL):

```bash
python scripts/toredex_short_rollout_daily.py \
  --season-id toredex_live_short_hybrid_shadow_20260304_v2 \
  --monitor-only \
  --window-days 10 \
  --append-log \
  --daily-log-path tmp/toredex_short_rollout_observations_shadow_v2.jsonl
```

Each JSONL row includes:

- `short_entries` (window total)
- `crash_boost_entries` (window total, `notes=CRASH_DIP_BOOST`)
- `latest.short_entries` (latest day)
- `latest.crash_boost_entries` (latest day)
