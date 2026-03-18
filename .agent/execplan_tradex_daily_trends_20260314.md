# ExecPlan: Tradex Daily Summary Enrichment and Trends

## Goal
- Persist richer daily research summaries with teacher/similarity context.
- Expose trend summaries so AI Research can show improving and weakening tags over time.

## Scope
- `external_analysis/models/state_eval_baseline.py`
- `app/backend/services/analysis_bridge/reader.py`
- `app/backend/api/routers/analysis_bridge.py`
- `app/frontend/src/routes/TradexTagValidationView.tsx`
- `app/frontend/src/styles.css`
- targeted tests only

## Steps
1. Extend daily summary payload with reason text derived from teacher/similarity.
2. Add backend trend summary endpoint based on recent tag rollups.
3. Surface trend summary in AI Research.
4. Run targeted pytest and frontend build.
