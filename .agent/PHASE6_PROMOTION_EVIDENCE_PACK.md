# Phase 6 Promotion Evidence Pack (Partial)

## ????
- Source DB: `C:\work\meemee-screener\tmp\phase6_eval\source_recent3m.duckdb`
- Export DB: `C:\work\meemee-screener\tmp\phase6_eval\export_recent3m.duckdb`
- Label DB: `C:\work\meemee-screener\tmp\phase6_eval\label_recent3m.duckdb`
- Result DB: `C:\work\meemee-screener\tmp\phase6_eval\result_phase6.duckdb`
- Similarity DB: `C:\work\meemee-screener\tmp\phase6_eval\similarity_phase6.duckdb`
- Ops DB: `C:\work\meemee-screener\tmp\phase6_eval\ops_phase6.duckdb`

## ????????
- publish_pointer: `[('latest_successful', 'pub_2026-02-26_20260313T052507Z_01', datetime.date(2026, 2, 26), datetime.datetime(2026, 3, 13, 5, 28, 31, 29756), 'phase1-v1', 'phase1-v1', 'fresh')]`
- candidate_daily rows: `40`
- regime_daily rows: `1`
- nightly_candidate_metrics: `[('pub_2026-02-26_20260313T052507Z_01', '2026-02-26', 'score_formula_v1', None, None, None, None)]`
- similarity_quality_metrics: `[('champion', 'pub_2026-02-26_20260313T052507Z_01', '2026-02-26', None, 0.136, 0.864, 0.04325925925925926, 0.9538467991111111)]`
- case_library_count: `22360`
- case_embedding_store: `[('champion', 22360)]`
- similarity_shadow_cases: `0`

## blocker ??
- `nightly_similarity_challenger_pipeline` ? real-data 3m chunk ?? `running` ??????????
- challenger shadow metrics ????
- review artifact ????
- quarantine ????

## promotion gate ????
- challenger ??????????????????????
- champion(candidate + deterministic similarity) ? publish ? isolated DB ????
