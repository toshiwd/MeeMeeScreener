# 月足ランキングへEDINET特徴量を段階導入する

このExecPlanは生きたドキュメントであり、`Progress`、`Surprises & Discoveries`、`Decision Log`、`Outcomes & Retrospective` を作業中に更新し続ける。`.agent/PLANS.md` の運用要件に従う。

## Purpose / Big Picture

月足ハイブリッドランキングで、EDINETDBの財務データを「可視化と監査蓄積」から安全に導入する。初期はスコア反映フラグをOFFにして順位への影響を止め、20営業日実績で効果検証できる状態を作る。最終的にユーザーはランキング画面でEDINET診断項目を確認しながら、補正ON/OFFの比較結果をAPIで継続監視できる。

## Progress

- [x] (2026-03-03 09:10Z) EDINET特徴量抽出モジュール `app/backend/services/edinet_rank_features.py` を追加。
- [x] (2026-03-03 09:18Z) `rankings_cache` の月足ハイブリッドへEDINET補正フィールドとFlag制御（`MEEMEE_RANK_EDINET_BONUS_ENABLED`）を導入。
- [x] (2026-03-03 09:24Z) 監査テーブル `ranking_edinet_audit_daily` の作成処理と監査Upsert/20営業日実績更新ロジックを追加。
- [x] (2026-03-03 09:27Z) `/api/rankings/edinet/monitor` を追加。
- [x] (2026-03-03 09:34Z) RankingViewでEDINETバッジ表示とモニタ要約表示を追加。
- [x] (2026-03-03 09:43Z) `tests/test_rankings_edinet_bonus.py` を追加し、`pytest -q tests/test_rankings_edinet_bonus.py tests/test_rankings_monthly_ml.py tests/test_rankings_dual_side.py` で9件成功を確認。
- [x] (2026-03-03 09:45Z) `docs/EDINETDB_RUNBOOK.md` と `docs/README.md` にDBパス統一、Flag、新APIの運用追記を反映。

## Surprises & Discoveries

- Observation: 実データのDBパスが分散しており、`temp_stocks.duckdb` にランキングデータ、`data/stocks.duckdb` にEDINETスキーマが分離しているケースが確認できた。  
  Evidence: ローカル確認で `temp_stocks.duckdb` は `ml_pred_20d` 有り・`edinetdb_*` 無し、`data/stocks.duckdb` はその逆。

## Decision Log

- Decision: 初期はEDINET補正の計算値を返却・監査保存するが、`entryScore` への加算はFlag有効時のみ実施する。  
  Rationale: 既存順位への影響を隔離し、比較検証を可能にするため。  
  Date/Author: 2026-03-03 / Codex

- Decision: 監査テーブルへの保存は `tf=M` かつ `mode=hybrid` の返却時だけ行う。  
  Rationale: 本変更の対象範囲を明確化し、不要データの肥大化を避けるため。  
  Date/Author: 2026-03-03 / Codex

## Outcomes & Retrospective

本ExecPlanの実装範囲（backend feature抽出、月足反映、監査、monitor API、frontend可視化、tests、docs更新）は完了。  
初期方針どおりEDINET補正はFlag OFFが既定で、運用側が `MEEMEE_RANK_EDINET_BONUS_ENABLED=1` を設定するまで順位への影響は出ない。  
実運用前に `STOCKS_DB_PATH` と `EDINETDB_DB_PATH` の同一化を確認することが残作業。
