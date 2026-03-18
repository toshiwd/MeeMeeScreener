# Reorg Milestone 1: DB Boundary

## Goal

MeeMee 本体 DB に残す正本テーブルと、外付け解析または削除対象へ回すテーブルを固定する。この文書は `C:\work\meemee-screener\.agent\MEEEMEE_REORG_MASTER_EXECPLAN.md` の Milestone 1 と 2 を支える下位仕様であり、ここで決めた boundary は後続の runtime 再編と repo 再配置の前提になる。

## Source of Truth

本体 DB の正本は `C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb` とする。ただし本体が将来参照してよいのは、本体閲覧と軽量 UI に必要なテーブルだけである。

外付け解析の正本は `external_analysis/` 配下の export DB, feature store, label store, result DB, ops DB, similarity DB とする。本体からこれらの internal store を読まない。

## Keep In Main DB

本体 DB に正本として残す対象は次で固定する。

- 価格と基本派生
  - `daily_bars`
  - `daily_ma`
  - `feature_snapshot_daily`
  - `monthly_bars`
  - `monthly_ma`
- 銘柄と取引
  - `stock_meta`
  - `tickers`
  - `trade_events`
  - `positions_live`
  - `position_rounds`
  - `initial_positions_seed`
- 本体補助
  - `daily_memos`
  - `daily_memo`
  - `favorites.sqlite`
  - `practice.sqlite`
  - `sys_jobs` の本体に必要な最小部分
- 開示・貸借・イベント
  - `industry_master`
  - `earnings_planned`
  - `ex_rights`
  - `events_meta`
  - `events_refresh_jobs`
  - `taisyaku_issue_master`
  - `taisyaku_balance_daily`
  - `taisyaku_fee_daily`
  - `taisyaku_restriction_notices`
  - `edinetdb_*`
  - `tdnet_*`
- 本体機能として残す場合のみ
  - `toredex_*`
  - `stock_scores`
  - `screener_snapshot_state`

`feature_snapshot_daily` は閲覧用途の軽量派生として許容する。列数を抑え、`daily_bars` の完全複製や ML 学習用の高次特徴量を持ち込まない。

## Demote To Compatibility Only

以下は本体正本から外す。段階的には empty schema または compatibility-only 扱いに落とし、本体主要導線は最終的に参照しない。

- `ml_feature_daily`
- `ml_label_20d`
- `ml_pred_20d`
- `label_20d`
- `phase_pred_daily`
- `sell_analysis_daily`
- `ml_model_registry`
- `ml_monthly_model_registry`
- `ml_monthly_label`
- `ml_monthly_pred`
- `ml_training_audit`
- `ml_live_guard_audit`
- `ranking_analysis_quality_daily`

これらは `external_analysis/` へ移すか、互換確認期間だけ empty schema を残す。新しい source of truth にはしない。

## Remove From Main DB

本体 DB 内で複製や一時退避を作る以下の方式は廃止する。

- `trade_events_bak`
- 本体 DB 内 `CREATE TABLE AS SELECT *` 型の丸ごとバックアップ
- 本体 DB 内に置く研究・walk-forward 中間成果物

一時退避は別 DB、別ファイル、または repo 外の退避先で行う。

## Known Re-Growth Paths

再膨張の主要経路は次で固定する。

- `app/backend/services/ml/ml_service.py`
  - `refresh_ml_feature_table()`
  - `predict_monthly_for_dt()` 内の空テーブル時リフレッシュ
- `app/backend/services/analysis/analysis_backfill_service.py`
  - 空の `ml_feature_daily` を埋める backfill 経路
- 旧 ranking / analysis service
  - `ml_pred_20d`, `sell_analysis_daily`, `phase_pred_daily` を参照して補助結果を組む導線
- `app/backend/core/csv_sync.py`
  - `trade_events_bak` の複製生成

この 4 経路は後続 milestone で止める対象である。

## Acceptance

この milestone の完了条件は次である。

- 本体正本テーブル一覧が決定している。
- compatibility-only テーブル一覧が決定している。
- main DB から除去すべき複製テーブルが決定している。
- `ml_feature_daily` の再生成経路が特定済みである。
- implementer が「どのテーブルを残すか」で追加判断をしなくてよい。

