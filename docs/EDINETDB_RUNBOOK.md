# EDINET DB Runbook (v1.0)

## 概要

MeeMeeにEDINET DBの有報データを取り込むCLIです。  
コマンドは以下の2つです。

- `python -m app.backend.edinetdb.cli backfill_700`
- `python -m app.backend.edinetdb.cli daily_watch`

Windowsでは `edinetdb.cmd` でも同じように実行できます。

- `edinetdb backfill_700`
- `edinetdb daily_watch`

## 必須環境変数

- `EDINETDB_API_KEY`

未設定時はクラッシュせず `skip` で終了します。

## 任意環境変数

- `EDINETDB_DAILY_BUDGET`
- `EDINETDB_TEXT_YEARS_MAX` (default: `6`)
- `EDINETDB_RAW_DIR` (default: `data/edinetdb/raw`)
- `EDINETDB_DB_PATH` (default: `STOCKS_DB_PATH` -> MeeMee既定DB)
- `EDINETDB_ROTATION_BUCKETS` (default: `7`)
- `EDINETDB_RANKING_LIMIT` (default: `100`)
- `EDINETDB_TIMEOUT_SEC` (default: `20`)

`EDINETDB_DAILY_BUDGET` 未指定時はJST日付で自動決定されます。

- 2026-03-07 まで: `1000`
- 2026-03-08 以降: `100`

## 動作ルール

- 429は当日打ち切り（未処理は `retry_wait` / `pending` で翌日繰越）
- 5xx/通信エラーは指数バックオフで再試行
- 4xx（429除く）は自動リトライせず失敗として台帳記録
- rawレスポンスは gzip で保存

## 出力先

- 正規化データ: MeeMee DuckDB (`edinetdb_*` テーブル)
- raw: `data/edinetdb/raw/<endpoint>/<edinet_code>/<timestamp>.json.gz`

## 日次運用例（タスクスケジューラ）

1. `daily_watch` を1日1回実行。
2. 必要に応じて週末や夜間に `backfill_700` を実行。
3. ログ出力の `budget_remaining` と `pending_tasks` を監視。

## ランキング連携時のDBパス統一

月足ランキングでEDINET特徴量を使う場合、価格系テーブル（`daily_bars`, `ml_pred_20d`）とEDINET系テーブル（`edinetdb_*`）が同じDuckDBに入っている必要があります。  
運用では `STOCKS_DB_PATH` と `EDINETDB_DB_PATH` を同一ファイルへ設定してください。

例:

- `STOCKS_DB_PATH=C:\work\meemee-screener\data\stocks.duckdb`
- `EDINETDB_DB_PATH=C:\work\meemee-screener\data\stocks.duckdb`

## ランキング補正フラグ（初期OFF）

月足ハイブリッドランキングのEDINET補正は、次の環境変数で有効化します。

- `MEEMEE_RANK_EDINET_BONUS_ENABLED` (`0` or `1`, default: `0`)

`0` の場合は診断値と監査保存のみ、`1` の場合は `entryScore` へEDINET補正を加算します。

## 監視API（ランキング側）

EDINET補正の有効性はランキングAPIから確認できます。

- `GET /api/rankings/edinet/monitor?lookback_days=365&dir=up&risk_mode=balanced&which=latest`

レスポンスには `groups.positive/negative/zero` ごとの件数・20営業日平均リターン・勝率、`insufficient_samples` が含まれます。
