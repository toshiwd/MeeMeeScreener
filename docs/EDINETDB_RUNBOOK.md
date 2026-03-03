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
