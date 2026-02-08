# TXT Update Runbook

## 目的
- TXT更新ジョブ失敗時に、一次切り分けを短時間で実施する。
- `queued/running/cancel_requested/canceled/success/failed` の状態に対して、確認手順を固定する。

## 対象エンドポイント
- 主要: `POST /api/jobs/txt-update`
- 互換(非推奨): `POST /api/system/update_data`
- 互換(非推奨): `POST /api/txt_update/run`

## 非推奨スケジュール
- 非推奨告知開始: 2026-02-08
- Sunsetヘッダー日時: 2026-06-30 00:00:00 UTC (`Tue, 30 Jun 2026 00:00:00 GMT`)
- 新規実装は即時 `POST /api/jobs/txt-update` のみ利用
- 既存クライアントは Sunset 日までに主要エンドポイントへ移行
- 強制停止フラグ: `MEEMEE_DISABLE_LEGACY_TXT_UPDATE_ENDPOINTS=1` で互換エンドポイントを `410 Gone` 化
- Sunset 以降は自動的に互換エンドポイントが `410 Gone` になる

## 状態確認
1. 現在ジョブ確認: `GET /api/jobs/current`
2. 個別ジョブ確認: `GET /api/jobs/{job_id}`
3. キャンセル実行: `POST /api/jobs/{job_id}/cancel`

## update_state.json の確認キー
- `last_pipeline_status`
- `last_pipeline_stage`
- `last_pipeline_stage_status`
- `last_pipeline_stage_at`
- `last_error`
- `last_error_message`
- `last_failed_stage`
- `last_failed_at`
- `last_canceled_stage`
- `last_canceled_at`
- `last_ingest_at`
- `last_phase_dt`
- `last_scoring_at`
- `last_scoring_rows`
- `last_cache_refresh_at`
- `last_txt_update_at`

## 一次切り分け手順
1. `status=conflict` なら既存ジョブの完了待ち、または `cancel_requested` へ移行できるかを確認。
2. `error=code_txt_missing` なら `PAN_CODE_TXT_PATH` の実ファイル存在を確認。
3. `error` が `vbs_not_found:*` なら `PAN_EXPORT_VBS_PATH` の実ファイル存在を確認。
4. `failed` の場合は `last_failed_stage` を確認し、以下の順で切り分ける。
- `export`: VBS実行環境・入力ファイルを確認。
- `ingest`: ingest処理ログと入力TXTの整合性を確認。
- `phase`: `feature_snapshot_daily` の最新 `dt` を確認。
- `scoring`: スコア計算依存のテーブル/データ有無を確認。
- `cache_refresh`: ランキングキャッシュ再構築の例外ログを確認。
5. `canceled` の場合は `last_canceled_stage` を見て再実行ポイントを判断。

## 再実行ポリシー
- `failed` 後の再実行は、原因解消後に `POST /api/jobs/txt-update` を再度呼ぶ。
- 互換エンドポイントは段階的に廃止予定のため、新規運用導線は主要エンドポイントへ統一する。
