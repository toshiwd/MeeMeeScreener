# PHASE5_IMPLEMENTATION_PLAN

## 目的
Phase 5 の目的は、nightly を待たずに過去の `as_of_date` 群で champion / challenger 比較を internal only で一括実行し、candidate / similarity の quality metrics を短期間で蓄積して promotion gate 判定に必要な観測数を早く集めることにある。MeeMee 本体の public 契約は一切変更せず、`publish_pointer` が指す current publish を維持したまま replay を回す。

本フェーズでも前提は固定する。
- `result DB only`
- `MeeMee read-only`
- `Parquet internal only`
- `publish_pointer` table 主体
- graceful degrade 優先
- public API / public schema 不変
- challenger は shadow only
- champion 置換はしない

## Phase 4 Checkpoint
Phase 4 までに以下が成立している。
- candidate baseline の nightly publish と metrics 保存
- deterministic similarity champion の nightly publish と metrics 保存
- `future_path_challenger_v1` の shadow run
- `similarity_quality_metrics`, `similarity_promotion_reviews`, `similarity_nightly_summaries` の internal 保存
- `publish_pointer` 起点の read-only bridge と graceful degrade

Phase 5 はこの上に replay runner を追加し、過去日の観測をまとめて積み増す。public bridge / API の契約は触らない。

## Phase 5 対象
- historical replay runner
- backfill run 記録
- candidate / similarity metrics の as_of_date 単位蓄積
- replay summary
- promotion readiness の internal 保存
- replay の end-to-end スモークテスト

## Phase 5 非対象
- champion 置換
- future-path aligned embedding の高度化 beyond v1
- ANN index 最適化
- `state_eval_daily` 実データ生成
- UI 完全切替
- 旧コード削除
- public API 追加変更
- candidate baseline の大改造

## Slice 分割
Phase 5 は 3 slice に分ける。今回着手するのは Slice M のみ。

### Slice M
対象:
- replay runner CLI
- `replay_id` / `as_of_date` 単位の ops 記録
- idempotent 再実行
- replay summary の internal 保存
- 最小の end-to-end replay スモークテスト

完成条件:
- 複数 `as_of_date` を internal で順次 replay できる
- `publish_pointer` が replay 中も current publish を維持する
- same `replay_id / as_of_date` 再実行で重複行を残さない
- replay summary が internal 保存される
- public bridge / API が不変であることを smoke で証明できる

### Slice N
対象:
- rolling 20 / 40 / 60 run 集計の本格実装
- promotion readiness の高度化
- replay summary の週次レビュー向け強化

状態:
- 今回は未着手

### Slice O
対象:
- replay / nightly の統合運用
- readiness と promotion review の運用固定
- replay 失敗日の再開戦略の拡張

状態:
- 今回は未着手

## 実装方針
Slice M では replay を internal only で完結させる。各日について以下を順に実行する。
- `export-sync`
- `label-build`
- `candidate-baseline-run`
- `similarity champion`
- `similarity challenger shadow`

ただし replay では public pointer を動かさない。candidate / similarity champion は replay 専用 publish id を使い、public publish は抑止する。MeeMee は current publish だけを読み続ける。

CLI では次を制御可能にする。
- 日付範囲
- 銘柄 universe
- replay 日数上限
- 銘柄数上限

ops DB には次を保存する。
- replay run 単位の状態
- replay day 単位の状態
- 最終 summary

retry / quarantine は replay にも適用する。失敗日は記録して隔離し、resume 時には successful day を再計算しない。

## 受入条件
Slice M の受入条件は次で固定する。
- `historical-replay-run` CLI が動く
- replay 中に `publish_pointer` が変わらない
- `external_replay_runs`, `external_replay_days`, `external_replay_summaries` が保存される
- `nightly_candidate_metrics` と `similarity_quality_metrics` に replay publish id の行が蓄積される
- failed day は quarantine へ記録される
- `/api/analysis-bridge/candidates`
- `/api/analysis-bridge/regime`
- `/api/analysis-bridge/similar-cases`
  が replay 後も不変である

## rollback 条件
以下のいずれかで Slice M を止める。
- replay が `publish_pointer` を更新する
- replay 実行で public API が 500 を返す
- same `replay_id / as_of_date` 再実行で duplicate rows が増える
- replay failure が quarantine されずに消える

rollback 方針:
- replay CLI を止める
- replay 用 internal rows は残してよい
- `publish_pointer` は stable current publish を維持する
- MeeMee public 契約は触らない
