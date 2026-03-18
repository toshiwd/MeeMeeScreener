# PHASE3_IMPLEMENTATION_PLAN

## 目的

この文書は Phase 2 完了後の Phase 3 実装順序を固定するための実行計画である。仕様の再説明ではなく、類似形検索をどの順で実装し、どこまでを Phase 3 の完了条件とするかを実装者向けに明記する。

Phase 3 の主目的は次の 4 点に限定する。
- `case_library` を internal 正本として成立させる
- success / failure separation を実データで保存する
- `similar_cases_daily` / `similar_case_paths` の運用を成立させる
- future-path aligned embedding へ移行するための学習・評価・保存経路を準備する

Phase 3 でも既存契約は壊さない。特に次は固定前提である。
- `result DB only`
- `MeeMee read-only`
- `Parquet internal only`
- `publish_pointer` table 主体
- graceful degrade 優先

## Phase 2 Checkpoint

Phase 2 の完了点は次で確定する。
- `candidate_daily` publish が動作する
- `regime_daily` publish が動作する
- `publish_pointer` 起点の read-only bridge が動作する
- `/api/analysis-bridge/candidates` と `/api/analysis-bridge/regime` の公開契約が固定されている
- `nightly_candidate_metrics` が internal only で定期保存できる
- metrics 失敗時も public publish を壊さない分離設計が入っている

Phase 3 の着手条件は、上記を壊さずに similarity 系の internal pipeline と public result を追加することである。

## Phase 2 Backlog

Phase 2 から持ち越す未解決事項は backlog として明示し、Phase 3 本体と混ぜない。
- `legacy_analysis_disabled=true` の follow-up job 側明示ログ
- JPX calendar の source DB distinct trading dates 依存
  - 現時点では暫定 canonical
  - 将来は独立 calendar source へ置換候補
- nightly scheduler の OS 常駐化やサービス化
- candidate baseline の高度化
- Phase 2 CI 定着

これらは Phase 3 の similarity 成立条件より優先しない。

## Phase 3 対象

- `case_library` の新設
- `case_window_bars` の保存
- `case_embedding_store` の新設
- success / failure separation の実データ保存
- `similar_cases_daily` / `similar_case_paths` の実データ publish
- similarity 系 run 記録、retry、quarantine の最小実装
- future-path aligned embedding の最小学習・評価・保存経路
- MeeMee bridge 既存 read-only 経路での `similar_cases_daily` / `similar_case_paths` 読み出し確認

## Phase 3 非対象

- `state_eval_daily` 実データ生成
- UI 完全切替
- 旧コード削除
- similarity embedding の高度化や ANN 最適化
- candidate baseline の高度化
- champion/challenger 自動昇格
- nightly retrain の本格自動化

## 実装方針

Phase 3 でも MeeMee 本体は重くしない。similarity の検索、埋め込み生成、case 抽出、case 分類、path 正規化はすべて external_analysis だけで完結させる。

公開出力は最小に絞る。
- 公開 table: `similar_cases_daily`, `similar_case_paths`
- internal table / store: `case_library`, `case_window_bars`, `case_embedding_store`, similarity run log, similarity metrics

`candidate_daily` / `regime_daily` の公開契約は維持し、similarity 追加のために既存 API payload を変更しない。

## 変更対象

Phase 3 で追加または変更してよい領域は次に限定する。
- `external_analysis/similarity/`
- `external_analysis/runtime/`
- `external_analysis/results/`
- `external_analysis/contracts/`
- `external_analysis/ops/`
- `tests/`
- 必要最小限の `app/backend/services/analysis_bridge/` 読取確認

MeeMee 本体の UI 完全切替はしない。既存 bridge と API 契約を壊さず、`similar_cases_daily` / `similar_case_paths` を同じ read-only 方針で扱えることの確認だけを行う。

## DB / Store 方針

### result DB

既存 public schema を壊さない。
- `similar_cases_daily` を実データ publish 対象にする
- `similar_case_paths` を実データ publish 対象にする
- `publish_pointer` / `publish_manifest` の契約は変更しない
- `candidate_daily`, `regime_daily` は現行 publish のまま維持する
- `state_eval_daily` は空のまま許容する

### internal similarity store

Phase 3 の既定は similarity 系 internal store を external_analysis 側に追加することである。MeeMee は読まない。

最低テーブル:
- `case_library`
- `case_window_bars`
- `case_embedding_store`
- `case_generation_runs`
- `similarity_eval_runs`

最低保存項目:
- `case_id`
- `case_type`
- `anchor_type`
- `code`
- `anchor_date`
- `asof_start_date`
- `asof_end_date`
- `outcome_class`
- `success_flag`
- `failure_reason`
- `future_path_signature`
- `embedding_version`
- `source_snapshot_id`

## Vertical Slice 分割

Phase 3 は次の 3 slice に分ける。

### Vertical Slice G

対象:
- `case_library` と `case_window_bars` の internal 実装
- success / failure separation の最小分類
- anchor window と daily window から case を抽出する run 実装

完了条件:
- success case と failed setup case が internal store に保存される
- `case_type`, `outcome_class`, `success_flag`, `failure_reason` が埋まる
- result DB publish はまだ行わない

依存:
- Phase 2 完了

非対象:
- embedding 学習
- `similar_cases_daily` publish
- MeeMee 表示確認

### Vertical Slice H

対象:
- future-path aligned embedding の最小実装
- `case_embedding_store` 保存
- top-k 類似事例生成
- `similar_cases_daily` / `similar_case_paths` publish

完了条件:
- current query に対して top-k 類似事例が返る
- success / failure を区別した public rows が publish される
- `publish_pointer` 切替と existing degrade が維持される

依存:
- Slice G

非対象:
- embedding 高速化
- UI 完全切替

### Vertical Slice I

対象:
- similarity nightly run の定期化
- similarity 評価蓄積
- candidate publish と similarity publish の並走確認

完了条件:
- similarity run が日次で再実行できる
- evaluation artifact が internal 保存される
- candidate / regime / similarity の同居で bridge 契約が壊れない

依存:
- Slice H

非対象:
- state evaluation
- old code removal

## タスク分解

### Task P3-1: case 抽出入力の整形

内容:
- `anchor_window_master`, `anchor_window_bars`, export DB 日次窓を結合する
- daily window と anchor window の query 単位を作る
- future path 署名を計算する

完了条件:
- 任意 `as_of_date` に対して case 候補 frame が生成できる
- JPX 営業日窓と anchor 窓の整合が崩れない

依存:
- Phase 1 Slice B 完了

### Task P3-2: success / failure separation

内容:
- `pre_big_up`, `pre_big_down`, `failed_setup` の最小分類を実装
- `case_library` に分類結果を保存する

完了条件:
- success と failure が別 case として保存される
- failure を捨てずに検索母集団へ残せる

依存:
- Task P3-1

### Task P3-3: embedding baseline

内容:
- future-path aligned embedding の最小学習または変換を実装
- `case_embedding_store` に version 付きで保存する

完了条件:
- `embedding_version` を持つ vector または等価表現が保存される
- same input で deterministic な top-k が返る

依存:
- Task P3-2

### Task P3-4: similarity publish

内容:
- top-k 類似事例を `similar_cases_daily` と `similar_case_paths` へ staging -> validation -> publish する
- `publish_manifest.table_row_counts` を更新する

完了条件:
- latest successful publish に similarity rows が載る
- MeeMee bridge 既存 read-only 経路で result DB 読取が崩れない

依存:
- Task P3-3

### Task P3-5: similarity nightly run

内容:
- similarity run の run 単位記録を ops DB に追加
- retry / quarantine を最小実装する
- metrics 失敗時に public publish を壊さない

完了条件:
- run ごとに internal 記録が残る
- similarity 内部失敗で `publish_pointer` を壊さない

依存:
- Task P3-4

## 受入条件

Phase 3 完了条件は次で固定する。
- `case_library` が success / failure を分離保持する
- `similar_cases_daily` と `similar_case_paths` が publish される
- `publish_pointer` と `publish_manifest` の契約は変更しない
- MeeMee は result DB だけを read-only で読む
- hard stale / schema mismatch / pointer corruption の degrade は維持される
- similarity 系 internal run 記録と quarantine が保存される
- `state_eval_daily` は未実装でも空テーブルのまま壊れない

## テスト計画

最低限追加するテストは次とする。
- `case_library` に success と failure の両方が保存される
- embedding version ごとに deterministic な top-k が返る
- `similar_cases_daily` / `similar_case_paths` が publish される
- bridge が internal embedding store を読まない
- similarity 失敗時も latest successful publish が維持される
- candidate / regime / similarity の同居で public API 契約が崩れない

## rollback 条件

Phase 3 実装中に次が発生した場合は rollback 条件とする。
- `similar_cases_daily` 追加で既存 bridge が 500 になる
- `publish_pointer` 切替が不安定になる
- similarity public rows が success / failure を区別できない
- similarity 内部失敗が candidate / regime publish を巻き込む

rollback 後は similarity publish を止め、`publish_pointer` を最後の stable publish へ戻す。candidate / regime 経路は維持する。
