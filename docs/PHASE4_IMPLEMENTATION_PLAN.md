# PHASE4_IMPLEMENTATION_PLAN

## 目的
Phase 3 完了時点で、MeeMee は `publish_pointer` 起点の read-only bridge で `candidate_daily`, `regime_daily`, `similar_cases_daily`, `similar_case_paths` を安定表示でき、similarity は deterministic baseline を champion として nightly 運用できている。Phase 4 はこの公開契約を一切壊さず、future-path aligned embedding を challenger として shadow 実行し、deterministic similarity と並走比較できる internal 基盤を整える。

本フェーズでも前提は固定する。
- `result DB only`
- `MeeMee read-only`
- `Parquet internal only`
- `publish_pointer` table 主体
- graceful degrade 優先
- deterministic similarity は champion として残す
- embedding は challenger として shadow 実行する

## Phase 3 Checkpoint
Phase 3 までに以下が成立している。
- `case_library`, `case_window_bars`, `case_embedding_store`, `case_generation_runs`, `similarity_quality_metrics` が internal store として稼働している
- deterministic similarity baseline が `similar_cases_daily` / `similar_case_paths` を publish できる
- similarity nightly pipeline が ops DB へ `external_job_runs` / `external_job_quarantine` を記録する
- similarity metrics 失敗時でも public publish は壊れない
- MeeMee 側は read-only bridge 経由で similarity public rows を読むだけで、embedding vector や internal metrics は見ない

Phase 4 はこの上に challenger を重ねる。champion の deterministic publish 経路は守り、challenger は shadow artifact と比較 metrics のみを追加する。

## Phase 3 Backlog
Phase 4 開始前の未解決事項は backlog として保持し、今回の対象と混ぜない。
- source DB distinct trading dates に依存する暫定 JPX calendar
- Phase 1 legacy disable の follow-up log/document 整備
- similarity nightly scheduler の OS 常駐化
- deterministic similarity の評価軸が `returned_case_count`, `avg_similarity_score` 中心で薄い
- embedding 学習用の purged walk-forward fold artifact がまだ固定されていない
- Phase 3 の similarity nightly は deterministic champion のみで、challenger 比較は未実装

## Phase 4 対象
- future-path aligned embedding の challenger 実装
- deterministic similarity を champion とした並走比較
- `case_embedding_store` の運用強化
- `similarity_quality_metrics` の比較指標拡張
- nightly challenger run

## Phase 4 非対象
- `state_eval_daily` 実データ生成
- UI 完全切替
- 旧コード削除
- ANN index 最適化
- champion の publish 契約変更
- MeeMee 側 API / bridge schema 変更

## 責務境界
Phase 4 でも MeeMee 本体は何も再計算しない。future-path aligned embedding の学習、shadow 推論、比較指標の算出、fold 管理、quarantine はすべて external_analysis 側で閉じる。

public 側は維持する。
- `similar_cases_daily`
- `similar_case_paths`
- `publish_manifest`
- `publish_pointer`

internal 側だけを強化する。
- `case_embedding_store`
- challenger artifact store
- challenger evaluation store
- similarity nightly run log
- comparison metrics store

## 実装方針
Phase 4 は 3 slice に分ける。順番は固定する。
1. Slice J: challenger embedding artifact と shadow scoring
2. Slice K: champion/challenger comparison metrics
3. Slice L: nightly challenger run と rollback gate

Slice J 完了前に Slice K へ入らない。Slice K 完了前に Slice L へ入らない。

## Vertical Slice J

### 対象
- future-path aligned embedding の最小 challenger 実装
- `case_embedding_store` の version 運用強化
- challenger vector 生成
- shadow top-k 推論
- public publish と切り離された challenger output 保存

### 変更対象
- `external_analysis/similarity/`
- `external_analysis/runtime/`
- `external_analysis/contracts/`
- `tests/`

### 追加/変更方針
- `case_embedding_store` を multi-version 運用にする
  - deterministic champion: `deterministic_similarity_v1`
  - challenger: `future_path_challenger_v1`
- challenger 学習入力は `case_window_bars` と label/anchor outcome を使う
- challenger の出力先は internal only
  - 例: `similarity_shadow_cases`, `similarity_shadow_paths`, `embedding_training_runs`
- challenger は `publish_pointer` を更新しない
- challenger は `similar_cases_daily` / `similar_case_paths` を上書きしない

### 完了条件
- same `as_of_date` に対して challenger embedding を deterministic に生成できる
- challenger top-k を internal store に保存できる
- champion の public publish が変化しない
- bridge / public API が無変更で通る

### 依存関係
- Phase 3 完了

## Vertical Slice K

### 対象
- champion/challenger 比較指標の追加
- `similarity_quality_metrics` 拡張
- success/failure separation に基づく比較集計
- nightly 比較レポートの internal 保存

### 変更対象
- `external_analysis/similarity/store.py`
- `external_analysis/similarity/`
- `external_analysis/runtime/`
- `tests/`

### 追加/変更方針
- `similarity_quality_metrics` に比較列を追加する
  - `engine_role` (`champion` / `challenger`)
  - `engine_version`
  - `query_source_breakdown_json`
  - `success_hit_rate_at_k`
  - `failure_hit_rate_at_k`
  - `big_drop_hit_rate_at_k`
  - `same_outcome_precision_at_k`
  - `future_path_distance_mean`
  - `champion_delta_json`
- challenger metrics は public publish と無関係に保存する
- comparison は同一 `publish_id` / `as_of_date` / `top_k` 単位で行う
- deterministic champion を比較基準に固定する

### 完了条件
- champion/challenger の metrics が同一 run 軸で比較できる
- same publish に対して比較結果を再実行しても idempotent である
- metrics 失敗が public publish を壊さない

### 依存関係
- Slice J

## Vertical Slice L

### 対象
- nightly challenger run
- run 記録
- retry / quarantine
- rollback gate

### 変更対象
- `external_analysis/runtime/`
- `external_analysis/ops/`
- `external_analysis/__main__.py`
- `tests/`

### 追加/変更方針
- nightly similarity pipeline を二段化する
  - champion deterministic publish
  - challenger shadow run
- challenger run 用 `job_type` を分離する
  - 例: `nightly_similarity_challenger_pipeline`
- retry / quarantine は internal only
- rollback gate は `publish_pointer` を触らない形で実装する
  - challenger quality 低下時は quarantine のみ
  - champion publish は継続
- nightly 実行は 1 command でよいが、内部で champion と challenger を別 run として記録する

### 完了条件
- nightly run で champion public publish + challenger shadow run が連続実行できる
- challenger failure でも `similar_cases_daily` / `similar_case_paths` の champion publish は維持される
- ops DB に run / retry / quarantine が残る

### 依存関係
- Slice K

## タスク分解

### Task P4-1: challenger embedding spec を code に固定
内容:
- future-path aligned teacher を最小セットに固定する
- `ret_5`, `ret_10`, `ret_20`, `mfe_20`, `mae_20`, `future_path_signature` を challenger teacher に使う
- deterministic champion と同じ input 窓から challenger 特徴を作る

完了条件:
- challenger version が string 定数で固定される
- same input で再現可能な embedding artifact が生成される

依存:
- Slice J

### Task P4-2: multi-version case_embedding_store
内容:
- `case_embedding_store` の multi-version 運用を整える
- champion / challenger の vector を同居保存する
- 同一 `case_id + embedding_version` の idempotent upsert を保証する

完了条件:
- deterministic champion と challenger が同一 case に共存する
- old champion rows を壊さない

依存:
- Task P4-1

### Task P4-3: challenger shadow top-k
内容:
- challenger vector を用いた top-k 検索を internal にだけ保存する
- public table には書かない
- `top_k` 上限は champion と同じ値を使う

完了条件:
- same query に対して challenger top-k が internal に残る
- MeeMee 側 payload は不変

依存:
- Task P4-2

### Task P4-4: comparison metrics
内容:
- champion/challenger の top-k を比較する
- success/failure/big-drop ごとの hit rate を保存する
- future path distance 系の最小比較指標を保存する

完了条件:
- same publish 単位で champion/challenger comparison row が得られる
- metrics は idempotent 保存される

依存:
- Task P4-3

### Task P4-4a: promotion gate 固定
内容:
- challenger の昇格判定を文書と code の両方で固定する
- ただしこの Phase では自動昇格しない
- 判定結果は internal review と summary にだけ保存する

昇格候補条件:
- `overlap_at_k >= 0.40`
- `success_hit_rate_at_k >= champion_success_hit_rate_at_k`
- `big_drop_hit_rate_at_k <= champion_big_drop_hit_rate_at_k + 0.05`
- 上記を `3` run 連続で満たす

完了条件:
- promotion gate 判定関数が code にある
- same publish 単位で review row が internal 保存される
- review 結果が nightly summary に含まれる

依存:
- Task P4-4

### Task P4-5: nightly challenger pipeline
内容:
- champion publish 後に challenger shadow run を起動する
- run 記録、retry、quarantine を残す
- challenger failure は `publish_pointer` に影響しない

完了条件:
- nightly command 1 回で champion + challenger が回る
- challenger failure でも public API smoke が落ちない

依存:
- Task P4-4

## DB / Store 方針

### result DB
public 契約は変更しない。
- `similar_cases_daily`
- `similar_case_paths`
- `publish_manifest`
- `publish_pointer`

必要なら `publish_manifest.table_row_counts` のみ現状どおり更新する。challenger は result DB public table に新規列を増やさない。

### similarity internal store
追加候補:
- `embedding_training_runs`
- `similarity_shadow_cases`
- `similarity_shadow_paths`
- `similarity_comparison_runs`

既存拡張:
- `case_embedding_store`
  - `embedding_version` の運用強化
- `similarity_quality_metrics`
  - champion/challenger 比較列の追加

### ops DB
既存 `external_job_runs` / `external_job_quarantine` を流用する。新 scheduler 専用テーブルはこの Phase では追加しない。job_type の追加だけで足りるようにする。

## 受入条件
Phase 4 完了条件は次で固定する。
- deterministic similarity champion の public publish が維持される
- challenger embedding が shadow 実行される
- challenger comparison metrics が internal に保存される
- nightly challenger run が ops DB に記録される
- challenger 失敗時でも `publish_pointer` は champion の latest successful publish を指し続ける
- MeeMee bridge / public API 契約が無変更で通る
- graceful degrade が従来どおり機能する

## テスト方針
最小で次を通す。
- multi-version embedding store が champion/challenger を共存保存できる
- challenger top-k は public table へ書かれない
- comparison metrics が same publish で idempotent 保存される
- nightly challenger failure 時に `external_job_quarantine` が増え、public publish は維持される
- `/api/analysis-bridge/candidates`
- `/api/analysis-bridge/regime`
- `/api/analysis-bridge/similar-cases`
- `/api/analysis-bridge/similar-case-paths`

## rollback 条件
以下のいずれかで Slice を止めて rollback する。
- challenger run により champion public publish が壊れる
- `publish_pointer` が challenger 側で更新される
- similarity public API schema が変わる
- comparison metrics 保存失敗が champion publish を巻き込む
- nightly challenger run 失敗で bridge が degrade ではなく 500 になる

rollback 方針:
- challenger shadow write を止める
- champion deterministic publish のみへ戻す
- `publish_pointer` は stable champion publish を維持する
- internal challenger tables は残してよいが MeeMee からは不可視のままにする

## 実装順
1. Slice J
2. Slice K
3. Slice L

順序拘束:
- Slice J 完了前に Slice K へ入らない
- Slice K 完了前に Slice L へ入らない
- `state_eval_daily`, UI 完全切替, 旧コード削除, ANN index 最適化へは入らない
