# PHASE2_IMPLEMENTATION_PLAN

## 目的
この文書は Phase 1 完了後の実装着手順を固定するための実行計画である。仕様の再説明ではなく、Phase 2 で何をどの順で作るか、何を作らないか、どこで止めるかを実装者向けに明記する。

Phase 2 の主目的は次の 3 点に限定する。
- candidate baseline を external_analysis 内で動かす
- `candidate_daily` と `regime_daily` を result DB に publish する
- nightly metrics を最小限 internal 保存する

Phase 2 では既存契約を破らない。特に次は固定前提である。
- `result DB only`
- `MeeMee read-only`
- `Parquet internal only`
- `publish_pointer` table 主体
- graceful degrade 優先

## Phase 1 Checkpoint
Phase 1 の完了点は次で確定する。
- result DB empty schema, `publish_pointer`, `publish_manifest`, atomic publish が動作する
- MeeMee 側 read-only bridge と graceful degrade が動作する
- export DB, JPX calendar, rolling labels, anchor windows が internal store に保存される
- 旧解析は `起動停止` と `更新停止` まで完了している

Phase 2 の着手条件は、上記を壊さずに `candidate_daily` と `regime_daily` だけを追加で有効化することである。

## Phase 1 Backlog
Phase 1 から持ち越す未解決事項は backlog として明示し、Phase 2 本体と混ぜない。
- `legacy_analysis_disabled=true` の follow-up job 側明示ログ
- JPX calendar の source DB distinct trading dates 依存
  - 現時点では暫定 canonical
  - 将来は独立 calendar source へ置換候補
- legacy disable 既定値の config/document 明文化強化
- Phase 1 通し確認は完了済みだが、CI 定着は未実施

これらは候補抽出 baseline より優先しない。Phase 2 では backlog を増やさず、必要最小限だけ別管理する。

## Phase 2 対象
- candidate retrieval/ranking baseline
- `candidate_daily` への publish
- `regime_daily` への publish
- nightly metrics の最小保存
- `publish_manifest` への candidate publish 情報反映
- MeeMee bridge 既存 read-only 経路での `candidate_daily` / `regime_daily` 読み出し確認

## Phase 2 非対象
- similarity embedding 本格実装
- `state_eval_daily` 実データ生成
- UI 完全切替
- 旧コード削除
- champion/challenger 自動昇格
- nightly retrain 自動化
- MeeMee 本体内での特徴量再計算や推論

## 実装方針
Phase 2 でも MeeMee 本体は重くしない。候補抽出は external_analysis だけで完結させる。

公開出力は最小に絞る。
- 公開 table: `candidate_daily`, `regime_daily`
- internal table / store: baseline 学習 artifact, candidate component score, nightly metrics

`state_eval_daily` は schema だけ既存維持とし、Phase 2 では空テーブルのまま許容する。

## 変更対象
Phase 2 で追加または変更してよい領域は次に限定する。
- `external_analysis/models/`
- `external_analysis/runtime/`
- `external_analysis/results/`
- `external_analysis/contracts/`
- `tests/`
- 必要最小限の `app/backend/services/analysis_bridge/` 読取確認

MeeMee 本体の API や UI は完全切替しない。既存 bridge が `candidate_daily` と `regime_daily` を読めることの確認だけ行う。

## DB / Store 方針
### result DB
既存 public schema を壊さない。
- `candidate_daily` を実データ publish 対象にする
- `regime_daily` を実データ publish 対象にする
- `publish_pointer` / `publish_manifest` の契約は変更しない
- `state_eval_daily`, `similar_cases_daily`, `similar_case_paths` は空のまま許容する

### internal metrics store
nightly metrics は MeeMee 公開対象にしない。
- 候補: model registry 配下の internal table
- 候補: ops DB 配下の internal metrics table

Phase 2 の既定は internal metrics table を external_analysis 側に作ることである。MeeMee は読まない。

最低保存項目:
- `run_id`
- `as_of_date`
- `model_key`
- `label_policy_version`
- `feature_version`
- `universe_count`
- `candidate_count_long`
- `candidate_count_short`
- `recall_at_20`
- `recall_at_10`
- `monthly_top5_capture`
- `avg_ret_20_top20`
- `avg_mfe_20_top20`
- `avg_mae_20_top20`
- `max_drawdown_proxy`
- `turnover_proxy`
- `regime_breakdown_json`
- `created_at`

## Vertical Slice 分割
Phase 2 は次の 3 slice に分ける。

### Vertical Slice D
対象:
- candidate baseline の internal 実装
- retrieval と ranking の最小分離
- internal candidate component score 保存
- internal nightly metrics schema 追加

完了条件:
- export DB / label store から baseline が日次入力を読める
- long/short 両側の score が internal で生成される
- nightly metrics 1 件を internal store に保存できる
- result DB publish はまだ行わない

依存:
- Phase 1 完了

非対象:
- `candidate_daily` publish
- `regime_daily` publish
- MeeMee 表示確認

### Vertical Slice E
対象:
- `candidate_daily` publish
- `regime_daily` publish
- `publish_manifest` への row count / freshness 反映
- existing `publish_pointer` 切替経路の再利用

完了条件:
- latest successful publish に `candidate_daily` と `regime_daily` が載る
- MeeMee bridge の既存 read-only 経路で result DB 読取が崩れない
- `warning stale`, `hard stale`, `result DB missing` の degrade が維持される

依存:
- Slice D

非対象:
- `state_eval_daily` 実データ
- UI 完全切替

### Vertical Slice F
対象:
- nightly metrics の定期保存
- baseline publish の通し検証
- candidate publish と legacy disabled 状態の並走確認

完了条件:
- nightly metrics が日次 run ごとに 1 レコード以上保存される
- `candidate_daily` publish の通しテストが通る
- legacy disabled 状態でも `publish_pointer` 経路の候補表示が壊れない

依存:
- Slice E

非対象:
- retrain 自動化
- champion/challenger

## タスク分解
### Task P2-1: baseline 入力フレーム作成
内容:
- export DB と label store を join する candidate 入力 loader を追加
- long/short 共通 universe filter を作る
- regime 判定に必要な最小特徴を抽出する

完了条件:
- 任意 `as_of_date` に対して baseline 入力 frame が生成できる
- JPX 営業日と label horizon の整合が崩れない

依存:
- Slice B 完了

### Task P2-2: retrieval baseline
内容:
- rule-based prefilter を実装
- long/short 別に top-N universe を internal 選抜

完了条件:
- long/short の候補母集団が日次で再現可能
- 同一入力で deterministic な結果になる

依存:
- Task P2-1

### Task P2-3: ranking baseline
内容:
- retrieval 出力に対し score を計算
- `candidate_component_scores` を internal 保存

完了条件:
- long/short の順位付き候補が生成できる
- `candidate_daily` に必要な列が埋められる

依存:
- Task P2-2

### Task P2-4: regime baseline
内容:
- market breadth / volatility の最小 regime 評価を作る
- `regime_daily` 行を生成する

完了条件:
- `regime_tag`, `regime_score`, `breadth_score`, `volatility_state` が日次で生成される

依存:
- Task P2-1

### Task P2-5: candidate publish
内容:
- `candidate_daily` と `regime_daily` を staging -> validation -> publish する
- `publish_manifest.table_row_counts` を更新する

完了条件:
- `publish_pointer` は latest successful publish のみを指す
- MeeMee bridge 既存経路で publish 結果が読める

依存:
- Task P2-3
- Task P2-4

### Task P2-6: nightly metrics 保存
内容:
- baseline 実行後に nightly metrics を internal 保存
- 最小 nightly summary を 1 run 1 row で残す

完了条件:
- metrics 保存失敗が candidate publish を壊さない
- metrics と publish を同一 `run_id` または `publish_id` で追跡できる

依存:
- Task P2-3
- Task P2-4

## 受入条件
Phase 2 完了条件は次で固定する。
- `candidate_daily` に long Top20 / short Top20 が publish される
- `regime_daily` が同じ publish に載る
- `publish_pointer` と `publish_manifest` の契約は変更しない
- MeeMee は result DB だけを read-only で読む
- result DB missing / stale / pointer corruption の degrade は維持される
- nightly metrics が internal 保存される
- `state_eval_daily` は未実装でも空テーブルのまま壊れない

## テスト計画
最低限追加するテストは次とする。
- baseline 入力 frame が export/label から生成できる
- retrieval が deterministic に候補を返す
- ranking が long/short の top-N を返す
- `candidate_daily` publish 後に bridge が既存契約のまま読める
- `regime_daily` publish 後に `publish_manifest` row count が一致する
- nightly metrics 保存失敗時でも publish が成功する
- legacy disabled 状態で candidate publish と bridge が並走できる

## rollback 条件
次のいずれかで Phase 2 を rollback する。
- `publish_pointer` の切替が不安定になる
- MeeMee bridge が 500 を返す
- `candidate_daily` publish が stale / mismatch 判定を壊す
- nightly metrics 追加が candidate publish を巻き込んで失敗させる

rollback 方法:
- `candidate_daily` と `regime_daily` の publish を止める
- `publish_pointer` を最後の stable publish へ戻す
- internal baseline / metrics 実装は残してよいが MeeMee 公開を止める

## 実装順序
1. Slice D
2. Slice E
3. Slice F

順序拘束:
- Slice D 完了前に Slice E へ入らない
- Slice E 完了前に Slice F へ入らない
- similarity / state evaluation / UI 切替 / 旧コード削除へ進まない
