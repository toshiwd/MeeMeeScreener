# PHASE1_IMPLEMENTATION_PLAN

## 目的

この文書は、承認済みの 9 文書を前提に、Phase 1 実装へ着手するための実行計画を固定する。ここで扱うのは仕様の再説明ではなく、実装順序、作業分解、追加や変更の対象、依存関係、完了条件、受入チェックである。

Phase 1 の目的は、external_analysis の骨格、差分 export、rolling labels、anchor windows、result DB empty schema、`publish_pointer` と `publish_manifest` による atomic publish、MeeMee の read-only bridge、graceful degrade、旧解析系の起動停止と更新停止を実装可能な単位へ分解することである。

## Phase 1 対象

Phase 1 で必須とする対象は次のとおりである。

- external_analysis の新設ディレクトリ骨格
- export DB と差分 export
- JPX 営業日基準の rolling labels
- anchor windows
- result DB の empty schema 固定
- `publish_pointer`
- `publish_manifest`
- atomic publish
- MeeMee read-only bridge
- graceful degrade
- 旧解析系の起動停止
- 旧解析系の更新停止
- Phase 1 受入テスト

## Phase 1 非対象

Phase 1 では次を実装しない。

- candidate model の本格実装
- `candidate_daily` 実データ生成
- similarity embedding の本格実装
- 類似検索の近傍索引本格構築
- `state_eval_daily` の実データ生成
- MeeMee UI の完全切替
- 旧解析系の物理削除

これらは schema 固定や空テーブル存在までは許容するが、機能実装や公開採用は後段へ回す。

## 追加/変更するディレクトリ

Phase 1 で追加または変更するディレクトリは次のとおりである。

- `external_analysis/`
- `external_analysis/runtime/`
- `external_analysis/exporter/`
- `external_analysis/labels/`
- `external_analysis/results/`
- `external_analysis/contracts/`
- `external_analysis/ops/`
- `app/backend/services/analysis_bridge/`
- `app/backend/api/routers/` の bridge 公開が必要な最小箇所
- `tests/`

`external_analysis/` は新規で作る。MeeMee 本体側の変更は read-only bridge と degrade 表示に必要な最小範囲に限定する。feature/model/similarity の本格処理用ディレクトリは後段で追加してよいが、Phase 1 では重い実装を入れない。

## 追加/変更するファイル

Phase 1 で最低限必要なファイルは次のとおりである。実装時は命名を多少調整してよいが、責務は分ける。

- `external_analysis/__init__.py`
- `external_analysis/__main__.py`
- `external_analysis/runtime/orchestrator.py`
- `external_analysis/runtime/job_types.py`
- `external_analysis/runtime/scheduler.py`
- `external_analysis/exporter/diff_export.py`
- `external_analysis/exporter/source_reader.py`
- `external_analysis/exporter/jpx_calendar.py`
- `external_analysis/labels/rolling_labels.py`
- `external_analysis/labels/anchor_windows.py`
- `external_analysis/results/result_schema.py`
- `external_analysis/results/publish.py`
- `external_analysis/results/manifest.py`
- `external_analysis/ops/ops_schema.py`
- `external_analysis/contracts/schema_versions.py`
- `app/backend/services/analysis_bridge/reader.py`
- `app/backend/services/analysis_bridge/degrade.py`
- `app/backend/services/analysis_bridge/contracts.py`
- `tests/test_external_analysis_result_schema.py`
- `tests/test_external_analysis_publish_pointer.py`
- `tests/test_external_analysis_diff_export.py`
- `tests/test_external_analysis_rolling_labels.py`
- `tests/test_external_analysis_anchor_windows.py`
- `tests/test_analysis_bridge_read_only.py`
- `tests/test_analysis_bridge_graceful_degrade.py`

既存旧解析系の起動停止と更新停止には、現行ジョブやサービス定義の変更も必要になる。対象は既存の旧解析起動箇所と旧解析更新箇所に限定し、UI 完全切替は行わない。

## DB schema

Phase 1 で新設または固定する DB schema は次の 3 系統である。

### 1. export DB

最低テーブル:

- `bars_daily_export`
- `bars_monthly_export`
- `indicator_daily_export`
- `pattern_state_export`
- `ranking_snapshot_export`
- `trade_event_export`
- `position_snapshot_export`
- `meta_export_runs`

完了条件:

- source DB から差分 export できる
- `meta_export_runs` に source signature と diff reason が残る
- JPX カレンダー参照が export 側で使える

依存関係:

- source reader
- JPX calendar loader

### 2. label store

最低テーブル:

- `label_daily_h5`
- `label_daily_h10`
- `label_daily_h20`
- `label_daily_h40`
- `label_daily_h60`
- `label_aux_monthly`
- `anchor_window_master`
- `anchor_window_bars`
- `label_generation_runs`

完了条件:

- rolling labels が JPX 営業日基準で生成される
- anchor windows が `-20..+20` 営業日で保存される
- `label_generation_runs` に purge / embargo version が残る

依存関係:

- export DB
- JPX calendar

### 3. result DB

最低テーブル:

- `publish_pointer`
- `publish_runs`
- `publish_manifest`
- `candidate_daily`
- `candidate_component_scores`
- `state_eval_daily`
- `similar_cases_daily`
- `similar_case_paths`
- `regime_daily`

`publish_pointer` 最小列:

- `pointer_name`
- `publish_id`
- `as_of_date`
- `published_at`
- `schema_version`
- `contract_version`
- `freshness_state`

完了条件:

- empty schema を含めて全 result テーブルが作成される
- `publish_pointer` の 1 行を起点に latest successful publish が解決できる
- staging / failed publish は pointer 更新前のため不可視である

依存関係:

- result schema 定義
- publish 実装

## ジョブ分解

Phase 1 で実装するジョブは次の 5 つに限定する。

### Job 1: `export_sync`

責務:

- source DB から差分抽出
- export DB への正規化反映
- `meta_export_runs` 記録

完了条件:

- 同一 source 状態で再実行しても不必要な全量再投入をしない
- source 修正時だけ対象 code/date の再 export が起きる

依存関係:

- source reader
- export DB schema

### Job 2: `label_build`

責務:

- rolling labels の生成
- 補助評価ラベルの分離保存

完了条件:

- `ret_5/10/20/40/60`, `mfe_20`, `mae_20`, `days_to_mfe_20`, `days_to_stop_20`, `rank_ret_20`, `top_1pct_20`, `top_3pct_20`, `top_5pct_20` が生成される
- JPX 営業日基準と purge / embargo が manifest に残る

依存関係:

- export_sync
- JPX calendar

### Job 3: `anchor_window_build`

責務:

- anchor 検出
- `anchor_window_master` と `anchor_window_bars` 生成

完了条件:

- 初期標準 anchor 群が `-20..+20` 営業日窓で保存される
- overlap / collision / embargo group が付与される

依存関係:

- export_sync
- label_build に使う JPX calendar

### Job 4: `result_schema_init`

責務:

- result DB の公開 schema 初期化
- empty table 作成

完了条件:

- `publish_pointer` を含む result DB 全テーブルが存在する
- `candidate_daily` など未実装テーブルは空でも schema が固定される

依存関係:

- contract version 定義

### Job 5: `publish_result`

責務:

- staging publish 構築
- validation
- `publish_manifest` 書込
- `publish_pointer` atomic switch

完了条件:

- valid publish のみ pointer 更新される
- failed publish と staging publish は不可視のまま残る
- MeeMee bridge が pointer 1 行から publish を解決できる

依存関係:

- result_schema_init
- label_build または最低限の publish 対象 metadata

## read-only bridge

Phase 1 の MeeMee 側 bridge は薄く保つ。責務は次の 4 つだけである。

- `publish_pointer` の 1 行解決
- `publish_id` による result テーブル filter
- freshness 判定
- degrade 分岐

bridge は補完計算、代替推論、結果再生成を行わない。MeeMee が読んでよいのは `publish_pointer`, `publish_manifest`, `candidate_daily`, `state_eval_daily`, `similar_cases_daily`, `similar_case_paths`, `regime_daily` のみであり、Phase 1 では実質的に `publish_pointer` と `publish_manifest` と空 schema の存在確認が中心になる。

完了条件:

- `publish_pointer` 不在時に no latest successful publish へ落ちる
- schema mismatch, manifest mismatch, result DB missing, pointer corruption 時に graceful degrade へ落ちる
- MeeMee 本体の通常機能は継続する

依存関係:

- result DB schema
- publish_result

## graceful degrade

Phase 1 で最低限動かす degrade ケースは次のとおりである。

- no latest successful publish
- pointer corruption
- manifest mismatch
- schema mismatch
- result DB missing

warning stale と hard stale の完全運用は後続の実データ publish と組み合わせて強化してよいが、Phase 1 でも状態判定ルート自体は用意する。

完了条件:

- 各ケースで解析パネルのみ degrade し、本体通常機能は継続する
- CTA 抑制フラグが bridge から返せる

依存関係:

- read-only bridge
- publish metadata

## 旧解析系の停止

Phase 1 に含める停止対象は「起動停止」と「更新停止」までである。UI 完全切替と物理削除は後段へ回す。

### 起動停止

責務:

- 旧解析 worker / 旧解析 job の自動起動を止める

完了条件:

- 通常起動時に旧解析 worker が起動しない
- 新しい external_analysis の Phase 1 ジョブに影響しない

依存関係:

- external_analysis の最小 runtime 骨格

### 更新停止

責務:

- 旧 `ml_pred_20d`, `phase_pred_daily`, `sell_analysis_daily` 更新経路を止める

完了条件:

- 新規の旧予測更新が入らない
- source DB 更新と MeeMee 通常機能は継続する

依存関係:

- read-only bridge による degrade 経路

## 実装順序

実装は次の順序で進める。

1. `external_analysis` ディレクトリ骨格、contract version、ops schema を追加する
2. export DB schema と `export_sync` を実装する
3. JPX calendar loader と rolling labels を実装する
4. anchor windows を実装する
5. result DB empty schema と `publish_pointer` / `publish_manifest` を実装する
6. `publish_result` を実装し、staging / validation / atomic switch を成立させる
7. MeeMee read-only bridge と graceful degrade を実装する
8. 旧解析系の起動停止を行う
9. 旧解析系の更新停止を行う
10. 受入テストを揃える

この順序を守る理由は、MeeMee 本体を壊さずに publish/read-only 契約を先に成立させるためである。

## 受入テスト

Phase 1 の受入テストは最低限次を含む。

- result DB に `publish_pointer` を含む empty schema が作られること
- `publish_pointer` の 1 行で latest successful publish を解決できること
- failed publish と staging publish が MeeMee から不可視であること
- diff export が全量再実行ではなく差分反映になること
- rolling labels が JPX 営業日基準で生成されること
- anchor windows が `-20..+20` 営業日で生成されること
- MeeMee bridge が read-only であり、補完計算を行わないこと
- no latest successful publish, pointer corruption, manifest mismatch, schema mismatch, result DB missing で graceful degrade すること
- 旧解析系が起動しないこと
- 旧解析系の更新が停止していること

## Phase 1 受入チェックリスト

- `external_analysis` 骨格が追加されている
- export DB schema が作成されている
- `export_sync` が差分反映で動く
- rolling label テーブルが作成される
- anchor window テーブルが作成される
- result DB empty schema が作成される
- `publish_pointer` と `publish_manifest` が動作する
- `publish_pointer` は result DB 内の単一テーブルである
- MeeMee read-only bridge が pointer 1 行起点で読める
- bridge は補完計算をしない
- graceful degrade が最低 5 ケースで成立する
- 旧解析系の起動停止が完了している
- 旧解析系の更新停止が完了している
- candidate model の本格実装を入れていない
- similarity embedding の本格実装を入れていない
- `state_eval_daily` 実データ生成を入れていない

この文書と上位文書の競合時の優先順位は `REBUILD_MASTER_PLAN.md > ARCHITECTURE_EXTERNAL_ANALYSIS.md > DATA_EXPORT_SPEC.md > LABELING_STRATEGY.md > ROADMAP_PHASES.md > RESOURCE_POLICY.md > FEATURE_STRATEGY.md > CANDIDATE_ENGINE_SPEC.md > SIMILARITY_ENGINE_SPEC.md > PHASE1_IMPLEMENTATION_PLAN.md` とする。競合時は `result DB only`、`MeeMee read-only`、`Parquet internal only`、`publish_pointer table 主体`、`graceful degrade` を優先する。
