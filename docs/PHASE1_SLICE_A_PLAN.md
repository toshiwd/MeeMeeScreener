# PHASE1_SLICE_A_PLAN

## 目的

この文書は `docs/PHASE1_IMPLEMENTATION_PLAN.md` を親として、Vertical Slice A を実装単位として固定する。Slice A の目的は、result DB の公開骨格、`publish_pointer`、`publish_manifest`、empty schema、MeeMee の read-only bridge、graceful degrade を先に成立させることである。

Slice A は Phase 1 の最優先 slice である。Slice A 完了前に Slice B へ進んではならない。

## 対象

Slice A の対象は次のとおりである。

- result DB schema 初期化
- `publish_pointer` テーブル
- `publish_manifest` テーブル
- result DB empty schema
- publish staging / validation / atomic pointer switch の最小実装
- MeeMee read-only bridge
- graceful degrade

## 非対象

Slice A では次を実装しない。

- diff export
- JPX calendar
- rolling labels
- anchor windows
- candidate model 本格実装
- similarity embedding 本格実装
- `state_eval_daily` 実データ生成
- UI 完全切替
- 旧解析系停止
- 旧コード物理削除

## 変更対象ファイル

最低限、次のファイルを追加または変更する。

- `external_analysis/results/result_schema.py`
- `external_analysis/results/publish.py`
- `external_analysis/results/manifest.py`
- `external_analysis/contracts/schema_versions.py`
- `external_analysis/ops/ops_schema.py`
- `app/backend/services/analysis_bridge/reader.py`
- `app/backend/services/analysis_bridge/degrade.py`
- `app/backend/services/analysis_bridge/contracts.py`
- `tests/test_external_analysis_result_schema.py`
- `tests/test_external_analysis_publish_pointer.py`
- `tests/test_analysis_bridge_read_only.py`
- `tests/test_analysis_bridge_graceful_degrade.py`

必要に応じて `external_analysis/__main__.py` に最小の schema 初期化または publish コマンドを追加してよい。

## DB schema

Slice A で固定する result DB テーブルは次のとおりである。

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

`publish_manifest` は最低でも `publish_id`, `as_of_date`, `schema_version`, `contract_version`, `status`, `published_at`, `table_row_counts`, `degrade_ready` を持つ。

`candidate_daily`, `state_eval_daily`, `similar_cases_daily`, `similar_case_paths`, `regime_daily` は empty table 許容とし、schema のみ先に作る。

## 実装タスク

### Task A1: result DB schema 初期化

作業:

- result DB 初期化関数を作成する
- 上記 9 テーブルを empty schema で作成する
- schema version と contract version を埋め込む

完了条件:

- 新規 DB に対して schema 初期化を 2 回実行しても壊れない
- `publish_pointer` を含む公開テーブルがすべて存在する

依存関係:

- なし

### Task A2: minimal publish 実装

作業:

- staging publish を受ける最小 API/CLI を作る
- validation 後に `publish_manifest` を書く
- 最後に `publish_pointer` を atomic に更新する

完了条件:

- valid publish のみ `publish_pointer` に反映される
- failed publish と staging publish は `publish_pointer` 更新前のため不可視である

依存関係:

- Task A1

### Task A3: read-only bridge 実装

作業:

- MeeMee 側 bridge で `publish_pointer` 1 行を読む
- `publish_id` フィルタで公開テーブルを参照する
- schema version を検査する

完了条件:

- bridge が `publish_pointer`, `publish_manifest`, 公開テーブルだけを読む
- `candidate_component_scores`, `publish_runs`, internal store を読まない

依存関係:

- Task A1
- Task A2

### Task A4: graceful degrade 実装

作業:

- no latest successful publish
- pointer corruption
- manifest mismatch
- schema mismatch
- result DB missing

の 5 ケースを bridge で分岐する。

完了条件:

- 各ケースで解析パネルだけが degrade する
- MeeMee 本体の通常機能は継続する
- CTA 抑制フラグを返せる

依存関係:

- Task A3

## 受入条件

- result DB empty schema が作成される
- `publish_pointer` が result DB 内の単一テーブルとして機能する
- `publish_manifest` が publish ごとに保存される
- failed/staging publish は MeeMee から見えない
- bridge が read-only であり補完計算をしない
- graceful degrade 5 ケースが成立する

## 手動確認手順

作業ディレクトリは `C:\work\meemee-screener` とする。

1. result DB 初期化コマンドを実行する。
2. `publish_pointer` と公開テーブルが存在することを確認する。
3. ダミー publish を 1 件作成し、`publish_manifest` が書かれ、`publish_pointer` が 1 行で更新されることを確認する。
4. staging publish または validation 失敗 publish を作成し、`publish_pointer` が更新されないことを確認する。
5. MeeMee 側 bridge 呼出しで latest successful publish を取得できることを確認する。
6. pointer 欠損、manifest 不整合、schema mismatch、result DB 不在を順に作り、解析パネルのみ degrade することを確認する。

## Slice B への進行条件

Slice B へ進んでよいのは、Slice A の受入条件と手動確認手順がすべて満たされた後だけである。
