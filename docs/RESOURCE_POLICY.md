# RESOURCE_POLICY

## 目的

この文書は、external_analysis が MeeMee 本体の操作性、応答性、安定性を阻害しないための実行ポリシーを固定する。low priority 標準、MeeMee 前面時の制約、idle 開始条件、CPU/GPU 上限、checkpoint / resume / quarantine、監視項目を明文化する。

この文書は `result DB only`、`MeeMee read-only`、`Parquet internal only`、`publish_pointer table 主体`、`graceful degrade` を前提とする。MeeMee 本体は解析計算を一切行わず、resource 制御は external_analysis 側で完結する。

## 固定原則

external_analysis は標準で low priority とする。MeeMee 本体の前面利用中に、重い学習、特徴量全量再構築、埋め込み再生成、索引再構築を開始してはならない。MeeMee 本体は resource 制御の主体ではなく、external_analysis が自律的に負荷を下げる。

固定文言として、MeeMee 本体は解析計算を一切行わない。MeeMee 本体は Parquet を直接参照しない。MeeMee 本体は feature store、label store、export DB、ops DB、model registry を読まない。MeeMee 本体は旧解析 worker を再起動しない。bridge は補完計算や代替推論を行わない。

## low priority 標準

external_analysis の全 worker は既定で low priority で起動する。OS 優先度、I/O 優先度、GPU 実行キュー優先度が選べる場合は最も低い安全側の設定を使う。

low priority 標準の対象ジョブは `export_sync`, `feature_build`, `label_build`, `anchor_window_build`, `candidate_train`, `candidate_score`, `similarity_embed`, `similarity_index_build`, `nightly_eval`, `registry_promote` である。例外を設けない。

## MeeMee 前面時の制約

MeeMee が前面ウィンドウで利用されている間は、新規に重いジョブを開始してはならない。ここでいう重いジョブとは `feature_build`, `label_build`, `candidate_train`, `similarity_embed`, `similarity_index_build`, `nightly_eval` を指す。

MeeMee 前面時に許可されるのは次だけである。

- 既に完了済み publish の read-only 参照
- 軽量な `candidate_score` の更新
- heartbeat, freshness 判定, publish validation のような短時間メタ処理

MeeMee 前面時に既に重いジョブが動いている場合は、新規チャンク投入を止め、checkpoint 到達後に pause へ入る。強制 kill を標準動作にしない。

## idle 開始条件

重いジョブの開始条件は、MeeMee が前面でないこと、かつユーザー入力 idle が 5 分以上であることを既定とする。夜間バッチは idle 条件を満たさなくても開始してよいが、MeeMee が前面化した時点で pause 判定に入る。

idle 判定に使う最低シグナルは次のとおりである。

- MeeMee 前面ウィンドウの有無
- OS ユーザー入力 idle 秒数
- external_analysis 自身の実行中ジョブ数

## CPU/GPU 上限

既定の CPU 上限は論理 CPU 使用率 35% とする。短時間の validation や publish 切替を除き、継続して 35% を超えてはならない。緊急上限は 50% とし、これを超えた場合は pause 判定へ入る。

既定の GPU 上限は 40% とする。緊急上限は 60% とし、これを超えた場合は新規 GPU チャンク投入を止める。GPU 温度や VRAM 圧迫が監視可能な場合は、温度上限 80 度、VRAM 上限 85% を補助停止条件とする。

Phase 1 の CPU/GPU 実装が完全でなくてもよいが、少なくとも low priority、MeeMee 前面時の新規重ジョブ禁止、pause/resume の拡張点が設計上固定されていなければならない。

## checkpoint

長時間ジョブは checkpoint 必須とする。checkpoint 間隔は、時間基準では 5 分、作業単位基準では 1 チャンク完了のいずれか早い方とする。

checkpoint に保存する最低情報は次のとおりである。

- `job_id`
- `job_type`
- `publish_target_as_of`
- `input_snapshot_id`
- `processed_partitions`
- `remaining_partitions`
- `artifact_uri_partial`
- `feature_version`
- `label_version`
- `model_version`
- `random_state`
- `updated_at`

checkpoint は ops DB と artifact sidecar の両方へ残してよいが、再開時の正本は ops DB とする。

## resume

pause されたジョブは次回再開時に checkpoint から resume する。resume 後に全量最初からやり直すことを標準にしてはならない。再開時は `job_id` と `checkpoint version` を照合し、壊れた checkpoint からの再開は拒否して quarantine へ送る。

MeeMee 前面化により pause されたジョブは、MeeMee が非前面化し、かつ idle 条件を再度満たした時点で resume 可能とする。

## quarantine

同一条件、同一コード、同一パラメータで失敗したジョブは 2 回まで再試行してよい。3 回目も進展がない場合は quarantine へ送る。quarantine されたジョブは自動再投入しない。

quarantine に送る最低条件は次のとおりである。

- checkpoint 破損
- schema mismatch
- source snapshot 不整合
- resource 制約違反の継続
- 同一例外で 2 回失敗

quarantine されたジョブは MeeMee 本体へ影響させない。latest successful publish を維持し、graceful degrade だけで吸収する。

## 監視項目

external_analysis は少なくとも次を監視する。

- process CPU %
- process GPU %
- process memory / VRAM
- job queue length
- running job count
- paused job count
- quarantined job count
- checkpoint age
- latest successful publish age
- latest successful publish `as_of_date`
- `publish_pointer` 更新時刻
- validation failure count
- pointer corruption detection count

監視は ops DB と logs に保存し、MeeMee 本体へは `freshness_state`, `published_at`, `degrade_reason` の最小情報だけを渡す。詳細監視ログを MeeMee 本体へ混ぜない。

## 実装単位

Phase 1 で最低限実装するのは、low priority 起動、MeeMee 前面時の新規重ジョブ禁止、idle 5 分条件、checkpoint schema、resume 契約、quarantine 条件、監視項目 schema の固定である。

Phase 2 以降で CPU/GPU 上限 enforcement の精度、温度監視、夜間スケジュール最適化を強化してよいが、Phase 1 の契約を変えてはならない。

## テスト観点

最低限、次のテストを実装する。

- MeeMee 前面時に `candidate_train` などの重ジョブが新規開始されないこと
- idle 5 分未満では重ジョブが開始されないこと
- checkpoint から resume して全量再実行にならないこと
- 同一失敗 3 回目で quarantine へ送られること
- quarantine 中でも latest successful publish が維持されること
- `publish_pointer` 更新前の失敗 publish が MeeMee から不可視であること

## 受入条件

実装完了後、開発者は external_analysis 実行中に MeeMee を前面へ出し、重ジョブが新規開始されないこと、checkpoint をまたいで再開できること、失敗ジョブが quarantine されても MeeMee が stale または直前 publish 表示で動作継続することを確認できなければならない。

この文書と上位文書の競合時の優先順位は `REBUILD_MASTER_PLAN.md > ARCHITECTURE_EXTERNAL_ANALYSIS.md > DATA_EXPORT_SPEC.md > RESOURCE_POLICY.md` とする。競合時は `result DB only`、`MeeMee read-only`、`Parquet internal only`、`publish_pointer table 主体`、`graceful degrade` を優先する。
