# MeeMee Multi-Timeframe Study Worker v2

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

If `.agent/PLANS.md` exists in this repository, this ExecPlan must be maintained in accordance with it.

## Purpose / Big Picture

ユーザーは、既存の `research train/evaluate/publish` 系を壊さずに、`daily` `weekly` `monthly` を個別研究できる `study_*` パイプラインを長時間 worker として回せるようになります。実装後は `python -m research study_loop ... --resume` で中断再開可能な研究を実行でき、`research_workspace/studies/<study_id>/` に top hypothesis と adopted hypothesis を分離保存し、MeeMee 本体には後者だけを手動または別バッチで返せます。

## Progress

- [x] (2026-03-09 07:00Z) 既存 `research` 構成、設定、CLI、snapshot 保存形式、walkforward 既存実装を確認。
- [x] (2026-03-09 07:25Z) `StudyConfig`、`study` 既定値、`studies/` 保存先、study 専用 storage helper を追加。
- [x] (2026-03-09 07:35Z) `ingest --sector-csv` と snapshot 内 `industry_master.csv` 保存、および sector 未指定時 fallback を実装。
- [x] (2026-03-09 07:55Z) `study_build` を追加し、timeframe 別 event-row dataset、future outcome、上位足 context、cluster/pivot 系列を生成可能にした。
- [x] (2026-03-09 08:10Z) `study_search_space` / `study_scoring` / `study_search` を追加し、deterministic random search、walkforward OOS、retention/adoption gate、`trial_state.json` による resume を実装。
- [x] (2026-03-09 08:18Z) `study_report` / `study_loop` と `top_hypotheses` / `adopted_hypotheses` 出力を追加し、CLI から到達可能にした。
- [x] (2026-03-09 08:24Z) 研究系 fixture テストを追加し、軽量 smoke 実行で ingest -> build -> search -> report の一連動作を確認。
- [x] (2026-03-09 08:32Z) `study_scoring` の fold mask 再利用による pandas warning を除去し、resume 後の bool 列解釈と completed combo の再実行抑止を追加。

## Surprises & Discoveries

- Observation: 現行 `research` は月次 Top20 ranker に強く寄っており、研究用途の保存先や CLI namespace は未分離。
  Evidence: `research/__main__.py` は `ingest/build_features/build_labels/train/evaluate/publish/loop/loop_all` のみを持つ。

- Observation: `research/features.py` には sector 前提の列が一部あるが、`research/ingest.py` は `industry_master` を snapshot に保存していない。
  Evidence: `research/features.py` 627-642 行付近で `sector` 条件分岐がある一方、`research/ingest.py` の出力は `daily.csv/calendar_month_ends.csv/universe_monthly.csv/manifest.json` のみ。

- Observation: `ResearchPaths` は `runs` と `snapshots` までは持つが、長時間 study 用の独立ルートがまだない。
  Evidence: `research/storage.py` の `ResearchPaths.ensure_base_dirs()` は `studies_root` を作成していない。

- Observation: 週足・月足の resample ラベルをそのまま `event_date` に使うと、月次 universe との結合がずれて context と universe 適用日が噛み合わない。
  Evidence: 初期実装では `W-FRI` / 月末ラベル日を使った結果、実際の最終売買日と一致しないケースがあり、`trade_date:last` へ切り替える必要があった。

- Observation: Python の組み込み `hash()` を探索 seed に使うとプロセスごとに値が変わり、resume を含む deterministic random search の前提が壊れる。
  Evidence: `study_search_space.py` で SHA1 ベースの安定 seed offset へ置き換えた。

- Observation: fold 内 subset に対して元 frame の boolean mask を再利用すると pandas の再インデックス警告が出る。
  Evidence: `study_scoring.py` の normalize/OOS 抽出で warning を確認し、subset 側 `month_bucket` から mask を再構築して解消した。

- Observation: resume 実装で「既に完了した combo」と「残り試行数」の区別がないと、再開時に base/refine trial が増殖する。
  Evidence: 軽量 smoke で `first_trials=6` に対して resume 後 `second_trials=10` を確認し、combo 完了時の skip と残予算ベース生成へ修正後に `6 -> 6` で安定した。

## Decision Log

- Decision: `study_*` は既存 `train.py` に混ぜず、新規モジュールへ分離する。
  Rationale: 月次 ranker と研究 worker の責務を分け、既存 publish 契約を壊さないため。
  Date/Author: 2026-03-09 / Codex

- Decision: `StudyConfig` は `ResearchConfig` にネストするが、`params_hash` からは除外する。
  Rationale: study の探索設定変更で既存 feature/label cache を無駄に無効化しないため。
  Date/Author: 2026-03-09 / Codex

- Decision: MeeMee 本体へ返す候補は `adopted_hypotheses.json` のみに制限する。
  Rationale: 研究上位と採択可能候補を混同させないため。
  Date/Author: 2026-03-09 / Codex

## Outcomes & Retrospective

`study_*` 系は既存 monthly ranker と分離したまま追加できた。`python -m research --help` で新 CLI が見え、軽量 smoke では `ingest -> study_build -> study_search -> study_report` の一連が通り、`top_hypotheses` と `adopted_hypotheses` の分離保存も確認できた。resume も `first_trials=6` と `second_trials=6` で件数増殖なしを確認した。

残件は広範な実データ検証と pytest 実行であり、コード上の導線と再開機構は初期版として揃った。特に deterministic search の seed 安定化、resample 後の実売買日採用、resume 後の bool 正規化は、長時間 worker としての再現性確保に効いた。

## Context and Orientation

この repository では `research/` が MeeMee 本体とは分離した研究 CLI です。snapshot は `research_workspace/snapshots/<snapshot_id>/`、run は `research_workspace/runs/<run_id>/` に保存されます。今回の変更では第三の保存系として `research_workspace/studies/<study_id>/` を追加し、worker 的な研究処理をそこへ閉じ込めます。

この ExecPlan でいう `study` は「銘柄イベント行を timeframe ごとに作り、加点式の仮説を walkforward OOS で比較する研究 run」です。`top hypothesis` は研究スコア上位の候補、`adopted hypothesis` は adoption gate 通過済みで MeeMee 本体へ返してよい候補を指します。

## Plan of Work

まず `research/config.py` と `research/storage.py` を拡張し、study 設定と `studies/` ルートを追加する。次に `research/ingest.py` に `--sector-csv` を追加して `industry_master.csv` を snapshot に保存する。

その後、新規 `research/study_build.py` で event-row dataset、future outcome、上位足 context、cluster/pivot 用の基礎列を作る。`research/study_search_space.py` と `research/study_scoring.py` で family 定義、trial 生成、walkforward 評価、cluster prior、retention/adoption gate を実装する。

最後に `research/study_search.py`、`research/study_report.py`、`research/__main__.py` をつないで CLI を成立させ、`trial_state.json` による resume を実装する。テストでは ingest fallback、dataset build、search、resume、report 出力を確認する。

## Validation and Acceptance

`python -m research --help` で `study_build` `study_search` `study_report` `study_loop` が見えること。

`python -m research ingest --sector-csv ...` 実行後に snapshot 配下へ `industry_master.csv` が生成され、sector CSV 未指定時でも fallback 行が作られること。

`python -m research study_loop --snapshot-id <id> --timeframes daily,weekly,monthly --resume` 実行後に `research_workspace/studies/<study_id>/` 配下へ `trial_state.json`、`search_trace.csv`、`top_hypotheses.json`、`adopted_hypotheses.json` が生成されること。

`python -m research study_report --study-id <id>` 実行時に top と adopted が分離された summary が返ること。

## Idempotence and Recovery

`study_build` は同一 `study_id + timeframe` で再実行すると dataset を再生成して上書きする。`study_search --resume` は `trial_state.json` に存在する committed trial を飛ばし、未完了分だけ継続する。詳細 fold artifact は retained trial のみ保存し、gate 不通過 trial は `bad_hypotheses_summary.csv` の要約だけを残す。

## Artifacts and Notes

初期証拠:

    research/__main__.py
    -> ingest/build_features/build_labels/train/evaluate/publish/loop/loop_all のみ

    research/ingest.py
    -> snapshot 出力は daily.csv/calendar_month_ends.csv/universe_monthly.csv/manifest.json のみ

    research/storage.py
    -> ResearchPaths は studies_root を持たない
