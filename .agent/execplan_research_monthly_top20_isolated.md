# Isolated Monthly Top20 Research Pipeline

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

If `.agent/PLANS.md` exists in this repository, this ExecPlan must be maintained in accordance with it.

## Purpose / Big Picture

ユーザーは、MEEMEE本体に影響を与えずに、月末エントリー翌月決済ルールのロング/ショートTop20を研究系パイプラインで生成し、合格したrunだけ公開スナップショットへ昇格できるようになります。実装後は `python -m research ...` で ingest から publish まで実行でき、`published/latest/long_top20.csv` と `published/latest/short_top20.csv` をアプリ側の唯一の読取対象として確認できます。

## Progress

- [x] (2026-02-24 03:25Z) 既存構成調査を実施し、`research` 領域が未作成であることと `toredex` が `app/backend` 依存であることを確認。
- [x] (2026-02-24 03:31Z) `research` パッケージのCLI骨格・設定読み込み・ストレージ管理を追加。
- [x] (2026-02-24 03:34Z) ingest / feature / label を実装し、snapshot/cacheへの保存を確認。
- [x] (2026-02-24 03:37Z) train / evaluate / publish / loop を実装し、run manifestと公開CSV契約を実装。
- [x] (2026-02-24 03:38Z) `tmp/research_smoke` の合成データで ingest→train→evaluate→publish→loop を実行し、`latest` 出力を確認。
- [x] (2026-02-24 03:55Z) `build_features/build_labels/train/loop` に `workers/chunk-size` を追加し、銘柄チャンクのプロセス並列と単一ライター構成を反映。
- [x] (2026-02-24 03:56Z) `publish` に Pareto 昇格ゲート（`evaluation.json` 必須、非Paretoは既定拒否）を追加。
- [x] (2026-02-24 04:13Z) ワーカーが一時CSVを書き、メインプロセスで最終マージする方式へ変更（temp dirの自動掃除も追加）。
- [x] (2026-02-24 04:13Z) ingest manifestに入力ファイルSHA1を追加し、train manifestへ snapshot manifest 全文を埋め込み。
- [x] (2026-02-24 04:13Z) Windows向け `scripts/research_loop_publish.ps1` を追加し、loop→Pareto選別→publish導線を実装。
- [x] (2026-02-24 04:20Z) walkforward分割を固定窓（train_years*12 + valid + test）へ厳格化し、不足時エラー化。
- [x] (2026-02-24 04:20Z) publish既定を `test + inference` に拡張し、月末ごとの複数asof Top20を公開できるよう変更。
- [x] (2026-02-24 04:38Z) evaluateに月次集計CSV（`evaluation_monthly.csv`）を追加し、asof単位の Hit/Return/Risk/Coverage/Drawdown を保存。
- [x] (2026-02-24 04:39Z) evaluateのCoverage算出を補正し、`expected=0` 月で過大評価しない実装（0扱い・0..1 clip・nunique基準）へ修正。
- [x] (2026-02-24 04:45Z) label境界を市場月末カレンダー基準へ統一し、`train` のラベル生成判定でも「次の市場月末」を利用するよう修正。
- [x] (2026-02-24 04:46Z) publish処理をゲート先行に変更し、Pareto/evaluation未達runで `published_vNNN` を生成しないよう修正。
- [x] (2026-02-24 04:53Z) evaluateを `valid/test` 両相集計に拡張し、Pareto判定を `selection_phase=valid`（valid欠損時のみtest）基準へ変更。

## Surprises & Discoveries

- Observation: 現在の `toredex` CLI は `app.backend.services.*` を直接importしており、分離契約を満たしていない。
  Evidence: `toredex/__main__.py` の 8-10行で `from app.backend.services.toredex_* import ...`。

- Observation: 月末営業日が暦月末より前（例: 2024-03-29）の場合、`asof + MonthEnd(1)` は同月末を返し、翌月ラベルが空になった。
  Evidence: 初回 `train` で `labels_2024-03-29.csv` と `labels_2024-06-28.csv` が空になり、`insufficient labeled months` が発生。

- Observation: `train` で月末カレンダー全件を処理すると、ユニバース未定義月で `no universe codes` 例外になる。
  Evidence: `2023-12-29` で失敗したため、ユニバース存在月に限定する修正を実施。

- Observation: `publish` を無条件許可すると「合格runのみ昇格」の運用契約に反する余地がある。
  Evidence: 初版実装では `evaluation.json` 非存在でも publish 可能だったため、ゲート実装へ変更。

- Observation: 並列ワーカー結果をメモリ集約のみで扱うと、要件の「ワーカー一時ファイル→単一ライターマージ」を満たさない。
  Evidence: 初回並列化は `future.result()` で dict配列を直接集約していたため、chunk CSV方式へ変更。

- Observation: 可変分割（総件数比率で train/valid/test を決める実装）は、固定walkforward要件に一致しない。
  Evidence: `_split_months` が `total//4` 由来の件数を使っていたため、固定窓へ置換。

- Observation: `train` 側のラベル生成可否判定が「暦月末 <= max_daily」に依存しており、翌月末営業日が暦月末より前の月でラベル生成を取りこぼす。
  Evidence: `max_daily=2025-11-28` のスモークで、旧判定だと `2025-10-31` ラベルをスキップしうる条件だったため、次の市場月末（calendar_month_ends）判定へ変更。

- Observation: Pareto履歴を `test` 指標で直接更新すると、チャレンジャー選別がtestに最適化される（検証設計の要件違反リスク）。
  Evidence: 旧 `evaluate` は `phase=test` 固定で `history.csv` を更新し、`scripts/research_loop_publish.ps1` がその値でbest run選別していた。

## Decision Log

- Decision: 既存 `toredex` を拡張せず、トップレベルに独立 `research` パッケージを新設する。
  Rationale: アプリ本体と研究処理の完全分離契約を最短で満たし、既存運用への回帰リスクを最小化できるため。
  Date/Author: 2026-02-24 / Codex

- Decision: 研究内部成果物は `research_workspace/`、公開物は `published/` のみに配置する。
  Rationale: 「公開済みスナップショットだけをアプリが読む」という運用契約に沿うため。
  Date/Author: 2026-02-24 / Codex

- Decision: `train` の学習対象月は「月末カレンダー」ではなく「ユニバース定義が存在する月」とした。
  Rationale: 運用上の対象600銘柄はユニバースCSVで復元する契約であり、未定義月の計算は失敗とリーク温床になるため。
  Date/Author: 2026-02-24 / Codex

- Decision: ラベルの翌月末境界は `next_month_start + MonthEnd(0)` で求める実装にした。
  Rationale: 営業日月末が暦月末より前でも必ず「翌月」を対象にできるため。
  Date/Author: 2026-02-24 / Codex

- Decision: `publish` は既定で Pareto ゲート必須とし、`--allow-non-pareto` のときのみ解除可能にした。
  Rationale: 「合格した結果だけを定期的に公開へ反映」の契約をCLIレベルで強制するため。
  Date/Author: 2026-02-24 / Codex

- Decision: 特徴量/ラベル生成は銘柄チャンクを `ProcessPoolExecutor` で並列実行し、最終CSV書き込みはメインプロセスのみで行う。
  Rationale: 要件の「月×銘柄チャンク並列」「単一ライター原則」を満たしつつ、既存実装への差分を最小化するため。
  Date/Author: 2026-02-24 / Codex

- Decision: ラベル期間の終端は `calendar_month_ends` の「asofの次行」を正とし、暦月末計算に依存しない。
  Rationale: 市場営業日ベースの月末定義を一貫して使い、祝日・週末月末での取りこぼしを防ぐため。
  Date/Author: 2026-02-24 / Codex

- Decision: `publish` はゲート判定（evaluation存在/Pareto）完了後にのみ `published_vNNN` を作成する。
  Rationale: 「合格runだけ公開へ反映」の運用契約を、`latest` だけでなく versioned 出力生成時点でも担保するため。
  Date/Author: 2026-02-24 / Codex

- Decision: Pareto評価軸は `valid` 指標を一次指標とし、`valid` が存在しない場合のみ `test` をフォールバックで使用する。
  Rationale: 「test期間は触らない。モデル選択はvalidまで」の設計要件を満たしつつ、履歴互換を維持するため。
  Date/Author: 2026-02-24 / Codex

## Outcomes & Retrospective

`research` パッケージを新規実装し、要求された7コマンド（`ingest/build_features/build_labels/train/evaluate/publish/loop`）を `python -m research` で実行可能にした。研究内部は `research_workspace/`、公開物は `published/` のみという分離を満たし、`publish` で `published_vNNN` 生成後に `latest` を差し替えるフローを追加した。

固定パラメタ（`tp_long/tp_short/cost/stop_loss`）は `research/default_config.json` に実装し、`run manifest` に記録されることを確認した。Top20公開CSVは必須カラム（`asof_date, code, score, pred_return, pred_prob_tp, risk_dn, model_version, feature_version, label_version, run_id, created_at`）を満たす。

評価は期間全体の集計（`evaluation.json`）に加え、月次asof単位の集計（`evaluation_monthly.csv`）を run artifact として保存し、要件の「毎月集計 + 期間全体集計」を満たすようにした。

残課題として、実データでの学習器高度化（L2R拡張）と、run数増加時の並列設定チューニングは今後のrun改善で対応する。

## Context and Orientation

このリポジトリは `app/` 以下がMEEMEE本体で、既存CLI `toredex/__main__.py` は `app/backend/services` を直接呼び出します。今回追加する研究系は本体と分離するため、新規 `research/` パッケージを作り、内部データは `research_workspace/` に保存します。

このExecPlanで使う「run manifest」は、1回の学習・評価runを再現するための設定・期間・コミット・入力スナップショット情報を保存したJSONです。`run_id` は一意な識別子で、`research_workspace/runs/<run_id>/manifest.json` に格納します。

「公開スナップショット」はアプリに見せる最小成果物で、`published/latest/` に `long_top20.csv` と `short_top20.csv` を置きます。publish処理では `published/published_vNNN/` を作成してから `latest` をディレクトリ単位で差し替えます。

## Plan of Work

`research/config.py` で設定を定義し、デフォルト値に `tp_long=0.10`, `tp_short=0.10`, `cost.enabled=true`, `cost.rate_per_side=0.001`, `stop_loss.enabled=false` を持たせます。設定はすべてmanifestへ記録します。

`research/storage.py` でディレクトリ構造を統一管理し、snapshot/run/cache/publishedのパス解決とJSON保存を提供します。キャッシュキーは `data_snapshot_id x feature_version x label_version x params_hash` を採用します。

`research/ingest.py` で日足OHLCV・月末カレンダー・監視ユニバースを読み込み、正規化して snapshot ディレクトリへ保存し、snapshot manifestを生成します。

`research/features.py` で `build_features --asof` を実装し、t時点までのデータのみで特徴量を生成してキャッシュへ保存します。`research/labels.py` で `build_labels --asof` を実装し、t→翌月末のTP/手仕舞いルールに基づく `realized_return/tp_hit/mae/mfe` を保存します。

`research/train.py` で候補生成(600→候補上限)と順位付け（線形回帰＋TP確率推定）を実装し、`run_id` ごとにモデル・予測・manifestを保存します。`research/evaluate.py` で Hit@20/Return@20/Risk/Coverage と Pareto 判定を集計します。

`research/publish.py` で run成果から `published_vNNN` と `latest` を更新し、契約カラムを持つ `long_top20.csv`/`short_top20.csv` を出力します。`research/loop.py` で複数パラメタのチャレンジャーrunを順次実行します。

`research/__main__.py` で CLI サブコマンドを束ね、`python -m research <command>` で全操作可能にします。

## Concrete Steps

作業ディレクトリは `c:\work\meemee-screener` とする。

1. 新規 `research/` パッケージと設定・ストレージ・CLI骨格を作成する。
2. ingest / features / labels を実装する。
3. train / evaluate / publish / loop を実装する。
4. `python -m research --help` と各サブコマンド `--help` でCLI到達を確認する。

期待されるヘルプ出力例（抜粋）:

    usage: research [-h] {ingest,build_features,build_labels,train,evaluate,publish,loop} ...

## Validation and Acceptance

受け入れ条件は次の挙動で判定する。

`python -m research --help` で7コマンドが表示される。

`python -m research ingest ...` 実行後に `research_workspace/snapshots/<snapshot_id>/manifest.json` が生成され、OHLCV/ユニバース/月末カレンダーが同一snapshotに保存される。

`python -m research train --asof YYYY-MM-DD --run_id <id>` 実行後に `research_workspace/runs/<id>/manifest.json` があり、固定パラメタ（tp/cost/sl）が記録される。

`python -m research publish --run_id <id>` 実行後に `published/latest/long_top20.csv` と `published/latest/short_top20.csv` が存在し、必須カラムを含む。

## Idempotence and Recovery

同一snapshot・同一run_idで再実行した場合は既存成果物を上書きする。途中失敗時は `published/latest` を更新しない。publishの途中で失敗した場合は `published/published_vNNN` を削除せず、次回は新しいバージョン番号で再試行できる。

## Artifacts and Notes

調査エビデンス:

    research missing

    toredex/__main__.py
    8:from app.backend.services.toredex_replay import replay_decision
    9:from app.backend.services.toredex_runner import run_backtest, run_live
    10:from app.backend.services.toredex_self_improve import run_self_improve, run_self_improve_loop

実装後の最小動作エビデンス:

    python -m research --workspace-root tmp/research_workspace_smoke --published-root tmp/published_smoke ingest --daily-csv tmp/research_smoke/daily.csv --universe-dir tmp/research_smoke/universe --snapshot-id smoke01
    -> {"ok": true, "snapshot_id": "smoke01", ...}

    python -m research --workspace-root tmp/research_workspace_smoke --published-root tmp/published_smoke train --snapshot-id smoke01 --asof 2024-08-30 --run_id smoke_run1
    -> {"ok": true, "run_id": "smoke_run1", "top20_long_rows": 20, "top20_short_rows": 20}

    python -m research --workspace-root tmp/research_workspace_smoke --published-root tmp/published_smoke publish --run_id smoke_run1
    -> {"ok": true, "published_version": "published_v001", "long_rows": 20, "short_rows": 20}

    tmp/published_smoke/latest/long_top20.csv
    asof_date,code,score,pred_return,pred_prob_tp,risk_dn,model_version,feature_version,label_version,run_id,created_at

昇格ゲートの追加検証:

    python -m research --workspace-root tmp/research_gatecheck_ws --published-root tmp/research_gatecheck_pub publish --run_id dummy_run
    -> {"ok": false, "error": "publish gate failed: evaluation.json is required. run evaluate first."}

    python -m research --workspace-root tmp/research_gatecheck_ws --published-root tmp/research_gatecheck_pub publish --run_id dummy_run
    -> {"ok": false, "error": "publish gate failed: run is not Pareto-optimal"}

並列/manifest強化の検証:

    python -m research --workspace-root tmp/research_parallel_ws --published-root tmp/research_parallel_pub build_features --snapshot-id psmoke01 --asof 2024-08-30 --workers 2 --chunk-size 2
    -> {"ok": true, "workers_used": 2, ...}

    python -m research --workspace-root tmp/research_parallel_ws --published-root tmp/research_parallel_pub build_labels --snapshot-id psmoke01 --asof 2024-08-30 --workers 2 --chunk-size 2
    -> {"ok": true, "workers_used": 2, ...}

    tmp/research_parallel_ws/runs/psmoke_run1/manifest.json
    -> data_snapshot_manifest.inputs.hashes.daily_csv_sha1 / universe_csv_sha1 を記録

固定walkforward + 月次公開行の検証:

    python -m research --config tmp/research_fixedwf_smoke/config.json --workspace-root tmp/research_fixedwf_ws --published-root tmp/research_fixedwf_pub train --snapshot-id wfsmoke01 --asof 2024-09-30 --run_id wfsmoke_run1 --workers 2 --chunk-size 3
    -> {"train_months": 12, "valid_months": 2, "test_months": 2, ...}

    python -m research --workspace-root tmp/research_fixedwf_ws --published-root tmp/research_fixedwf_pub publish --run_id wfsmoke_run1
    -> {"ok": true, "long_rows": 42, "short_rows": 42}

月次評価CSVの検証:

    python -m research --workspace-root .agent/tmp_research_smoke/workspace --published-root .agent/tmp_research_smoke/published --config .agent/tmp_research_smoke/config_smoke.json evaluate --run_id smoke_run_eval_monthly
    -> {"ok": true, "run_id": "smoke_run_eval_monthly", ...}

    .agent/tmp_research_smoke/workspace/runs/smoke_run_eval_monthly/evaluation_monthly.csv
    run_id,snapshot_id,created_at,side,asof_date,hit_at20,return_at20,mae_mean,mae_p90,coverage,expected_symbols,labeled_symbols,predicted_symbols,drawdown

市場月末境界の検証（`max_daily` が翌月の暦月末より前）:

    python -m research --workspace-root .agent/tmp_research_boundary/ws --published-root .agent/tmp_research_boundary/pub --config .agent/tmp_research_boundary/cfg.json train --snapshot-id s1 --asof 2025-11-28 --run_id run_boundary --workers 2 --chunk-size 4
    -> {"ok": true, "train_months": 12, "valid_months": 1, "test_months": 1, ...}

    .agent/tmp_research_boundary/ws/cache/.../labels_2025-10-31.csv
    -> 24 rows（long/short各12銘柄）が生成されることを確認

publishゲート先行の検証:

    python -m research ... train --run_id run_no_eval
    python -m research ... publish --run_id run_no_eval
    -> publish exit=1（evaluation未実施で失敗）
    -> published_v件数 before=0 / after=0（失敗時にversionディレクトリ未生成）

valid主体Paretoの検証:

    python -m research --workspace-root .agent/tmp_research_evalphase/ws --published-root .agent/tmp_research_evalphase/pub --config .agent/tmp_research_evalphase/cfg.json evaluate --run_id run_evalphase
    -> {"ok": true, "selection_phase": "valid", ...}

    .agent/tmp_research_evalphase/ws/runs/run_evalphase/evaluation.json
    -> metrics_by_phase.valid / metrics_by_phase.test 両方を保存
    -> metrics は selection_phase(valid) の値を反映

    .agent/tmp_research_evalphase/ws/evaluations/history.csv
    -> evaluation_phase=valid 列を保持してPareto集計

## Interfaces and Dependencies

追加CLIは次のインターフェースを提供する。

    python -m research ingest --daily-csv <path> --universe-dir <dir> [--calendar-csv <path>] [--snapshot-id <id>]
    python -m research build_features --asof YYYY-MM-DD [--snapshot-id <id>] [--workers N] [--chunk-size M]
    python -m research build_labels --asof YYYY-MM-DD [--snapshot-id <id>] [--workers N] [--chunk-size M]
    python -m research train --asof YYYY-MM-DD --run_id <id> [--snapshot-id <id>] [--workers N] [--chunk-size M]
    python -m research evaluate --run_id <id>
    python -m research publish --run_id <id> [--allow-non-pareto]
    python -m research loop --asof YYYY-MM-DD [--cycles N] [--workers N] [--chunk-size M]

主要依存は標準ライブラリ + `pandas` + `numpy` + `pyarrow`（既存 backend requirements に含まれる）を使う。`app/` 以下のモジュールはimportしない。

Updated at 2026-02-24: 初版作成後、実装進捗・不具合修正（翌月境界/ユニバース月制約）に加え、Pareto昇格ゲート、ワーカー一時CSVマージ方式、manifestハッシュ拡張、Windows運用スクリプトを反映した。
