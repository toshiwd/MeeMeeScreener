# TOREDEX Self-Improvement Loop Completion (Net-PnL Objective)

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

If `.agent/PLANS.md` exists in this repository, this ExecPlan must be maintained in accordance with it.

## Purpose / Big Picture

この変更で、TOREDEX の最適化対象を「予測精度」から「資金1000万円でのネット利益（コスト控除後）」へ切り替える。ユーザーは Champion（本番）と Challenger（研究）を分離して運用し、研究側ではロールバック閾値を緩めつつ多段評価で高速探索し、昇格条件を満たす設定だけを本番へ昇格できる。動作確認は `python -m toredex run-backtest ...` と自己改善ジョブ実行で、出力 JSON と DB レコードに `gross/fees/slippage/borrow/net` とリスクゲート合否が残ることで観測できる。

## Progress

- [x] (2026-02-17 17:35Z) 既存コードの呼び出し経路と制約を調査し、ExecPlanを作成した。
- [x] (2026-02-17 18:10Z) Phase1: コストモデル実装（fees/slippage/borrow）とネット評価への統一を実装した。
- [x] (2026-02-17 18:18Z) Phase2: リスクゲート（DD/月次/回転/露出）の合否判定を実装した。
- [x] (2026-02-17 18:24Z) Phase3: Long-Short + 分散 + ネット露出制御（gross/net units、銘柄・セクター・流動性制約）を実装した。
- [x] (2026-02-17 18:32Z) Phase4: 自己改善ループ（Stage0/1/2、多段評価、永続化、重複回避）を実装した。
- [x] (2026-02-17 18:36Z) Phase5: 19:05定期実行の導線（PowerShell）とRunbook更新を実装した。
- [x] (2026-02-17 18:38Z) 最低限テストを追加し、主要メトリクスと設定反映を検証した（`tests/test_toredex_phase1.py`）。
- [x] (2026-02-18 03:40Z) 自己改善ループの採点を `net_return` 単独から `risk-adjusted objective` へ更新し、DD/最悪月/回転/露出/コストドラッグ/取引不足を罰則化した。
- [x] (2026-02-18 03:44Z) `stage1` 評価対象を `stage0` 上位（目的関数順）へ絞る候補予算を追加し、探索計算を有望候補へ集中させた。
- [x] (2026-02-18 03:46Z) `optimization` 設定に `stage1MaxCandidates` / `minTradesStage*` / `scoreWeights` / `optimizeCostModel` を追加し、採点・探索挙動の再現性を向上した。
- [x] (2026-02-18 04:37Z) 2か月レンジで `self-improve` を検証し、`score_objective`/`trade_count`/`stage_pass_reason` が各Stageに反映されることを確認した。
- [x] (2026-02-18 05:15Z) 時刻依存を外すため `self-improve-loop`（目標到達またはmax cycles停止）をサービス/CLI/Job/API/Runbookに追加した。
- [x] (2026-02-20 03:10Z) `self-improve` に並列評価パラメータ（`parallelWorkers` / `parallelDbPaths`）を追加し、DBパス単位でStage評価を並列化できるようにした。
- [x] (2026-02-20 03:18Z) `run_backtest` に `rollup`（`worst_month_pct` / `max_turnover_pct_per_month` / `max_abs_net_units`）を追加し、並列実行時でも採点指標を結果JSONから直接計算可能にした。
- [ ] Phase6: 速度改善（Stage0/1向け軽量化・再利用）は既存キャッシュ活用まで完了。特徴量キャッシュの専用層は未実装。

## Surprises & Discoveries

- Observation: 3年バックテストは初期実装で約70分かかり、最適化ループに不向き。
  Evidence: `tradex_bt3y_v8_fast_baseline_20260217` 実行時に `elapsed_sec=4207.82`。

- Observation: `fees_bps` が `0.0` 固定で、ネット評価が実運用コストと乖離。
  Evidence: `app/backend/services/toredex_execution.py` の trade 保存で `fees_bps: 0.0`。

- Observation: DuckDB は複数UNIQUE/PK制約があるテーブルで `INSERT OR REPLACE` を使うと conflict target 指定を要求するケースがあった。
  Evidence: `toredex_optimization_runs` への保存で `Conflict target has to be provided...` が発生し、`DELETE + INSERT` に切替後に解消。

- Observation: コスト感度（5/10/15bps）を日次メトリクスに永続化しないと、`run_backtest` 最終結果に感度情報が残らない。
  Evidence: 初回実装時の `performance_breakdown.sensitivity=[]` を確認し、`cost_sensitivity_json` 列追加で解消。

- Observation: Stage期間を1か月未満に短縮すると、取引ゼロでも `pass` 扱いになりうるため、探索が「動かない設定」を拾ってしまう。
  Evidence: `python -m toredex self-improve --mode challenger --iterations 2 --stage2-topk 1 --stage0-months 1 --stage1-months 1 --stage2-months 1 --seed 20260218` 実行で、取引ゼロ候補が `MIN_TRADES(0<1)` により除外されることを確認。

- Observation: 2か月レンジでも `stage0→1→2` を通すと1候補あたり数分単位の計算が必要で、探索回転は依然として速度課題が残る。
  Evidence: `python -m toredex self-improve --mode challenger --iterations 1 --stage2-topk 1 --stage0-months 2 --stage1-months 2 --stage2-months 2 --seed 20260218` が約266秒で完了。

- Observation: Windows環境のDuckDBファイルは同時書き込みで `IO Error: Cannot open file ... file is already open` が発生し、単一DBファイルへの素朴な並列実行は失敗する。
  Evidence: 2026-02-20の `ThreadPoolExecutor(max_workers=2)` による `run_backtest` 同時実行で、両タスクが `stocks.duckdb` openエラーとなった。

## Decision Log

- Decision: 既存の `run_live/run_backtest` は互換を維持し、追加情報はメトリクス拡張として返す。
  Rationale: 既存 CLI/API/Runbook の破壊的変更を避けるため。
  Date/Author: 2026-02-17 / Codex

- Decision: 自己改善ループは新規サービス `toredex_self_improve.py` と専用DBテーブルで実装し、既存TOREDEX本流を分離する。
  Rationale: 本番運用と研究運用の責務分離、重複実行回避の容易化。
  Date/Author: 2026-02-17 / Codex

- Decision: Stage0/1 は短期レンジで早期失格判定、Stage2 だけ3年評価を実施する。
  Rationale: 探索回転を上げ、重い3年評価の回数を抑えるため。
  Date/Author: 2026-02-17 / Codex

- Decision: `run_live/run_backtest` は `config_override` を受け付け、`toredex_config.json` を書き換えずに研究設定を注入できるようにした。
  Rationale: Champion/Challenger 分離と再現性確保（config_hash固定）を両立するため。
  Date/Author: 2026-02-17 / Codex

- Decision: 自己改善ループの重複回避キーは `config_hash + stage + start_date + end_date + operating_mode` を採用した。
  Rationale: 探索再開時に同一条件を確実にスキップしつつ、範囲違いの再評価を許可するため。
  Date/Author: 2026-02-17 / Codex

- Decision: Stage選抜を `score_net_return_pct` 単独から `score_objective`（リスク調整後ネット収益）へ切替えた。
  Rationale: 資金成長を狙う探索で、下振れが大きい候補や回転過多候補を早期に落とす必要があるため。
  Date/Author: 2026-02-18 / Codex

- Decision: 取引不足（`minTradesStage*` 未満）を各Stageの失格条件にした。
  Rationale: 取引ゼロ/超低回転の「見かけ上安定」な候補を除外し、実際に資産を増やす戦略探索へ寄せるため。
  Date/Author: 2026-02-18 / Codex

- Decision: Phase5の19:05固定スケジュール依存は保留とし、連続自己改善ループを優先する方針へ切り替えた。
  Rationale: 目的は時刻運用ではなく、目標到達までの改善速度最大化であるため。
  Date/Author: 2026-02-18 / Codex

- Decision: 並列化は「同一DB共有の多重書き込み」を避け、`parallelDbPaths` に指定したDBパスへワーカーを分離して `run-backtest` サブプロセスを実行する方式を採用した。
  Rationale: DuckDBロック競合を回避しつつ、既存バックテスト実装を保ったまま実行本数を増やすため。
  Date/Author: 2026-02-20 / Codex

## Outcomes & Retrospective

Phase1〜4 は実装完了。ネット評価（gross/net/cost内訳）とリスクゲート、Long-Short制約、自己改善ループ（Stage0/1/2）をCLI/API/Jobから実行可能にした。2026-02-18に `self-improve-loop` を追加し、時刻に依存しない連続改善（目標達成またはmax cycles停止）を実行できるようにした。`fees_bps` を0→10へ変えると月次バックテストで `net_cum_return_pct` が低下することを確認し、目的関数の切替が機能していることを検証した。

未達は Phase6 の専用高速化（特徴量/シグナルの専用キャッシュ層）で、現状は既存キャッシュ活用と多段評価での計算削減まで。2026-02-18 時点で自己改善ループは `risk-adjusted objective` と `minTrades` ゲートを備え、同じ試行回数でも有望候補へ計算資源を寄せられる状態になった。次の改善候補は Stage0/1 向けデータセット固定キャッシュを追加し、自己改善1反復あたりの実行時間をさらに短縮すること。

## Context and Orientation

TOREDEX の実行入口は `toredex/__main__.py` で、`run-live` と `run-backtest` が `app/backend/services/toredex_runner.py` を呼び出す。`toredex_runner` は `build_snapshot`（`app/backend/services/toredex_snapshot_service.py`）でランキングと保有情報を統合し、`build_decision`（`app/backend/services/toredex_policy.py`）で売買アクションを生成し、`execute_live_decision`（`app/backend/services/toredex_execution.py`）で約定後の資金・ポジション・日次メトリクスを更新する。

データ永続化は `app/backend/services/toredex_repository.py` 経由で `app/db/schema.py` 定義の `toredex_*` テーブルに保存される。ジョブ実行は `app/backend/core/jobs.py` の JobManager が担当し、`app/backend/core/toredex_live_job.py` と `app/backend/api/routers/jobs.py` がAPI連携点である。

「ネット利益」は gross（価格変動による損益）から fees（売買手数料）、slippage（滑りコスト）、borrow（ショート保有コスト）を差し引いた結果を指す。本ExecPlanではこの定義を全ステージで統一する。

## Plan of Work

Phase1 では `toredex_config.json` と `ToredexConfig` にコスト設定を追加し、`execute_live_decision` で売買ごとの fees/slippage と日次 borrow を計算する。`toredex_daily_metrics` を拡張して gross/net とコスト内訳、回転率、露出を保存し、`run_backtest` の返却にもサマリを追加する。既存キー（`cum_return_pct` など）は互換維持のため残す。

Phase2 では Champion/Challenger の運用モードを設定化し、`max_drawdown_pct`、worst_month、turnover、net_exposure を合否ゲートとして評価する。ゲート評価は日次実行結果に記録し、バックテスト最終結果でも `gate_pass` と失格理由を返す。

Phase3 では `build_decision` にロングショート前提の露出制約を入れる。具体的には gross units 上限、ネット露出上限、1銘柄units上限、maxHoldings、流動性フィルタ、セクター上限（情報がある場合）を執行前に判定する。ショート不可は `shortable` フラグまたは設定ブラックリストで除外する。

Phase4 では自己改善サービスを追加する。ランダム探索で候補設定を生成し、Stage0（1-2か月）→ Stage1（6-12か月）→ Stage2（3年）で段階評価して、各結果をDBへ永続化する。レコードには `config_hash`, `git_commit`, `data_range`, `stage`, `metrics`, `artifacts_path` を保存し、同一設定の再評価をスキップする。JobManager に `toredex_self_improve` を登録し、APIから起動できるようにする。

Phase5 は固定時刻スケジューラ接続を保留し、代わりに「時刻非依存の連続自己改善ループ」を実行入口として確立する。`self-improve-loop` で目標達成または上限サイクル到達まで連続評価できるようにし、Runbook の運用手順もこの前提へ更新する。

Phase6 では Stage0/1 向け高速化を入れる。既存キャッシュの活用に加えて、同一設定・同一期間の再実行回避、軽量レンジ固定を使い、夜間の試行回数を増やす。

## Concrete Steps

作業ディレクトリは `c:\work\meemee-screener`。

1. 設定・スキーマ・実行ロジックを拡張する。

    - `app/backend/services/toredex_config.py`
    - `toredex_config.json`
    - `app/db/schema.py`
    - `app/backend/services/toredex_repository.py`
    - `app/backend/services/toredex_execution.py`
    - `app/backend/services/toredex_runner.py`
    - `app/backend/services/toredex_policy.py`

2. 自己改善ループとジョブ入口を追加する。

    - `app/backend/services/toredex_self_improve.py`（新規）
    - `app/backend/core/toredex_self_improve_job.py`（新規）
    - `app/backend/api/routers/jobs.py`
    - `app/main.py`, `app/backend/main.py`
    - `toredex/__main__.py`

3. 連続運用導線とドキュメントを更新する。

    - `toredex/__main__.py`（`self-improve-loop` 追加）
    - `app/backend/core/toredex_self_improve_job.py`（連続ループ対応）
    - `app/backend/api/routers/jobs.py`（連続ループ引数対応）
    - `docs/TOREDEX_RUNBOOK.md`

4. 最低限テストを追加する。

    - `tests/test_toredex_phase1.py` へ追加、または `tests/test_toredex_self_improve.py`（新規）

## Validation and Acceptance

受け入れ判定は以下を満たすこと。

- `python -m toredex run-backtest --season-id <id> --start-date <s> --end-date <e>` の結果に、`gross/net/fees/slippage/borrow` とリスクゲート合否が出る。
- `fees_bps` を変えると最終 `net` が変化する。
- long-short 有効時に `|uLong-uShort|` が設定上限を超えない。
- Stage0/1/2 を含む自己改善ジョブが動き、同一 `config_hash` の重複評価がスキップされる。
- 時刻に依存しない `self-improve-loop` の運用手順が Runbook に記載され、CLI/API/Job から起動できる。

## Idempotence and Recovery

DBスキーマ変更は `CREATE TABLE IF NOT EXISTS` と `ALTER TABLE ... ADD COLUMN` を使い、再実行しても壊れない形にする。新規テーブルは既存テーブルに影響しない。自己改善ジョブは `config_hash + stage + data_range` で重複を避ける。

失敗時は `toredex_config.json` の旧設定に戻し、研究用season_idを新規作成して再実行する。本番season_idには研究設定を混在させない。

## Artifacts and Notes

確認用の代表コマンド（実装後に実測値で更新する）。

    python -m toredex run-backtest --season-id tradex_bt_check --start-date 2025-01-01 --end-date 2025-03-31

    python -m toredex self-improve --mode challenger --iterations 10 --stage2-topk 2

    python -m tools.analytics.toredex_reason_scorecard --season-id tradex_bt_check

## Interfaces and Dependencies

`ToredexConfig` は以下を提供する。

- `cost_model`（fees/slippage/borrow の設定）
- `risk_gates`（champion/challenger 別閾値）
- `portfolio_constraints`（gross/net units、max units per ticker、流動性、セクター上限）
- `optimization`（stage期間、試行回数）

`execute_live_decision` は以下を返す。

- `costs`: `gross_pnl`, `fees_cost`, `slippage_cost`, `borrow_cost`, `net_pnl`
- `metrics` 拡張: turnover, long/short/net/gross exposure, gate status

`run_backtest` は以下を返す。

- `final_metrics`（互換維持 + 拡張）
- `performance_breakdown`
- `risk_gate`（pass/fail, reasons）

自己改善サービスは以下APIを持つ。

    def run_self_improve(*, mode: str, iterations: int | None, stage2_topk: int | None, seed: int | None, stage0_months: int | None, stage1_months: int | None, stage2_months: int | None) -> dict[str, Any]

改訂履歴:
- 2026-02-17 初版。ユーザー要求（A〜G）を満たすためのPhase構成、対象ファイル、検証条件を追加。
- 2026-02-17 実装追記。Phase1〜5の完了状況、実行時発見事項（DuckDB upsert制約・感度永続化）、未完了Phase6を反映。
