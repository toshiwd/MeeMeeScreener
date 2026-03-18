# MeeMee Reorganization Master ExecPlan

## Purpose

MeeMee は本来、軽量な株価 DB/UI/閲覧ソフトとして使い続けられるべきですが、現在の `C:\work\meemee-screener` には本体、旧解析 worker、外付け解析、研究コード、運用スクリプト、実験成果物が混在しています。その結果、`stocks.duckdb` のような本体 DB に `ml_feature_daily` などの学習中間物が再生成され、DB 膨張と責務混在を起こしています。

この再編が終わると、実装者は「本体に残すもの」「external_analysis へ移すもの」「削除するもの」を迷わず判断できます。利用者視点では、本体の起動性と軽さを維持したまま、解析は外付け基盤だけで進化し、本体は結果だけを読む構造になります。

動作確認は、MeeMee の主要画面が軽量 DB で起動し、`result DB only` の公開契約が維持され、`ml_feature_daily` などの旧解析テーブルが本体再膨張の原因にならないことを確認して行います。

## Repository Orientation

作業対象の repo は `C:\work\meemee-screener` です。主要な領域は次のとおりです。

- `app/` は MeeMee 本体です。`app/frontend`, `app/backend`, `app/desktop`, `app/db` があり、将来的にはここを DB/UI/閲覧と read-only bridge に限定します。
- `external_analysis/` は外付け解析の実装場所です。export, label, candidate, similarity, nightly, replay, ops をここに集約します。
- `docs/` は採用済み仕様と runbook です。この再編では milestone 文書群をここに置きます。
- `research/`, `scripts/`, `tools/` は現在混在していますが、再編後は production path と非 production path を明確に分けます。
- live data は repo 外の `C:\Users\enish\AppData\Local\MeeMeeScreener\data` にあり、`stocks.duckdb` の正本もそこにあります。

本 plan の前提は、単一 repo 再編です。`external_analysis/` は将来別 repo に切り出せる境界を保ちますが、今回は `C:\work\meemee-screener` の中で責務を整理します。旧解析スクリプト互換は優先しません。

## Non-Negotiable Constraints

この再編で implementer が破ってはいけない制約を最初に固定します。

MeeMee 本体は DB/UI/閲覧に限定します。重い特徴量生成、長時間学習、GPU 占有、研究用 replay、walk-forward 評価、promotion 判定は本体に戻しません。

本体と外付け解析の公開契約はすでに固定済みです。MeeMee 本体が読むのは result DB だけで、起点は `publish_pointer` テーブル 1 行です。Parquet は internal only、feature store / label store / export DB / ops DB / model registry は本体から読ませません。

旧解析 worker の延命はしません。`ml_feature_daily` の本体常駐、`ml_pred_20d` など旧解析テーブルの本体再生成、`trade_events_bak` のような本体内バックアップ複製は是正対象です。

大規模非互換は許容しますが、主要画面の起動性は守ります。最低限、Positions, Favorites, Practice, Candidates, Similar Cases, Detail が graceful degrade 付きで起動する状態を維持します。

## Current State

現状で確認済みの重要事実をここに固定します。

`stocks.duckdb` は live data で約 65GB まで膨張していました。縮退コピーで `ml_feature_daily`, `ml_pred_20d`, `sell_analysis_daily`, `phase_pred_daily`, `label_20d`, `ml_*_registry`, `trade_events_bak` を empty schema 化すると、およそ 1GB まで縮小できました。つまり本体 DB の主な肥大要因は旧解析テーブルです。

`app/backend/services/ml/ml_service.py` と `app/backend/services/analysis/analysis_backfill_service.py` には `ml_feature_daily` の自動再生成経路が残っています。これは単発の軽量化では根治しません。

`app/db/schema.py` は本体標準スキーマの中に `feature_snapshot_daily` と `ml_feature_daily` を両方持ち、閲覧用の軽量スナップショットと旧学習用特徴量を二重保存しています。

`app/backend/core/csv_sync.py` は `trade_events_bak` を本体 DB 内に `CREATE TABLE AS SELECT *` で複製しています。`strategy_walkforward_research_daily` などの研究・評価用保存も本体 DB 側に残っています。

一方で `external_analysis/` には candidate, similarity, nightly, replay, promotion gate までの基盤ができており、public 契約は `publish_pointer`, `publish_manifest`, `candidate_daily`, `regime_daily`, `similar_cases_daily`, `similar_case_paths` に限定されています。

## End State

再編後の最終形は次です。

`app/` は MeeMee 本体だけを持ちます。本体 DB 正本テーブルは `daily_bars`, `daily_ma`, `feature_snapshot_daily`, `monthly_bars`, `monthly_ma`, `stock_meta`, `tickers`, `positions_live`, `position_rounds`, `trade_events`, `favorites`, `practice`, EDINET/貸借/イベント系に限定します。

`external_analysis/` は解析の唯一の実装場所です。feature store, label store, candidate, similarity, nightly, replay, ops, review artifact, promotion gate をここに集約します。

`docs/` は正本仕様と runbook を持ちます。`research/`, `scripts/`, `tools/` は production path と非 production path を明示的に分離し、再現不能な古い試験コードは削除します。

本体から旧 `ml_*` / `sell_analysis_daily` / `phase_pred_daily` への依存はなくなり、必要な公開結果はすべて result DB 経由で読みます。旧解析スキーマは最終的に削除または empty compatibility schema に格下げされます。

## Milestones

### Milestone 1: Current-State Inventory

この milestone では、どのテーブルとコードが本体正本で、どれが外付け解析や研究の残骸かを決めます。`app/db/schema.py`, `app/backend/services/ml`, `app/backend/services/analysis`, `external_analysis/`, `research/`, `scripts/`, `tools/` を読み、再膨張経路と重複保存経路を台帳化します。

完了すると、`docs/REORG_MILESTONE_1_DB_BOUNDARY.md` に本体正本テーブル、縮退対象、削除候補、再生成経路が明記されます。

確認コマンドは、最小限なら次です。

    cd C:\work\meemee-screener
    rg -n "ml_feature_daily|ml_pred_20d|sell_analysis_daily|phase_pred_daily|trade_events_bak" app docs external_analysis

受入条件は、`ml_feature_daily` などの主な膨張因子と、それを再生成するコード経路が書かれていることです。

### Milestone 2: DB Boundary Redefinition

この milestone では、本体 DB に残す正本テーブルと、compatibility-only に落とす旧解析テーブルを固定します。`app/db/schema.py` の将来像と、runtime 起動時に自動作成してよいもの・いけないものを決めます。

完了すると、`docs/REORG_MILESTONE_1_DB_BOUNDARY.md` が implementation-ready になり、`ml_feature_daily` を本体正本から外す方針が決定済みになります。

確認は、軽量 DB でも主要画面が起動できることを手動で確認できる条件まで落とします。

### Milestone 3: Runtime Boundary Redefinition

この milestone では、本体、external_analysis、research、scripts、tools の責務を固定します。どの機能が `app/` に残り、どのジョブや保存先が `external_analysis/` に移るかを決定します。

完了すると、`docs/REORG_MILESTONE_2_RUNTIME_BOUNDARY.md` に本体禁止事項、external_analysis 側の唯一の実装場所、legacy 停止対象、graceful degrade の維持条件が実装粒度で書かれます。

### Milestone 4: Repo Layout Finalization

この milestone では、top-level フォルダ構成と主要サブディレクトリの最終配置を決めます。`research/`, `scripts/`, `tools/`, `tmp/`, `published/` の扱いを固定し、production path と非 production path を切り分けます。

完了すると、`docs/REORG_MILESTONE_3_REPO_LAYOUT.md` に「どこへ移すか」「どこを削るか」「どこを残すか」が decision complete で書かれます。

### Milestone 5: Migration Runbook

この milestone では、どの順に壊し、どの順に止め、どの順に戻せるようにするかを runbook 化します。軽量 DB 置換、旧解析停止、本体参照切替、rollback の順番を明文化します。

完了すると、`docs/REORG_MILESTONE_4_MIGRATION_RUNBOOK.md` に実行手順、停止条件、rollback、検証観点が入ります。

### Milestone 6: Implementation Start

この milestone から実装です。以後は各 milestone ごとに code change を進め、終わるたびにこの master ExecPlan の `Progress`, `Decision Log`, `Outcomes & Retrospective` を更新します。

## Implementation Rules

実装者は次の原則で進めます。

一度に 1 症状だけ直します。たとえば `ml_feature_daily` 再生成停止と repo 配置換えを同じ patch に混ぜません。

本体の read-only 公開契約は守ります。`publish_pointer`, `publish_manifest`, `candidate_daily`, `regime_daily`, `similar_cases_daily`, `similar_case_paths` の wire shape は変えません。

外付け解析を本体に戻しません。本体 DB への解析中間物常駐は禁止です。

危険な変更では必ず rollback 先を残します。live DB や data directory を触る場合は、退避 copy と起動確認をセットにします。

## Validation

各 implementation milestone では、少なくとも次を確認します。

backend import が通ること。

    cd C:\work\meemee-screener
    python -c "import app.main"

frontend build が通ること。

    cd C:\work\meemee-screener\app\frontend
    npm run build

external_analysis 側の主要 smoke が通ること。変更範囲に応じて、candidate, similarity, nightly, replay の最小 pytest を選んで実行します。

MeeMee 本体の手動確認では、Positions, Favorites, Practice, Candidates, Similar Cases, Detail を開き、result DB missing や stale でも 500 にならないことを確認します。

## Progress

- [x] 親方針を決めた。単一 repo 再編、本体優先、旧解析延命なし。
- [x] `stocks.duckdb` の膨張要因が旧解析テーブルであることを実測した。
- [x] lightweight DB 置換で本体起動を確認した。
- [x] `legacy_analysis_disabled` 時の `ml_feature_daily` 自動再生成を止める最初の実装に着手した。
- [x] `stock_repo` の旧解析 read path を legacy disabled で short-circuit し、`phase_pred_daily` / `ml_pred_20d` / `sell_analysis_daily` への最初の read 導線を遮断した。
- [x] `app/db/schema.py` の標準初期化から旧解析テーブルを切り離し、legacy disabled 時は本体正本テーブルだけを作るようにした。
- [x] `csv_sync.py` の `trade_events_bak` 永続複製を廃止し、同一接続内の一時バックアップだけで rollback するようにした。
- [x] `screener_repo.py` の `phase_pred_daily` read path も legacy disabled で short-circuit し、screener 導線から旧解析 table を読まないようにした。
- [x] `phase_batch.py` の `label_20d` / `phase_pred_daily` write path も legacy disabled で short-circuit し、job 側から旧解析 table を再生成しないようにした。
- [x] `sell_analysis_accumulator.py` の `sell_analysis_daily` write path も legacy disabled で short-circuit し、旧 short 分析 table を再生成しないようにした。
- [x] `strategy_backtest_service.py` の `ml_pred_20d` join を legacy disabled で無効化し、backtest 導線が旧予測 table なしでも動くようにした。
- [x] `swing_expectancy_service.py` の `ml_pred_20d` 前提 refresh/read path も legacy disabled で short-circuit し、旧予測 table への依存を減らした。
- [x] `rankings_cache.py` の公開入口で `ml/hybrid/turn` を legacy disabled 時に `rule` へ縮退させ、旧予測 table を読む ranking path を広く回避するようにした。
- [x] `app/db/schema.py` から旧解析 schema 定義を `ensure_legacy_analysis_schema()` へ分離し、本体標準初期化とは完全に切り離した。
- [x] `app/desktop/launcher.py` の `ml_model_registry` seed/self-heal を legacy disabled 時に停止し、本体起動時の旧解析 schema 再注入を止めた。
- [x] `ml_service.py` の `_ensure_ml_schema()` から旧解析 schema の自前定義を外し、`ensure_legacy_analysis_schema()` を使う形に揃えた。
- [x] `analysis_backfill_service.py` も `ml_service._ensure_ml_schema()` 依存を外し、互換 schema の明示入口として `ensure_legacy_analysis_schema()` を使うようにした。
- [ ] Milestone 1 の台帳を確定する。
- [ ] Milestone 2 の DB boundary を文書化する。
- [ ] Milestone 3 の runtime boundary を文書化する。
- [ ] Milestone 4 の repo layout を文書化する。
- [ ] Milestone 5 の migration runbook を文書化する。
- [ ] 実装に着手する。

## Surprises & Discoveries

- `ml_feature_daily` を empty にした compact DB は約 1GB まで縮小した。一方で MeeMee 起動後に `ml_feature_daily` が再生成され、DB は約 2.2GB まで戻った。これは本体再膨張経路が残っている証拠である。
- `displayName is not defined` の UI 例外は DB 欠損ではなく `app/frontend/src/routes/PositionsView.tsx` の未定義変数参照だった。軽量 DB 置換で名前欠落ケースが顕在化した。
- `trade_events_bak` のようなバックアップ複製が本体 DB 内に残っていた。件数は小さいが、設計方向としては望ましくない。
- `legacy_analysis_disabled` は既に標準化済みだったため、再膨張停止は新フラグ追加ではなく既存フラグで short-circuit できる。

## Decision Log

- 2026-03-13: 大規模再編は単一 repo のまま進める。後日の別 repo 切り出しは可能だが、今回は境界整備を優先する。
- 2026-03-13: 旧スクリプト互換より本体の軽量性と責務分離を優先する。
- 2026-03-13: 分割計画を採用するが、唯一の正本はこの master ExecPlan とする。
- 2026-03-13: 本体 DB から `ml_feature_daily` などの旧解析テーブルを排除する方針を固定する。

## Rollback

live data を触る変更では、必ず次を残します。

- 元 `stocks.duckdb` の退避 copy
- data directory override の現在値
- 置換前後のサイズと主要テーブル件数
- 起動確認結果

もし主要画面が壊れたら、退避した full DB を元位置へ戻し、`.wal` と `app.lock` を整理して起動確認します。

## Outcomes & Retrospective

現時点では再編の初手として、`ml_feature_daily` の自動再生成停止に着手した。実データで膨張要因を特定し、軽量 DB で主要画面が動くことは確認できている。repo 構成と runtime 境界の本格整理はまだ残っている。
## ASCII Update 2026-03-13

- Added service-level short-circuit for ranking analysis quality endpoints when legacy analysis is disabled.
- Added service-level short-circuit for live guard and ml status endpoints when legacy analysis is disabled.
- Unified legacy analysis control source of truth to `app/backend/core/legacy_analysis_control.py`.
- Updated `_ensure_ml_schema()` so legacy tables are not created or altered when legacy analysis is disabled.
- Added regression tests covering quality, ml status, legacy control single source, and ml schema boundary behavior.
- Extracted legacy/monthly/audit ML schema creation into `app/backend/services/ml/legacy_schema_runtime.py`.
- Reduced `ml_service.py` responsibility so `_ensure_ml_schema()` is now a thin compatibility wrapper.
- Added direct regression coverage for the extracted ML runtime schema helper.
- Moved launcher-side `ml_model_registry` seed SQL into `app/desktop/legacy_ml_seed.py`.
- Kept desktop startup compatibility wrappers while removing direct legacy registry SQL from `app/desktop/launcher.py`.
- Added `app/backend/services/ml/legacy_predict_runtime.py` and moved legacy prediction/live-guard public entrypoints behind a compat runtime module.
- Kept `get_ml_status()` in `ml_service.py` as the compatibility-preserving status payload source after verifying the disabled payload shape mattered to tests.
- Tried extracting `analysis_backfill_service` and `train_models` public entrypoints behind compat wrappers, then reverted the public wrappers after confirming existing monkeypatch-based tests relied on the original module globals.
- Kept `app/backend/services/analysis/legacy_backfill_runtime.py` and `app/backend/services/ml/legacy_train_runtime.py` on disk as staging modules, but restored the original public entrypoints to preserve runtime and test compatibility.
- Added patch-forwarding behavior to lazy module proxies in `app/backend/services/__init__.py` and the `ml` / `analysis` / `data` subpackages so `patch()` and `monkeypatch.setattr()` keep reaching the real modules.
- Updated txt update pipeline expectations to match the current legacy-disabled policy: legacy ML follow-up work is skipped, while walk-forward recompute still runs on pan-finalize forced refresh.
- Re-ran the backend reorg regression bundle after the above stabilization and confirmed 38 targeted tests passed.
- Re-enabled compat wrappers for `analysis_backfill_service` and `ml_service.train_models` after the lazy-module patch-forwarding fix proved stable.
- Kept the original public module paths intact while routing the implementation through `legacy_backfill_runtime.py` and `legacy_train_runtime.py`.
- Stopped `txt_update_job.py` and `txt_followup_job.py` from persisting `strategy_walkforward_research_daily` snapshots when legacy analysis is disabled, so walk-forward execution can still run without writing new research artifacts into the main DB.
- Added service-level short-circuit for `save_daily_walkforward_research_snapshot()`, `get_latest_strategy_walkforward_research_snapshot()`, and `prune_strategy_walkforward_history()` when legacy analysis is disabled.
- Explicitly left `strategy_walkforward_runs` / `strategy_walkforward_gate_reports` persistence untouched for now because the current gate flow still depends on the latest saved run in the main DB.
- Added service-level short-circuit for `get_latest_strategy_walkforward()` and `get_latest_strategy_walkforward_gate()` when legacy analysis is disabled, so read paths no longer surface main-DB walkforward history in the disabled regime.
- Updated `run_strategy_walkforward_gate()` so callers can provide the just-computed walkforward summary/windowing directly instead of requiring a latest persisted run lookup.
- Updated `txt_update_job.py` and `txt_followup_job.py` to pass the in-memory walkforward report into gate evaluation, reducing the remaining dependency on `strategy_walkforward_runs` persistence.
- Stopped `run_strategy_walkforward()` from persisting `strategy_walkforward_runs` when legacy analysis is disabled; callers still receive the full in-memory report.
- Stopped `run_strategy_walkforward_gate()` from persisting `strategy_walkforward_gate_reports` when legacy analysis is disabled; callers still receive the full gate result.
- Added regression coverage that the disabled regime skips both walkforward run persistence and walkforward gate persistence while preserving non-dry-run execution results.
- Verified that the `PositionsView` source fix already existed in `app/frontend/src/routes/PositionsView.tsx`, but the packaged frontend assets were stale.
- Rebuilt the frontend with `npm run build` and synchronized the new dist output into both `app/backend/static` and `release/MeeMeeScreener/_internal/app/backend/static`.
- Confirmed the packaged app now serves the rebuilt `PositionsView-B5MfQfgm.js` asset instead of the stale `PositionsView-CQNrrG8G.js` bundle.
- Re-ran a partial UI smoke after the asset sync and confirmed `Positions` and `Candidates` render again without the previous `displayName is not defined` frontend error.
- Recorded the remaining high-priority reorg tasks as: further physical separation of legacy logic from `app/backend/services/ml/ml_service.py`, further physical separation of legacy logic from `app/backend/services/analysis/analysis_backfill_service.py`, and automation of frontend build-to-release static asset synchronization.
