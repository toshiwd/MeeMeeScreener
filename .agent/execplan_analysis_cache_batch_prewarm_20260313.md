# ExecPlan: Analysis Cache Batch Prewarm

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

If `.agent/PLANS.md` exists in this repository, this ExecPlan must be maintained in accordance with it.

## Purpose / Big Picture

MeeMee の売買判定は、株価そのものが変わっていない限り毎回リアルタイム再計算する必要がない。すでに `ml_pred_20d` と `sell_analysis_daily` には日付単位のキャッシュがあり、`model_version` と `calc_version` でロジック更新も検知できる。今回の変更では、その既存キャッシュをユーザーが明示的に温められるようにして、一覧画面を開いた瞬間の待ち時間を減らす。

変更後は、一覧画面の設定パネルから「最新の売買判定を一括計算」を押すと、現在の対象銘柄全体に対して最新営業日の ML 予測と売り判定キャッシュがジョブとして作られる。ジョブ完了後は `ml_pred_20d` と `sell_analysis_daily` の最新日付が約 700 銘柄分そろい、以後の一覧読み込みは既存キャッシュを使う。ロジックが変わった場合でも、ML は active `model_version`、売り判定は `calc_version` が変わると stale 扱いになり、同じ導線で再計算できる。

## Progress

- [x] (2026-03-13 10:35 JST) 既存の analysis backfill / prewarm / UI 導線を調査し、今回の変更を既存 `analysis_backfill` に寄せる方針を確定した。
- [x] (2026-03-13 11:05 JST) 既存 `analysis_backfill` を latest-only で呼ぶ非 legacy endpoint と UI 導線を追加した。
- [x] (2026-03-13 11:18 JST) Grid の設定パネルに「売買判定キャッシュ」セクションと実行ボタンを追加した。
- [x] (2026-03-13 11:24 JST) 実 DB で最新判定の事前計算を実行し、API 応答と DB 件数を確認した。
- [x] (2026-03-13 11:25 JST) 実装結果に合わせて本 ExecPlan を更新した。

## Surprises & Discoveries

- Observation: 既存の `analysis_backfill` は「欠けている日だけ」ではなく `force_recompute=true` も持っており、さらに ML は active `model_version`、売り判定は `SELL_ANALYSIS_CALC_VERSION` を使って stale 判定している。  
  Evidence: `app/backend/services/analysis/analysis_backfill_service.py` の `_query_existing_ml_dates`, `_query_existing_sell_dates`, `_resolve_analysis_cache_coverage`.
- Observation: 一覧画面の設定パネルにはすでに手動ジョブ起動ボタン群があり、ここへ追加するのが最小変更。  
  Evidence: `app/frontend/src/routes/GridView.tsx` の general settings panel 内に CSV 取り込み、Phase 再計算、MM_DATA_DIR 設定がある。
- Observation: `analysis_backfill` を JobManager に積むと、この環境では `No handler for type analysis_backfill` になるケースがあった。  
  Evidence: `sys_jobs` の `analysis_backfill` 行に `error='No handler for type analysis_backfill'` が残り、UI submit 直後の job detail でも同じエラーが返った。
- Observation: 同期 endpoint に切り替えると隔離 backend `http://127.0.0.1:8010` で `{"ok":true,"mode":"sync","message":"ml=1/2 sell=3 phase=skip"}` を返し、`sell_analysis_daily` の最新 dt には 698 銘柄が存在した。  
  Evidence: 2026-03-13 の検証コマンドと `stocks.duckdb` への `SELECT MAX(dt), COUNT(DISTINCT code)`。

## Decision Log

- Decision: 新しい専用キャッシュテーブルは作らず、既存の `ml_pred_20d` と `sell_analysis_daily` をそのまま使う。  
  Rationale: データの実体はすでに保存されており、ロジック更新時の無効化も `model_version` / `calc_version` で表現済み。新テーブルを足すと無効化規則が二重化する。  
  Date/Author: 2026-03-13 / Codex

- Decision: 一括計算の対象は「最新営業日 1 日」を既定にする。  
  Rationale: 一覧表示の待ち時間を下げる主目的に対して必要十分で、700 銘柄 * 最新日付のキャッシュを先に作るのが最速。履歴再構築が必要なケースは既存 `/api/jobs/analysis/backfill-missing` が残る。  
  Date/Author: 2026-03-13 / Codex

- Decision: 新 endpoint `/api/jobs/analysis/prewarm-latest` は JobManager 経由ではなく同期実行にする。  
  Rationale: この環境では `analysis_backfill` の job submit が handler 不整合で不安定だった一方、同期で `backfill_missing_analysis_history()` を呼べば結果を確実に返せた。ユーザー要求は「好きなタイミングで前計算する」ことであり、長押し不要の単発同期実行で満たせる。  
  Date/Author: 2026-03-13 / Codex

## Milestones

### Milestone 1: 一覧設定パネルから起動する

`app/frontend/src/routes/GridView.tsx` に「売買判定キャッシュ」セクションを追加し、最新判定の一括計算ボタンを置く。押下時は `/api/jobs/analysis/prewarm-latest` を `force_recompute=true` で呼ぶ。レスポンスは同期で返り、完了後に `ML=<件数>, 売り=<件数>` のトーストを表示して `loadList()` を叩く。UI は latest-only に固定し、オプションを増やし過ぎない。

作業後は、一覧右上の設定パネルを開くと新セクションが見え、ボタン押下でトーストとジョブ進捗が動くことが受け入れ条件。

### Milestone 2: 実 DB で約700銘柄分を前計算し、結果を確認する

実運用 DB を使って最新日 1 日の事前計算を実行する。終了後に `ml_pred_20d` と `sell_analysis_daily` の `MAX(dt)` に対する distinct code 数を確認し、対象 universe とほぼ同数までそろっていることを確認する。フロントでは設定パネルに新ボタンが表示されることをスクリーンショットで確認する。

受け入れ条件は、実 DB で最新日の cache rows が約 700 銘柄分あり、UI から再実行でき、ボタン押下後に同期応答で完了結果が返ること。

## Validation

バックエンド変更後は、作業ディレクトリ `C:\work\meemee-screener` で以下を実行する。

    python -m py_compile app/backend/api/routers/jobs.py
    PYTHONPATH=. pytest tests/test_analysis_backfill_service.py -q

フロント変更後は、作業ディレクトリ `C:\work\meemee-screener\app\frontend` で以下を実行する。

    npm run lint
    npm run build

実 DB 検証では、必要な環境変数を本番データディレクトリへ向けてバックエンドを起動し、API または UI から latest prewarm を実行する。完了後は DuckDB に対して最新日の distinct code 数を確認する。

## Outcomes & Retrospective

- 一覧設定パネルに「売買判定キャッシュ」ボタンを追加し、legacy analysis 無効化中でも使える専用 endpoint `/api/jobs/analysis/prewarm-latest` を実装した。
- endpoint は `backfill_missing_analysis_history(lookback_days=1, include_sell=True, force_recompute=True)` を同期実行し、完了件数をそのまま返す。
- 実 DB 検証では endpoint が `{"ok":true,"mode":"sync","message":"ml=1/2 sell=3 phase=skip"}` を返し、`sell_analysis_daily` の最新 dt `1773273600` に 698 銘柄分の row があることを確認した。
- UI は `[GridView.tsx]` の設定パネルにボタンが表示されるところまで Playwright snapshot で確認した。隔離 frontend の一覧 404 は既存 dev ルート問題で、この機能追加とは別だった。
