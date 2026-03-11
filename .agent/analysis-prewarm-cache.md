# Analysis Prewarm Cache

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

If `.agent/PLANS.md` exists in this repository, this ExecPlan must be maintained in accordance with it.

## Purpose / Big Picture

チャート閲覧中に `/api/ticker/analysis` や `/api/ticker/analysis/sell` がその場で欠損データを生成すると、カーソル移動のたびに重い同期処理が発生し、30 秒 timeout やラグの原因になる。今回の変更後は、閲覧系 API は生成をせず「既に保存済みの分析結果」を即返すだけにする。代わりに backend 起動後に最近 6 か月分の分析キャッシュをバックグラウンドで温めるので、ユーザーは一度計算済みの期間ならすぐ表示される。

人間が確認する方法は単純で、アプリ起動後に detail 画面でカーソルを動かしても `/api/ticker/analysis/sell` timeout が起きず、最近 6 か月の判定が即座に出ることを観察する。

## Progress

- [x] (2026-03-07 12:20 JST) 根因調査を実施し、閲覧 API が欠損時に同期 backfill を呼んでいることを確認した。
- [x] (2026-03-07 12:34 JST) backend 起動後に最近 6 か月分の analysis/sell 欠損を補う prewarm scheduler を追加した。
- [x] (2026-03-07 12:36 JST) `/ticker/analysis`, `/ticker/analysis/timeline`, `/ticker/analysis/sell` から同期 backfill を外した。
- [x] (2026-03-07 12:38 JST) `txt_update` 完了後にも analysis prewarm を queue する導線を追加した。
- [x] (2026-03-07 12:49 JST) cached analysis の auto invalidation を追加した。ML は active `model_version`、sell は `calc_version` の不一致を stale とみなして再生成する。

## Surprises & Discoveries

- Observation: `/ticker/analysis/sell` timeout は frontend の 30 秒 timeout で、HTTP status 未達のまま落ちていた。
  Evidence: `app/frontend/src/routes/DetailView.tsx` の `timeoutMs: 30000` と、`app/backend/api/routers/ticker.py` の `ensure_sell=True` 同期 backfill。

- Observation: 既に欠損日をまとめて埋める `analysis_backfill` job があり、最近 6 か月の事前生成には新規バッチよりこちらの再利用が適している。
  Evidence: `app/backend/services/analysis_backfill_service.py`.

## Decision Log

- Decision: 新しい分析生成ロジックは作らず、既存の `analysis_backfill` を startup scheduler から呼ぶ。
  Rationale: 既に `ml_pred_20d` と `sell_analysis_daily` の欠損補完が実装済みで、重複実装を避けられる。
  Date/Author: 2026-03-07 / Codex

- Decision: 閲覧 API から同期 backfill を削除する。
  Rationale: ユーザー操作中の request path に重い生成処理を残すと timeout の根本原因が消えない。
  Date/Author: 2026-03-07 / Codex

## Outcomes & Retrospective

- 現時点で、閲覧 API は分析データをその場で生成しなくなった。起動後の background scheduler と `txt_update` 後の queue で、最近 6 か月の欠損だけを埋める方針へ移行した。
- ML 予測 row は active `model_version` と一致しない日を stale とみなし、sell 分析 row は `calc_version` と一致しない日を stale とみなして再生成するようになった。
- 純粋な判定ロジック変更は API 応答時に live 計算されるので、基本的には DB 再生成は不要。再生成が必要なのは、予測モデルや sell 集計式の入力キャッシュが変わった時である。
