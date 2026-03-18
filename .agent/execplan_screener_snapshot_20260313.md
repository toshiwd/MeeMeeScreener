# Screener Snapshot Delivery

## Purpose

一覧画面の初回表示を `GET /api/grid/screener` に一本化し、backend が保持する screener snapshot を frontend がそのまま消費できる状態にする。これにより、旧 `/list` fallback に依存した 404 や、stale snapshot の状態が UI に反映されない問題を避ける。

この変更後は、一覧取得時に backend が `items` と snapshot metadata をまとめて返し、frontend は `listSnapshotMeta` を通じて stale 表示を維持できる。動作確認は `GET /api/grid/screener` の object response、`loadList()` が `/list` を叩かないこと、stale metadata が store に残ること、の 3 点で観測する。

## Progress

- [x] 現状の一覧ロード経路と `/list` 404 の原因を特定した。
- [x] snapshot service と startup/job トリガーの配置方針を決めた。
- [x] backend snapshot service と job/scheduler の配線を確認し、`app.main` で handler 登録と startup/shutdown 呼び出しが有効であることを確認した。
- [x] frontend `loadList()` が `/grid/screener` の object response を正規化し、`/list` fallback を使わないことを確認した。
- [x] backend stale metadata 再利用テスト、frontend normalizer テスト、backend import/pytest による最小検証を完了した。

## Implementation Notes

backend の canonical endpoint は `app/backend/api/routers/grid.py` の `get_screener_rows()` で、実体は `app/backend/services/screener_snapshot_service.py` に寄せる。snapshot は DuckDB の `screener_snapshot_state` に保持し、`generation`、`asOf`、`updatedAt`、`lastError` などの metadata を payload と同時に返す。

startup/job 配線は `app/main.py` と `app/backend/core/screener_snapshot_job.py` が担う。アプリ起動時に scheduler を開始し、shutdown で停止する。force sync、Yahoo ingest、txt followup、analysis backfill、watchlist 更新など既存ジョブからは `schedule_screener_snapshot_refresh()` で unique job を積む。

frontend は `app/frontend/src/store.ts` から `app/frontend/src/listSnapshot.ts` の `normalizeScreenerListResponse()` を使い、snapshot object response を `tickers` と `listSnapshotMeta` に分けて保持する。legacy array payload も読めるが、新しい canonical response は object shape を前提にする。

## Surprises & Discoveries

- 旧 `loadList()` は `"/list"` fallback が前提だったため、backend に route がない環境では 404 になっていた。
- `npx vitest run src/store.loadList.test.ts src/listSnapshot.test.ts` は sandbox では `spawn EPERM` で停止したが、昇格実行では 4 tests passed まで確認できた。
- `npx tsc --noEmit` はこの変更とは無関係な既存 TypeScript error 群で失敗した。主な失敗箇所は `DetailChart.tsx`、`Header.tsx`、`GridView.tsx`、`store.ts` 周辺で、snapshot 導入の成否判定には使えなかった。
- backend 側では stale snapshot を再読込したときも `generation` と `lastError` を保持したまま返せるため、frontend が stale 警告を安定表示できる。

## Decision Log

- snapshot は JSON payload を 1 テーブルに保持する方式を採用し、別テーブルへ正規化しない。理由は一覧 1 画面ぶんの再利用が主目的で、更新単位を単純に保つほうが安全だから。
- acceptance は UI 実装詳細ではなく、`items + stale metadata + /list fallback 不使用` を backend/service test と frontend normalizer test で固定する方針にした。
- frontend build 全体は既存エラーで不安定なため、このタスクでは変更範囲の unit test と backend pytest を優先し、失敗は ExecPlan に証拠付きで残す。

## Outcomes & Retrospective

2026-03-13 JST: `/api/grid/screener` の canonical response、startup/job scheduler 配線、frontend `loadList()` / normalizer、backend stale metadata 再利用をテストで固定した。`python -c "import app.backend.main"` は成功し、昇格実行した `python -m pytest tests/test_screener_snapshot_service.py tests/test_grid_screener_snapshot_api.py` は 6 passed、`npx vitest run src/store.loadList.test.ts src/listSnapshot.test.ts` は 4 passed だった。`tsc --noEmit` は既存の unrelated TypeScript error 群で失敗したため、本タスクの回帰とは切り分けて記録した。
