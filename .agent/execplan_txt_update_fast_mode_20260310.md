# TXT更新を practical_fast で早期完了させる

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

If `.agent/PLANS.md` exists in this repository, this ExecPlan must be maintained in accordance with it.

## Purpose / Big Picture

MeeMee の `TXT更新` は、日次データの取り込みだけでなく ML、分析バックフィル、ウォークフォワード検証まで同期待ちしているため、ボタンを押してから完了トーストが出るまで長くかかります。今回の変更後は、ユーザーは一覧に必要な日次更新を先に完了させ、重い後続処理はバックグラウンドで継続できます。

この変更が効いていることは 2 つの方法で確認できます。第一に、`TXT更新` 実行後の UI で先に「日次更新は完了。重い後続処理はバックグラウンドで継続中」と表示されます。第二に、ジョブ履歴で新しい `txt_followup` が後続で完了または失敗し、完了時に一覧が再読み込みされます。

## Progress

- [x] (2026-03-10 15:20 JST) `txt_update` の現在の段階と既存ジョブ群、既存テストの位置を確認した。
- [ ] `completion_mode` を API 契約へ追加し、UI から `practical_fast` を送る。
- [ ] `txt_update` の同期段を helper 化し、`practical_fast` では cache refresh 後に成功終了する。
- [ ] `txt_followup` job を追加し、ML/backfill/walkforward 系を後続ジョブへ移す。
- [ ] 既存テストを更新し、`practical_fast` と `txt_followup` の回帰を追加する。
- [ ] 実装結果に合わせて本 ExecPlan の結果欄を更新する。

## Surprises & Discoveries

- Observation: `ingest_txt.py` は既に変更 TXT ファイルだけを読む incremental モードを持っている。
  Evidence: `app/backend/ingest_txt.py` の `Incremental Mode: Found ... changed files` と `changed_files` / `skipped_files` の返却。

- Observation: 体感遅延の主因は ingest 後にも `phase -> ML -> backfill -> cache -> walkforward` が同一 `txt_update` job で直列実行される点にある。
  Evidence: `app/backend/core/txt_update_job.py` の `handle_txt_update()` で `job_manager._update_db(... progress=92..99)` がそのまま後続段へ進む。

## Decision Log

- Decision: 外部 API の既定は `full` のままにし、デスクトップ UI だけ `practical_fast` を opt-in する。
  Rationale: 既存の API 利用者やテストの互換性を崩さず、体感速度だけを UI で改善できるため。
  Date/Author: 2026-03-10 / Codex

- Decision: 既存の `ml_train` / `strategy_walkforward` などをそのまま個別投入する代わりに、新しい `txt_followup` handler を 1 本作る。
  Rationale: `txt_update` にある monthly-only スキップ、run 成功後だけ gate を評価する条件、strict 系の制御を崩さず再利用しやすいため。
  Date/Author: 2026-03-10 / Codex

## Outcomes & Retrospective

- 未記入。実装完了後に更新する。

## Context and Orientation

`app/backend/api/routers/jobs.py` は `POST /api/jobs/txt-update` の API 契約を持ち、FastAPI の引数を payload に詰め直して `submit_txt_update_job()` へ渡しています。`app/backend/core/txt_update_job.py` は実際の日次更新パイプラインで、Pan import、Pan Rolling export、DuckDB への ingest、Phase 再構築、ML 更新、売りスコア更新、分析系の後処理をまとめて実行しています。

フロント側では `app/frontend/src/routes/GridView.tsx` の `handleUpdateTxt()` がこの API を叩き、`applyTxtUpdateStatus()` と `notifyTerminalJob()` がジョブ完了時のトーストや一覧再読み込みを行います。`app/main.py` は `job_manager.register_handler()` でジョブ種別を登録するアプリ起動ポイントです。

今回追加する `txt_followup` は「日次一覧に不要だが重い後続処理を実行する job」の意味です。`txt_update` が早めに成功したあと、同じキュー基盤で順番待ちしながら実行されます。

## Plan of Work

まず `jobs.py` に `completion_mode` を追加し、`submit_txt_update()` が payload へ `"completion_mode": "full" | "practical_fast"` を含められるようにします。`GridView.tsx` は `handleUpdateTxt()` から `completion_mode=practical_fast` を送るように変更し、`formatJobTypeLabel()` と後続ジョブ通知文言に `txt_followup` を追加します。

次に `txt_update_job.py` を整理します。Pan import から ingest、phase、scoring、sell analysis、cache refresh までを同期段として扱い、ML 以降の後続処理は helper に切り出します。`completion_mode=practical_fast` のときは同期段の成功直後に `job_manager.submit("txt_followup", payload)` を行い、update state に `last_followup_job_id` と関連時刻を残したうえで `txt_update` を成功終了します。`completion_mode=full` は従来どおり同一 job 内で後続段まで完走させます。

その後、新しい handler `handle_txt_followup()` を同ファイルまたは新規 core module に実装し、既存 `txt_update` の後続段ロジックを helper 経由で再利用します。最後に `app/main.py` へ登録を足し、`tests/test_txt_update_submission_contract.py` と `tests/test_txt_update_pipeline_state.py` に新契約と fast/follow-up の挙動テストを追加します。

## Concrete Steps

作業ディレクトリは `C:\work\meemee-screener` を使います。

API とジョブ周りの確認:

    rg -n "submit_txt_update|completion_mode|handle_txt_update|txt_followup" app/backend/api/routers/jobs.py app/backend/core/txt_update_job.py app/main.py

フロント変更点の確認:

    rg -n "handleUpdateTxt|formatJobTypeLabel|notifyTerminalJob" app/frontend/src/routes/GridView.tsx

テスト実行:

    pytest tests/test_txt_update_submission_contract.py tests/test_txt_update_pipeline_state.py tests/test_job_integration.py -q

成功時は、追加した fast/follow-up 系テストが pass し、既存の txt_update 契約テストも壊れないことを確認します。

## Validation and Acceptance

`POST /api/jobs/txt-update` をパラメータなしで呼ぶ既存テストは、引き続き `full` として受理されなければなりません。`completion_mode=practical_fast` を送る新テストでは、`txt_update` が初回 cache refresh 後に success となり、`txt_followup` が 1 件だけ enqueue されることを確認します。

UI では `GridView.tsx` のユニットテストがないため、API パラメータと job label の変更をコード差分で確認し、バックエンド job 通知経路と同じラベル解決に乗ることを acceptance とします。手動確認では `TXT更新` 実行後に早い success toast、その後 `txt_followup` 完了時に一覧再読込が起きることが観測ポイントです。

## Idempotence and Recovery

この変更は additive です。`completion_mode` を送らない既存呼び出しは `full` のままです。途中で失敗した場合でも `txt_update` と `txt_followup` は別 job status を持つため、失敗箇所の切り分けがしやすくなります。re-run は安全で、同時実行抑止は既存の `unique=True` と active-job チェックを維持します。

## Artifacts and Notes

重要な証拠として次を保持する。

    app/backend/core/txt_update_job.py
      - 同期段: pan_import -> export -> ingest -> phase -> scoring -> sell_analysis -> cache_refresh
      - 後続段: ml_train -> ml_predict -> ml_live_guard -> analysis_backfill -> prewarm -> cache_refresh -> walkforward

    app/frontend/src/routes/GridView.tsx
      - handleUpdateTxt() が /jobs/txt-update を呼ぶ唯一の UI 入口
      - notifyTerminalJob() が txt_update 以外の完了トーストを処理

## Interfaces and Dependencies

`app/backend/api/routers/jobs.py` の `submit_txt_update()` には次の引数が存在する必要があります。

    completion_mode: str = "full"

送信 payload には次のキーが存在する必要があります。

    "completion_mode": "full" | "practical_fast"

`app/backend/core/txt_update_job.py` には次の handler が存在する必要があります。

    def handle_txt_update(job_id: str, payload: dict) -> None
    def handle_txt_followup(job_id: str, payload: dict) -> None

`app/main.py` は次を登録する必要があります。

    job_manager.register_handler("txt_followup", handle_txt_followup)

更新履歴: 2026-03-10 に初版を追加。実装開始前の調査結果と採用方針を反映した。
## 2026-03-10 Implementation Update

- Implemented `completion_mode` forwarding in `app/backend/api/routers/jobs.py`.
- Split `txt_update` practical-fast completion in `app/backend/core/txt_update_job.py`.
- Added new background handler `app/backend/core/txt_followup_job.py`.
- Registered `txt_followup` in `app/main.py`.
- Updated `app/frontend/src/routes/GridView.tsx` to send `completion_mode=practical_fast` and show follow-up aware toasts.
- Added backend contract/state tests for practical-fast and follow-up behavior.
- Verification:
  - `pytest tests/test_txt_update_submission_contract.py tests/test_txt_update_pipeline_state.py tests/test_job_integration.py -q` passed (`22 passed`).
  - `build_release.cmd` completed successfully and produced `release\\MeeMeeScreener`.
  - Release selftest launched backend and reached heatmap fetch, but failed on bundled empty market data (`industry_master_present is false` in selftest log). This is a release-data issue, not a practical-fast pipeline regression.
