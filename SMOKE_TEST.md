# MeeMee Screener Smoke Test Procedure

**Time Required:** 5-10 minutes
**Goal:** 起動・更新・チャート表示の主要フローが回帰していないことを確認する。
**Pass Condition:** REQUIRED 手順でクラッシュ、UIフリーズ、永続エラーが発生しないこと。

---

## 0. Preflight (REQUIRED)

- [ ] `run.ps1` または `MeeMeeScreener.exe` で起動する。
- [ ] API が `http://127.0.0.1:8000` で応答することを確認する。

---

## 1. Launch & Readiness (REQUIRED)

1. Action: アプリを起動。
2. Check:
- [ ] 起動直後に UI が先行して壊れた状態にならない。
- [ ] `GET /api/health` が `ready=true` を返してから UI が通常表示になる。
- [ ] `GET /api/health` の主要キー `ok,status,ready,phase,message,errors,retryAfterMs` が存在する。

---

## 2. Health Display Integrity (REQUIRED)

1. Action: Grid 画面で health 情報を確認。
2. Check:
- [ ] `/api/health/deep` 成功時に `txt_count/code_count/pan_out_txt_dir` が表示できる。
- [ ] `/api/health/deep` が失敗した場合でも `/api/health` フォールバックで画面が壊れない。

---

## 3. Basic View Integrity (REQUIRED)

- [ ] メイン画面で銘柄一覧が表示される。
- [ ] `/` -> `/ranking` -> `/detail/:code` -> `/favorites` -> `/` の遷移が成功する。
- [ ] スクロール中にチャートが継続表示され、永続的な「読み込み中」に張り付かない。

---

## 4. TXT Update Job Path (REQUIRED)

### 4.1 Start/Complete

1. Action: TXT Update を1回実行。
2. Check:
- [ ] ジョブ開始が受理される (`200`)。
- [ ] 完了時に `success` もしくは `failed` の終端状態になる（中間状態で停止しない）。
- [ ] `update_state.json` に `last_pipeline_stage` と `last_pipeline_status` が記録される。

### 4.2 Double Trigger Guard

1. Action: 実行中に再度 TXT Update を実行。
2. Check:
- [ ] 2回目は `409 conflict` で拒否される。
- [ ] 競合レスポンスに `error_detail` が含まれる場合、lock起因か通常重複か判別できる。

### 4.3 Retry Trace

1. Action: lock 再試行が発生するケースを実行（必要なら同時処理で再現）。
2. Check:
- [ ] `update_state.json` に `retry_trace[]` が追記される。
- [ ] `last_retry_summary` / `last_retry_exhausted_stage` / `last_retry_exhausted_kind` が更新される。

---

## 5. API Performance Check (REQUIRED)

```powershell
python tools/analytics/benchmark_api.py --runs 20 --warmup 3 --batch-codes 48 --limit 240 --output tmp/api_benchmark_after_v3.json
```

Check:
- [ ] `batch_bars_v3_daily` の p95 が 250ms 以下（目標）。
- [ ] `batch_bars_v3_monthly` の p95 を記録し、退行がない。
- [ ] `grid_screener` の p95 が 1.2s 以下（目標）。

---

## 6. Optional API Sanity (Dev)

- `POST /api/batch_bars_v3`
  - payload: `{ "codes": ["7203"], "timeframes": ["daily","weekly","monthly"], "limit": 240, "includeProvisional": true }`
  - expect: `200`, `items[code].daily|weekly|monthly` が存在
- `POST /api/jobs/txt-update` を連続実行
  - 1回目: `200` (accepted)
  - 2回目: `409` (`update_in_progress`)

---

**STOP RULE:** REQUIRED 手順で失敗した場合はマージしない。原因を切り分けてから再実施する。
