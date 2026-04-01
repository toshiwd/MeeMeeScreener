# TRADEX Export Snapshot Monitoring Runbook

## 目的
- `export-snapshot-build` の長時間実行を、実行中 output を汚さずに監視する。
- `snapshot_progress.json` と `snapshot_status.json` の読み方を固定する。
- `complete` 到達後に `image-rerank-research-run` へ進む判断を揺らさない。

## 対象コマンド
```powershell
python -m external_analysis export-snapshot-build `
  --source-db-path C:\work\meemee-screener\.local\meemee\research_db\stocks_research_20230101_20260226.duckdb `
  --export-db-path C:\Users\enish\AppData\Local\Temp\tradex_full_universe_confirm7\full_universe_export.duckdb
```

## 監視対象 artifact
- `C:\Users\enish\AppData\Local\Temp\tradex_full_universe_confirm7\full_universe_export.duckdb.snapshot_progress.json`
- `C:\Users\enish\AppData\Local\Temp\tradex_full_universe_confirm7\full_universe_export.duckdb.snapshot_status.json`

## 読み取りコマンド
```powershell
Get-Content C:\Users\enish\AppData\Local\Temp\tradex_full_universe_confirm7\full_universe_export.duckdb.snapshot_progress.json
Get-Content C:\Users\enish\AppData\Local\Temp\tradex_full_universe_confirm7\full_universe_export.duckdb.snapshot_status.json
```

## `snapshot_progress.json` の見方
### 主要項目
- `status`
  - `running`: current run が継続中
  - `failed`: current run が途中失敗
  - `complete`: 全 step 完了
- `current_step`
  - 今まさに実行中の step 名
- `completed_steps`
  - 完了済み step 名の一覧
- `steps[]`
  - 各 step の `status`, `started_at`, `finished_at`, `row_count`, `max_trade_date`, `details`

### step の読み方
- `source` step
  - `daily_bars`
  - `monthly_bars`
  - `daily_ma`
  - `feature_snapshot_daily`
  - `positions_live`
  - `position_rounds`
- `export` step
  - `bars_daily_export`
  - `bars_monthly_export`
  - `indicator_daily_export`
  - `pattern_state_export`
  - `trade_event_export`
  - `position_snapshot_export`
  - `meta_export_runs`

### 判断ルール
- `bars_daily_export`
  - 最初の重い step。
  - `current_step` が長時間このままでも、DB/WAL サイズや process CPU が増えていれば即 blocker とはみなさない。
  - ただし step 内進捗は coarse なので、ETA は artifact 単体では出せない。
- `indicator_daily_export`
  - `daily_ma` / `feature_snapshot_daily` 由来の export 段。
  - `bars_daily_export` 完了後にここで長時間止まる場合は、指標生成が主ボトルネック候補。
- `pattern_state_export`
  - `pattern_count` は required。
  - ここが未完了なら reusable 判定は `complete` にならない。
  - `bars_count` や `indicator_count` が揃っていても、pattern が 0 のままなら先へ進まない。

## `snapshot_status.json` の見方
### 主要項目
- `status`
  - `incomplete`: build 途中、または final verification 未通過
  - `failed`: build 失敗
  - `complete`: reusable snapshot として確認済み
- `reason_code`
  - `export_incomplete`: build 未完了
  - `meta_missing`: `meta_export_runs` 不在
  - `required_count_mismatch`: required count 不一致
  - `max_trade_date_mismatch`: `max_trade_date` 不一致
  - `source_signature_mismatch`: source 側と export 側が別物
  - `complete_match`: reusable 判定成功
- `source_signature`
- `export_signature`
- `source_counts`
- `export_counts`
- `required_fields`

### 判断ルール
- `status=incomplete` かつ `reason_code=export_incomplete`
  - 正常な build 中の可能性が高い。
- `status=incomplete` かつ `reason_code=meta_missing`
  - export table は進んでいても final success 扱いではない。
  - partial export を reusable とみなさない。
- `status=complete` かつ `reason_code=complete_match`
  - 次段へ進んでよい。

## probe による再確認
### 実行
```powershell
@'
from external_analysis.exporter.snapshot_status import probe_export_snapshot_readiness
result = probe_export_snapshot_readiness(
    r"C:\work\meemee-screener\.local\meemee\research_db\stocks_research_20230101_20260226.duckdb",
    r"C:\Users\enish\AppData\Local\Temp\tradex_full_universe_confirm7\full_universe_export.duckdb",
)
print(result)
'@ | python -
```

### 見る項目
- `status`
- `reason_code`
- `progress_status`
- `progress_path`
- `last_completed_step`
- `incomplete_steps`

## `complete` 後の次手順
1. `snapshot_progress.json.status=complete` を確認する。
2. `snapshot_status.json.status=complete` を確認する。
3. `probe_export_snapshot_readiness(...).status=complete` と `reason_code=complete_match` を確認する。
4. 同じ source DB / export DB を使って `image-rerank-research-run` を実行する。

```powershell
python -m external_analysis image-rerank-research-run `
  --source-db-path C:\work\meemee-screener\.local\meemee\research_db\stocks_research_20230101_20260226.duckdb `
  --export-db-path C:\Users\enish\AppData\Local\Temp\tradex_full_universe_confirm7\full_universe_export.duckdb `
  --session-id full-universe-20260327-v7 `
  --top-k 10 `
  --renderer-backend agg
```

## やらないこと
- 実行中 output の delete / move / overwrite
- 実行中 process の restart
- incomplete snapshot を前提に `image-rerank-research-run` を先行実行
- compare 条件や lever の見直し
