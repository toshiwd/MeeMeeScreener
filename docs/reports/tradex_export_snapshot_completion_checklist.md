# TRADEX Export Snapshot Completion Checklist

## 目的
- real full source の export 完了後に、confirm から first analysis artifact 到達までを迷わず進める。
- ここでは到達確認だけを扱う。
- `keep/drop/hold` は扱わない。

## 前提
- source DB と export DB は同一 run の組み合わせを使う。
- compare 条件、split contract、challenger lever は変えない。
- `challenger_kind=image_rerank_rank_improver`
- `base_weight=0.70`
- `image_weight=0.30`

## 手順
1. `snapshot_progress.json.status=complete` を確認する。
2. `snapshot_status.json.status=complete` を確認する。
3. `probe_export_snapshot_readiness(...).status=complete` と `reason_code=complete_match` を確認する。
4. `image-rerank-research-run` を実行する。
5. `full_universe_confirm.json.ok=true` を確認する。
6. `challenger_first_analysis.json` の生成を確認する。
7. `run.json` / `split.json` / `phase3_compare.json` の生成を確認する。

## 実行コマンド
```powershell
python -m external_analysis image-rerank-research-run `
  --source-db-path C:\work\meemee-screener\.local\meemee\research_db\stocks_research_20230101_20260226.duckdb `
  --export-db-path C:\Users\enish\AppData\Local\Temp\tradex_full_universe_confirm7\full_universe_export.duckdb `
  --session-id full-universe-20260327-v7 `
  --top-k 10 `
  --renderer-backend agg
```

## 到達確認項目
### confirm
- `full_universe_confirm.json`
  - `ok=true`
  - `confirm_stage=complete`
  - `blocked_before_confirm=false`

### analysis
- `challenger_first_analysis.json`
  - file が存在する
  - `analysis_run_id` が入っている
- `run.json`
  - file が存在する
- `split.json`
  - file が存在する
- `phase3_compare.json`
  - file が存在する

## 失敗時の扱い
- `snapshot_progress.json.status!=complete`
  - export build 継続中または失敗。analysis へ進まない。
- `snapshot_status.json.status!=complete`
  - reusable snapshot ではない。analysis へ進まない。
- `probe_export_snapshot_readiness(...).status!=complete`
  - typed reason を確認し、blocker として扱う。
- `full_universe_confirm.json.ok=false`
  - `blocker_reason_code` と `export_probe` を正本にして止める。
- `challenger_first_analysis.json` が無い
  - confirm success 後の analysis 未到達。compare 改善議論へ進まない。

## やらないこと
- `keep/drop/hold` 判定
- compare 条件の変更
- lever 追加
- fallback scope での再実行
