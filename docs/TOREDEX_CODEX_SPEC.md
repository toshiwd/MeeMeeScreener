# TOREDEX CODEX Spec (Phase1)

本ファイルは TOREDEX Phase1 の固定仕様を定義する。

## Scope

- 手動 `run_live` のみ実装する。
- 定時実行（19:30の外部スケジューラ）は Phase2 で実装する。
- Blind backtest / narrative / monthly summary は Phase2 以降。

## Runtime

- 推奨実行時刻: 19:30 (JST)
- 実行順:
  1. TXT 更新
  2. MeeMee 更新
  3. TOREDEX run_live

## Runs Path Resolution

論理パス: `runs/<season_id>/...`

実体パス優先順位:

1. `config.runsDir`
2. `TOREDEX_RUNS_DIR`
3. `MEEMEE_DATA_DIR/runs`
4. `<repo>/.local/meemee/runs`

## Field Mapping (Fixed)

- `revRisk = mlPTurnDownShort` (欠損時 `mlPDownShort`)
- `regime`:
  - `trendUpStrict=true` -> `UP`
  - `trendDownStrict=true` -> `DOWN`
  - `trendUp=true and trendDown=false` -> `UP_WEAK`
  - `trendDown=true and trendUp=false` -> `DOWN_WEAK`
  - その他 -> `RANGE`
- `gate`:
  - `ok = entryQualified`
  - `reason`:
    - `ok=true` -> `ENTRY_OK`
    - `ok=false` -> `SETUP_<setupType>`

## Decision Hash

- canonical JSON を SHA-256 化する。
- Canonicalization:
  - object key は昇順
  - 配列順序は保持
  - `null` は除外
  - float は小数点以下6桁に丸める
  - `createdAt` 等の実行時刻・環境依存値は hash 対象外

## Determinism

- 同一 snapshot から同一 decision を出力する。
- 同一日 rerun は idempotent とし、hash 不一致時は `K_POLICY_INCONSISTENT` で停止する。
