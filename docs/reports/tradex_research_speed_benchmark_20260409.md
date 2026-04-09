# TRADEX Research Speed Benchmark

- measured_at: `2026-04-09`
- code_revision: `working tree`
- stocks_db_path: `C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb`
- runtime_root: `G:\Tradex`
- env:
  - `MEEMEE_DISABLE_LEGACY_ANALYSIS=0`
  - `STOCKS_DB_PATH=C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb`

## 実測条件

- 単発 session:
  - command: `python -m app.backend.tools.tradex_research_runner --session-id benchr2 --random-seed 7 --universe-size 20 --max-candidates-per-family 1 --ret20-source-mode precomputed`
- 2-seed stability sweep:
  - command: `python -m app.backend.tools.tradex_research_runner --session-id benchs2 --random-seed 7 --stability-sweep --stability-seeds 7,11 --universe-size 20 --max-candidates-per-family 1 --ret20-source-mode precomputed`

## 実測結果

| lane | elapsed_sec | status | sample_count | eval_window_mode | selected_universe |
| --- | ---: | --- | ---: | --- | ---: |
| single `benchr2` | 71.143 | `complete` | 267 | `fallback` | 4 |
| stability `benchs2` | 107.585 | `complete` | 267 / 267 | `fallback` / `fallback` | 4 / 4 |
| single resume `benchr2` | 28.635 | `complete` | reused | reused | reused |
| stability resume `benchs2` | 19.490 | `complete` | reused | reused | reused |

## 観測

- 単発 session は約 71 秒で完了した。
- 2-seed stability sweep は約 108 秒で完了した。
- 2 session 合計でも単純 2 倍にはならず、shared rollup 遅延最終化と in-memory cache の効果が出ている可能性が高い。
- resume path でも単発 28.6 秒、2-seed sweep 19.5 秒を要した。
- 次の主ボトルネック候補は、完了済み session に対する `family_leaderboard` / `session_leaderboard_rollup` 再生成である。

## 制約

- before/after 比較ではなく current code の単独実測である。
- `eval_window_mode` は両測定とも `fallback` で、standard window 条件は満たしていない。
- `selected_universe` は 4 まで clamp されており、広い universe 条件の benchmark ではない。
