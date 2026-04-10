# TRADEX Research Speed Benchmark

- measured_at: `2026-04-09` and `2026-04-10`
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

## Baseline

| lane | elapsed_sec | status | sample_count | eval_window_mode | selected_universe |
| --- | ---: | --- | ---: | --- | ---: |
| single `benchr2` | 71.143 | `complete` | 267 | `fallback` | 4 |
| stability `benchs2` | 107.585 | `complete` | 267 / 267 | `fallback` / `fallback` | 4 / 4 |
| single resume `benchr2` | 28.635 | `complete` | reused | reused | reused |
| stability resume `benchs2` | 19.490 | `complete` | reused | reused | reused |

## After Rerun No-Op

| lane | elapsed_sec | delta_vs_baseline | improvement |
| --- | ---: | ---: | ---: |
| single resume `benchr2` | 17.769 | -10.866 | 37.9% |
| stability resume `benchs2` | 4.652 | -14.838 | 76.1% |

## 観測

- 単発 session baseline は約 71 秒、2-seed stability sweep baseline は約 108 秒だった。
- rerun no-op 最適化後、2-seed stability sweep resume は 19.490 秒から 4.652 秒まで短縮した。
- single resume は 28.635 秒から 17.769 秒まで短縮したが、10 秒以下と 60% 以上短縮の目標は未達。
- 残る主ボトルネックは complete rerun でも必要な manifest 再構築、confirmed universe 判定、period segment 解決である可能性が高い。
- `session_leaderboard_rollup` と stability rollup の無駄な再生成はほぼ解消できている。

## 判定

- `stability_2seed_resume`:
  - 目標達成。40% 以上短縮かつ 10 秒以下。
- `single_resume`:
  - 改善は確認できたが未達。relative 改善は 37.9%、実測は 17.769 秒。

## 制約

- before/after は同じ DB と command shape で測ったが、working tree baseline 比較であり commit 固定の A/B ではない。
- `eval_window_mode` は baseline session も rerun も `fallback`。
- `selected_universe` は 4 まで clamp されており、広い universe 条件の benchmark ではない。
