# TRADEX Trader Benchmark

## 目的
TRADEX Trader Foundation が生成する 3 artifact を benchmark row に materialize し、adapter ごとの比較指標を出す。

入力 source of truth は次だけ。

- `observation_snapshot.json`
- `strategy_judgement.json`
- `teacher_evaluation_row.json`

lineage 補助として `judge_input.json` を読むことはあるが、compare truth や decision policy は変更しない。

## canonical outputs

- `keep/research_os/trader_benchmark/v1/trader_benchmark_manifest.json`
- `keep/research_os/trader_benchmark/v1/trader_benchmark_rows.jsonl`
- `keep/research_os/trader_benchmark/v1/trader_adapter_scoreboard.json`

## row unit

benchmark row の単位は `1 experiment × 1 adapter`。

主キーは次で固定する。

- `experiment_id`
- `adapter_id`
- `observation_snapshot_hash`

## row fields

最低限の row field は次。

- lineage
  - `experiment_id`
  - `hypothesis_id`
  - `family_id`
  - `method_family`
  - `as_of_date`
  - `code`
- adapter judgement
  - `adapter_id`
  - `machine_action_state`
  - `human_readable_judgement`
  - `buy_score`
  - `environment_score`
  - `trend_score`
  - `trigger_score`
  - `risk_score`
  - `invalidation_price`
  - `invalidation_reason_code`
  - `reason_codes`
  - `confidence`
  - `is_primary_adapter`
- realized outcome
  - `teacher_horizon_bars`
  - `future_bar_count`
  - `complete_horizon`
  - `anchor_close_price`
  - `next_open_price`
  - `final_close_price`
  - `return_close_basis`
  - `return_next_open_basis`
  - `max_favorable_excursion_close_basis`
  - `max_adverse_excursion_close_basis`
- label policy output
  - `close_positive_20`
  - `next_open_positive_20`
  - `mfe_ge_10pct_20`
  - `mae_worse_than_7pct_20`
  - `judgement_outcome_class`
  - `label_policy_version`
- artifact references
  - `observation_snapshot_hash`
  - `strategy_judgement_hash`
  - `teacher_evaluation_row_hash`

## scoreboard

`trader_adapter_scoreboard.json` は adapter ごとの summary を出す。

continuous 指標:

- `sample_count`
- `complete_horizon_count`
- `labeled_sample_count`
- `primary_count`
- `enter_count`
- `wait_count`
- `skip_count`
- `avg_buy_score`
- `avg_confidence`
- `avg_return_close_basis_all`
- `avg_return_close_basis_enter`
- `median_return_close_basis_enter`
- `avg_return_next_open_basis_enter`
- `avg_mfe_enter`
- `avg_mae_enter`

label 指標:

- `close_positive_rate_all`
- `close_positive_rate_enter`
- `next_open_positive_rate_enter`
- `mfe_ge_10pct_rate_enter`
- `mae_worse_than_7pct_rate_enter`
- `good_outcome_rate_enter`
- `bad_outcome_rate_enter`

`enter` 系 rate は `machine_action_state == "enter"` の row のみで集計する。

## skip rules

次は row 化しない。

- preflight failed experiment
- trader foundation artifact 不足
- malformed artifact
- label inputs incomplete

`complete_horizon = false` は skip しない。row は残し、label は `null` / `incomplete` にする。

## CLI

```powershell
python -m app.backend.tools.tradex_research_os_cli rebuild-trader-benchmark
```

v1 は full rebuild のみ。partial rebuild と incremental append は後回しにする。
