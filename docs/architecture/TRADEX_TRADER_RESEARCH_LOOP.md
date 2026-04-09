# TRADEX Trader Research Loop

## 目的
この文書は、foundation 完了後の daily research loop を固定する。

この loop は次の 2 コマンドで回す。

- `run-hypothesis`
- `rebuild-trader-benchmark`

## source of truth

- judge truth:
  - family-level `compare.json`
- provisional:
  - `judge_decision.json`
- authoritative:
  - `authoritative_decision.json`
- trader comparison layer:
  - `observation_snapshot.json`
  - `strategy_judgement.json`
  - `teacher_evaluation_row.json`
  - `trader_benchmark_rows.jsonl`
  - `trader_adapter_scoreboard.json`

benchmark layer は judge の source of truth ではない。adapter comparison layer である。

## daily loop

1. hypothesis を実行する

```powershell
python -m app.backend.tools.tradex_research_os_cli run-hypothesis --hypothesis-path <hypothesis.json>
```

2. trader artifact を確認する

- `observation_snapshot.json`
- `strategy_judgement.json`
- `teacher_evaluation_row.json`

3. benchmark を full rebuild する

```powershell
python -m app.backend.tools.tradex_research_os_cli rebuild-trader-benchmark
```

4. 次を読む

- `keep/research_os/trader_benchmark/v1/trader_benchmark_manifest.json`
- `keep/research_os/trader_benchmark/v1/trader_benchmark_rows.jsonl`
- `keep/research_os/trader_benchmark/v1/trader_adapter_scoreboard.json`

5. adapter ごとの差分を review する

- continuous outcome
  - `avg_return_close_basis_enter`
  - `avg_return_next_open_basis_enter`
  - `avg_mfe_enter`
  - `avg_mae_enter`
- label outcome
  - `close_positive_rate_enter`
  - `next_open_positive_rate_enter`
  - `good_outcome_rate_enter`
  - `bad_outcome_rate_enter`

## triage

- `preflight_failed`
  - upstream readiness / normalization の問題
- `missing_trader_artifacts`
  - trader foundation artifact 未生成
- `malformed_artifact`
  - artifact contract 破損
- `label_inputs_incomplete`
  - complete horizon のはずなのに label 用 outcome が欠けている

`complete_horizon = false` は skip ではない。row は残し、label は `null` / `incomplete` にする。

## non-goals

この loop では次を入れない。

- partial rebuild
- incremental append
- interactive review UI
- training pipeline
- image-assisted adapter
- multi-session aggregate judge
