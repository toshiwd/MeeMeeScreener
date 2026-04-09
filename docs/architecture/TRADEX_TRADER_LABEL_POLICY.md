# TRADEX Trader Label Policy

## 目的
この文書は、TRADEX Trader Benchmark v1 が raw realized outcome からどの label を導出するかを固定する。

この layer は adapter 比較専用であり、次は変更しない。

- family-level `compare.json`
- `judge_decision.json`
- `authoritative_decision.json`
- single-session runner path

## 入力
label policy の入力は benchmark row の raw outcome のみ。

- `teacher_horizon_bars`
- `future_bar_count`
- `complete_horizon`
- `return_close_basis`
- `return_next_open_basis`
- `max_favorable_excursion_close_basis`
- `max_adverse_excursion_close_basis`

## policy source of truth

- machine-readable:
  - `config/tradex/trader_label_policy_v1.json`
- code:
  - `app/backend/services/tradex_research_trader_label_policy.py`

## horizon rule

- v1 の label は `teacher_horizon_bars = 20` を前提にする
- `complete_horizon = false` は label 未確定として扱う
- `complete_horizon = true` なのに required raw outcome が欠ける場合は benchmark rebuild で `label_inputs_incomplete` として skip する

## derived labels

- `close_positive_20`
  - `return_close_basis > 0`
- `next_open_positive_20`
  - `return_next_open_basis > 0`
- `mfe_ge_10pct_20`
  - `max_favorable_excursion_close_basis >= 0.10`
- `mae_worse_than_7pct_20`
  - `max_adverse_excursion_close_basis <= -0.07`

## judgement_outcome_class

- `good`
  - `close_positive_20 = true`
  - `mae_worse_than_7pct_20 = false`
- `bad`
  - `close_positive_20 = false`
  - または `mae_worse_than_7pct_20 = true`
- `mixed`
  - 上記の `good` / `bad` に入らない
- `incomplete`
  - `complete_horizon = false`

## non-goals

この phase では次を入れない。

- cross-sectional rank label
- `top_1pct_20`
- `top_3pct_20`
- `top_5pct_20`
- precision / recall
- keep / hold / drop の再判定

理由は、v1 の benchmark layer の source of truth が per-decision raw outcome だから。
