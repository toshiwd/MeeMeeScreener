# TRADEX Trader Foundation

## 目的

TRADEX Research OS を、ハイブリッドなトレーダー判断モデルの研究土台として固定する。

この層の責務は次の 3 artifact を正本化すること:

- `observation_snapshot.json`
- `strategy_judgement.json`
- `teacher_evaluation_row.json`

外側の研究制御は既存 Research OS が担当する。

- preflight
- single-session execution
- family-level `compare.json` を judge truth とする比較
- provisional `judge_decision.json`
- authoritative `authoritative_decision.json`
- `research_memory.json`

## 入力方針

- primary input mode は structured-led
- screenshot は v1 では補助監査用途のみ
- close-based judgement でも close-only にはしない
- confirmed OHLC / volume / moving average / gap / wick / range / breakout / sequence を含める

## Observation Snapshot 契約

`observation_snapshot.json` の v1 必須要素:

- `target`
- `confirmed_bar`
- `recent_bars`
- `derived_features`
- `market_context`
- `lineage`

解決ルール:

- `strategy_target.as_of_date` と target bar は完全一致で解決する
- silent fallback で別日を使わない
- source of truth は screenshot ではなく `daily_bars`

## Strategy Judgement 契約

`strategy_judgement.json` の v1 で固定する top-level semantics:

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
- `adapter_outputs`

top-level judgement は `primary_adapter_id` の出力を mirror する。

## Adapter 境界

adapter interface は 1 つに固定する。

- input: validated `observation_snapshot`
- output: validated adapter output row

v1 adapter:

- `numeric_baseline_v1`
- `structured_reasoner_v1`

`structured_reasoner_v1` は OpenAI-compatible endpoint を使う実 LLM adapter。
設定は env で行う。

- `TRADEX_TRADER_LLM_ENDPOINT_URL`
- `TRADEX_TRADER_LLM_MODEL`
- `TRADEX_TRADER_LLM_API_KEY`
- `TRADEX_TRADER_LLM_TIMEOUT_SEC` (optional)

LLM が未設定または invalid output を返した場合は explicit failure とし、baseline へ silent fallback しない。

## Teacher Evaluation 契約

`teacher_evaluation_row.json` は raw realized outcome を正本として保持する。

- thresholded label は v1 では作らない
- replay-safe lineage を必須にする
- observation snapshot hash と strategy judgement hash を必ず持つ

## 非対象

この v1 では次をやらない。

- image-first judge
- MeeMee integration
- compare contract の変更
- decision policy の変更
- multi-session aggregate judge
- regime model / setup taxonomy / symbol-specific adjustment の本体実装
