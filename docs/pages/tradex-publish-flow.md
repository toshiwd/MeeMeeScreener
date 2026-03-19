# TradeX Publish Flow

このページは、TradeX で研究したロジックを MeeMee Screener に載せる最小運用フローを示す。

## Flow

1. TradeX で research を行う。
2. backtest / replay / walk-forward / live-like review を行う。
3. champion と challenger を比較する。
4. promotion 条件を満たしたら、内部 API から promote する。
5. MeeMee Screener の runtime selection は default pointer を経由して新しい champion を読む。
6. 問題が見つかったら rollback する。
7. 重大な失敗は demotion して retired にする。

## User-visible rules

- ranking は「見る順番」であり、execution とは別である。
- provisional intraday data は表示補助だけで、analysis の基準にはしない。
- confirmed data は解析基準である。
- published logic artifact は宣言的成果物であり、MeeMee はそれを local confirmed data に適用する。

## Operational signals

promotion が通ると、`/api/system/runtime-selection` の `resolved_source` は通常 `default_logic_pointer` になる。
rollback が通ると、`default_logic_pointer` が前の champion に戻る。

audit は `runtime_selection/publish_promotion_audit.jsonl` に残る。

