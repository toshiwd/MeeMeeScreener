# Publish Promotion / Rollback

この文書は、TradeX で検証したロジックを MeeMee Screener の runtime に昇格・退役させる最小仕様を定義する。

## 目的

MeeMee Screener は「使う製品」であり、TradeX は「育てる製品」である。したがって、ロジックの採用可否は TradeX の検証結果を根拠にしつつ、実際の runtime の採用状態は MeeMee 側の publish registry で管理する。

ここでいう publish promotion は、`logic_id:logic_version` を champion に昇格させる操作である。rollback は、現在の champion を前の champion へ戻す操作である。demotion は、特定の logic を champion / challenger から外して retired にする操作である。

## State Model

publish registry は `config/publish_registry.json` に保存する。最低限の役割は次の 4 つである。

- `champion`: 現在採用中の logic
- `challenger`: 候補として追跡する logic
- `retired`: 採用対象から外した logic
- `blocked`: 失敗した validation または checksum 不一致などで採用できない logic

identity は `logic_id:logic_version` とする。`artifact_uri` は locator であり、identity にはしない。

## Comparison Criteria

promotion の判断では、TradeX 側の review data を使う。最低限の確認項目は次の通りである。

- `readiness_pass` が true
- `sample_count` が十分であること
- `expectancy_delta` が 0 以上であること
- `improved_expectancy` が true であること
- `mae_non_worse` が true であること
- `adverse_move_non_worse` が true であること
- `stable_window` が true であること
- `alignment_ok` が true であること

必要に応じて `external_promotion_decisions` の最新決定も参照する。`rejected` の決定が付いている場合は promotion を止める。

## Promotion / Demotion / Rollback

promotion は、検証済みの challenger を champion に昇格させる。promotion 成功時は、`default_logic_pointer` をその logic_key に更新する。runtime selection はこの default pointer を見て採用候補を決める。

demotion は、特定 logic を retired にする。対象が current champion の場合は、前の champion を復元できるなら rollback として扱う。復元できない場合は champion を空にせず、runtime selection の safe fallback へ落ちる。

rollback は、current champion を `previous_champion_logic_key` に戻す操作である。rollback も audit を残す。

## Runtime Selection との関係

runtime selection の解決順は変えない。

1. `selected_logic_override`
2. `default_logic_pointer`
3. `last_known_good`
4. `safe fallback`

publish promotion が更新するのは主に `default_logic_pointer` である。override が存在する場合は override が優先される。`last_known_good` は復旧用の local cached artifact であり、promotion の source of truth ではない。

`/api/system/runtime-selection` は次を返す。

- `resolved_source`
- `selected_logic_id`
- `selected_logic_version`
- `logic_key`
- `artifact_uri`
- `snapshot_created_at`
- `override_present`
- `last_known_good_present`
- `validation_state`
- `publish_registry`
- `publish_registry_state`

`publish_registry_state` は runtime 向けの派生ビューであり、`champion_logic_key`、`challenger_logic_key`、`default_logic_pointer` を含む。

## Audit

promotion / demotion / rollback の操作は `runtime_selection/publish_promotion_audit.jsonl` に記録する。少なくとも次の情報を残す。

- `previous_logic_key`
- `new_logic_key`
- `changed_at`
- `source`
- `reason`

## Runtime Files

- `config/logic_selection.json`: override と last_known_good の保存先
- `config/publish_registry.json`: champion / challenger / retired の状態モデル
- `runtime_selection/logic_selection_audit.jsonl`: runtime selection の変更監査
- `runtime_selection/publish_promotion_audit.jsonl`: promotion / demotion / rollback の監査

## Source Of Truth

The authoritative publish registry now lives in `external_analysis` result DB tables. MeeMee keeps `config/publish_registry.json` only as a mirror and fallback copy. If external_analysis is unavailable, MeeMee may read the mirror for continuity, but it must not claim a local-only promotion as successful.
