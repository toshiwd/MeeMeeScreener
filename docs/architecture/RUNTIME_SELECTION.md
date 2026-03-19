# Runtime Selection v3

## 目的

MeeMee Screener が、どの logic artifact を runtime に使うかを明示的に解決するための骨組みを固定する。

## 対象

- MeeMee Screener runtime
- TradeX publish registry
- local cached artifact
- safe fallback

## 保存先

### `selected_logic_override`

- 保存先: MeeMee Screener の local config / local state
- 役割: ユーザーや運用が明示 pin した選択
- 挙動: 明示解除されるまで維持する

### `default_logic_pointer`

- 保存先: TradeX の publish registry / manifest
- 役割: 通常時の既定参照先
- 挙動: publish によって更新される

### `last_known_good`

- 保存先: MeeMee Screener の local cache
- 役割: 直近で正常に使えた immutable artifact
- 挙動: live selection が壊れた時のローカル退避先

### `safe fallback`

- 保存先: builtin / bundled safe artifact
- 役割: boot 継続用の最低限の退避先
- 挙動: source of truth ではない

## 解決順

1. `selected_logic_override`
2. `default_logic_pointer`
3. `last_known_good`
4. `safe fallback`

重要:

- 解決順は固定
- 各候補は availability / validity を満たす必要がある
- override があっても壊れていれば次に進む
- safe fallback は boot continuity 用であって、標準運用の入口ではない

## Runtime state

runtime は少なくとも次の状態を扱う。

- `selected_logic_override`
- `default_logic_pointer`
- `last_known_good`
- `available_logic_manifest`

### available_logic_manifest

- role: runtime で読める候補一覧
- source: publish registry からの読み込み
- use: override / default / lkg の可用性判定

## Semantics

- override は pin
- default は publish の既定
- lkg は local cached artifact
- snapshot は cache / audit artifact
- artifact が source of truth
- ranking snapshot は source of truth ではない

## 失敗時の振る舞い

- override が無効なら default を試す
- default が無効なら last_known_good を試す
- last_known_good が無効なら safe fallback を使う
- safe fallback も使えないなら unresolved として明示的に失敗させる

## Skeleton code

この解決順は `shared/runtime_selection.py` の pure resolver で固定する。

- I/O は持たない
- DB write は持たない
- publish は持たない
- 純粋に pointer 解決だけを行う

## 将来の拡張

- logic_family ごとの pin
- user profile ごとの pin
- device ごとの local override
- rollback 時の last_known_good 差し替え
## Publish Registry Source

Publish registry data is read from `external_analysis` first, then from the local mirror in `config/publish_registry.json`, and finally from an empty safe state. The runtime selection order itself does not change: `selected_logic_override -> default_logic_pointer -> last_known_good -> safe fallback`.
