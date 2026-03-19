# Tradex Publish Flow

## 目的

TradeX の研究成果を MeeMee に安全に公開する境界を固定する。

## 基本ルール

- MeeMee は publish 済みロジックだけを読む
- TradeX の研究中成果物を MeeMee に直接混ぜない
- publish 前の成果物は MeeMee から不可視にする

## 責務分離

- artifact: 研究や検証の実体
- manifest: どれを公開対象にしたかを示す最小メタデータ
- result: MeeMee が読む公開済み結果

`logic_manifest` は論理名として扱い、既存実装では `publish_manifest` 相当の役割を持つ。

## publish 方針

- 当面は手動 publish とする
- publish は validation を通したものだけを採用する
- MeeMee 側は latest successful publish だけを読む

## 読み取り境界

- MeeMee は publish 済みの artifact と manifest を参照する
- ranking_output / published_ranking_snapshot は監査・比較用の補助であり、解析基準そのものではない
- 未 publish の比較結果や途中経過は表示しない

## 運用上の注意

- publish 失敗を成功として扱わない
- manifest 不整合は公開しない
- stale や missing のときは graceful degrade に落とす

## Open Question / TODO

- 手動 publish のオペレーション手順をどこに置くか
- 自動 publish へ移行する条件
