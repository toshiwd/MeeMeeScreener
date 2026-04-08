# MeeMee Product Boundary

## Purpose

MeeMee は確認画面であり、TRADEX は研究・検証・比較の層である。

## Boundary Rules

- MeeMee は publish 済みの結果だけを読む。
- TRADEX の研究途中の内部事情は MeeMee に持ち込まない。
- `confirmed` は分析・ランキングの基準、`provisional` は表示補助、`research-only` は TRADEX 内部に閉じる。
- MeeMee 側の UI には研究用の追加面を増やさない。

## Source Of Truth

- データ契約の正本は `docs/architecture/DATA_CONTRACTS.md`。
- runtime 選択の正本は `docs/architecture/RUNTIME_SELECTION.md`。
- publish 境界の正本はこのファイル。
- 画面ごとの責務は `docs/pages/*.md` と `docs/features/*.md` に分ける。

## V1 Policy

- MeeMee v1 は `ranking / detail / positions` の確認導線を壊さないことを優先する。
- TRADEX v1 は研究 artifact と publish artifact の分離を締めることを優先する。
- どちらの層でも、未確定データを source of truth として扱わない。
