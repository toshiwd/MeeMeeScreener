# Reorg Milestone 2: Runtime Boundary

## Goal

MeeMee 本体、external_analysis、research、scripts、tools の責務境界を固定する。ここで決めることは「どのコードが production path で、どのコードが実験・補助・廃止候補か」である。

## Main App Boundary

`app/` は MeeMee 本体だけに限定する。許容する責務は次で固定する。

- desktop launcher と設定読込み
- backend API
- frontend UI
- 本体 DB の軽量 CRUD
- `result DB only` の read-only bridge
- graceful degrade
- favorites / practice / positions / memo / event 閲覧

本体に禁止する責務は次で固定する。

- 重い特徴量生成
- `ml_feature_daily` のような全銘柄日次学習特徴生成
- 長時間学習
- replay / walk-forward / promotion gate
- similarity challenger shadow
- feature / label / ops / model registry の直接管理

## External Analysis Boundary

`external_analysis/` は解析の唯一の実装場所である。ここに置く責務は次で固定する。

- source export
- JPX calendar
- rolling labels / anchor windows
- candidate baseline / nightly metrics
- similarity champion / challenger / replay / review artifact
- ops / retry / quarantine / readiness / promotion evidence

本体へ返すのは public result DB だけであり、internal store を本体へ見せない。

## Research Boundary

`research/` は production path に含めない。ここは以下のいずれかで整理する。

- external_analysis へ昇格するもの
- `scripts/` / `tools/` へ格下げするもの
- 削除するもの

`research/` 配下のコードが本体起動や日次更新に必須であってはいけない。

## Scripts And Tools Boundary

`scripts/` は一回限りの補助処理に限定する。live path で常時呼ばれるものはここに置かない。

`tools/` は開発補助、メンテナンス、検証補助に限定する。production service の本体経路を持たせない。

## Legacy Runtime

旧解析 worker と旧 ML service は compatibility 停止対象である。残す場合も次を守る。

- 本体主要導線から参照しない
- 再生成経路を stop できる
- main DB を膨らませない
- graceful degrade で欠落を吸収できる

## Acceptance

この milestone の完了条件は次である。

- implementer が各ディレクトリの役割を迷わない。
- `app/` に残してはいけない処理が明記されている。
- `external_analysis/` の唯一責務が明記されている。
- `research/`, `scripts/`, `tools/` の整理基準が明記されている。

