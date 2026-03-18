---
name: exe-release-check
description: Use this before or after changes that may affect MeeMee desktop packaging, startup size, bundled assets, runtime dependencies, or offline behavior. Trigger for EXE release readiness and packaging sanity checks. Do not use for generic feature debugging unless packaging is part of the risk.
---

# EXE Release Check

目的は、MeeMeeのEXE配布性を守ること。
変更が desktop packaging, 起動サイズ, 同梱物, 実行依存, オフライン挙動を悪化させていないか確認する。

## 優先して読むファイル
- `app/desktop/launcher.py`
- `build_release.cmd`
- `README.md`

## 必須手順
1. 実行系依存と開発依存を分けて確認する
2. 新しい依存追加が本当に必要か確認する
3. 研究用依存や検証用ファイルが配布物に混ざっていないか確認する
4. 起動時に読むモジュールと資産が増えすぎていないか確認する
5. 大きい同梱物、不要データ、巨大キャッシュを洗う
6. オフラインでも成立する導線を確認する
7. WebView2、`.NET 4.8`、backend health、bundling を含めて EXEビルド / 起動 / 主要導線の確認結果をまとめる

## 重点観点
- 不要依存の追加
- 開発専用依存の runtime 混入
- 同梱ファイルの肥大化
- 研究用ファイルの混入
- 起動に不要なアセット読込
- ビルド手順の破綻
- オフライン時の壊れ方
- ローカルDB前提導線の確認
- 初期起動時間の悪化
- WebView2 ランタイム前提
- `.NET Framework 4.8` 前提
- backend health チェックの破綻

## 禁止事項
- 配布のために本体設計を無理に複雑化しない
- 検証成果物やログをそのまま同梱しない
- runtime 問題を packaging 設定だけで隠そうとしない

## 出力形式
## 配布リスクの観測
- 何が悪化しそうか
- どこに出ているか

## 依存関係チェック
- 新規依存
- 削れる依存
- runtime / dev の混入有無

## 同梱物チェック
- 不要に大きいもの
- 研究用 / 検証用混入物
- 削除候補

## 起動・実行チェック
- 起動確認
- 初期画面
- 主要導線
- オフライン影響

## 今回の是正候補
- 候補1
- 候補2
- 候補3

## 推奨1手
- 先にやる変更
- 効果
- 副作用

## リリース可否の見立て
- 可 / 条件付き / 不可
- 理由
