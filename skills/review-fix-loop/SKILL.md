---
name: review-fix-loop
description: Use this for the default MeeMee maintenance loop: review, prioritize, pick one task, make the smallest safe fix, review again, validate, and decide the next task. Trigger for iterative cleanup, stabilization, and daily maintenance work. Do not use for initial broad audits without first identifying candidate issues.
---

# Review -> Fix Loop

目的は、MeeMeeの保守を小さい反復で進めること。
毎ループ1テーマだけ扱う。

## 基本フロー
1. レビュー
2. タスク化
3. 最優先1件を選ぶ
4. 最小変更で修正
5. 再レビュー
6. 検証
7. 残課題確認
8. 次タスク要否を判断

## ルール
- いきなり広範囲に書き換えない
- ついで修正を広げない
- エラーを握りつぶさない
- optional chaining や fallback だけで根本原因を隠さない
- `any` で逃げない
- viewer と研究処理の境界を壊さない
- DBに新しい恒久保存を増やす前に必要性を確認する
- 修正後は必ず再レビューする

## コマンド運用
実行コマンドは repo 内から発見する。
優先して確認:
- `package.json`
- `pyproject.toml`
- `Makefile`
- `README.md`
- `scripts/`
- CI 設定

存在しないコマンドを invent しない。

## 検証順
1. 型チェック
2. lint
3. 関連テスト
4. フロント起動確認
5. EXE化影響確認
6. 主要機能スモークテスト

## スモークテスト観点
- 初期画面
- 一覧表示
- 銘柄詳細
- 軽量チャート
- DB参照
- 検索 / 絞り込み
- お気に入り / 建玉 / メモ
- 起動時に重い解析が走らないこと

## 停止条件
- 同じ失敗が2回続く
- 環境依存で再現できない
- 影響範囲が想定以上に広い
- 局所修正より分離設計見直しが先と判明した

## 出力形式
## 現在の観測
- 今見えている問題
- 層
- 影響範囲

## タスク一覧
- 候補1
- 候補2
- 候補3

## 今回着手する1件
- なぜそれを選ぶか
- 期待効果
- 想定副作用

## 修正内容
- 何を変えたか
- なぜ最小変更か

## 再レビュー結果
- 直った点
- 残るリスク
- 追加で見るべき点

## 検証結果
- 実行した確認
- 成功 / 失敗
- 失敗なら次課題化

## 残課題
- まだ残るもの
- 次ループ候補

## 次ループ要否
- 要 / 不要
- 理由
