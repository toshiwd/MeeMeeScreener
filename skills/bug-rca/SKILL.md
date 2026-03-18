---
name: bug-rca
description: Use this when a single bug or error message needs root-cause analysis before any code change. Trigger for stack traces, undefined/null crashes, wrong data shape issues, one-screen failures, and API contract mismatches. Do not use for broad performance audits, packaging checks, or repo-wide refactors.
---

# Bug RCA

目的は、単発不具合の原因特定と最小修正方針の計画。
このスキルでは、まず原因を絞る。まだ修正しない。

## 入力として期待するもの
- エラーメッセージ
- ログ / スタックトレース
- 優先的に読むファイル
- 発生条件
- 再現手順があればそれも使う

## 必須手順
1. 症状を一文で言い換える
2. スタックトレースから起点を確認する
3. 指定ファイルを優先順で読む
4. 必要に応じて import元 / 呼び出し元 / 型定義 / レスポンス整形箇所へ広げる
5. 「どの値が undefined/null になりうるか」を特定する
6. 発生源、伝播経路、参照地点を分けて整理する
7. 原因トップ3を確信度順に並べる
8. 最有力原因を第一候補として明示する
9. 最小修正方針を提示する
10. まだコードは書かない

## 優先して読む順番
- Frontend 起点の不具合:
  - `route/component`
  - `hook/store`
  - `api.ts` や API 呼び出し箇所
  - 型定義、schema、レスポンス整形
- Backend 起点の不具合:
  - `router`
  - `service`
  - `repo` / `infra`
  - schema、DTO、永続化境界

## 重点観点
- APIレスポンスの型と実データ形状の不一致
- hook / state / context / return value の初期値不整合
- 呼び出し側の前提崩れ
- `optional` 値へのガード漏れ
- 想定オブジェクトのネスト階層違い
- 非同期完了前の参照
- 保存済みデータの欠損
- 例外を catch して壊れた state で継続していないか

## 禁止事項
- いきなり修正しない
- 推測だけで断定しない
- optional chaining を足すだけの雑な結論にしない
- 型だけ合わせて runtime の破綻を見逃さない

## 出力形式
## 現在の症状まとめ
- 発生条件
- どの値が undefined/null の可能性が高いか
- 想定される呼び出し経路

## 考えられる原因トップ3（確信度順）
- 原因候補
- 確信度（高 / 中 / 低）
- 根拠
- 反証ポイント

## 検証すべきポイント・追加で読みたいファイル
- 次に見るべきこと
- 追加ファイル
- 目的

## 推奨修正方針（最小変更で済む順）
- 最小変更案
- 安全性重視案
- 必要なら型見直し案
- この段階ではコードは書かない

## 予想される副作用・注意点
- 壊れうる周辺
- 互換性への影響
- 再現確認で見る点

最後に、最有力原因を「第一候補」として1つだけ明示する。
