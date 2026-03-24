# app/backend AGENTS.md

## 責務
- API 契約、ユースケース、データ境界の維持を担う。
- api/routes は HTTP 入出力変換と検証に閉じる。
- services はユースケースの orchestration を担う。
- domain は業務ルールを担う。
- core は共通設定・共通契約・共通基盤を担う。
- infra は DB、外部 I/O、永続化を担う。
- まず原因を routes / services / domain / core のどこに置くべきか切り分ける。

## 依存関係
- api/routes -> services は可。
- services -> domain は可。
- services -> infra は可。
- core は共通基盤として使えるが、業務ルールを置かない。
- domain -> infra/api は不可。
- api/routes -> domain/infra の直参照は不可。
- 横断 import と循環依存は禁止。
- 「backend 内だから自由に参照してよい」と読める余地を残さない。

## 禁止
- UI 都合の条件分岐を backend に増やす。
- DB アクセスと業務ルールを混在させる。
- 例外握りつぶしで一時回避する。
- 返却スキーマ変更の破壊範囲を曖昧にする。
- services を何でも入れる置き場にする。
- domain に I/O、HTTP、DB、ファイル、UI 文脈を持ち込む。

## 変更前
- 症状、原因仮説、変更対象、非対象、影響範囲、回帰リスク、検証手順を整理する。
- routes / services / domain / core / infra のどこに原因があるかを先に決める。
- public interface 変更や 3 ファイル以上の変更は plan 先行。

## 計画が必要な時
- API 契約を変える時。
- routes、services、domain、infra をまたぐ時。
- shared type 変更が入る時。
- 返却スキーマやエラー形式を変える時。
- DB スキーマや migration が絡む時。

## 停止条件
- 原因が未確定のまま層をまたいで試行錯誤するしかない時。
- 例外握りつぶし以外の選択ができない時。
- 循環依存や横断 import を増やすしかない時。
- 検証で失敗しても根因切り分けが進まない時。

## 検証
- import の向き、契約、主要な副作用を確認する。
- 必要な unit / integration / import check を実行する。
- 未検証は未検証と明記する。
- テストが落ちたまま次へ進まない。

## Done
- 契約と依存方向が壊れていない。
- 破壊範囲を説明できる。
- 検証結果と残件を明記した。
