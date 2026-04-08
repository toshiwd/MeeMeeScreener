# Ranking Feature

## Goal

ランキングは MeeMee の確認用一覧であり、売買判断の研究画面ではない。

## Rules

- ranking は confirmed data を基準にする。
- detailed reason は一覧と詳細で同じ根拠を参照する。
- provisional は表示補助としてのみ扱う。
- ranking の並び順や score 定義は勝手に変えない。

## Regression Targets

- 一覧順位と詳細理由の不一致を防ぐ。
- as-of のズレを防ぐ。
- publish 済み logic と表示結果の不一致を防ぐ。
