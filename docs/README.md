# Documentation Index

このリポジトリで現在運用しているドキュメントの一覧です。

## Read Order

- まず `AGENTS.md` を読む。
- MeeMee の判断軸や画面方針を触るなら `docs/MEEMEE_PRINCIPLES.md` を読む。
- 次に変更対象が `backend` / `frontend` のどちらかなら、対応する領域 `AGENTS.md` を読む。
- 詳細仕様や Runbook が必要な時だけ `docs/CODEX.md` を開く。

## Core

- `README.md`: 起動手順、開発手順、運用上の最小ルール
- `AGENTS.md`: 作業時のグローバルガードレール
- `app/backend/AGENTS.md`: Backend 変更時のローカルルール
- `app/frontend/AGENTS.md`: Frontend 変更時のローカルルール
- `docs/MEEMEE_PRINCIPLES.md`: MeeMee 固有のプロダクト原則、AI出力原則、画面分離方針
- `docs/CODEX.md`: 必要時のみ参照する詳細仕様、Runbook、開発時の補足

## Architecture / Pages / Features

- `docs/architecture/DATA_CONTRACTS.md`: MeeMee Screener / TradeX v3 のデータ契約と境界定義
- `docs/architecture/RUNTIME_SELECTION.md`: logic pointer の解決順、保存先、safe fallback の骨組み
- `docs/pages/meemee-grid.md`: 一覧画面の軽仕様
- `docs/pages/meemee-detail.md`: 個別詳細画面の軽仕様
- `docs/pages/meemee-trades.md`: 取引履歴と日次建玉を確認するための軽仕様
- `docs/features/yahoo-provisional-overlay.md`: Yahoo 仮データの表示契約
- `docs/features/trade-history-import.md`: 楽天 / SBI 取引履歴 CSV の正規化契約
- `docs/features/event-badges.md`: 決算日、権利日などのイベントバッジ契約
- `docs/features/tradex-publish-flow.md`: TradeX 研究成果の publish/read-only 契約

## Operational

- `docs/TXT_UPDATE_RUNBOOK.md`: TXT 更新ジョブの運用手順
- `docs/EDINETDB_RUNBOOK.md`: EDINET DB 自動取得ジョブの運用手順
- `docs/EDINETDB_RUNBOOK.md` (補足): 月足ランキング連携時の `STOCKS_DB_PATH` / `EDINETDB_DB_PATH` 統一、`MEEMEE_RANK_EDINET_BONUS_ENABLED` の運用
- `SMOKE_TEST.md`: 最小回帰テスト手順
- `docs/PERF_BENCHMARK.md`: API 性能計測手順（p50/p95）

## Domain / UI

- `docs/heatmap_api.md`: ヒートマップ API の仕様
- `docs/UI_SPEC_HEADER.md`: ヘッダー UI の実装仕様
- `docs/ux/UX_POLICY.md`: UI/UX 方針
- `docs/NOTE_TRADE_REPRO_STUDY.md`: note記事から売買局面と玉操作を抽出し、再現性研究に使うためのルール整理

## Tools

- `tools/README.md`: `tools/` 配下スクリプトの説明

## Cleanup Policy

以下は履歴・一時メモ・重複ガイドとして整理済みです。

- 旧 Quickstart / Launch ガイド
- 一時検討メモ・実装計画メモ
- `docs/archive/` 配下の完了レポート類
