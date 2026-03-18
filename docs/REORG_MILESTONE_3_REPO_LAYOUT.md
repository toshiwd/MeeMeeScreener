# Reorg Milestone 3: Repo Layout

## Goal

`C:\work\meemee-screener` のトップレベル構成を整理し、production path と非 production path を分離する。実装者はこの文書を見れば「そのファイルをどこへ置くべきか」を決定できる状態にする。

## Final Top-Level Layout

最終形は次で固定する。

- `app/`
  - MeeMee 本体のみ
- `external_analysis/`
  - 解析 runtime と internal store 管理のみ
- `docs/`
  - 正本仕様、runbook、移行文書
- `tests/`
  - 自動テスト
- `fixtures/`
  - テスト fixture
- `resources/`
  - 配布・静的資産
- `build/`, `release/`
  - ビルド成果物
- `scripts/`
  - 一回限りの補助スクリプト
- `tools/`
  - 手動ツール、調査補助

下記は production path に含めない。

- `research/`
- `research_workspace/`
- `tmp/`
- `output/`
- `published/`
- repo 直下の ad-hoc `.txt`, `.duckdb`, `.json` 試験物

## Migration Rules

配置変更のルールは次で固定する。

- 本体必須ロジックは `app/` か `external_analysis/` に昇格する。
- 再利用されない検証コードは `scripts/` へ下げるか削除する。
- 実験結果ファイルは repo 直下に置かない。
- runbook と仕様は `docs/` に集約する。
- live data path は repo 内ではなく data override で管理する。

## Cleanup Targets

後続で整理対象とするものは次である。

- `research/` 内の本体非依存コード
- repo 直下の `test_debug_db*.duckdb`
- repo 直下の単発レポート txt
- `tmp/`, `output/`, `published/` の古い生成物

## Acceptance

この milestone の完了条件は次である。

- top-level の各ディレクトリに明確な役割がある。
- production path と非 production path が分かれる。
- 新規コードをどこへ置くべきかで implementer が迷わない。

