# TDNET Stage 1

## Purpose

この変更で、TDNET MCP から取得した開示データを今の DuckDB に保存できる基盤を追加する。実装後は、外部取得処理が `disclosure_id`・銘柄コード・タイトル・開示日時・URL・生JSON を保存でき、詳細画面用 API から銘柄ごとの最新開示を参照できる。動作確認は、DuckDB にサンプル開示を保存して `GET /api/ticker/tdnet/disclosures?code=XXXX` がその内容を返すことで行う。

## Scope

この Stage 1 では、本文PDF解析や定期ジョブは扱わない。DuckDB の保存先は既存の `stocks.duckdb` を使う。新しく追加するのは TDNET 用テーブル、保存リポジトリ、銘柄詳細用の参照 API だけとする。

## Files

主に編集するファイルは `app/backend/api/routers/ticker.py`、`app/db/session.py` 周辺の既存 DB 利用箇所を踏まえた新規 `app/backend/tdnetdb/schema.py`、新規 `app/backend/tdnetdb/repository.py` である。必要なら `app/backend/tdnetdb/__init__.py` を追加して import を明示する。

## Milestones

### Milestone 1

TDNET 用の DuckDB テーブルを追加する。保存対象は `tdnet_disclosures` のみとし、キーは `disclosure_id`、検索用に `sec_code` と `published_at` を持たせる。完了後は新規リポジトリを経由してテーブルが自動作成される。

### Milestone 2

TDNET 開示を upsert するリポジトリを追加する。完了後は Python から `upsert_disclosures()` を呼ぶだけで同一 `disclosure_id` の再保存が上書きになる。

### Milestone 3

`/api/ticker/tdnet/disclosures` を追加する。完了後は `code` と `limit` を渡すと、その銘柄の最新 TDNET 開示一覧を JSON で受け取れる。

## Progress

- [x] 実装範囲を Stage 1 に固定した
- [x] TDNET schema を追加した
- [x] TDNET repository を追加した
- [x] Ticker API を追加した
- [x] 保存と参照の最小確認をした
- [x] JSON 取込入口を追加した
- [x] MCP コマンド取込ジョブを追加した

## Surprises & Discoveries

2026-03-09: PowerShell の標準出力では日本語タイトルが `?` 表示になったが、保存件数と API 件数は一致した。今回の確認は文字化け検証ではなく、DuckDB への保存と API 応答経路の生存確認として扱う。
2026-03-09: MCP 直結基盤は repo 内にまだ存在しなかったため、先に HTTP とローカル JSON ファイルの両方で投入できる入口を追加した。これで外部取得側は配列 JSON を作れれば取り込みできる。
2026-03-09: `str.format()` ベースのコマンドテンプレートは JSON 本文の `{}` と衝突した。`{code}` と `{limit}` の単純置換に切り替えると、JSON を含むコマンドでも安全に扱えた。

## Decision Log

2026-03-09: 初回実装は `tdnet_disclosures` だけに絞る。理由は、MCP の取得形式が固まる前に財務正規化まで進めると列設計が先走るため。

## Outcomes & Retrospective

Stage 1 として、TDNET 開示を DuckDB に保存する基盤、銘柄コード単位で最新開示を返す API、JSON 取込入口、そして MCP コマンドを実行して保存するジョブまで追加できた。まだ開示種別ごとの財務正規化とフロント表示は未実装であり、次段階で進める。
