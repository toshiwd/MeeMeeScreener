# main分割整理 & 不具合修正プラン

対象: `app/backend/main.py` を分割した結果出ている複数不具合（例: 保有銘柄が表示されない、起動経路/設定のズレ、後方互換の崩れ）。

## ゴール（Doneの定義）

- 通常起動（`run.ps1` / `python -m app.desktop.launcher`）で以下が成立
  - `Positions`（保有）に保有銘柄が表示される（Trade CSV取り込み済みの場合）
  - `Health`/`Diagnostics` 等のAPIが 500 を返さない
  - `txt_update`/`jobs` 等の主要APIが動作（少なくともエラーが分かる形で返る）
- `app/backend/main.py` に寄生した「巨大なユーティリティ群」を、用途別に分割しても機能回帰しない
- 後方互換 import（legacy tooling / tests）が壊れない
- 最低限の自動テスト（pytest）が通る

## 現状把握（観測された問題/リスク）

### 1) 設定/パス解決の二重化によるデータ参照ズレ

- Desktop Launcher は `app.backend.core.config` を使ってデータディレクトリや DB を準備し、`STOCKS_DB_PATH` などを env に設定している
- 一方 API/サービス層は `app.core.config` を参照して DB を開く箇所が多い
- `app.core.config` が `STOCKS_DB_PATH` 等を見ていないと、**別のDB（空DB）を開く** → `trades` が空 → `保有銘柄が表示されない` になり得る

暫定対応方針:
- Launcher から `MEEMEE_DATA_DIR` を必ず設定して、`app.core.config` の DataDir 解決と一致させる
- 併せて `app.core.config` 自体も `STOCKS_DB_PATH`/`FAVORITES_DB_PATH`/`PRACTICE_DB_PATH` を尊重して、分割後の環境変数運用に強くする

### 2) main分割の後方互換（import先）問題

- 旧: `app/backend/main.py` に大量の関数/変数が居た
- 新: `app.main` / `app.core.*` / `app.services.*` へ分散
- 既存の import が `from app.backend import main as main_module` に依存している箇所が多い

対応方針:
- `app/backend/main.py` は「互換レイヤ」に寄せ、実装は `app/*` に集約
- 互換レイヤは `__all__` を明示し、使ってよい API を絞る
- 破壊的変更は段階的に行い、まずは動作回復優先

### 3) “保有銘柄が表示されない” の切り分け観点

保有表示（フロント `PositionsView.tsx`）は主に以下のどれかが原因で消える:
- `/api/trades` が空（DBが違う/テーブルが無い/読み取り失敗）
- Trade ingest は通っているが、trade_events の列名差異で code が取れていない
- 例外が握りつぶされ、空配列が返っている（`errors: ["trades_failed:..."]` のような形）

確認項目（優先順）:
1. 実行時の `config.DB_PATH` が期待DBか（Launcher準備DBと一致しているか）
2. `trade_events` テーブルが存在し、行が入っているか
3. `/api/trades` のレスポンスに `errors` が入っていないか

## 進め方（フェーズ）

### Phase 0: 計測・再現手順を固定

- どの起動方法で不具合が出るか明確化
  - `python -m app.backend.main`（バックエンド単体）
  - `python -m app.desktop.launcher`（通常）
- UI の “保有銘柄が表示されない” を再現し、同時に以下を確認
  - `/api/health` の `db_path`（Diagnostics）
  - `/api/trades` の `errors`

### Phase 1: 設定/パス解決の統一（最優先）

- Launcher: `MEEMEE_DATA_DIR` を必ずセットして、API 側の config と一致させる
- `app.core.config`:
  - `STOCKS_DB_PATH`/`FAVORITES_DB_PATH`/`PRACTICE_DB_PATH` を尊重する
  - （必要なら）`TRADE_CSV_DIR` なども段階的に整備

完了条件:
- Launcher 起動時に `/api/trades` が期待通りイベントを返す
- UI の保有が復活する

### Phase 2: main分割の整理（段階的）

- `app/backend/main.py` の責務を明確化
  - “互換レイヤ” に限定し、実装は `app/services` / `app/utils` へ
- `app/backend/api/*` と `app/api/endpoints/*` の重複/役割整理
  - 現行ルーティングは `app/api/routes.py` で組まれているので、legacy 側は re-export で足りるか確認

完了条件:
- 主要な参照元が「どのモジュールを使うべきか」迷わない構造になる
- 後方互換 import を残しつつ、依存が増殖しない

### Phase 3: 回帰テスト/スモーク

- `pytest` 実行（環境により Temp など書き込みが必要）
- 可能なら簡易スモーク
  - `python -c "import app.backend.main as m; print(m.APP_ENV, m.APP_VERSION)"` 等
  - `/health` が 200

## 直近の作業TODO（チェックリスト）

- [ ] （実機）Launcher 起動時の `/api/diagnostics` で `db_path` を確認
- [ ] `/api/trades` の `errors` を確認
- [ ] `MEEMEE_DATA_DIR`/`STOCKS_DB_PATH` の取り扱いを統一して保有復活を確認
- [ ] main分割で壊れやすい import を棚卸し（`from app.backend import main as main_module`）
- [ ] legacy/新API の整理方針を決めて段階的にリファクタ

