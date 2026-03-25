# docs/CODEX.md

このドキュメントは「長文の仕様・Runbook・設計思想」を格納する。
AGENTS.md には入れず、必要時のみ参照する（常時注入するとクレジット消費が増えるため）。

使い方:

- 先に `AGENTS.md` を読み、変更対象が `backend` / `frontend` なら対応する領域 `AGENTS.md` を読む。
- MeeMee 固有の判断軸、`売-買` 表記、通常画面と検証画面の分離方針は `docs/MEEMEE_PRINCIPLES.md` を参照する。
- この文書は必要な章だけ開く。最初から最後まで通読しない。
- 実装は `Plan -> Evidence -> Execute -> Verify` の順で進める。
- 依頼が曖昧でも、まず現状コードと既存仕様の証拠を集め、勝手に仕様を変えない。
- 検証は変更範囲の最小単位で行い、重い手順や Runbook を毎回全量実行しない。

### TRADEX Review Gate

TRADEX の render contract を含む変更レビューでは、次の短い gate を必ず通す。

1. 変更した reader は consumer か compatibility か
2. consumer なら `candidate_image_render_consumption_summary` だけを読むか
3. `latest_image_render_consumption_summary` / `latest_image_render_consumption_status` / `latest_image_authoritative_render_field_name` を consumer / read path で参照していないか
4. `docs/tradex/render-contract-boundary.md` を確認したか
5. `test_tradex_consumer_paths_do_not_read_latest_render_consumption_fields` に抵触していないか
6. silent fallback を入れていないか

詳細ルールは `docs/tradex/render-contract-boundary.md` を正本とし、ここは review 入口用の short gate に限定する。

#### Minimal Verify

render contract に触る変更なら、最低限これだけ回す。

```powershell
python tools/run_tradex_render_contract_review_check.py
```

この 1 コマンドが consumer path の source scan、targeted pytest、詳細ルール正本の確認をまとめて実行する。

---

完了条件:

- 上記すべてのディレクトリが存在する
- 各ディレクトリに `__init__.py` が存在する
- `python -c "import app.backend.domain.indicators"` 等がエラーなく通る
- （推奨）`python -c "import app.backend.api.routers"` も通る

---

## 9. 優先度順マッピング（関数移動リスト）

AIはこの順序に従ってリファクタリングを行うこと。  
Step N が完了し、動作確認が取れてから Step N+1 へ進むこと。

※関数名が現状コードと不一致の場合は、勝手に仕様を変えず「同等責務の関数」を探索してマッピングを補正すること。

---

### Step 1: Domain/Indicators & Bars（依存なし・純粋関数）

最も安全な移動。DBやAPIを知らない計算ロジックのみ。

| 移動元関数 (main.py)                                                                                                                                           | 移動先 (新規作成)                                  | 備考                                                            |
| --------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- | ------------------------------------------------------------- |
| `_build_ma_series`, `_compute_atr`, `_calc_slope`, `_pct_change`, `_count_streak`                                                                         | `app/backend/domain/indicators/basic.py`    | I/O禁止。数値配列の処理のみ。                                              |
| `_build_weekly_bars`, `_build_quarterly_bars`, `_build_yearly_bars`, `_drop_incomplete_weekly`, `_drop_incomplete_monthly`（または `_normalize_monthly_rows`） | `app/backend/domain/bars/resample.py`       | 週足/月足の仕様を固定。 `_build_monthly_bars` が無い場合は `_normalize_` 等を使用。 |
| `_detect_body_box`, `_build_box_metrics`                                                                                                                  | `app/backend/domain/indicators/patterns.py` | 箱検知ロジック。                                                      |

**Step 1 完了条件**

- `main.py` から上記関数定義が消え、import参照になっている
- import検証・起動検証が通る（10章）

---

### Step 2: Domain/Scoring & Positions（ビジネスロジック）

アプリの核となる判定ロジック。Step 1 の関数を import して使う。

| 移動元関数 (main.py)                                                                | 移動先 (新規作成)                                    | 備考                                   |
| ------------------------------------------------------------------------------ | --------------------------------------------- | ------------------------------------ |
| `_calc_short_a_score`, `_calc_short_b_score`, `_check_short_prohibition_zones` | `app/backend/domain/scoring/short_selling.py` | 入力は「計算済み指標＋足データ」。                    |
| `_score_weekly_candidate`, `_score_monthly_candidate`                          | `app/backend/domain/scoring/judge.py`         | `score_breakdown` を欠落させない。           |
| `_parse_trade_csv`（ロジックのみ）, `_build_daily_positions`                           | `app/backend/domain/positions/parser.py`      | **重要**: ファイル読込/DB書込禁止。文字列/行データの変換のみ。 |

**Step 2 完了条件**

- scoring/positions がdomainに収まり、main.pyは呼び出しのみ
- breakdownが欠落しない

---

### Step 3: Infra/Repositories（データアクセスの隔離）

`get_conn` / SQL / `open()` を main から完全排除する。

| 移動元関数 (main.py)                                                      | 移動先 (新規作成)                                   | 備考                   |
| -------------------------------------------------------------------- | -------------------------------------------- | -------------------- |
| `_fetch_daily_rows`, `_fetch_monthly_rows`, `_delete_ticker_db_rows` | `app/backend/infra/duckdb/stock_repo.py`     | DuckDB操作。            |
| `init_schema`                                                        | `app/backend/infra/duckdb/schema.py`         | DDL定義。               |
| `_load_favorite_codes`, `favorites_add/remove`（DB部）                  | `app/backend/infra/sqlite/favorites_repo.py` | favorites.sqlite 操作。 |
| `practice_session*`（DB部）, `_init_practice_schema`                    | `app/backend/infra/sqlite/practice_repo.py`  | practice.sqlite 操作。  |
| `_load_watchlist_codes`, `_update_watchlist_file`                    | `app/backend/infra/files/watchlist_repo.py`  | code.txt 操作。         |
| `_load_rank_config` 等                                                | `app/backend/infra/files/state_repo.py`      | JSON設定ファイル操作。        |

**Step 3 完了条件**

- `main.py` から `duckdb.connect` / `sqlite3.connect` / `open()` が消える

---

### Step 4: Jobs（重い処理・事前計算）

APIレスポンスと切り離すべき処理。

| 移動元関数 (main.py)                                                | 移動先 (新規作成)                        | 備考                       |
| -------------------------------------------------------------- | --------------------------------- | ------------------------ |
| `txt_update_run`, `_run_txt_update_job`, `_run_ingest_command` | `app/backend/jobs/txt_update.py`  | PanRolling連携・取込制御。       |
| `_build_weekly_ranking`（計算制御部）                                 | `app/backend/jobs/scoring_job.py` | 計算結果を保存し、APIは読むだけに寄せる準備。 |

**Step 4 完了条件**

- ranking/scoreの重計算がAPIホットパスから外れ始めている（例外は明示）

---

### Step 5: API/Routers（エンドポイント定義）

最後に残った `@app.get/post` を移動する。

| 移動元 (main.py)                                  | 移動先 (新規作成)                            | 備考                      |
| ---------------------------------------------- | ------------------------------------- | ----------------------- |
| `rank_weekly`, `rank_monthly`, `rank_dir`      | `app/backend/api/routers/ranking.py`  |                         |
| `trades`, `trades_by_code`, `trade_csv_upload` | `app/backend/api/routers/trades.py`   | Upload受取とParser呼び出しの接続。 |
| `practice_*`（Endpoint）                         | `app/backend/api/routers/practice.py` |                         |
| `favorites_*`, `watchlist_*`                   | `app/backend/api/routers/lists.py`    |                         |
| `health`, `diagnostics`, `list_tickers`        | `app/backend/api/routers/system.py`   |                         |

**Step 5 完了条件**

- `main.py` から `@app.get/post` が消える
- `main.py` は create_app + include_router + DI のみ

---

## 10. 動作確認（各Stepで必須）

各Stepごとに最低限これを通してから次へ進むこと（失敗したらStepを進めない）。

- import検証: ImportError がないこと  
  例: `python -c "import app.backend.main"`
- 起動検証: サーバが起動すること（最低限）
- 主要APIスモーク（最低限）:
  - ランキングが1回表示できる
  - チャートが1回表示できる
  - watchlist/favorites が読める・更新できる
  - 取引CSV取込が1回通る（可能なら）

---

## 11. 実装粒度・命名規約（迷いを減らす）

目的: 「どこに書くか」「どの粒度で切るか」を機械的に決め、Codexのブレ（過剰分割・過小分割）を抑止する。

### 11.1 ファイル命名と責務単位

- 1ファイル=1責務（例: `domain/indicators/basic.py` は “基本指標” のみ）。
- 既存関数を移動する際は **関数名・引数・戻り値の意味を原則維持**（仕様変更は別PR/別Stepで扱う）。
- 1コミット=1移動単位（例: Step1の `basic.py` だけ移す）。

### 11.2 import規約

- ルール: **常に絶対import**（`from app.backend... import ...`）を優先し、相対importは最小化。
- Domainは `core` 以外をimportしない。
- Infraは `core` 以外をimportしない（domain依存を作らない）。

### 11.3 例外・型の置き場所

- domain/infra で共有したい例外・Enum・dataclass・Protocol などは **必ず `app/backend/core/` に置く**。
- 型ヒントが循環の原因になる場合は `TYPE_CHECKING` と `from __future__ import annotations` を優先。

### 11.4 Routerの薄さ（上限）

- `api/routers/*` は「入出力＋呼び出し＋例外変換」以外を書かない。
- 目安: 1エンドポイント関数は **30行以内**（例外処理込み）。超える場合は jobs/domain/infra 側へ押し出す。

---

## 12. Step別チェックリスト（コピペ用）

目的: 「Stepを進めて良い/ダメ」を曖昧にせず、機械的に判定する。

### 共通（全Stepで毎回）

- import検証: `python -c "import app.backend.main"`
- コンパイル検証: `python -m compileall app/backend`（SyntaxErrorの早期検出）
- 起動検証: サーバが起動する（最低限）

### Step 0（Scaffolding）

- ディレクトリと `__init__.py` が揃っている
- `python -c "import app.backend.domain.indicators"` が通る
- `python -c "import app.backend.api.routers"` が通る

### Step 1（domain/indicators & domain/bars）

- `app/backend/domain` 配下に I/O が無いこと（目視 + grep）
  - 例: `duckdb.connect` / `sqlite3.connect` / `open(` / `subprocess` が domain に出現しない
- `main.py` から Step1 対象関数の **定義が消え**、import参照になっている

### Step 2（domain/scoring & domain/positions）

- `score_breakdown` のキー/構造が欠落していない（既存UIが壊れない）
- positions は「文字列/行データ → 正規化データ」の変換に限定（ファイル/DB操作なし）

### Step 3（infra/*）

- `main.py` から `duckdb.connect` / `sqlite3.connect` / `open()` が消える
- SQLは `infra` に集約され、domain/api/jobs にSQLが残っていない

### Step 4（jobs/*）

- heavy計算は jobs に寄り、APIホットパスで全再計算が走らない
- 例外的にオンデマンド計算を残す場合は「明示スイッチ/専用エンドポイント」のみに限定

### Step 5（api/routers/*）

- `main.py` から `@app.get/post` が消える
- `main.py` が create_app + include_router + DI のみ

---

## 13. 変更禁止領域（互換性を守る）

目的: 「リファクタ中に仕様が微妙に変わる」事故を防ぐ。

### 13.1 計算仕様の凍結（Step 1〜4で変更しない）

- 週足/月足の区切り・OHLCV集計ルール（`domain/bars` で固定した仕様を維持）
- 既存の指標定義（MA/ATR/傾き/箱判定の閾値など）
- スコア計算式（重み、判定条件、内訳キー）

### 13.2 API契約の凍結（Step 5まで原則変更しない）

- 既存フロントが参照しているレスポンスキーは維持
- 変更が必要な場合は `api/schemas` を先に作り、移行期間を設ける（互換キーを残す）

---

## 14. 循環参照の予防パターン（運用ルール）

目的: 「起動しない」を最短で回避する。

### 14.1 禁止パターン

- `domain -> infra` の import（絶対禁止）
- `infra -> domain` の import（絶対禁止）

### 14.2 推奨パターン（依存の分断）

- domain/infra の両方で使う型は `core` に寄せる
- infra が返すものは「coreの型」またはプリミティブ（dictを乱用しない）
- jobs は infra から取得 → domain に入力して計算 → infra に保存、という一方向パイプにする

### 14.3 破綻時の切り戻し規則

- ImportError が出たら、直前コミットで追加した import を最小単位で戻し、
  - ① 型を core へ移動
  - ② `TYPE_CHECKING` 化
  - ③ 関数引数をプリミティブ化 の順で修正する。

---

## 15. ルール変更（スクリーニング/並び替え/重み付け）への耐性

結論: **この構造は「ルール変更が頻発する前提」に適合する**。ただし、下記の追加ルールを採用しないと、将来 `domain/scoring` が再び「暗黙ルールの塊」になり、変更コストが上がる。

### 15.1 原則: 「コードに埋め込まず、設定で切り替える」

- スクリーニング条件（閾値、ON/OFF、対象レジーム/足種別）
- 並び替え（ランキングキー、合成ロジック、フィルタ順）
- スコア重み（weight、cap、正規化方式）

これらは **関数内の定数・if分岐に直書きしない**。

### 15.2 RuleSet / Profile という“切替単位”を固定する

- **RuleSet**: 候補抽出（フィルタ）に使う条件セット（例: 「下落候補」「上昇候補」「レンジ下限押し目」）。
- **Profile**: 並び替え（ランキング）に使う合成スコアの定義（例: `bearish_v1`, `bullish_v1`, `neutral_v1`）。

UIは「Profileを選ぶだけ」で並び替えが変わる。

### 15.3 置き場所（設計ルール）

- **Profile/RuleSet のスキーマ（型）**: `core/` に置く（domain/infraの共有物）。
- **Profile/RuleSet の保存場所**: `infra/files/state_repo.py`（JSON）または `infra/sqlite/*`（UI編集や履歴が必要ならSQLite）。
- **Profile適用（計算）**: `domain/scoring/*` は *必ず* `config`（Profile/RuleSet）を引数として受け取る。
  - 例: `score_candidate(bars, indicators, profile)`
- **いつ反映するか（計算タイミング）**: `jobs/*` が責務を持つ。

### 15.4 バージョン管理（再現性と事故防止）

ルール変更があるプロジェクトで一番揉めるのは「昨日と今日で結果が違う理由が追えない」こと。 そのため以下を必須とする。

- `profile_id` / `ruleset_id`
- `profile_version`（または `config_hash`）
- `scoring_job_version`（ロジック側の版）

を **スコア結果と一緒に保存**する（DuckDBのscoreテーブル等）。

### 15.5 score_breakdown を“設定ドリブン”に保つ

- `score_breakdown` のキーは Profileで定義し、
  - 例: `{"ma_slope": +3.2, "atr_expand": -1.0, "box": +2.0}`
- 合成は `score_total = Σ(breakdown[k] * weight[k])` のように **機械的に計算**できる形に寄せる。

これにより重み変更は「設定だけ」で済み、ロジック改修頻度が激減する。

### 15.6 設定変更時の“反映手順”を固定する

- 原則: **設定変更 → scoring_job を実行 → APIは保存結果を返す**。
- 例外（オンデマンド）を許す場合:
  - 専用エンドポイント（例: `/rank/preview?profile=bearish_v2`）
  - レート制限/対象銘柄制限（全銘柄は禁止）
  - 結果は保存しない（プレビュー扱い）

### 15.7 テスト方針（ルール変更に強い形）

- Profileごとに **ゴールデンテスト**（少数銘柄×少数期間で期待順位/内訳を固定）を作る。
- 「重みだけ変えた」場合は、
  - breakdown（素点）は不変
  - total と順位だけが変わる ことを検証する。

---

### 15.8 運用上の結論（あなたの懸念への回答）

- スクリーニングルール変更: **RuleSet** として吸収（UI/JSON差し替えで運用可能）。
- 並び替え方法変更: **Profile** を追加・切替で吸収（APIは `profile_id` を受けるだけ）。
- 重み付け変更: `profile.weights` の変更で吸収（domainのロジック改修を最小化）。

※この15章のルールを守る限り、将来の改修は「コード編集」ではなく **設定編集＋Job再計算**に寄り、破壊的変更になりにくい。

---

## 16. PanRolling連携（VBA/VBS）と EXE配布（別PC運用）

目的: 「PanRolling利用者向けツール」を **ZIP→インストーラー→ダブルクリック起動** の導線で配布し、(1)常用PC（普段使い） と (2)テスト専用PC（検証だけ） の完全分離を前提に、

- クリーン環境でも起動する
- 既存ユーザーがアップデートで壊れない
- 詰んだ時にログ/診断だけで切り分けできる

状態を“仕様”として固定する。

### 16.1 前提（配布対象の定義）

- 対象ユーザー: **PanRolling（チャートギャラリー等）を導入済み** のPC利用者。
- ただし、PanRollingの自動操作（VBA/VBS）は環境差が大きいので、配布版は次を原則とする。
  - **Manualモード（推奨）**: ユーザーがPanRolling側でTXTを生成 → 本アプリは取り込みのみ
  - **Producerモード（任意）**: VBA/VBSで生成を支援（17章のSafetyを満たす範囲で）

### 16.2 配布成果物の標準形（ファイル構成）

- 配布物は「インストーラー1本」に寄せる（サポート事故を避ける）。
  - `MeeMeeScreener-Setup-vX.Y.Z.exe`（インストーラー本体）
  - `MeeMeeScreener-Setup-vX.Y.Z.zip`（外側ZIP。中身はSetup exeのみ）
- バージョン命名は必ず一意にする（`vX.Y.Z`）。
- パッケージ方式は 19章の方針に従う（推奨: onedir）。

### 16.3 Google Drive 配布（アップデート運用の固定）

目的: 「どれが最新か分からない」を根絶する。

- Drive側に **最新版フォルダ（Latest）** を1つ作り、常に同じファイル名で上書きする。
  - `MeeMeeScreener-Setup-Latest.exe`
  - `version.txt`（例: `0.9.3` の1行）または `version.json`
- アプリ側は最小でよい（自動更新を急がない）。
  - 「ヘルプ → アップデート」導線で Drive の最新版を開ける
  - 可能なら `version.txt` を参照して「更新あり」を表示（自動DL/自動差し替えは後回し）

### 16.4 テスト環境運用ルール（常用PC / テスト専用PCの完全分離）

原則: **dev環境分離（dev/prodプロファイル）を作らない代わりに、テストPCの手順を標準化して再現性を担保する。**

- (1) 常用PC（開発）
  - ソース編集、ビルド、インストーラー作成、ZIP化、Drive更新
- (2) テスト専用PC（検証）
  - **成果物のみ**を扱う（ソース/開発環境/CLI/PowerShell手順に依存しない）
  - “利用者PC”の導線（インストール→起動→更新）でのみ評価する

### 16.5 テストシナリオ固定（毎リリース必須）

テストPCで必ず **A / B の両方** を実施し、合格したSetupだけを `Latest` としてDriveに反映する。

- A. クリーンインストール検証（初回起動の保証）
  1. 旧バージョンをアンインストール
  2. ユーザーデータ領域（例: `%LOCALAPPDATA%\MeeMeeScreener`）を削除して“完全初回”状態にする


#### Spec: local data dir and trade CSV/TXT paths (do not change silently)

- Local data dir MUST be used for CSV/TXT persistence.
- Default local data dir (when MEEMEE_DATA_DIR is not set):
  - `C:\Users\enish\AppData\Local\MeeMeeScreener\data` (example on this machine)
  - General form: `%LOCALAPPDATA%\MeeMeeScreener\data`
- Trade CSV canonical filenames (stored under the local data dir or its `csv` subdir):
  - `????????.csv`
  - `SBI??????.csv`
- Stock data TXT (code.txt and related) must be read/written under the same local data dir.
- Any change to local path resolution or filenames must be recorded here in this spec.
  3. インストーラーで新規インストール
  4. ダブルクリック起動 → 初回DB生成/初期設定/ログ生成が成功する
  5. 最低限スモーク: ランキング1回、チャート1回、TXT取り込み1回

- B. アップデート検証（既存ユーザー保護）
  1. 旧バージョンをインストール済＋データありの状態を用意
  2. 起動してデータが読めることを確認
  3. 新版インストーラーで上書き更新
  4. 起動 → データ保持（DB/設定/state）が壊れていないことを確認
  5. 最低限スモーク: ランキング/チャート/TXT取り込みが動く

補足:
- Producerモードを配布対象に含める場合は、A/Bに加えて「Producerあり/なし」の差分確認を最低1回は行う。

### 16.6 Done条件（別PC運用・配布の完成判定）

- テスト専用PCで A/B が毎回成功し、合格したSetupだけが `Latest` に反映されている
- ZIP→Setup起動→インストール→ダブルクリック起動、の導線で詰まらない
- 失敗時に logs/diagnostics（19.6）だけで原因分類できる
- ユーザーデータはユーザーデータ領域に集約され、アップデートで消えない（19.3）

---

## 17. TXT更新で詰む問題の予防設計（Runbook + Safety）

目的: 「TXT更新で詰む」を“仕様”として吸収し、原因切り分けと復旧を最短化する。これはコーディング規約というより **運用安全（Safety）** と **配布耐性（EXE運用）** のための設計ルール。

### 17.1 典型的な詰みパターン（原因分類）

1. **Producerが起動できない**（VBA/VBSが叩けない）
   - Office未インストール / マクロ無効 / セキュリティポリシー / 実行パス不一致
2. **外部実行は成功したが、TXTが生成されない**
   - 出力先が違う / 権限不足 / OneDrive/同期フォルダの競合
3. **TXTが“途中まで”の状態で取り込みが走る**
   - 生成中にjobsが読み込む（ファイルサイズが増え続ける）
4. **TXT形式が変わった**（PanRolling側の更新・設定差）
   - 列数/区切り/エンコーディング/日付形式が変化
5. **取り込み後にDB更新で失敗**
   - DuckDBロック / 破損 / パーティション不整合 / 例外でstateが中途半端

### 17.2 設計ルール（詰みにくくする）

- **Producer / Ingest分離（16章）を破らない**: 本体は「TXTが存在する前提」で動ける。
- **取り込みの“完了判定”を明確化**: Producerは可能なら
  - 一時名（例: `.tmp`）で書き出し → 完了後にリネーム
  - もしくは「完了フラグファイル（例: `DONE.json`）」を最後に生成
- **入力検証を必須化（早期失敗）**: jobs側で取り込み前に
  - ファイル存在、最終更新時刻、サイズ下限
  - 先頭N行のフォーマット（ヘッダ/列数/日付パース） をチェックし、NGならDB更新に進まない。
- **idempotent（再実行安全）**: 同じTXTを2回取り込んでも壊れない（UPSERT、重複排除）。
- **state管理を1箇所に集約**: `infra/files/state_repo.py`（例: `update_state.json`）に
  - 最終成功日時 / 最終成功ファイル / 最終取り込み銘柄範囲 / エラー履歴（直近）
  - 「処理中」フラグ（クラッシュ時に復旧できる）
- **ロック戦略**: jobs起動中の多重実行を防ぐ（プロセスロック/ファイルロック）。

### 17.3 API/CLIとして“必須”にする操作（詰んだ時の逃げ道）

EXE配布では、ユーザー（あなた）が「何が起きたか」を見られないと詰む。以下を **必須エンドポイント（またはCLI）** として用意する。

- **状態確認**: `GET /jobs/txt_update/status`
  - last_success_at / last_error_at / last_error_reason / current_phase（producer/ingest/resample/indicators/scoring）
- **実行**: `POST /jobs/txt_update/run`（manual）
  - 「Producerを実行しない」= 既存TXTだけで ingest するモード
- **実行（Producer含む）**: `POST /jobs/txt_update/run?mode=vba|vbs`
  - タイムアウト・リトライ回数は設定から読む
- **復旧**: `POST /jobs/txt_update/reset`
  - stateの「処理中」解除、途中生成物の掃除（安全に）
- **ログ閲覧**: `GET /jobs/txt_update/logs?tail=200`
  - EXE運用では画面上で直近ログが見えることが重要

※実装場所: `jobs/txt_update.py` が制御、ログとstateは `infra/files/*` に集約。

### 17.4 エラーメッセージの“型”を固定する（原因切り分け最短化）

- 例外は丸めず、**分類コード** と **次のアクション** を必ず返す。
  - `PRODUCER_NOT_AVAILABLE`（Office無し等）→ Manualモード案内
  - `INPUT_INCOMPLETE`（生成中）→ リトライ（待機）
  - `INPUT_FORMAT_CHANGED` → 先頭N行の検査結果を表示
  - `DB_LOCKED` → リトライ or アプリ再起動案内

この「分類コード」は `core/errors.py`（例: Enum）に置く。

### 17.5 Done条件（TXT更新が“詰まない”完成判定）

- Producerが動かないPCでも、Manualモードで ingest→ランキング/チャートが動く
- status/logs/reset が用意され、原因切り分けが UI/ログだけでできる
- 途中ファイルを誤取り込みしない（完了判定が働く）

---

## 18. 画面崩れ・チャート表示の品質ゲート（フロント/レイアウト）

結論: これは「main.py分割ルール」そのものではないが、**配布アプリの品質**を左右するため、codexに“最低限の品質ゲート”として明記する価値がある。

### 18.1 何をcodexに書くべきか（範囲）

- codexに書く: **再発防止のための“完成判定”と“責務の置き場所”**
- codexに書かない: CSSの具体実装（それはフロント側の設計書/UIガイドへ）

### 18.2 置き場所ルール（フロント側の責務）

- レイアウト（3×3グリッド、ブレークポイント、スクロール）: フロントのレイアウト層で完結
- チャート描画（canvasのサイズ・リサイズ追従）: チャートコンポーネント責務
- バックエンドは「表示に必要な最小データとメタ情報」を返すだけ
  - 例: 推奨表示期間、足種別、必要な系列名（OHLCV + MA + box + positions）

### 18.3 品質ゲート（必須チェック）

- **3×3が1画面に収まらない**場合、必ず次のどれかにフォールバックする:

  1. 自動で2×2に落とす（残りはページング）
  2. 3×3を維持しつつ、縦スクロールを許容（ヘッダ固定）
  3. ユーザーが明示的に「密度」を選べるUI（設定保存）

- **必須のスモークテスト（配布前）**

  - 代表解像度（例: 1920×1080、2560×1440）で
    - 3×3表示が崩れない（または自動フォールバックする）
    - 文字がボタンと被らない
    - スクロールしてもヘッダ（操作部）が迷子にならない

### 18.4 バックエンド側に要求する“表示崩れ対策のフック”

フロントが安全にレイアウトできるよう、レスポンスに以下のメタを許可する（ただし計算はしない）。

- `ui_hints`: 推奨行列（例: `{"grid":"3x3","fallback":"2x2"}`）
- `series_meta`: 系列の表示優先度（例: OHLCV必須、指標は任意）

### 18.5 Done条件（画面崩れが“運用で詰まない”完成判定）

- 3×3が収まらない環境でも、ユーザーが操作不能にならない（フォールバックが機能）
- バックエンドはUIの“制御”を持たず、必要なメタ情報のみを提供

---

## 19. EXE起動で詰む問題の予防設計（配布・依存関係・初回起動）

目的: 「別PCでEXEが開かない」「PS1をいじると動く」といった**配布運用での詰み**を、仕様として吸収し、原因切り分けと復旧を最短化する。

> 原則: **起動にPowerShell（ExecutionPolicy）や外部スクリプトの“手動介入”を要求しない。** すべてEXE内部（起動ブートストラップ）で完結させる。

### 19.1 典型的な“起動しない”パターン（原因分類）

1. **前提ランタイム不足**
   - WebView2 Runtime 未導入（pywebview / Edge WebView依存）
   - MSVCランタイム不足（vcruntime系）
2. **パス/権限問題**
   - `Program Files` 配下で書き込み不可（DB/ログ/設定を書こうとして落ちる）
   - OneDrive配下/同期フォルダでロック競合
3. **相対パス前提の崩壊**
   - `cwd` が想定と違い、DB/設定/テンプレ/静的ファイルが見つからない
4. **パッケージング由来のimport失敗**
   - hidden import / data file 未同梱 / DLL未収集
5. **セキュリティブロック**
   - SmartScreen / AV隔離 / ダウンロード由来のブロック（Zone.Identifier）

### 19.2 配布方式の基本方針（推奨）

- **推奨: onedir配布（フォルダ配布）**
  - onefileは展開先や初回実行の差で詰みやすい（ログも追いにくい）。
  - onedirは「同梱物が見える」「差分更新しやすい」「原因切り分けしやすい」。
- Python同梱EXE（例: PyInstaller / Nuitka 等）は、
  - **依存の固定（ロック）**
  - **dataファイル同梱**
  - **起動前の自己診断** をセットで設計する。

### 19.3 絶対ルール: 書き込み先は“ユーザーデータ領域”に統一

- アプリ直下（実行フォルダ）へ書き込まない（配布先/権限で即死する）。
- **必須の保存先**（Windows想定）
  - DB（duckdb/sqlite）
  - ログ
  - update_state / 設定JSON
  - 生成物（週足/月足キャッシュ等）

これらは原則として `%LOCALAPPDATA%\MeeMeeScreener`（または同等）へ集約する。

- 実行フォルダは「読み取り専用」とみなす。

### 19.4 起動ブートストラップ（EXE内）で必ず行う“事前チェック”

起動直後（UI起動前）に以下をチェックし、NGなら**分類コード付きで明確に落とす**（沈黙で落ちない）。

- **Paths**
  - 実行ファイル位置の解決（PyInstallerの場合は一時展開/同梱の差を吸収）
  - user data dir の作成・書き込みテスト
- **Runtime**
  - WebView2の存在チェック（未導入なら案内）
  - 必要DLL/モジュールのimportチェック（duckdb, pywebview, fastapi等の主要依存）
- **Data**
  - duckdb/sqliteを“開ける”こと（空でもよい）
  - code.txt / rank_config 等が無い場合の初期生成（または初期ウィザード）

※このチェック結果は必ずログへ出す（19.6）。

### 19.5 依存関係の固定（再現性）

- **ロックファイルを必須化**（例: requirements.lock / uv.lock / poetry.lock など）
- ビルドは「ロックからのみ」行う（手元の環境依存を混ぜない）。
- EXEビルド時に以下を埋め込む:
  - `build_version`（git shaでも可）
  - `deps_hash`（ロックのハッシュ）
  - `packager_version`（PyInstaller等の版）

これにより「別PCで動かない」問題が、**再現→修正→再配布**のループに乗る。

### 19.6 ログと診断（“開けない”の最大の保険）

- ログ出力先: user data dir 配下（例: `logs/app.log`）
- 起動失敗時でも、最低限次を残す:
  - 起動フェーズ（preflight / db_open / server_start / ui_start）
  - 例外スタックトレース
  - パス情報（実行パス / data dir / cwd）

加えて、**診断モード**を用意する:

- 例: `MeeMeeScreener.exe --diagnostics`
  - 事前チェックだけ実行し、結果を `diagnostics.json` として保存

### 19.7 “PS1で直る”を根絶するためのルール

- 起動に必要な環境変数設定・パス調整は、
  - **PS1ではなくEXE内部で完結**する（起動コードで `os.environ` を設定）。
- 外部コマンドが必要な場合は（PanRolling等）、
  - `infra/panrolling` に集約し、
  - **存在確認 / 権限確認 / タイムアウト / 失敗理由の分類**を必須化する（17章と整合）。

### 19.8 Done条件（配布で“起動が安定”した完成判定）

- クリーンな別PCで、ダブルクリック起動が安定して成功する
- 失敗しても、ユーザーが **logs/diagnostics** だけで原因分類できる
- 書き込み先が user data dir に統一され、配布先の権限に依存しない
- WebView2等の前提不足は、沈黙せず「案内付きで停止」できる

---

（以下、あなたが貼ってくれた Codex 用 role/workflow/policy も保持したい場合はここに追記する）
