# app/backend/AGENTS.md (Backend Refactor Rules)

目的:
- main.py の巨大化を解消し、domain/infra/jobs/api を分離する。
- 仕様変更を混ぜず、Stepごとに動作確認しながら移動する。

---

## A. 変更禁止（絶対）
- domain -> infra の import 禁止
- infra -> domain の import 禁止
- domain 配下で I/O 禁止（duckdb/sqlite/open/subprocess 等）
- 関数移動は「関数名・引数・戻り値の意味」を原則維持（仕様変更は別Step/別PR）

## B. import 規約
- 絶対importを優先: `from app.backend... import ...`
- 循環が出る型は `core/` に寄せる。必要なら `TYPE_CHECKING` と `from __future__ import annotations` を使う。

---

## C. Step 0: Scaffolding（必須）
完了条件:
- すべてのディレクトリが存在する
- 各ディレクトリに `__init__.py` が存在する
- `python -c "import app.backend.domain.indicators"` が通る
- （推奨）`python -c "import app.backend.api.routers"` が通る

---

## D. 優先度順マッピング（関数移動リスト）
AIはこの順序に従ってリファクタリングすること。
Step N が完了し、動作確認が取れてから Step N+1 へ進むこと。
関数名が不一致の場合は勝手に仕様を変えず「同等責務の関数」を探索しマッピングを補正すること。

### Step 1: Domain/Indicators & Bars（依存なし・純粋関数）
| 移動元関数 (main.py) | 移動先 (新規作成) | 備考 |
| --- | --- | --- |
| `_build_ma_series`, `_compute_atr`, `_calc_slope`, `_pct_change`, `_count_streak` | `app/backend/domain/indicators/basic.py` | I/O禁止。数値配列の処理のみ。 |
| `_build_weekly_bars`, `_build_quarterly_bars`, `_build_yearly_bars`, `_drop_incomplete_weekly`, `_drop_incomplete_monthly`（または `_normalize_monthly_rows`） | `app/backend/domain/bars/resample.py` | 週足/月足の仕様を固定。 |
| `_detect_body_box`, `_build_box_metrics` | `app/backend/domain/indicators/patterns.py` | 箱検知ロジック。 |

Step 1 完了条件:
- `main.py` から上記関数定義が消え、import参照になっている

### Step 2: Domain/Scoring & Positions（ビジネスロジック）
| 移動元関数 (main.py) | 移動先 (新規作成) | 備考 |
| --- | --- | --- |
| `_calc_short_a_score`, `_calc_short_b_score`, `_check_short_prohibition_zones` | `app/backend/domain/scoring/short_selling.py` | 入力は「計算済み指標＋足データ」。 |
| `_score_weekly_candidate`, `_score_monthly_candidate` | `app/backend/domain/scoring/judge.py` | `score_breakdown` を欠落させない。 |
| `_parse_trade_csv`（ロジックのみ）, `_build_daily_positions` | `app/backend/domain/positions/parser.py` | 重要: ファイル読込/DB書込禁止。変換のみ。 |

Step 2 完了条件:
- scoring/positions がdomainに収まり、main.pyは呼び出しのみ
- breakdownが欠落しない

### Step 3: Infra/Repositories（データアクセスの隔離）
| 移動元関数 (main.py) | 移動先 (新規作成) | 備考 |
| --- | --- | --- |
| `_fetch_daily_rows`, `_fetch_monthly_rows`, `_delete_ticker_db_rows` | `app/backend/infra/duckdb/stock_repo.py` | DuckDB操作。 |
| `init_schema` | `app/backend/infra/duckdb/schema.py` | DDL定義。 |
| `_load_favorite_codes`, `favorites_add/remove`（DB部） | `app/backend/infra/sqlite/favorites_repo.py` | favorites.sqlite 操作。 |
| `practice_session*`（DB部）, `_init_practice_schema` | `app/backend/infra/sqlite/practice_repo.py` | practice.sqlite 操作。 |
| `_load_watchlist_codes`, `_update_watchlist_file` | `app/backend/infra/files/watchlist_repo.py` | code.txt 操作。 |
| `_load_rank_config` 等 | `app/backend/infra/files/state_repo.py` | JSON設定ファイル操作。 |

Step 3 完了条件:
- `main.py` から `duckdb.connect` / `sqlite3.connect` / `open()` が消える

### Step 4: Jobs（重い処理・事前計算）
| 移動元関数 (main.py) | 移動先 (新規作成) | 備考 |
| --- | --- | --- |
| `txt_update_run`, `_run_txt_update_job`, `_run_ingest_command` | `app/backend/jobs/txt_update.py` | PanRolling連携・取込制御。 |
| `_build_weekly_ranking`（計算制御部） | `app/backend/jobs/scoring_job.py` | APIホットパスから重計算を外す準備。 |

Step 4 完了条件:
- ranking/scoreの重計算がAPIホットパスから外れ始めている（例外は明示）

### Step 5: API/Routers（エンドポイント定義）
| 移動元 (main.py) | 移動先 (新規作成) | 備考 |
| --- | --- | --- |
| `rank_weekly`, `rank_monthly`, `rank_dir` | `app/backend/api/routers/ranking.py` | |
| `trades`, `trades_by_code`, `trade_csv_upload` | `app/backend/api/routers/trades.py` | Upload受取とParser呼び出し接続。 |
| `practice_*`（Endpoint） | `app/backend/api/routers/practice.py` | |
| `favorites_*`, `watchlist_*` | `app/backend/api/routers/lists.py` | |
| `health`, `diagnostics`, `list_tickers` | `app/backend/api/routers/system.py` | |

Step 5 完了条件:
- `main.py` から `@app.get/post` が消える
- `main.py` は create_app + include_router + DI のみ

---

## E. 動作確認（各Stepで必須）
各Stepごとに最低限これを通してから次へ進むこと（失敗したらStepを進めない）。

- import検証: `python -c "import app.backend.main"`
- コンパイル検証: `python -m compileall app/backend`
- 起動検証: サーバが起動する（最低限）
- 主要APIスモーク（最低限）:
  - ランキングが1回表示できる
  - チャートが1回表示できる
  - watchlist/favorites が読める・更新できる
  - 取引CSV取込が1回通る（可能なら）

---

## F. 長文仕様書
- 将来設計/配布/Runbook は docs/CODEX.md を参照（必要時だけ）。
