# MeeMee Screener デザインレビュー (2026-03-10)

コードベース全体を調査し、**設計（デザイン）面で改善できる箇所**を優先度順にまとめた。

---

## 🔴 深刻度: 高

### 1. 巨大ファイル（God Object / God Component）

単一ファイルが数千〜数万行に達しており、可読性・保守性・テスト容易性が著しく低下している。

| ファイル | サイズ | 問題 |
|---|---|---|
| `app/frontend/src/routes/DetailView.tsx` | **284KB** | 詳細画面の全ロジック・UIが1ファイルに集約 |
| `app/frontend/src/routes/GridView.tsx` | **177KB** | グリッド画面の全機能が1ファイルに |
| `app/frontend/src/routes/PracticeView.tsx` | **104KB** | 練習画面の全機能が1ファイルに |
| `app/frontend/src/components/DetailChart.tsx` | **93KB** | チャート描画の全ロジックが1コンポーネントに |
| `app/frontend/src/styles.css` | **183KB** | 全スタイルが単一CSSファイルに |
| `app/frontend/src/store.ts` | **76KB (2153行)** | 全アプリ状態が単一 Zustand ストアに |
| `app/backend/services/ml_service.py` | **248KB** | ML推論サービスが単一ファイル |
| `app/backend/services/rankings_cache.py` | **237KB** | ランキングキャッシュ全体が1ファイル |
| `app/backend/services/strategy_backtest_service.py` | **190KB** | バックテストロジック全体が1ファイル |
| `app/services/screener_engine.py` | **81KB** | スクリーニングエンジン全体が1ファイル |
| `app/backend/ingest_txt.py` | **60KB** | テキスト取り込みロジック全体が1ファイル |

**目安**: 1ファイル 300〜500行。バグ修正時に影響範囲を把握しにくく、コンフリクトも頻発する。

**推奨**:
- `DetailView.tsx` → `DetailHeader`, `DetailAnalysisPanel`, `DetailChartSection`, `DetailMemoSection` 等に分離
- `store.ts` → `barsStore`, `settingsStore`, `favoritesStore`, `eventsStore` 等のスライスに分割
- `styles.css` → コンポーネント単位の CSS Modules に分割

---

### 2. ストア設計の肥大化 (`store.ts`)

2153行の単一 Zustand ストアに以下が全て含まれている:
- 型定義（`Ticker`, `EventsMeta`, `MaSetting`, `Box` 等）
- ソートキー定義
- ヘルパー関数（`normalizeColor`, `parseIsoMs`, `normalizeBool` 等）
- バッチリクエスト管理のモジュールレベル変数群
- ポーリングロジック（`startEventsMetaPolling`）
- LocalStorage の永続化ロジック
- API通信ロジック

**推奨**: 型定義・ヘルパー・API通信を外部に切り出し、Zustand スライスパターンで分割。

---

### 3. フロントエンド/バックエンド間の型の重複

`Ticker` 型は134行のフィールドを持ち、バックエンド Python 側でも同等のフィールドが存在。API型を一元管理する仕組みが無い。

**推奨**: OpenAPI / JSON Schema でAPI仕様を一元管理し、型を自動生成。

---

## 🟡 深刻度: 中

### 4. `services/` ディレクトリの責務混在

`app/backend/services/` に33ファイルが平坦に配置され、異なる関心事が混在:
- ML推論: `ml_service.py`, `ml_config.py`
- ランキング: `rankings_cache.py`, `ranking_analysis_quality.py`
- バックテスト: `strategy_backtest_service.py`
- Toredex関連: `toredex_*.py` (10ファイル)
- データ取り込み: `yahoo_daily_ingest.py`, `yahoo_provisional.py`

**推奨**: `services/ml/`, `services/ranking/`, `services/toredex/`, `services/ingest/` 等のサブパッケージに整理。

### 5. モジュールレベル可変状態（グローバル変数）

`store.ts` で `inFlightBatchRequests`, `recentBatchRequests`, `batchRequestCount`, `eventsPollPromise` 等がモジュールレベルの可変変数として定義。テスト困難で状態リセットも難しい。

**推奨**: ストアの内部状態またはクラスベースのサービスにカプセル化。

### 6. バックエンドの重複ファイル

同名・類似のファイルが複数箇所に存在:
- `app/services/box_detector.py` と `app/backend/box_detector.py`
- `app/backend/events.py` と `app/backend/services/events.py`

**推奨**: 重複を整理し、参照先を一本化。

---

## 🟢 深刻度: 低

### 7. CSS の管理

183KB の単一CSSファイルは変更時の影響範囲が把握しにくく、名前衝突のリスクもある。

**推奨**: CSS Modules（`.module.css`）を導入し、コンポーネント毎に分割（`DailyMemoPanel.css` は分離済み）。

### 8. legacy フィールドの残存

`Ticker` 型に `// legacy` とコメントされたフィールドが複数（`shortScore`, `aScore`, `bScore`, `buyRiskDistance`）。`SortKey` 型にもレガシー互換の重複あり。

**推奨**: 使用箇所を調査し、不要であれば順次削除。

### 9. domain ディレクトリの活用不足

`app/backend/domain/` には `bars`, `indicators`, `positions`, `scoring`, `screening`, `similarity` のサブディレクトリがあるが、ビジネスロジックの大半は `services/` の巨大ファイルに集中。

**推奨**: ドメインロジックを `domain/` に移し、`services/` はオーケストレーション層として薄く保つ。

---

## 📊 改善の優先順位

| 優先度 | 施策 | 効果 |
|---|---|---|
| **1** | 巨大Viewコンポーネントの分割 | 可読性・保守性の大幅向上 |
| **2** | `store.ts` のスライス分割 | 状態管理の見通し改善 |
| **3** | `services/` のサブパッケージ整理 | バックエンドの関心事分離 |
| **4** | CSS のモジュール化 | スタイルの影響範囲の局所化 |
| **5** | レガシーフィールドの棚卸し | 型の簡潔化・認知負荷の低減 |

> **注意**: 一気に行わず、機能追加やバグ修正のタイミングで段階的にリファクタリングするのが現実的。特に優先度1〜2は日常の開発効率に直結するため、最も早く着手する価値がある。
