# Frontend Performance Fix Plan (2026-03-11)

`app/frontend` の表示速度レビューを、修正順に整理した予定表。
一覧優先で、重複修正を避けるために「画面別」ではなく「原因別」にまとめる。

## 総括

今回のフロントエンド遅延は、主に次の4系統に集約される。

1. Zustand の広い購読で親コンポーネントが再描画されやすい
2. 一覧画面が可視範囲を超えてバー取得・描画準備を始める
3. セル/カード単位で `favorites` やチャート描画条件を重く購読している
4. `DetailView` が解析計算とチャート初期化を同時多発で行う

このため、個別画面ごとに場当たりで直すより、先に購読境界と取得境界を整理した方が効果が大きい。

## 修正予定表

| 優先 | 区分 | 対象 | 症状 | 証拠 | 修正方針 |
|---|---|---|---|---|---|
| P1 | store購読 | `GridView.tsx` | バー取得やイベント更新で一覧親が再評価される | `barsCache`/`boxesCache`/`maSettings`/`eventsMeta` を親で直接購読 | 大きい object 購読を分割し、イベント表示部と一覧本体を分離 |
| P1 | store購読 | `StockTile.tsx` | お気に入り更新で可視タイル全体が再描画される | 各タイルが `favorites` 全体と `loadFavorites()` を持つ | `isFavorite` を親で解決、`loadFavorites()` を上位1回に集約 |
| P1 | 横断 | `Header.tsx` / `DetailView.tsx` | favorites の読込責務が複数箇所に散る | 詳細ヘッダとタイル側の両方が favorites 読込を持つ | favorites 初期化責務を上位へ一本化 |
| P2 | 一覧取得 | `RankingView.tsx` | 表示外も含めて全件バー取得する | `sortedItems.map(... )` をそのまま `ensureBarsForVisible()` に渡している | 可視範囲取得または段階取得へ変更 |
| P2 | 一覧取得 | `FavoritesView.tsx` / `CandidatesView.tsx` | 検索結果や一覧全件に対して一括取得しやすい | `searchCodes` や `sortedItems` 起点で取得している | `RankingView` と同じ取得戦略へ統一 |
| P2 | 一覧取得 | `PositionsView.tsx` | held/active の全件に対してバー取得する | `heldItems` / `activeItems` 全件で `ensureBarsForVisible()` 実行 | タブ内可視範囲ベースに変更 |
| P2 | 共通部品 | `ChartListCard.tsx` | in-view 遅延はあるが取得量は減っていない | `deferUntilInView` は描画だけ抑制 | 親側取得戦略とセットで使う |
| P3 | 詳細画面 | `DetailView.tsx` | 初回表示で解析計算と比較チャート初期化が重い | `buildCandlesWithStats()` の重複、複数 `DetailChart` の同時 mount | 解析結果共有、比較チャート遅延 mount |
| P3 | 詳細画面 | `DetailChart.tsx` | インスタンスごとの `createChart()` が重い | 比較表示時にチャート初期化が多発 | 使っていないチャートを mount しない構造へ寄せる |
| P3 | サムネイル描画 | `ThumbnailCanvas.tsx` | 一覧セル描画ごとに PNG 化が走りメインスレッド負荷が高い | `canvas.toDataURL()` を描画フロー内で実行 | キャッシュ保存頻度を下げ、初回描画とスナップショット生成を分離 |
| P3 | イベント更新 | `storeHelpers.ts` / `backendReady.tsx` | イベント更新完了後に一覧再読込が走りやすい | `loadEventsMeta()` 後に `loadList()` を自動実行 | 一覧再読込を必要時だけに限定し、UI とポーリングを疎結合化 |
| P3 | ヘッダUI | `UnifiedListHeader.tsx` | 同一の document listener を二重登録している | 同内容の `useEffect` が2本ある | 1本に統合して無駄なイベント処理を削除 |
| P4 | 検索パネル | `SimilarSearchPanel.tsx` | スライダ/日付変更のたびに即 API 再検索する | `alpha` と `targetDate` がそのまま検索 effect 依存 | debounce 追加、または明示実行式へ変更 |
| P4 | 練習画面 | `PracticeView.tsx` | 1画面内でバー変換と派生計算が多重に連鎖する | `dailyBars` から `trainingBars`、週足/月足、candles、volume を段階生成 | 変換パイプラインを共有し、表示中チャートだけ計算する |
| P4 | 付随 | `UnifiedListHeader.tsx` | イベントメタ更新が一覧 UI を定期刺激する | `eventsMeta` を購読しつつ更新導線も持つ | イベント状態表示を isolated な小部品へ分離 |

## 追加チェック結果

### 1. 一覧画面の重複パターン

`RankingView` だけでなく、`FavoritesView`、`CandidatesView`、`PositionsView` も同じ構造を持つ。

- store から `barsCache`、`barsStatus`、`boxesCache`、`maSettings`、各種 `settings` を直接購読
- `sortedItems` または一覧系 code 配列をそのまま `ensureBarsForVisible()` に渡す
- `ChartListCard` を単純 `map` で並べる

このため、`RankingView` だけ直しても、別画面で同じ遅さが残る。修正は共通方針でまとめるべき。

### 2. favorites 読込責務の分散

favorites 周りは少なくとも次の箇所に責務が散っている。

- `StockTile.tsx`
- `Header.tsx`
- `DetailView.tsx`

初回ロード時の effect の競合と、更新時の広い再描画が起きやすい。`favoritesLoaded` の判定とロードは上位で一度だけ行う形へ寄せる。

### 3. 仮想化の適用差

- `GridView` は `react-window` を使用
- `RankingView` / `FavoritesView` / `CandidatesView` / `PositionsView` は実質フルリスト描画

したがって一覧改善の第1段は `GridView` の再描画抑制、第2段はリスト系画面の取得境界統一が妥当。

### 4. サムネイル生成コスト

`ThumbnailCanvas` は描画後に `canvas.toDataURL("image/png")` でスナップショットを作り、サムネイルキャッシュへ保存している。

- 描画回数が多い一覧では、canvas 描画に加えて PNG エンコードがメインスレッドに載る
- `cacheKey` が変わる条件に `maSettings`、`showBoxes`、サイズが含まれ、設定変更時に再生成が広がる

一覧側の再描画を減らした上で、スナップショット生成頻度も下げないと改善幅が頭打ちになる。

### 5. イベント更新の波及

イベント更新監視は `BackendReadyProvider` と `storeHelpers` の両方から一覧へ波及する。

- ready 後に `loadEventsMeta()` の定期実行が始まる
- refresh 完了時に `loadList()` を自動で呼び直す

この構造だと、一覧画面で何も操作していなくても全体更新が差し込まれやすい。イベント状態 UI と銘柄一覧更新は分けて扱う方が安全。

### 6. PracticeView は別系統で重い

`PracticeView` は一覧系とは別に、単一画面内で多数の派生配列を段階生成している。

- `dailyBars`
- `trainingBars`
- `visibleDailyBars`
- `weeklyAggregate` / `monthlyAggregate`
- `dailyCandles` / `weeklyCandles` / `monthlyCandles`
- `dailyVolume` / `weeklyVolume` / `monthlyVolume`

一覧優先では後回しでよいが、詳細画面と同じく「変換共有」と「非表示チャートの計算抑制」が必要。

### 7. ヘッダと補助パネルの小さな無駄

`UnifiedListHeader` には同一内容の `useEffect` が二重にあり、`mousedown` / `keydown` listener を重複登録している。

`SimilarSearchPanel` は `alpha` と `targetDate` の変更ごとに即座に API を叩くため、スライダ操作中に短時間で連続リクエストが発生する。

どちらも最優先ではないが、一覧の体感やバックエンド負荷に悪影響があるため、横断修正時に一緒に潰す価値がある。

## 実装フェーズ案

### Phase 1: 再描画伝播の抑制

- `GridView` の大きい selector を分解
- `eventsMeta` 表示を一覧本体から切り離す
- `StockTile` の `favorites` 全体購読を廃止
- `loadFavorites()` の起点を上位へ一本化

期待効果:
- スクロール中の引っ掛かり低減
- お気に入り切替時の巻き込み再描画低減
- 定期イベント更新による不要再描画低減

### Phase 2: 一覧取得戦略の統一

- `RankingView` の全件取得を可視範囲取得へ変更
- `FavoritesView` / `CandidatesView` / `PositionsView` も同一戦略へ寄せる
- `ChartListCard` の `deferUntilInView` を取得境界と一致させる

期待効果:
- 初回表示の API 負荷低減
- 検索/ソート変更時の待ち時間短縮
- 大きいリストでの体感改善

### Phase 3: 詳細画面の高コスト処理削減

- `buildCandlesWithStats()` の重複計算を排除
- 比較チャート/補助チャートを必要時のみ mount
- `DetailChart` の同時初期化数を減らす

期待効果:
- 詳細画面初回表示短縮
- 比較表示切替の引っ掛かり低減

## 先に触るべきファイル

1. `app/frontend/src/routes/GridView.tsx`
2. `app/frontend/src/components/StockTile.tsx`
3. `app/frontend/src/components/layout/Header.tsx`
4. `app/frontend/src/routes/RankingView.tsx`
5. `app/frontend/src/routes/FavoritesView.tsx`
6. `app/frontend/src/routes/CandidatesView.tsx`
7. `app/frontend/src/routes/PositionsView.tsx`
8. `app/frontend/src/routes/DetailView.tsx`
9. `app/frontend/src/components/DetailChart.tsx`

## 受け入れ観点

- 一覧画面でお気に入り1件切替時に、可視セル全体が再描画されない
- `GridView` でイベントメタ更新中もスクロールが詰まらない
- `RankingView` / `FavoritesView` / `CandidatesView` / `PositionsView` で、表示外アイテムのバー取得を抑えられる
- `DetailView` で比較表示なしの初回表示が軽くなる

## 備考

- この文書は修正順を決めるための予定表であり、まだ実装は含まない
- `lint` / `test` / `build` は未実行

## バックエンド依存レビュー

フロント表示速度と直結する API 境界だけ追加確認した。

### 1. `/api/grid/screener`

対象:
- `app/backend/api/routers/grid.py`
- `app/backend/infra/duckdb/screener_repo.py`

確認結果:
- `/api/grid/screener` は DB mtime / 日付 / limit をキーにした API キャッシュを持つ
- キャッシュミス時は `fetch_screener_batch()` で全銘柄分のメタ、日足、月足、イベント、ML 補助情報をまとめて組み立てる
- 返却自体は重いが、同一条件ではバックエンド側である程度吸収される

示唆:
- フロント側の `loadList()` 乱発を減らす価値は高い
- 一覧表示を軽くしたい主戦場は、やはりフロントの再描画とバー取得境界

### 2. `/api/batch_bars_v3`

対象:
- `app/backend/api/routers/bars.py`

確認結果:
- 複数 code / 複数 timeframe をまとめて返せる設計
- 48件単位のバッチ取得をフロント store がすでに前提にしている

示唆:
- フロント一覧での改善余地は API 変更より「呼ぶ量を減らす」方にある
- `RankingView` などの全件取得を止めるだけで効果が出る見込み

### 3. イベント更新

対象:
- `app/backend/api/events_routes.py`
- `app/backend/services/data/events.py`

確認結果:
- イベント refresh 完了時にバックエンド側で screener cache を無効化している
- つまりフロントが即 `loadList()` を再発行しなくても、次回一覧取得時に最新データへ寄る前提はある

示唆:
- `storeHelpers.ts` の refresh 完了後 `loadList()` は弱めてよい
- 一覧の即時再描画より、イベントメタだけ先に更新する構成へ寄せられる

### 4. 類似検索

対象:
- `app/backend/api/routers/similar.py`
- `app/backend/similarity.py`

確認結果:
- 類似検索は in-memory artifact を読み込んだ後に pandas/numpy ベースで検索する
- refresh は別スレッドで走るが、検索自体は入力変更ごとに普通に計算される

示唆:
- `SimilarSearchPanel` 側で debounce を入れる意義は十分ある
- バックエンド都合で即時検索が必要という依存は見当たらない

## バックエンド依存を見た上での結論

- 先にバックエンドを見た価値はあったが、大規模なバックエンド修正は不要
- フロント主導で進めてよい
- ただし次の2点はバックエンド依存を踏まえて方針を確定できる
  - イベント refresh 後の `loadList()` 自動再実行は縮小候補
  - 一覧バー取得は API 変更ではなく可視範囲化で改善する
