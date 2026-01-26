# ヘッダーUIレイアウト仕様書 (Header UI Specification)

このドキュメントは、`GridView`および`UnifiedListHeader`（ランキング画面等）におけるヘッダーレイアウトの仕様と、既知の不具合（ボタン重なり、中央寄せ崩れ）を防ぐための実装ルールをまとめたものです。
今後の改修時には本仕様を遵守してください。

## 1. 共通実装ルール (Critical CSS Rules)

### 1.1 アイコンボタン (`.icon-button`)
ボタン内のテキストとアイコンが重なったり、勝手に縮小されることを防ぐため、以下のスタイルを必須とします。

```css
.icon-button {
  /* フレックスボックスの挙動制御 */
  display: inline-flex;       /* 親コンテナの縮小圧力を受けにくくする */
  align-items: center;
  justify-content: center;
  
  /* サイズと縮小の防止 */
  width: auto;                /* コンテンツに合わせて広がる */
  max-width: none;            /* 幅制限を解除 */
  min-width: 34px;            /* 最小サイズ保証 */
  flex-shrink: 0;             /* 親コンテナが狭くても絶対に縮まない */
  
  /* テキスト表示の保証 */
  white-space: nowrap;        /* テキストの折り返しを禁止 */
}
```

### 1.2 ヘッダー行コンテナ (`.header-row-top`, `.header-row-bottom`)
画面幅が広い場合に要素が中央に寄ってしまう現象（Flexboxの不確定な挙動）を防ぐため、幅を明示します。

```css
.header-row-top,
.header-row-bottom {
  width: 100%;                /* 画面幅いっぱいを使う */
  display: flex;
  align-items: center;
  /* 必要に応じて justify-content: space-between 等を使用 */
}
```

---

## 2. GridView (スクリーナー一覧) ヘッダー仕様

### 2.1 構造
ヘッダーは「左側グループ」と「右側アクション」に明確に分離し、`justify-content: space-between` で配置します。

*   **Header Top Row (`header-row-top`)**:
    *   **左側 (Left Group)**:
        *   `top-bar-branding` (ロゴ) と `TopNav` (ナビゲーション) を一つの `div` (flex container) で囲むこと。
        *   これにより、ウィンドウ幅が広がってもロゴとナビは常に左側に固定される。
        *   ロゴ: `app-brand` クラスを使用。タイトルとサブタイトルを含む。
    *   **右側 (Right Group)**:
        *   `list-header-actions`: ソート、表示設定、フィルタ等の操作ボタン群。
        *   `margin-left: auto` を使うか、親の `space-between` に任せる。

### 2.2 レイアウト図
```text
[Header Row Top (width: 100%)]
+-------------------------------------------------------+
| [Left Group (gap: 16px)]               [Right Group]  |
| [[Logo] [Nav(Screener|Ranking...)]]    [[Buttons...]] |
+-------------------------------------------------------+
```

---

## 3. UnifiedListHeader (ランキング詳細等) ヘッダー仕様

### 3.1 構造
情報は2段組みで構成し、縦方向のスペースを節約します。

*   **Row 1 (`header-row-top`)**:
    *   ナビゲーションタブまたは戻るボタン。
    *   右側にフィルタ、メニュー等のアクション。
*   **Row 2 (`header-row-bottom`)**:
    *   **左側**: 時間足選択 (`list-timeframe`)、期間選択 (`list-range`)。
    *   **中央/右側**: 検索バー (`list-search`)。
        *   検索バーは `width: 160px` 程度の固定幅または最大幅を持ち、大きくなりすぎないようにする。
    *   **右端**: イベント更新ステータス (`list-events-inline`)。
        *   以前は3段目にあったステータス行を、この行の右側にインラインで統合する。
        *   `margin-left: auto` 等で右寄せ、またはFlexフローに従う。

### 3.2 レイアウト図
```text
[Header Row Bottom (width: 100%)]
+-----------------------------------------------------------------------+
| [Month/Week] [3M/6M] [Search Box(160px)]  [Status: Waiting... (Text)] |
+-----------------------------------------------------------------------+
```

---

## 4. 今後の改修時の注意点 (Regression Testing)

コードを変更した際は、以下のポイントを必ず確認してください。

1.  **ロゴの有無**: スクリーナー画面で左上のロゴが消えていないか。
2.  **配置の偏り**: ウィンドウを最大化した際、ナビゲーションやボタンが中央に不自然に寄っていないか（常に左右に展開されているか）。
3.  **文字被り**: 「並び替え」「表示」などのボタン内テキストが、隣のアイコンと重なっていないか。
4.  **ステータス表示**: ランキング画面で、イベント更新状態がヘッダー内に収まっているか（行が増えていないか）。

以上
