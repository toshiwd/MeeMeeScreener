# Position Calculation Memo (SBI / Rakuten)

最終更新: 2026-02-01

## 共通（保有判定 / 出力）
- 目的: 銘柄ごとの `spot_qty`（現物残）, `margin_long_qty`（信用買建残）, `margin_short_qty`（信用売建残）を算出。
- 保有判定: `spot_qty != 0 OR margin_long_qty != 0 OR margin_short_qty != 0`。
- 表示（売-買）: `sell = margin_short_qty`, `buy = spot_qty + margin_long_qty`。
- ルート: `trade_events` → `rebuild_positions()` → `positions_live`。
- UI `/positions/held` は **ロット換算（÷100）** で返却（表示は 0-1 など）。

## 楽天証券（取引履歴CSV）
### 入力/正規化
- エンコーディング: cp932。
- 必須列（実CSV）: `約定日`, `受渡日`, `銘柄コード`, `取引区分`, `売買区分`, `数量［株］`, `単価［円］`, `受渡金額［円］`, `建約定日`, `建単価［円］`。
- 文字列: 前後空白除去。`nan/none/--/-/－` は空扱い。
- 数値: カンマ除去して整数化。失敗時は警告として記録（行はスキップ）。

### 取引区分 × 売買区分 → アクション
- **現物**: `買付` → `SPOT_BUY` / `売付` → `SPOT_SELL`
- **信用新規**: `買建` → `MARGIN_OPEN_LONG` / `売建` → `MARGIN_OPEN_SHORT`
- **信用返済**: `売埋` → `MARGIN_CLOSE_LONG` / `買埋` → `MARGIN_CLOSE_SHORT`
- **現渡**: `DELIVERY_SHORT`（spot と short を同時に減算）
- **入庫**: `SPOT_IN`（取引区分より優先）
- **現引**: `MARGIN_SWAP_TO_SPOT`（long→spot へ振替）
- **出庫**: `SPOT_OUT`
- 未対応ラベル: `unknown_labels_by_code` に警告として蓄積。

### 重複排除・ハッシュ
- **dedup**: 推奨フィールドを正規化して連結 → `dedup_key`。
- 同一 `dedup_key` が再登場したら警告（`duplicate_skipped`）。
- `row_hash` は `dedup_key + row_index` で作成し、同一内容の別行は落とさない。

## SBI証券（取引履歴CSV）
### 入力/正規化
- エンコーディング: cp932。
- ヘッダ自動検出後、`取引ラベル`（売買/取引区分の混在ラベル）から判定。
- 数量は `to_float` で取得、`100株` 単位以外は警告。

### 取引ラベル → イベント種別（event_kind）
`determine_event_kind()` が以下のようなキーワードで分類:
- `BUY_OPEN`, `SELL_CLOSE`, `SELL_OPEN`, `BUY_CLOSE`
- `DELIVERY` / `TAKE_DELIVERY`
- `INBOUND` / `OUTBOUND`
（具体キーワードは `TradeParser.determine_event_kind` を参照）

### event_kind → ポジションアクション
`_map_parser_row_to_event()` で `memo`（ラベル）を見て spot/margin を判定:
- `現物/現渡/現引/入庫/出庫` を含む場合は現物系 (`SPOT_*`) を優先
- それ以外は信用系 (`MARGIN_*`) へ

### 重複排除・ハッシュ
- `make_dedup_key` に `row_id` を含むため、**同内容でも行番号が違えば別扱い**。
- そのため「同一CSVの重複取込」は `trade_events` 側の `source_row_hash` で抑止。

## 残高再構成（rebuild_positions）
- `trade_events` を銘柄別に時系列で加算・減算。
- 負数は **0丸めしない**。負数発生は `issue_notes` と `has_issue` に反映。
- `positions_live` に spot/margin の残高と `buy_qty`/`sell_qty` を保存。

