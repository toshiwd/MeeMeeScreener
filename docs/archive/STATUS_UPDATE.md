# イベントデータ更新の問題と解決方法

## 現在の状況

✅ **保有銘柄データ**: 正常にインポート完了
- 43件の保有銘柄
- 733件の完了ラウンド
- 5,218件の取引イベント

❌ **イベントデータ (決算・権利落ち)**: 取得失敗
- エラー: `earnings_excel_urls_not_found` / `rights_excel_urls_not_found`
- 原因: JPXのウェブサイトからExcelファイルのURLを自動検出できない

## 解決方法

### 方法1: JPXのウェブサイトを確認 (推奨)

JPXのウェブサイトが変更されている可能性があります。以下のURLにアクセスして、Excelファイルのダウンロードリンクを確認してください:

**決算予定:**
https://www.jpx.co.jp/listing/event-schedules/financial-announcement/

**権利落ち:**
https://www.jpx.co.jp/listing/others/ex-rights/

ページ内で `.xlsx` または `.xls` ファイルへのリンクを探してください。

### 方法2: 環境変数で手動設定

Excelファイルの直接URLが分かる場合、環境変数で設定できます:

**Windows (PowerShell):**
```powershell
$env:JPX_EARNINGS_XLSX_URLS = "https://www.jpx.co.jp/path/to/earnings.xlsx"
$env:JPX_RIGHTS_XLSX_URLS = "https://www.jpx.co.jp/path/to/rights.xlsx"
```

**Windows (コマンドプロンプト):**
```cmd
set JPX_EARNINGS_XLSX_URLS=https://www.jpx.co.jp/path/to/earnings.xlsx
set JPX_RIGHTS_XLSX_URLS=https://www.jpx.co.jp/path/to/rights.xlsx
```

複数のURLがある場合はカンマ区切り:
```powershell
$env:JPX_EARNINGS_XLSX_URLS = "https://url1.xlsx,https://url2.xlsx"
```

### 方法3: コードを修正してURL検出ロジックを改善

JPXのウェブサイトのHTML構造が変わった場合、`app/backend/events.py` の `_discover_excel_urls()` 関数を修正する必要があります。

## 暫定的な対処

イベントデータなしでも、保有銘柄の表示は正常に動作します。決算日・権利落ち日の情報が必要な場合のみ、上記の方法でイベントデータを取得してください。

## 次のステップ

1. ✅ **保有銘柄の確認**
   - フロントエンドの「保有/履歴」画面をリロード
   - 43件の保有銘柄が表示されるはずです

2. ⚠️ **イベントデータの取得** (オプション)
   - JPXのウェブサイトでExcelファイルのURLを確認
   - 環境変数で設定
   - または、イベントデータなしで使用を継続

3. ✅ **チャート上の建玉表示**
   - すでに動作しています (CSVファイルから直接読み込み)

## 問題のある保有銘柄について

診断レポートで「⚠要確認」と表示されている32件の銘柄は、数量がマイナスになった履歴があります。これは以下の原因が考えられます:

1. CSVデータに不足がある
2. 取引タイプの解析ミス
3. 実際の取引で数量の不整合があった

これらの銘柄は `has_issue` フラグが立っており、フロントエンドで「要確認」と表示されます。

## まとめ

- ✅ 保有銘柄データは正常にインポートされました
- ✅ 「保有/履歴」画面で保有銘柄が表示されるようになりました
- ❌ イベントデータ (決算・権利落ち) の自動取得は現在失敗中
- 💡 イベントデータは手動設定で対応可能
