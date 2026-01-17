# Tools ディレクトリ

開発・デバッグ・テスト用のツールとスクリプトを格納しています。

## ディレクトリ構成

### debug/
デバッグ用スクリプト

- `check_db_status.py` - データベースの状態を確認
- `check_rights_data.py` - 権利落ち日データを確認
- `debug_*.py` - 各種デバッグスクリプト
- `diagnose_issues.py` - 問題診断ツール

### setup/
初期セットアップ用スクリプト

- `init_db_schema.py` - データベーススキーマの初期化
- `init_events_meta.py` - イベントメタデータの初期化
- `import_csv_to_db.py` - CSVデータのインポート
- `list_tables.py` - データベーステーブル一覧表示

### test/
テストスクリプト

- `test_api.py` - API エンドポイントのテスト
- `test_events_fetch.py` - イベントデータ取得のテスト
- `test_positions_api.py` - ポジションAPIのテスト

## ビルド関連

- `build_release.ps1` - リリースビルドスクリプト（メイン）
- `build_release.cmd` - リリースビルドスクリプト（バッチファイル）

## その他

- `export_pan.vbs` - パン・ローリングデータエクスポート
- `code.txt` - 銘柄コードリスト
- `convert_moomoo_ebk.ps1` - moomooデータ変換
