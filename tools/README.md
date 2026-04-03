# Tools

MeeMee の build / release / selftest 用ツールの入口はここに集約する。

## 正規入口

- `build_release.cmd`
  - 公開 build 入口はこれだけ
  - 既定は `release/MeeMeeScreener/` の onedir build のみ
  - 既定の DuckDB 同梱元は `%LOCALAPPDATA%\\MeeMeeScreener\\data\\stocks.duckdb`
  - 別 DB を使う場合だけ `MEEMEE_RELEASE_DB_PATH` を明示する
  - `-PackageZip` を付けたときだけ `release/MeeMeeScreener-portable.zip` を作る
  - `-SmokeRun` を付けたときだけ build 後に exe 起動確認を行う
- `build_release.ps1`
  - `build_release.cmd` の内部実装

## 配布後ランチャー

- `portable_bootstrap.cmd`
- `portable_bootstrap.ps1`
  - ZIP 展開後に runtime 前提を確認して `MeeMeeScreener.exe` を起動する
  - build はしない

## selftest

- `selftest.ps1`
  - `-Mode dev`: 開発環境 selftest
  - `-Mode release`: build 済み `release/MeeMeeScreener/` に対する selftest

## 補助ファイル

- `export_pan.vbs`
  - PAN export 用
- `code.txt`
  - `code.txt` の既定配置
- `setup/`
  - DB / seed / import 系のセットアップ補助
- `debug/`
  - 手元診断用スクリプト
