# MeeMee Screener

MeeMee Screener は MOOMOO Desktop 風の高速株式スクリーナーです。  
本リポジトリは Windows 向けのデスクトップ配布（pywebview + FastAPI + onedir）に対応しています。

## 配布版の使い方（Windows）

### 1. WebView2 Runtime を入れる（未導入PCのみ）
MeeMee Screener は Microsoft WebView2 Runtime が必要です。  
以下のどちらかで導入してください。

- 公式インストーラ（推奨）  
  Evergreen Standalone Installer を実行  
  例: `MicrosoftEdgeWebView2RuntimeInstallerX64.exe`

- winget で導入（管理者権限）  
  `winget install Microsoft.EdgeWebView2Runtime`

### 2. ZIP を展開して起動
`release/MeeMeeScreener-portable.zip` を展開し、  
`MeeMeeScreener/MeeMeeScreener.exe` を起動します。

## デスクトップ配布ビルド（Windows）

### 事前準備
- アイコンを `resources/icons/app_icon.ico` に配置（必須）
- 依存: Node.js / Python / backend requirements / pyinstaller / pywebview

### 実行
`tools/build_release.cmd` をダブルクリック、または実行します。

出力:
- `release/MeeMeeScreener/`（onedir）
- `release/MeeMeeScreener/MeeMeeScreener.exe`
- `release/MeeMeeScreener-portable.zip`

## moomoo の ebk から code.txt を作る
moomoo の EBK 形式（例: `JP#4366`）を `code.txt` に変換できます。

例:
```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools/convert_moomoo_ebk.ps1 `
  -InputFile C:\path\to\moomoo.ebk `
  -OutputFile tools\code.txt
```

`tools/run_export.cmd` に EBK を渡すと自動で変換します。
```cmd
tools\run_export.cmd C:\path\to\moomoo.ebk
```

## データ・設定・ログの保存先
配布物直下には書き込まず、以下を推奨します。

- DB: `%LOCALAPPDATA%\MeeMeeScreener\data\`
- 設定: `%LOCALAPPDATA%\MeeMeeScreener\config\`
- 状態: `%LOCALAPPDATA%\MeeMeeScreener\state\`
- ログ: `%LOCALAPPDATA%\MeeMeeScreener\logs\launcher.log`

## 開発用（Windows PowerShell）

### Backend
```powershell
cd C:\work\meemee-screener\app\backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python ingest_txt.py
uvicorn main:app --reload --port 8000
```

### Frontend
```powershell
cd C:\work\meemee-screener\app\frontend
npm install
npm run dev
```

### ヘルスチェック
```powershell
curl http://localhost:8000/health
curl http://localhost:8000/api/health
curl http://localhost:8000/api/list
```

ブラウザで `http://localhost:5173` を開きます。  
TXT データが無い場合は `data/txt` に配置するよう UI が案内します。
