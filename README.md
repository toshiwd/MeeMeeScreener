# MeeMee Screener

MeeMee Screener は、銘柄スクリーニングと売買履歴連携を行う Windows 向けデスクトップアプリです。  
構成は `pywebview + FastAPI + React (Vite)` です。

## 起動（通常）

```powershell
.\run.ps1
```

または:

```powershell
python -m app.desktop.launcher
```

## 起動（デバッグ）

```powershell
.\run_debug.ps1
```

## 開発環境（ローカル）

### Backend

```powershell
cd app\backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

### Frontend

```powershell
cd app\frontend
npm install
npm run dev
```

### ヘルスチェック

```powershell
curl http://localhost:8000/health
curl http://localhost:8000/api/health
curl http://localhost:8000/api/list
```

## データ配置（既定）

- DB: `%LOCALAPPDATA%\MeeMeeScreener\data\`
- 設定: `%LOCALAPPDATA%\MeeMeeScreener\config\`
- 状態: `%LOCALAPPDATA%\MeeMeeScreener\state\`
- ログ: `%LOCALAPPDATA%\MeeMeeScreener\logs\launcher.log`

## 取引 CSV ファイル名（運用ルール）

アプリは `MEEMEE_DATA_DIR`（通常 `%LOCALAPPDATA%\MeeMeeScreener\data`）配下の CSV を読み込みます。

| ブローカー | ファイル名 |
| --- | --- |
| 楽天証券 | 楽天証券取引履歴.csv |
| SBI証券 | SBI証券取引履歴.csv |

## リリースビルド

```powershell
tools\build_release.cmd
```

成果物:

- `release/MeeMeeScreener/`
- `release/MeeMeeScreener/MeeMeeScreener.exe`
- `release/MeeMeeScreener-portable.zip`

## 関連ドキュメント

- `AGENTS.md`（作業時の基本ルール）
- `app/backend/AGENTS.md`（Backend 作業ルール）
- `app/frontend/AGENTS.md`（Frontend 作業ルール）
- `docs/MEEMEE_PRINCIPLES.md`（MeeMee 固有のプロダクト原則）
- `docs/README.md`（ドキュメント索引）
- `docs/CODEX.md`（必要時のみ参照する詳細仕様 / Runbook）
- `docs/TXT_UPDATE_RUNBOOK.md`（TXT 更新ジョブ運用）
- `SMOKE_TEST.md`（最小回帰テスト手順）
- `tools/README.md`（ツール類の概要）
