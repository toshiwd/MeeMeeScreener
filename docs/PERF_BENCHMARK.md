# API Performance Benchmark

MeeMee Screener の主要 API (`/api/batch_bars`, `/api/grid/screener`) の p50/p95 を簡易計測する手順です。

## Prerequisites

1. バックエンドを起動しておく（例: `python -m app.desktop.launcher` または `uvicorn`）。
2. `http://127.0.0.1:8000` で API に到達できることを確認する。

## Run

```powershell
python tools/analytics/benchmark_api.py --runs 20 --warmup 3 --batch-codes 48 --limit 240
```

結果を JSON 保存する場合:

```powershell
python tools/analytics/benchmark_api.py --output tmp/api_benchmark.json
```

## Optional Arguments

- `--base-url`: 既定 `http://127.0.0.1:8000`
- `--codes`: カンマ区切りのコードを明示指定（例: `7203,6758,9984`）
- `--batch-codes`: batch 計測で使う銘柄数（既定 48）
- `--limit`: バー本数（既定 240）
- `--runs`: 計測回数（既定 20）
- `--warmup`: ウォームアップ回数（既定 3）

## KPI Reference

- `/api/batch_bars` p95: 48銘柄 x 240本で 250ms 以下
- `/api/grid/screener` p95: 600銘柄相当で 1.2s 以下
