# TRADEX Trader Live Verify

## 目的
prepared runtime profile 上で、unshimmed single-session の TRADEX Research Foundation を end-to-end で確認する。

この verify で確認するもの:

- preflight が pass すること
- existing single-session runner path がそのまま動くこと
- `observation_snapshot.json`
- `strategy_judgement.json`
- `teacher_evaluation_row.json`
- `judge_input.json`
- `judge_decision.json`
- `authoritative_decision.json`
- `research_memory.json`

この verify では compare truth と decision policy を変えない。

## 公式 runtime profile

- artifact root: `G:\Tradex`
- prepared data DB: `STOCKS_DB_PATH` で明示

既定候補:

- `G:\Tradex\db\stocks.duckdb`
- `%LOCALAPPDATA%\MeeMeeScreener-dev\data\stocks.duckdb`
- `%LOCALAPPDATA%\MeeMeeScreener\data\stocks.duckdb`

現状の運用前提:

- `G:\Tradex` は artifact root として使う
- prepared DuckDB は `STOCKS_DB_PATH` override で指定してよい
- Research OS 側は DB を自動生成・自動修復しない

## live verify lane

通常回帰と live verify は混ぜない。

- 通常回帰:
  - `python -m pytest -q ...`
- live verify:
  - `tests/test_tradex_research_live_acceptance.py` を単体で実行
  - もしくは `tools/verify_tradex_trader_foundation.ps1` を使う

理由:

- Windows 上の DuckDB file lock を通常 suite failure と誤認しないため
- prepared DB を使う verify を別 Python process に分離するため

## 事前条件

- `MEEMEE_DISABLE_LEGACY_ANALYSIS=0`
- runtime root が存在する
- prepared DuckDB が存在する
- prepared DuckDB が少なくとも以下を持つ
  - `daily_bars`
  - `market_regime_daily`
- hypothesis target の `code` / `as_of_date` が `daily_bars` に存在する

## numeric-only live verify

推奨コマンド:

```powershell
powershell -ExecutionPolicy Bypass -File tools/verify_tradex_trader_foundation.ps1 `
  -Mode numeric `
  -RuntimeRoot G:\Tradex `
  -StocksDbPath C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb `
  -Code 6963 `
  -AsOfDate 20260403
```

pytest を直接使う場合:

```powershell
$env:MEEMEE_DISABLE_LEGACY_ANALYSIS='0'
$env:MEEMEE_ENABLE_TRADEX_LIVE_VERIFY='1'
$env:MEEMEE_TRADEX_ROOT='G:\Tradex'
$env:STOCKS_DB_PATH='C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb'
$env:TRADEX_LIVE_HYPOTHESIS_CODE='6963'
$env:TRADEX_LIVE_HYPOTHESIS_DATE='20260403'
python -m pytest -q tests/test_tradex_research_live_acceptance.py -k single_session
```

## LLM live verify

numeric-only verify の後に実行する。

必須 env:

- `TRADEX_TRADER_LLM_ENDPOINT_URL`
- `TRADEX_TRADER_LLM_MODEL`
- `TRADEX_TRADER_LLM_API_KEY`
- `TRADEX_TRADER_LLM_TIMEOUT_SEC` は任意

推奨コマンド:

```powershell
powershell -ExecutionPolicy Bypass -File tools/verify_tradex_trader_foundation.ps1 `
  -Mode llm `
  -RuntimeRoot G:\Tradex `
  -StocksDbPath C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb `
  -Code 6963 `
  -AsOfDate 20260403 `
  -LlmEndpointUrl https://example.invalid/v1 `
  -LlmModel gpt-4.1-mini `
  -LlmApiKey <secret>
```

pytest を直接使う場合:

```powershell
$env:MEEMEE_DISABLE_LEGACY_ANALYSIS='0'
$env:MEEMEE_ENABLE_TRADEX_LIVE_VERIFY='1'
$env:MEEMEE_ENABLE_TRADEX_LIVE_LLM_VERIFY='1'
$env:MEEMEE_TRADEX_ROOT='G:\Tradex'
$env:STOCKS_DB_PATH='C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb'
$env:TRADEX_LIVE_HYPOTHESIS_CODE='6963'
$env:TRADEX_LIVE_HYPOTHESIS_DATE='20260403'
$env:TRADEX_TRADER_LLM_ENDPOINT_URL='https://example.invalid/v1'
$env:TRADEX_TRADER_LLM_MODEL='gpt-4.1-mini'
$env:TRADEX_TRADER_LLM_API_KEY='<secret>'
python -m pytest -q tests/test_tradex_research_live_acceptance.py -k llm_adapter
```

## 失敗時の読み方

- preflight failure:
  - runtime / DB / readiness の不足
- runner failure:
  - upstream artifact shape か current TRADEX execution path の失敗
- LLM failure:
  - env 未設定
  - provider timeout
  - invalid JSON output
  - schema validation failure

どの failure でも compare truth と decision policy は変えない。
