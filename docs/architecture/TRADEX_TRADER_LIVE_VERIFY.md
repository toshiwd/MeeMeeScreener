# TRADEX Trader Live Verify

## 目的
prepared runtime profile 上で、unshimmed single-session の TRADEX Trader Foundation を end-to-end で確認する。

この verify で確認するもの:

- preflight が pass する
- existing single-session runner path がそのまま動く
- 次の artifact が生成される
  - `observation_snapshot.json`
  - `strategy_judgement.json`
  - `teacher_evaluation_row.json`
  - `judge_input.json`
  - `judge_decision.json`
  - `authoritative_decision.json`
  - `research_memory.json`

この verify は compare truth と decision policy を変更しない。

## official runtime profile

- artifact root: `G:\Tradex`
- prepared data DB: `STOCKS_DB_PATH` で明示する

v1 の前提:

- `G:\Tradex` は artifact root として使う
- prepared DuckDB は `STOCKS_DB_PATH` override で与える
- Research OS は DB を自動生成しない
- `G:\Tradex\db\stocks.duckdb` を official prepared DB とみなす作業は upstream task とする

## live verify lane

通常回帰と live verify は混ぜない。

- 通常回帰:
  - `python -m pytest -q ...`
- live verify:
  - `tests/test_tradex_research_live_acceptance.py` を単体実行
  - または `tools/verify_tradex_trader_foundation.ps1`

理由:

- Windows 上では DuckDB file lock が通常 suite failure と混ざりやすい
- prepared DB を使う verify は別 Python process で実行した方が安定する

live hypothesis の `session_id` は既定で短く固定する。

- numeric-only: `tradex-live-num`
- LLM: `tradex-live-llm`

## prerequisites

- `MEEMEE_DISABLE_LEGACY_ANALYSIS=0`
- runtime root が存在する
- prepared DuckDB が存在する
- prepared DuckDB に次がある
  - `daily_bars`
  - `market_regime_daily`
- hypothesis target の `code` / `as_of_date` が実データと一致する

## numeric-only live verify

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

必要 env:

- `TRADEX_TRADER_LLM_ENDPOINT_URL`
- `TRADEX_TRADER_LLM_MODEL`
- `TRADEX_TRADER_LLM_API_KEY`
- `TRADEX_TRADER_LLM_TIMEOUT_SEC` は任意

2026-04-09 の確認済み構成:

- endpoint: `https://api.ai.sakura.ad.jp/v1/chat/completions`
- working model: `Qwen3-Coder-30B-A3B-Instruct`

確認済みの失敗:

- `preview/Kimi-K2.5` は `400 This model is not available.`

```powershell
powershell -ExecutionPolicy Bypass -File tools/verify_tradex_trader_foundation.ps1 `
  -Mode llm `
  -RuntimeRoot G:\Tradex `
  -StocksDbPath C:\Users\enish\AppData\Local\MeeMeeScreener-dev\data\stocks.duckdb `
  -Code 6963 `
  -AsOfDate 20260403 `
  -LlmEndpointUrl https://api.ai.sakura.ad.jp/v1/chat/completions `
  -LlmModel Qwen3-Coder-30B-A3B-Instruct `
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
$env:TRADEX_TRADER_LLM_ENDPOINT_URL='https://api.ai.sakura.ad.jp/v1/chat/completions'
$env:TRADEX_TRADER_LLM_MODEL='Qwen3-Coder-30B-A3B-Instruct'
$env:TRADEX_TRADER_LLM_API_KEY='<secret>'
python -m pytest -q tests/test_tradex_research_live_acceptance.py -k llm_adapter
```

## failure triage

- preflight failure:
  - runtime / DB / readiness の問題
- runner failure:
  - upstream artifact shape または current TRADEX execution path の問題
- LLM failure:
  - env 未設定
  - provider timeout / HTTP error
  - invalid JSON output
  - schema validation failure

どの failure でも compare truth と decision policy は変えない。

## next step after foundation verify

foundation verify が通ったら、次は benchmark rebuild を実行する。

```powershell
python -m app.backend.tools.tradex_research_os_cli rebuild-trader-benchmark
```

この後の運用手順は次を参照する。

- `docs/architecture/TRADEX_TRADER_BENCHMARK.md`
- `docs/architecture/TRADEX_TRADER_LABEL_POLICY.md`
- `docs/architecture/TRADEX_TRADER_RESEARCH_LOOP.md`
