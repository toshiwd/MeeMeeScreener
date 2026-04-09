param(
    [ValidateSet("numeric", "llm", "both")]
    [string]$Mode = "numeric",
    [string]$RuntimeRoot = "G:\Tradex",
    [string]$StocksDbPath = "",
    [string]$Code = "",
    [string]$AsOfDate = "",
    [string]$LlmEndpointUrl = "",
    [string]$LlmModel = "",
    [string]$LlmApiKey = "",
    [string]$LlmTimeoutSec = ""
)

$ErrorActionPreference = "Stop"

function Resolve-PreparedStocksDbPath {
    param(
        [string]$ExplicitPath
    )

    $candidates = @()
    if ($ExplicitPath) { $candidates += $ExplicitPath }
    if ($env:STOCKS_DB_PATH) { $candidates += $env:STOCKS_DB_PATH }
    if ($env:TRADEX_LIVE_STOCKS_DB_PATH) { $candidates += $env:TRADEX_LIVE_STOCKS_DB_PATH }
    $candidates += "G:\Tradex\db\stocks.duckdb"
    $candidates += (Join-Path $env:LOCALAPPDATA "MeeMeeScreener-dev\data\stocks.duckdb")
    $candidates += (Join-Path $env:LOCALAPPDATA "MeeMeeScreener\data\stocks.duckdb")

    foreach ($candidate in $candidates | Where-Object { $_ } | Select-Object -Unique) {
        if (Test-Path -LiteralPath $candidate) {
            return (Resolve-Path -LiteralPath $candidate).Path
        }
    }

    throw "prepared stocks DuckDB not found; pass -StocksDbPath or set STOCKS_DB_PATH"
}

function Invoke-PytestLive {
    param(
        [string]$Expression
    )

    $cmd = @("python", "-m", "pytest", "-q", "tests/test_tradex_research_live_acceptance.py", "-k", $Expression)
    Write-Host ("[live-verify] " + ($cmd -join " "))
    & $cmd[0] $cmd[1..($cmd.Length - 1)]
    if ($LASTEXITCODE -ne 0) {
        throw "live verify failed: $Expression"
    }
}

$resolvedRuntimeRoot = if (Test-Path -LiteralPath $RuntimeRoot) {
    (Resolve-Path -LiteralPath $RuntimeRoot).Path
} else {
    throw "runtime root missing: $RuntimeRoot"
}
$resolvedStocksDbPath = Resolve-PreparedStocksDbPath -ExplicitPath $StocksDbPath

$env:MEEMEE_TRADEX_ROOT = $resolvedRuntimeRoot
$env:TRADEX_LIVE_RUNTIME_ROOT = $resolvedRuntimeRoot
$env:STOCKS_DB_PATH = $resolvedStocksDbPath
$env:TRADEX_LIVE_STOCKS_DB_PATH = $resolvedStocksDbPath
$env:MEEMEE_DISABLE_LEGACY_ANALYSIS = "0"
$env:MEEMEE_ENABLE_TRADEX_LIVE_VERIFY = "1"

if ($Code) { $env:TRADEX_LIVE_HYPOTHESIS_CODE = $Code }
if ($AsOfDate) { $env:TRADEX_LIVE_HYPOTHESIS_DATE = $AsOfDate }

Write-Host "[live-verify] runtime_root=$resolvedRuntimeRoot"
Write-Host "[live-verify] stocks_db_path=$resolvedStocksDbPath"
Write-Host "[live-verify] mode=$Mode"

Invoke-PytestLive -Expression "single_session"

if ($Mode -in @("llm", "both")) {
    $endpoint = if ($LlmEndpointUrl) { $LlmEndpointUrl } elseif ($env:TRADEX_TRADER_LLM_ENDPOINT_URL) { $env:TRADEX_TRADER_LLM_ENDPOINT_URL } else { "" }
    $model = if ($LlmModel) { $LlmModel } elseif ($env:TRADEX_TRADER_LLM_MODEL) { $env:TRADEX_TRADER_LLM_MODEL } else { "" }
    $apiKey = if ($LlmApiKey) { $LlmApiKey } elseif ($env:TRADEX_TRADER_LLM_API_KEY) { $env:TRADEX_TRADER_LLM_API_KEY } else { "" }
    $timeout = if ($LlmTimeoutSec) { $LlmTimeoutSec } elseif ($env:TRADEX_TRADER_LLM_TIMEOUT_SEC) { $env:TRADEX_TRADER_LLM_TIMEOUT_SEC } else { "" }

    if (-not $endpoint -or -not $model -or -not $apiKey) {
        throw "LLM live verify requires TRADEX_TRADER_LLM_ENDPOINT_URL, TRADEX_TRADER_LLM_MODEL, and TRADEX_TRADER_LLM_API_KEY"
    }

    $env:TRADEX_TRADER_LLM_ENDPOINT_URL = $endpoint
    $env:TRADEX_TRADER_LLM_MODEL = $model
    $env:TRADEX_TRADER_LLM_API_KEY = $apiKey
    if ($timeout) { $env:TRADEX_TRADER_LLM_TIMEOUT_SEC = $timeout }
    $env:MEEMEE_ENABLE_TRADEX_LIVE_LLM_VERIFY = "1"

    Invoke-PytestLive -Expression "llm_adapter"
}

Write-Host "[live-verify] completed"
