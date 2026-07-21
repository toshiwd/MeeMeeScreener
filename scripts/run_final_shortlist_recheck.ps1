param(
    [string]$DbPath = "C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb",
    [string]$BaseUrl = "http://127.0.0.1:5174",
    [string]$ApiBase = "http://127.0.0.1:28888/api",
    [string]$OutputRoot = "G:\Tradex\short_entry_shape_family_probe_v1\final_shortlist_recheck_pipeline",
    [int]$ScreenshotTimeoutMs = 30000,
    [int]$BackendPort = 28888
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

function New-UtcTag {
    return (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
}

function Convert-YmdToIsoDate([int]$Ymd) {
    $text = [string]$Ymd
    return "$($text.Substring(0, 4))-$($text.Substring(4, 2))-$($text.Substring(6, 2))"
}

function Stop-BackendIfRunning([int]$Port) {
    $connections = @(Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue | Where-Object { $_.State -eq "Listen" })
    foreach ($connection in $connections) {
        if ($connection.OwningProcess -and $connection.OwningProcess -ne 0) {
            Write-Host "[MeeMee] stopping backend process PID=$($connection.OwningProcess) for DB read..."
            Stop-Process -Id $connection.OwningProcess -Force -ErrorAction SilentlyContinue
        }
    }
    Start-Sleep -Seconds 2
}

function Start-Backend([int]$Port, [string]$RuntimeDbPath) {
    $listening = @(Get-NetTCPConnection -LocalPort $Port -ErrorAction SilentlyContinue | Where-Object { $_.State -eq "Listen" })
    if ($listening.Count -gt 0) {
        return
    }
    Write-Host "[MeeMee] starting backend for screenshot capture on port $Port..."
    $env:MEEMEE_RUNTIME_DB_PATH = $RuntimeDbPath
    Start-Process -FilePath python `
        -ArgumentList @("-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "$Port") `
        -WorkingDirectory $repoRoot `
        -WindowStyle Hidden
    Start-Sleep -Seconds 8
}

$runDir = Join-Path $OutputRoot "$(New-UtcTag)-final_shortlist_recheck_pipeline_v1"
New-Item -ItemType Directory -Force -Path $runDir | Out-Null

Write-Host "[TRADEX] running final shortlist recheck..."
Stop-BackendIfRunning $BackendPort
python scripts\tradex_final_shortlist_recheck_v1.py --db-path $DbPath
if ($LASTEXITCODE -ne 0) {
    throw "final shortlist recheck failed with exit code $LASTEXITCODE"
}

$latestRecheck = "G:\Tradex\short_entry_shape_family_probe_v1\final_shortlist_recheck\latest_final_shortlist_recheck.json"
$recheck = Get-Content -Path $latestRecheck -Raw | ConvertFrom-Json
$keepRows = @($recheck.rows | Where-Object { $_.candidate_local_decision -eq "keep_strong_review" })

$screenshotRunDir = $null
$screenshotAudit = $null
if ($keepRows.Count -gt 0) {
    Start-Backend $BackendPort $DbPath
    $samples = ($keepRows | ForEach-Object {
        $ymd = [int]$_.recheck.evaluated_bar.ymd
        "$($_.code):$(Convert-YmdToIsoDate $ymd)"
    }) -join ","

    Write-Host "[MeeMee] capturing keep_strong_review screenshots: $samples"
    $screenshotOutputRoot = Join-Path $runDir "screenshots"
    node scripts\meemee_detail_clean_screenshot_batch_v1.mjs `
        --base-url $BaseUrl `
        --api-base $ApiBase `
        --output-root $screenshotOutputRoot `
        --samples $samples `
        --viewport 960x640 `
        --timeout-ms $ScreenshotTimeoutMs `
        --viewport-fallback
    if ($LASTEXITCODE -ne 0) {
        throw "keep screenshot capture failed with exit code $LASTEXITCODE"
    }

    $auditPath = Get-ChildItem -Path $screenshotOutputRoot -Recurse -Filter "export_audit.json" |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    if ($auditPath) {
        $screenshotAudit = Get-Content -Path $auditPath.FullName -Raw | ConvertFrom-Json
        $screenshotRunDir = Split-Path -Parent $auditPath.FullName
    }
}

$report = [ordered]@{
    schema_version = "final_shortlist_recheck_pipeline_v1"
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    boundary_owner = "TRADEX"
    db_path = $DbPath
    source_recheck = $latestRecheck
    keep_strong_count = $keepRows.Count
    keep_strong_rows = $keepRows
    screenshot_run_dir = $screenshotRunDir
    screenshot_audit = $screenshotAudit
    decision = [ordered]@{
        candidate_local_decision = if ($keepRows.Count -gt 0) { "keep_strong_review_screenshots_available" } else { "no_keep_strong_review" }
        authoritative_rollup_decision = "research_candidate_not_trade_signal"
        reason = "final keep candidates were rechecked and screenshots were refreshed when present"
    }
    production_ranking_changed = $false
    runtime_db_write = $false
    meemee_unchanged = $true
}

$reportPath = Join-Path $runDir "final_shortlist_recheck_pipeline.json"
$latestPath = Join-Path $OutputRoot "latest_final_shortlist_recheck_pipeline.json"
$json = $report | ConvertTo-Json -Depth 16
Set-Content -Path $reportPath -Value $json -Encoding UTF8
Set-Content -Path $latestPath -Value $json -Encoding UTF8

Write-Host "[TRADEX] pipeline report: $reportPath"
Write-Host "[TRADEX] latest report:   $latestPath"
