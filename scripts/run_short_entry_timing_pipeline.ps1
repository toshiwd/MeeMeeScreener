param(
    [string]$DbPath = "C:\Users\enish\AppData\Local\MeeMeeScreener\data\stocks.duckdb",
    [switch]$IncludeConfirmedScan
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$pipelineArgs = @(
    "scripts\tradex_short_entry_timing_pipeline_v1.py",
    "--db-path", $DbPath
)
if (-not $IncludeConfirmedScan) {
    $pipelineArgs += "--skip-confirmed-scan"
}

Write-Host "[TRADEX] running short entry timing pipeline..."
python @pipelineArgs
if ($LASTEXITCODE -ne 0) {
    throw "short entry timing pipeline failed with exit code $LASTEXITCODE"
}

Write-Host "[TRADEX] generating short entry timing summary..."
python scripts\tradex_short_entry_timing_summary_v1.py
if ($LASTEXITCODE -ne 0) {
    throw "short entry timing summary failed with exit code $LASTEXITCODE"
}

$pipelineLatest = "G:\Tradex\short_entry_timing_rule_probe_v1\pipeline\latest_short_entry_timing_pipeline.json"
$summaryLatest = "G:\Tradex\short_entry_timing_rule_probe_v1\summary\latest_short_entry_timing_summary.md"

Write-Host "[TRADEX] latest pipeline: $pipelineLatest"
Write-Host "[TRADEX] latest summary:  $summaryLatest"
