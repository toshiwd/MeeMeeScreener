param(
    [Parameter(Mandatory = $true)]
    [string]$AsOf,
    [string]$SnapshotId = "",
    [int]$Cycles = 3,
    [int]$Workers = 2,
    [int]$ChunkSize = 120,
    [string]$WorkspaceRoot = "research_workspace",
    [string]$PublishedRoot = "published",
    [string]$Config = "",
    [string]$PythonExe = "python",
    [switch]$LowPriority
)

$ErrorActionPreference = "Stop"

if ($LowPriority) {
    try {
        [System.Diagnostics.Process]::GetCurrentProcess().PriorityClass = "BelowNormal"
    } catch {
        Write-Host "Warning: failed to set process priority: $($_.Exception.Message)"
    }
}

$commonArgs = @(
    "-m", "research",
    "--workspace-root", $WorkspaceRoot,
    "--published-root", $PublishedRoot
)
if ($Config -ne "") {
    $commonArgs += @("--config", $Config)
}

$loopArgs = @()
$loopArgs += $commonArgs
$loopArgs += @("loop", "--asof", $AsOf, "--cycles", "$Cycles", "--workers", "$Workers", "--chunk-size", "$ChunkSize")
if ($SnapshotId -ne "") {
    $loopArgs += @("--snapshot-id", $SnapshotId)
}

Write-Host "Running challenger loop..."
$loopRaw = & $PythonExe @loopArgs
if ($LASTEXITCODE -ne 0) {
    throw "research loop failed"
}

$loopJson = $loopRaw | Out-String | ConvertFrom-Json
if (-not $loopJson.ok) {
    throw "research loop returned non-ok response"
}

$pareto = @()
foreach ($item in $loopJson.results) {
    if ($item.evaluate.is_pareto -eq $true) {
        $pareto += $item
    }
}

if ($pareto.Count -eq 0) {
    Write-Host "No Pareto run found in this loop cycle. Skip publish."
    exit 0
}

$best = $pareto |
    Sort-Object `
        @{ Expression = { [double]$_.evaluate.overall.return_at20 }; Descending = $true }, `
        @{ Expression = { [double]$_.evaluate.overall.hit_at20 }; Descending = $true } |
    Select-Object -First 1

$runId = [string]$best.run_id
Write-Host "Publishing run_id=$runId"

$publishArgs = @()
$publishArgs += $commonArgs
$publishArgs += @("publish", "--run_id", $runId)
$publishRaw = & $PythonExe @publishArgs
if ($LASTEXITCODE -ne 0) {
    throw "research publish failed"
}

Write-Output $publishRaw
