param(
    [string]$BaseUrl = "http://127.0.0.1:8000",
    [int]$LookbackDays = 260,
    [int]$MaxMissingDays = 260,
    [string]$IncludeSell = "true",
    [string]$IncludePhase = "false",
    [string]$PythonExe = "python",
    [int]$PollIntervalSec = 30,
    [int]$BackendStartupTimeoutSec = 120,
    [int]$JobTimeoutSec = 21600,
    [ValidateSet("ps1", "cmd")][string]$BuildTool = "ps1",
    [switch]$SkipBuild,
    [string]$ResultPath = "",
    [int64]$ExpectedMinMlPredDt = 1770940800,
    [int64]$MlTrainStartDt = 0,
    [int64]$MlTrainEndDt = 0
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$runStamp = Get-Date -Format "yyyyMMdd_HHmmss"
$tmpDir = Join-Path $repoRoot "tmp"
if (-not (Test-Path $tmpDir)) {
    New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null
}

$pipelineLogPath = Join-Path $tmpDir ("overnight_short_pipeline_{0}.log" -f $runStamp)
if (-not $ResultPath) {
    $ResultPath = Join-Path $tmpDir ("overnight_short_pipeline_result_{0}.json" -f $runStamp)
}

$script:RunStartedAt = Get-Date
$script:FatalError = $null
$script:PipelineFailed = $false
$script:StepResults = @()
$script:Artifacts = [ordered]@{
    pipelineLog = $pipelineLogPath
}
$script:BackendProc = $null
$script:BackendStarted = $false

function Parse-BoolText {
    param(
        [string]$Value,
        [bool]$Default
    )
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return [bool]$Default
    }
    switch ($Value.Trim().ToLowerInvariant()) {
        "1" { return $true }
        "true" { return $true }
        "yes" { return $true }
        "y" { return $true }
        "on" { return $true }
        "0" { return $false }
        "false" { return $false }
        "no" { return $false }
        "n" { return $false }
        "off" { return $false }
        default { throw "Invalid boolean value: '$Value'" }
    }
}

$IncludeSellBool = Parse-BoolText -Value $IncludeSell -Default $true
$IncludePhaseBool = Parse-BoolText -Value $IncludePhase -Default $false

function Write-Log {
    param([string]$Message)
    $line = "{0:u} {1}" -f (Get-Date), $Message
    $line | Tee-Object -FilePath $pipelineLogPath -Append | Out-Host
}

function Add-StepResult {
    param(
        [string]$Name,
        [int]$Attempt,
        [string]$Status,
        [datetime]$StartedAt,
        [datetime]$FinishedAt,
        [object]$Details,
        [string]$ErrorMessage
    )
    $script:StepResults += [pscustomobject]@{
        name = $Name
        attempt = $Attempt
        status = $Status
        startedAt = $StartedAt.ToString("o")
        finishedAt = $FinishedAt.ToString("o")
        details = $Details
        error = $ErrorMessage
    }
}

function Invoke-Step {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][scriptblock]$Action
    )
    $maxAttempts = 2
    for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
        $startedAt = Get-Date
        try {
            Write-Log ("[step:{0}] attempt={1} started" -f $Name, $attempt)
            $details = & $Action
            $finishedAt = Get-Date
            Add-StepResult -Name $Name -Attempt $attempt -Status "success" -StartedAt $startedAt -FinishedAt $finishedAt -Details $details -ErrorMessage $null
            Write-Log ("[step:{0}] attempt={1} success" -f $Name, $attempt)
            return $details
        } catch {
            $finishedAt = Get-Date
            $errorMessage = $_.Exception.Message
            Add-StepResult -Name $Name -Attempt $attempt -Status "failed" -StartedAt $startedAt -FinishedAt $finishedAt -Details $null -ErrorMessage $errorMessage
            Write-Log ("[step:{0}] attempt={1} failed: {2}" -f $Name, $attempt, $errorMessage)
            if ($attempt -ge $maxAttempts) {
                throw
            }
            Write-Log ("[step:{0}] retrying once after failure" -f $Name)
            Start-Sleep -Seconds 2
        }
    }
}

function To-BoolText {
    param([bool]$Value)
    if ($Value) { return "true" }
    return "false"
}

function Build-QueryString {
    param([hashtable]$Params)
    $pairs = @()
    foreach ($entry in $Params.GetEnumerator()) {
        if ($null -eq $entry.Value -or [string]$entry.Value -eq "") {
            continue
        }
        $k = [uri]::EscapeDataString([string]$entry.Key)
        $v = [uri]::EscapeDataString([string]$entry.Value)
        $pairs += "$k=$v"
    }
    return ($pairs -join "&")
}

function Test-BackendHealthy {
    try {
        $resp = Invoke-WebRequest -Uri "$BaseUrl/health" -UseBasicParsing -TimeoutSec 4
        return ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 500)
    } catch {
        return $false
    }
}

function Stop-RunningProcesses {
    $stopped = @()
    $appProcs = Get-Process -Name "MeeMeeScreener" -ErrorAction SilentlyContinue
    if ($appProcs) {
        $appIds = @($appProcs | Select-Object -ExpandProperty Id)
        $appProcs | Stop-Process -Force -ErrorAction SilentlyContinue
        $stopped += "MeeMeeScreener:" + ($appIds -join ",")
    }

    $pyProcs = @(Get-CimInstance Win32_Process -Filter "Name='python.exe'" | Where-Object {
        $_.CommandLine -like "*uvicorn*" -and
        $_.CommandLine -like "*app.backend.main:app*" -and
        $_.CommandLine -like "*--port 8000*"
    })
    if ($pyProcs.Count -gt 0) {
        $pyIds = @($pyProcs | Select-Object -ExpandProperty ProcessId)
        foreach ($proc in $pyProcs) {
            Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
        }
        $stopped += "uvicorn:" + ($pyIds -join ",")
    }
    return @{
        stopped = $stopped
    }
}

function Start-Backend {
    if (Test-BackendHealthy) {
        return @{
            started = $false
            reason = "already_healthy"
        }
    }

    $backendOutLogPath = Join-Path $tmpDir ("backend_pipeline_{0}.out.log" -f $runStamp)
    $backendErrLogPath = Join-Path $tmpDir ("backend_pipeline_{0}.err.log" -f $runStamp)
    $script:Artifacts.backendOutLog = $backendOutLogPath
    $script:Artifacts.backendErrLog = $backendErrLogPath

    $proc = Start-Process `
        -FilePath $PythonExe `
        -ArgumentList @("-m", "uvicorn", "app.backend.main:app", "--host", "127.0.0.1", "--port", "8000") `
        -WorkingDirectory $repoRoot `
        -WindowStyle Hidden `
        -RedirectStandardOutput $backendOutLogPath `
        -RedirectStandardError $backendErrLogPath `
        -PassThru
    if (-not $proc) {
        throw "Failed to start backend process."
    }

    $script:BackendProc = $proc
    $script:BackendStarted = $true

    $deadline = (Get-Date).AddSeconds($BackendStartupTimeoutSec)
    while ((Get-Date) -lt $deadline) {
        if ($proc.HasExited) {
            throw "Backend exited before health check passed (exit=$($proc.ExitCode))."
        }
        if (Test-BackendHealthy) {
            return @{
                started = $true
                pid = $proc.Id
                stdout = $backendOutLogPath
                stderr = $backendErrLogPath
            }
        }
        Start-Sleep -Seconds 2
    }
    throw "Backend did not become healthy within ${BackendStartupTimeoutSec}s."
}

function Wait-JobTerminal {
    param(
        [Parameter(Mandatory = $true)][string]$JobId,
        [Parameter(Mandatory = $true)][string]$Label
    )
    $deadline = (Get-Date).AddSeconds($JobTimeoutSec)
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds $PollIntervalSec
        try {
            $status = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/jobs/$JobId"
            $progress = if ($null -eq $status.progress) { "-" } else { [string]$status.progress }
            $message = if ($status.message) { [string]$status.message } else { "" }
            Write-Log ("[job:{0}] status={1} progress={2} message={3}" -f $Label, $status.status, $progress, $message)
            if ($status.status -notin @("queued", "running", "cancel_requested")) {
                return $status
            }
        } catch {
            $httpCode = $null
            if ($_.Exception -and $_.Exception.Response -and $_.Exception.Response.StatusCode) {
                $httpCode = [int]$_.Exception.Response.StatusCode
            }
            if ($httpCode -in @(404, 500, 503)) {
                Write-Log ("[job:{0}] transient polling error HTTP {1}, continuing..." -f $Label, $httpCode)
                continue
            }
            throw
        }
    }
    throw "$Label timed out after ${JobTimeoutSec}s (job_id=$JobId)."
}

function Submit-JobAndWait {
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$SubmitUri
    )
    Write-Log ("Submitting {0}: {1}" -f $Label, $SubmitUri)
    $resp = Invoke-RestMethod -Method Post -Uri $SubmitUri
    $jobId = if ($resp.job_id) { [string]$resp.job_id } else { "" }
    if (-not $jobId) {
        throw "$Label did not return job_id."
    }
    $final = Wait-JobTerminal -JobId $jobId -Label $Label
    if ([string]$final.status -ne "success") {
        $message = if ($final.message) { [string]$final.message } else { "" }
        throw "$Label failed (job_id=$jobId, status=$($final.status), message=$message)"
    }
    return @{
        jobId = $jobId
        status = [string]$final.status
        message = [string]$final.message
    }
}

function Run-FullRetraceAnalysis {
    $scriptPath = Join-Path $repoRoot "tools/analytics/full_retrace_impact.py"
    $inputCsv = Join-Path $repoRoot "tmp/monthly_box3_range_trade_events.csv"
    $outDir = Join-Path $repoRoot "tmp"
    if (-not (Test-Path $scriptPath)) {
        throw "analysis script not found: $scriptPath"
    }
    if (-not (Test-Path $inputCsv)) {
        throw "range trade events csv not found: $inputCsv"
    }

    & $PythonExe $scriptPath --input-csv $inputCsv --out-dir $outDir
    if ($LASTEXITCODE -ne 0) {
        throw "full retrace analysis failed (exit=$LASTEXITCODE)"
    }

    $summary = Join-Path $outDir "full_retrace_impact_summary.csv"
    $progress = Join-Path $outDir "full_retrace_progress_bins.csv"
    $breakout = Join-Path $outDir "full_retrace_by_breakout_dir.csv"
    foreach ($path in @($summary, $progress, $breakout)) {
        if (-not (Test-Path $path)) {
            throw "analysis output missing: $path"
        }
    }
    $script:Artifacts.fullRetraceSummary = $summary
    $script:Artifacts.fullRetraceProgress = $progress
    $script:Artifacts.fullRetraceByBreakout = $breakout
    return @{
        summary = $summary
        progress = $progress
        byBreakout = $breakout
    }
}

function Run-FrontendBuild {
    $frontendDir = Join-Path $repoRoot "app/frontend"
    $frontendBuildLogPath = Join-Path $tmpDir ("frontend_build_{0}.log" -f $runStamp)
    Push-Location $frontendDir
    try {
        $output = & npm run build 2>&1
        $output | Out-File -FilePath $frontendBuildLogPath -Encoding utf8
        if ($LASTEXITCODE -ne 0) {
            throw "npm run build failed (see $frontendBuildLogPath)"
        }
        $outputText = $output | Out-String
        if ($outputText -match "chunk size limit") {
            throw "Vite chunk size warning detected (see $frontendBuildLogPath)"
        }
    } finally {
        Pop-Location
    }
    $script:Artifacts.frontendBuildLog = $frontendBuildLogPath
    return @{
        logPath = $frontendBuildLogPath
    }
}

function Run-ReleaseBuild {
    $buildReleaseLogPath = Join-Path $tmpDir ("build_release_{0}.log" -f $runStamp)
    if ($BuildTool -eq "ps1") {
        $buildScript = Join-Path $repoRoot "tools/build_release.ps1"
        & powershell -NoProfile -ExecutionPolicy Bypass -File $buildScript -LogPath $buildReleaseLogPath
        if ($LASTEXITCODE -ne 0) {
            throw "build_release.ps1 failed (exit=$LASTEXITCODE)"
        }
    } else {
        $buildCmd = Join-Path $repoRoot "build_release.cmd"
        & cmd /c $buildCmd
        if ($LASTEXITCODE -ne 0) {
            throw "build_release.cmd failed (exit=$LASTEXITCODE)"
        }
    }

    $releaseDir = Join-Path $repoRoot "release/MeeMeeScreener"
    $releaseZip = Join-Path $repoRoot "release/MeeMeeScreener-portable.zip"
    if (-not (Test-Path $releaseDir)) {
        throw "release directory not found: $releaseDir"
    }
    if (-not (Test-Path $releaseZip)) {
        throw "release zip not found: $releaseZip"
    }

    $script:Artifacts.buildReleaseLog = $buildReleaseLogPath
    $script:Artifacts.releaseDir = $releaseDir
    $script:Artifacts.releaseZip = $releaseZip
    return @{
        logPath = $buildReleaseLogPath
        releaseDir = $releaseDir
        releaseZip = $releaseZip
    }
}

function Get-MlPredMaxDt {
    $py = @'
import os
from pathlib import Path
import duckdb

base = os.environ.get("MEEMEE_DATA_DIR")
if not base:
    local_app = os.environ.get("LOCALAPPDATA")
    if not local_app:
        raise SystemExit("LOCALAPPDATA is not set")
    base = str(Path(local_app) / "MeeMeeScreener" / "data")

db_path = Path(base) / "stocks.duckdb"
if not db_path.exists():
    raise SystemExit(f"db_not_found:{db_path}")

con = duckdb.connect(str(db_path), read_only=True)
try:
    row = con.execute("SELECT MAX(dt) FROM ml_pred_20d").fetchone()
finally:
    con.close()
value = row[0] if row else None
print("" if value is None else int(value))
'@
    $value = $py | & $PythonExe -
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to query ml_pred_20d max dt."
    }
    if ([string]::IsNullOrWhiteSpace([string]$value)) {
        return $null
    }
    return [int64]([string]$value).Trim()
}

function Verify-Outputs {
    $summaryPath = Join-Path $repoRoot "tmp/full_retrace_impact_summary.csv"
    if (-not (Test-Path $summaryPath)) {
        throw "missing summary csv: $summaryPath"
    }
    $rows = Import-Csv -Path $summaryPath
    $segments = @($rows | Select-Object -ExpandProperty segment)
    foreach ($required in @("all", "full_retrace", "not_full_retrace")) {
        if ($segments -notcontains $required) {
            throw "missing segment '$required' in $summaryPath"
        }
    }

    $maxDt = Get-MlPredMaxDt
    if ($null -eq $maxDt) {
        throw "ml_pred_20d max dt is null."
    }
    if ($maxDt -lt $ExpectedMinMlPredDt) {
        throw "ml_pred_20d max dt too old: actual=$maxDt expected>=$ExpectedMinMlPredDt"
    }

    if (-not $SkipBuild) {
        $releaseDir = Join-Path $repoRoot "release/MeeMeeScreener"
        $releaseZip = Join-Path $repoRoot "release/MeeMeeScreener-portable.zip"
        if (-not (Test-Path $releaseDir)) {
            throw "missing release directory: $releaseDir"
        }
        if (-not (Test-Path $releaseZip)) {
            throw "missing release zip: $releaseZip"
        }
    }

    $script:Artifacts.mlPredMaxDt = [int64]$maxDt
    return @{
        mlPredMaxDt = [int64]$maxDt
        expectedMinMlPredDt = [int64]$ExpectedMinMlPredDt
        segments = $segments
    }
}

try {
    Write-Log "Short analysis and build pipeline started."
    Write-Log "BaseUrl=$BaseUrl LookbackDays=$LookbackDays MaxMissingDays=$MaxMissingDays IncludeSell=$IncludeSellBool IncludePhase=$IncludePhaseBool BuildTool=$BuildTool SkipBuild=$SkipBuild MlTrainStartDt=$MlTrainStartDt MlTrainEndDt=$MlTrainEndDt"

    Invoke-Step -Name "pre_stop" -Action { Stop-RunningProcesses } | Out-Null
    Invoke-Step -Name "start_backend" -Action { Start-Backend } | Out-Null

    $backfillUri = "$BaseUrl/api/jobs/analysis/backfill-missing?lookback_days=$LookbackDays&max_missing_days=$MaxMissingDays&include_sell=$(To-BoolText $IncludeSellBool)&include_phase=$(To-BoolText $IncludePhaseBool)"
    $backfillResult = Invoke-Step -Name "analysis_backfill" -Action { Submit-JobAndWait -Label "analysis_backfill" -SubmitUri $backfillUri }
    $script:Artifacts.analysisBackfillJobId = $backfillResult.jobId

    $trainParams = @{}
    if ($MlTrainStartDt -gt 0) { $trainParams["start_dt"] = [int64]$MlTrainStartDt }
    if ($MlTrainEndDt -gt 0) { $trainParams["end_dt"] = [int64]$MlTrainEndDt }
    $trainUri = "$BaseUrl/api/jobs/ml/train"
    $trainQuery = Build-QueryString -Params $trainParams
    if ($trainQuery) { $trainUri = "${trainUri}?$trainQuery" }
    $trainResult = Invoke-Step -Name "ml_train" -Action { Submit-JobAndWait -Label "ml_train" -SubmitUri $trainUri }
    $script:Artifacts.mlTrainJobId = $trainResult.jobId

    $predictResult = Invoke-Step -Name "ml_predict" -Action { Submit-JobAndWait -Label "ml_predict" -SubmitUri "$BaseUrl/api/jobs/ml/predict" }
    $script:Artifacts.mlPredictJobId = $predictResult.jobId

    Invoke-Step -Name "full_retrace_analysis" -Action { Run-FullRetraceAnalysis } | Out-Null
    Invoke-Step -Name "frontend_build" -Action { Run-FrontendBuild } | Out-Null

    if (-not $SkipBuild) {
        Invoke-Step -Name "release_build" -Action { Run-ReleaseBuild } | Out-Null
    } else {
        Write-Log "SkipBuild is enabled. release_build step was skipped."
    }

    Invoke-Step -Name "verify_outputs" -Action { Verify-Outputs } | Out-Null
    Write-Log "Pipeline completed successfully."
} catch {
    $script:PipelineFailed = $true
    $script:FatalError = $_.Exception.Message
    Write-Log ("ERROR: " + $script:FatalError)
} finally {
    if ($script:BackendStarted -and $script:BackendProc) {
        try {
            if (-not $script:BackendProc.HasExited) {
                Write-Log "Stopping backend process pid=$($script:BackendProc.Id)"
                Stop-Process -Id $script:BackendProc.Id -Force -ErrorAction Stop
            }
        } catch {
            Write-Log ("Failed to stop backend process: " + $_.Exception.Message)
        }
    }

    $finishedAt = Get-Date
    $summary = [ordered]@{
        startedAt = $script:RunStartedAt.ToString("o")
        finishedAt = $finishedAt.ToString("o")
        status = if ($script:PipelineFailed) { "failed" } else { "success" }
        error = $script:FatalError
        baseUrl = $BaseUrl
        lookbackDays = [int]$LookbackDays
        maxMissingDays = [int]$MaxMissingDays
        includeSell = [bool]$IncludeSellBool
        includePhase = [bool]$IncludePhaseBool
        skipBuild = [bool]$SkipBuild
        buildTool = $BuildTool
        expectedMinMlPredDt = [int64]$ExpectedMinMlPredDt
        mlTrainStartDt = [int64]$MlTrainStartDt
        mlTrainEndDt = [int64]$MlTrainEndDt
        artifacts = $script:Artifacts
        steps = $script:StepResults
    }

    $json = $summary | ConvertTo-Json -Depth 8
    $resultDir = Split-Path -Parent $ResultPath
    if ($resultDir -and -not (Test-Path $resultDir)) {
        New-Item -ItemType Directory -Force -Path $resultDir | Out-Null
    }
    Set-Content -Path $ResultPath -Value $json -Encoding UTF8
    Write-Log "Result summary saved: $ResultPath"
}

if ($script:PipelineFailed) {
    throw "Pipeline failed. See $ResultPath and $pipelineLogPath"
}

Write-Log "Pipeline finished. Result: $ResultPath"
