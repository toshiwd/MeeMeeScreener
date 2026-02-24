param(
    [string]$BaseUrl = "http://127.0.0.1:8000",
    [string]$StartDate = "",
    [string]$EndDate = "",
    [string]$PredictDate = "",
    [int]$PollIntervalSec = 30,
    [int]$BackendStartupTimeoutSec = 90,
    [string]$PythonExe = "python",
    [string]$LogPath = "",
    [switch]$DryRun,
    [switch]$StartBackendIfDown,
    [switch]$StopBackendWhenDone
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
if (-not $LogPath) {
    $LogPath = Join-Path $repoRoot ("tmp/ml_overnight_{0}.log" -f (Get-Date -Format "yyyyMMdd"))
}
$logDir = Split-Path -Parent $LogPath
if ($logDir -and -not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
}
$backendOutLogPath = Join-Path $repoRoot ("tmp/backend_overnight_{0}.out.log" -f (Get-Date -Format "yyyyMMdd"))
$backendErrLogPath = Join-Path $repoRoot ("tmp/backend_overnight_{0}.err.log" -f (Get-Date -Format "yyyyMMdd"))

$backendProc = $null
$backendStartedByScript = $false

function Write-Log {
    param([string]$Message)
    $line = "{0:u} {1}" -f (Get-Date), $Message
    $line | Tee-Object -FilePath $LogPath -Append | Out-Host
}

function Convert-ToUnixSeconds {
    param([string]$InputValue)
    if (-not $InputValue) {
        return $null
    }
    if ($InputValue -match "^\d+$") {
        return [int64]$InputValue
    }
    try {
        $parsed = [datetime]::Parse($InputValue)
        $utc = $parsed.ToUniversalTime()
        return [int64][double](($utc - [datetime]"1970-01-01").TotalSeconds)
    } catch {
        throw "Invalid date value: $InputValue"
    }
}

function Build-QueryString {
    param([hashtable]$Params)
    $pairs = @()
    foreach ($entry in $Params.GetEnumerator()) {
        if ($null -eq $entry.Value -or $entry.Value -eq "") {
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

function Wait-BackendReady {
    $deadline = (Get-Date).AddSeconds($BackendStartupTimeoutSec)
    while ((Get-Date) -lt $deadline) {
        if ($backendProc -and $backendProc.HasExited) {
            throw "Backend process exited before health check passed (exit=$($backendProc.ExitCode))."
        }
        if (Test-BackendHealthy) {
            return
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
    while ($true) {
        Start-Sleep -Seconds $PollIntervalSec
        $status = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/jobs/$JobId"
        $progress = if ($null -eq $status.progress) { "-" } else { [string]$status.progress }
        $message = if ($status.message) { [string]$status.message } else { "" }
        Write-Log ("{0} [{1}] progress={2} message={3}" -f $Label, $status.status, $progress, $message)
        if ($status.status -notin @("queued", "running", "cancel_requested")) {
            return $status
        }
    }
}

try {
    Write-Log "ML overnight pipeline started."

    if (-not (Test-BackendHealthy)) {
        if (-not $StartBackendIfDown) {
            throw "Backend is not reachable at $BaseUrl. Use -StartBackendIfDown or start backend first."
        }
        Write-Log "Backend is down. Starting uvicorn..."
        $backendProc = Start-Process `
            -FilePath $PythonExe `
            -ArgumentList @("-m", "uvicorn", "app.backend.main:app", "--host", "127.0.0.1", "--port", "8000") `
            -WorkingDirectory $repoRoot `
            -WindowStyle Hidden `
            -RedirectStandardOutput $backendOutLogPath `
            -RedirectStandardError $backendErrLogPath `
            -PassThru
        if (-not $backendProc) {
            throw "Failed to start backend process."
        }
        $backendStartedByScript = $true
        Write-Log "Backend process started (pid=$($backendProc.Id), stdout=$backendOutLogPath, stderr=$backendErrLogPath)"
        Wait-BackendReady
        Write-Log "Backend started (pid=$($backendProc.Id))."
    } else {
        Write-Log "Backend health check OK."
    }

    $startDt = Convert-ToUnixSeconds $StartDate
    $endDt = Convert-ToUnixSeconds $EndDate
    $predictDt = Convert-ToUnixSeconds $PredictDate

    $trainParams = @{
        dry_run = $DryRun.IsPresent.ToString().ToLowerInvariant()
    }
    if ($null -ne $startDt) { $trainParams["start_dt"] = $startDt }
    if ($null -ne $endDt) { $trainParams["end_dt"] = $endDt }

    $trainUri = "$BaseUrl/api/jobs/ml/train"
    $trainQuery = Build-QueryString $trainParams
    if ($trainQuery) { $trainUri = "$trainUri?$trainQuery" }

    Write-Log "Submitting ml_train: $trainUri"
    $trainResp = Invoke-RestMethod -Method Post -Uri $trainUri
    if (-not $trainResp.job_id) {
        throw "ml_train did not return job_id."
    }
    $trainId = [string]$trainResp.job_id
    Write-Log "ml_train queued: job_id=$trainId"

    $trainFinal = Wait-JobTerminal -JobId $trainId -Label "ml_train"
    if ($trainFinal.status -ne "success") {
        throw "ml_train failed: $($trainFinal.message)"
    }
    Write-Log "ml_train completed."

    $predictUri = "$BaseUrl/api/jobs/ml/predict"
    $predictParams = @{}
    if ($null -ne $predictDt) { $predictParams["dt"] = $predictDt }
    $predictQuery = Build-QueryString $predictParams
    if ($predictQuery) { $predictUri = "$predictUri?$predictQuery" }

    Write-Log "Submitting ml_predict: $predictUri"
    $predictResp = Invoke-RestMethod -Method Post -Uri $predictUri
    if (-not $predictResp.job_id) {
        throw "ml_predict did not return job_id."
    }
    $predictId = [string]$predictResp.job_id
    Write-Log "ml_predict queued: job_id=$predictId"

    $predictFinal = Wait-JobTerminal -JobId $predictId -Label "ml_predict"
    if ($predictFinal.status -ne "success") {
        throw "ml_predict failed: $($predictFinal.message)"
    }
    Write-Log "ml_predict completed."

    $mlStatus = Invoke-RestMethod -Method Get -Uri "$BaseUrl/api/jobs/ml/status"
    Write-Log ("ml_status=" + ($mlStatus | ConvertTo-Json -Depth 6 -Compress))
    Write-Log "ML overnight pipeline finished successfully."
} catch {
    Write-Log ("ERROR: " + $_.Exception.Message)
    throw
} finally {
    if ($backendStartedByScript -and $StopBackendWhenDone -and $backendProc) {
        try {
            Write-Log "Stopping backend process pid=$($backendProc.Id)"
            Stop-Process -Id $backendProc.Id -Force -ErrorAction Stop
        } catch {
            Write-Log ("Failed to stop backend process: " + $_.Exception.Message)
        }
    }
    Write-Log "Log file: $LogPath"
}
