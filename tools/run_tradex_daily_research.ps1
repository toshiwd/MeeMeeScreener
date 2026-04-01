param(
    [string]$PythonExe = "python",
    [string]$AsOfDate = "",
    [string]$PublishId = "",
    [string]$FreshnessState = "fresh",
    [switch]$PrepareOnly,
    [switch]$Loop,
    [int]$MaxTradingDays = 5,
    [string]$ReportPath = "",
    [string]$TextReportPath = "",
    [string]$ProgressPath = "",
    [string]$LogPath = ""
)

$ErrorActionPreference = "Stop"

if ($PrepareOnly -and $Loop) {
    throw "Specify either -PrepareOnly or -Loop, not both."
}

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$tradexRoot = "G:\Tradex"
$reportDir = Join-Path $tradexRoot "reports"
$logDir = Join-Path $tradexRoot "logs"
foreach ($dir in @($reportDir, $logDir)) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
    }
}

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
if ($PrepareOnly) {
    $defaultJsonName = "tradex_daily_research_prepare_{0}.json" -f $stamp
    $defaultTextName = "tradex_daily_research_prepare_{0}.txt" -f $stamp
    $defaultProgressName = "tradex_daily_research_prepare_{0}.progress.json" -f $stamp
    $defaultLogName = "tradex_daily_research_prepare_{0}.log" -f $stamp
} elseif ($Loop) {
    $defaultJsonName = "tradex_daily_research_loop_{0}.json" -f $stamp
    $defaultTextName = "tradex_daily_research_loop_{0}.txt" -f $stamp
    $defaultProgressName = "tradex_daily_research_loop_{0}.progress.json" -f $stamp
    $defaultLogName = "tradex_daily_research_loop_{0}.log" -f $stamp
} else {
    $defaultJsonName = "tradex_daily_research_{0}.json" -f $stamp
    $defaultTextName = "tradex_daily_research_{0}.txt" -f $stamp
    $defaultProgressName = "tradex_daily_research_{0}.progress.json" -f $stamp
    $defaultLogName = "tradex_daily_research_{0}.log" -f $stamp
}
if (-not $ReportPath) {
    $ReportPath = Join-Path $reportDir $defaultJsonName
}
if (-not $TextReportPath) {
    $TextReportPath = Join-Path $reportDir $defaultTextName
}
if (-not $ProgressPath) {
    $ProgressPath = Join-Path $reportDir $defaultProgressName
}
if (-not $LogPath) {
    $LogPath = Join-Path $logDir $defaultLogName
}

function Write-Log {
    param([string]$Message)
    $line = "{0:u} {1}" -f (Get-Date), $Message
    $line | Tee-Object -FilePath $LogPath -Append | Out-Host
}

function Get-ProgressSummary {
    param([string]$Path)
    if (-not $Path -or -not (Test-Path $Path)) {
        return $null
    }
    try {
        $payload = Get-Content -Path $Path -Raw | ConvertFrom-Json
    } catch {
        return $null
    }
    if ($null -eq $payload) {
        return $null
    }
    $phase = ""
    if ($payload.PSObject.Properties.Name -contains "current_phase" -and $payload.current_phase) {
        $phase = [string]$payload.current_phase
    } elseif ($payload.PSObject.Properties.Name -contains "current_step" -and $payload.current_step) {
        $phase = [string]$payload.current_step
    } elseif ($payload.PSObject.Properties.Name -contains "status" -and $payload.status) {
        $phase = [string]$payload.status
    } else {
        $phase = "unknown"
    }
    $eta = $null
    if ($payload.PSObject.Properties.Name -contains "eta_seconds" -and $null -ne $payload.eta_seconds) {
        $eta = [int]$payload.eta_seconds
    } elseif ($payload.PSObject.Properties.Name -contains "steps" -and $payload.steps) {
        $runningStep = $null
        foreach ($step in $payload.steps) {
            if ($step.status -eq "running" -and $step.details -and $step.details.PSObject.Properties.Name -contains "eta_seconds" -and $null -ne $step.details.eta_seconds) {
                $runningStep = $step
                break
            }
        }
        if ($null -ne $runningStep) {
            $eta = [int]$runningStep.details.eta_seconds
        }
    }
    $status = if ($payload.PSObject.Properties.Name -contains "status") { [string]$payload.status } else { "" }
    return "{0}|{1}|{2}" -f $status, $phase, ($(if ($null -eq $eta) { "unknown" } else { [string]$eta }))
}

function Get-ProgressPayload {
    param([string]$Path)
    if (-not $Path -or -not (Test-Path $Path)) {
        return $null
    }
    try {
        return Get-Content -Path $Path -Raw | ConvertFrom-Json
    } catch {
        return $null
    }
}

try {
    $modeName = if ($PrepareOnly) { "prepare" } elseif ($Loop) { "loop" } else { "run" }
    Write-Log ("Tradex daily research {0} started." -f $modeName)
    $subcommand = if ($PrepareOnly) { "daily-research-prepare" } elseif ($Loop) { "daily-research-loop" } else { "daily-research-run" }
    $args = @(
        "-m", "external_analysis",
        $subcommand,
        "--progress-path", $ProgressPath
    )
    if (-not $PrepareOnly) {
        $args += @(
            "--freshness-state", $FreshnessState,
            "--report-path", $ReportPath,
            "--text-report-path", $TextReportPath
        )
    } else {
        $args += @("--manifest-path", $ReportPath)
    }
    if ($Loop) {
        $args += @("--max-trading-days", "$MaxTradingDays")
    }
    if ($AsOfDate -and -not $Loop -and -not $PrepareOnly) {
        $args += @("--as-of-date", $AsOfDate)
    }
    if ($PublishId -and -not $Loop -and -not $PrepareOnly) {
        $args += @("--publish-id", $PublishId)
    }
    Write-Log ("Executing: {0} {1}" -f $PythonExe, ($args -join " "))
    $stdoutPath = Join-Path $logDir ("stdout_{0}.log" -f $stamp)
    $stderrPath = Join-Path $logDir ("stderr_{0}.log" -f $stamp)
    $process = Start-Process -FilePath $PythonExe -ArgumentList $args -PassThru -NoNewWindow -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
    $lastProgressLine = ""
    while (-not $process.HasExited) {
        Start-Sleep -Seconds 10
        $progressLine = Get-ProgressSummary -Path $ProgressPath
        if ($progressLine -and $progressLine -ne $lastProgressLine) {
            $parts = $progressLine.Split("|", 3)
            Write-Log ("Progress: status={0} phase={1} eta_seconds={2}" -f $parts[0], $parts[1], $parts[2])
            $lastProgressLine = $progressLine
        }
    }
    $process.WaitForExit()
    $finalProgress = Get-ProgressPayload -Path $ProgressPath
    $exitCode = $process.ExitCode
    if ($null -eq $exitCode -or [string]::IsNullOrWhiteSpace("$exitCode")) {
        if ($null -ne $finalProgress -and [string]$finalProgress.status -eq "complete") {
            $exitCode = 0
        } else {
            throw "daily research command exited without a readable exit code"
        }
    }
    if ([int]$exitCode -ne 0) {
        throw "daily research command failed with exit code $exitCode"
    }
    if (Test-Path $stdoutPath) {
        foreach ($line in (Get-Content -Path $stdoutPath)) {
            Write-Log ([string]$line)
        }
    }
    if (Test-Path $stderrPath) {
        foreach ($line in (Get-Content -Path $stderrPath)) {
            Write-Log ([string]$line)
        }
    }
    if ($PrepareOnly) {
        Write-Log ("Prepare manifest: {0}" -f $ReportPath)
    } else {
        Write-Log ("JSON report: {0}" -f $ReportPath)
        Write-Log ("Text report: {0}" -f $TextReportPath)
    }
    Write-Log ("Progress report: {0}" -f $ProgressPath)
    Write-Log ("Tradex daily research {0} finished successfully." -f $modeName)
} catch {
    Write-Log ("ERROR: " + $_.Exception.Message)
    throw
}
