param(
    [string]$TaskName = "MeeMee-ML-Overnight",
    [string]$StartTime = "23:30",
    [string]$BaseUrl = "http://127.0.0.1:8000",
    [string]$StartDate = "",
    [string]$EndDate = "",
    [string]$PredictDate = "",
    [string]$PythonExe = "python",
    [string]$LogPath = "",
    [switch]$DryRun,
    [switch]$RunNow
)

$ErrorActionPreference = "Stop"

if ($StartTime -notmatch "^\d{2}:\d{2}$") {
    throw "StartTime must be HH:MM format."
}

$runnerPath = Join-Path $PSScriptRoot "run_ml_overnight.ps1"
if (-not (Test-Path $runnerPath)) {
    throw "Runner script not found: $runnerPath"
}

$defaultBaseUrl = "http://127.0.0.1:8000"
$defaultPythonExe = "python"

$argParts = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", ('"{0}"' -f $runnerPath),
    "-StartBackendIfDown",
    "-StopBackendWhenDone"
)
if ($BaseUrl -ne $defaultBaseUrl) { $argParts += @("-BaseUrl", ('"{0}"' -f $BaseUrl)) }
if ($PythonExe -ne $defaultPythonExe) { $argParts += @("-PythonExe", ('"{0}"' -f $PythonExe)) }
if ($LogPath) { $argParts += @("-LogPath", ('"{0}"' -f $LogPath)) }
if ($DryRun) { $argParts += "-DryRun" }
if ($StartDate) { $argParts += @("-StartDate", ('"{0}"' -f $StartDate)) }
if ($EndDate) { $argParts += @("-EndDate", ('"{0}"' -f $EndDate)) }
if ($PredictDate) { $argParts += @("-PredictDate", ('"{0}"' -f $PredictDate)) }

$taskCommand = "powershell.exe " + ($argParts -join " ")

Write-Host "Creating scheduled task: $TaskName"
Write-Host "Schedule: daily at $StartTime"
Write-Host "Command: $taskCommand"

& schtasks.exe /Create /TN $TaskName /SC DAILY /ST $StartTime /TR $taskCommand /F | Out-Host
if ($LASTEXITCODE -ne 0) {
    throw "schtasks /Create failed with exit code $LASTEXITCODE"
}

Write-Host "Task created."
& schtasks.exe /Query /TN $TaskName /V /FO LIST | Out-Host
if ($LASTEXITCODE -ne 0) {
    throw "schtasks /Query failed with exit code $LASTEXITCODE"
}

$xml = (& schtasks.exe /Query /TN $TaskName /XML) -join "`n"
if ($xml -notmatch "-StopBackendWhenDone") {
    throw "Task arguments appear truncated. Reduce options or shorten paths."
}

if ($RunNow) {
    Write-Host "Running task now: $TaskName"
    & schtasks.exe /Run /TN $TaskName | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "schtasks /Run failed with exit code $LASTEXITCODE"
    }
}
