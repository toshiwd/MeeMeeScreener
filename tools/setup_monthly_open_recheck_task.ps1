param(
    [string]$TaskName = "MeeMee-Monthly-Open-Recheck",
    [string]$StartTime = "08:45",
    [string]$PythonExe = "python",
    [string]$PlanFile = "",
    [string]$BasePlanFile = "",
    [string]$OutputDir = "",
    [switch]$DryRun,
    [switch]$RunNow
)

$ErrorActionPreference = "Stop"

if ($StartTime -notmatch "^\d{2}:\d{2}$") {
    throw "StartTime must be HH:MM format."
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$runnerPath = Join-Path $repoRoot "scripts\monthly_open_recheck_batch.ps1"
if (-not (Test-Path $runnerPath)) {
    throw "Runner script not found: $runnerPath"
}

$tradexRoot = [Environment]::GetEnvironmentVariable("MEEMEE_TRADEX_ROOT")
if ([string]::IsNullOrWhiteSpace($tradexRoot)) {
    $tradexRoot = "G:\Tradex"
}
$tradexScratchRoot = Join-Path $tradexRoot "scratch"
$defaultMonthlyOpenDir = Join-Path $tradexScratchRoot "monthly_open"
$defaultPlanFile = Join-Path $defaultMonthlyOpenDir "monthly_execution_plan_202603_strictplus_backups.json"
$defaultBasePlanFile = Join-Path $defaultMonthlyOpenDir "monthly_execution_plan_202603.json"
$defaultOutputDir = $defaultMonthlyOpenDir
$PlanFile = if ([string]::IsNullOrWhiteSpace($PlanFile)) { $defaultPlanFile } else { $PlanFile }
$BasePlanFile = if ([string]::IsNullOrWhiteSpace($BasePlanFile)) { $defaultBasePlanFile } else { $BasePlanFile }
$OutputDir = if ([string]::IsNullOrWhiteSpace($OutputDir)) { $defaultOutputDir } else { $OutputDir }

$defaultPythonExe = "python"

$argParts = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", ('"{0}"' -f $runnerPath)
)
if ($PythonExe -ne $defaultPythonExe) { $argParts += @("-PythonExe", ('"{0}"' -f $PythonExe)) }
if ($PlanFile -ne $defaultPlanFile) { $argParts += @("-PlanFile", ('"{0}"' -f $PlanFile)) }
if ($BasePlanFile -ne $defaultBasePlanFile) { $argParts += @("-BasePlanFile", ('"{0}"' -f $BasePlanFile)) }
if ($OutputDir -ne $defaultOutputDir) { $argParts += @("-OutputDir", ('"{0}"' -f $OutputDir)) }

$taskCommand = "powershell.exe " + ($argParts -join " ")

Write-Host "Preparing scheduled task: $TaskName"
Write-Host "Schedule: MON-FRI at $StartTime"
Write-Host "Command: $taskCommand"

if ($DryRun) {
    Write-Host "DryRun enabled. Skip schtasks /Create."
    exit 0
}

& schtasks.exe /Create /TN $TaskName /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST $StartTime /TR $taskCommand /F | Out-Host
if ($LASTEXITCODE -ne 0) {
    throw "schtasks /Create failed with exit code $LASTEXITCODE"
}

Write-Host "Task created."
& schtasks.exe /Query /TN $TaskName /V /FO LIST | Out-Host
if ($LASTEXITCODE -ne 0) {
    throw "schtasks /Query failed with exit code $LASTEXITCODE"
}

if ($RunNow) {
    Write-Host "Running task now: $TaskName"
    & schtasks.exe /Run /TN $TaskName | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "schtasks /Run failed with exit code $LASTEXITCODE"
    }
}
