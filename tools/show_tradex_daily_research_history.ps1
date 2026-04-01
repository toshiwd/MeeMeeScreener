param(
    [string]$PythonExe = "python",
    [int]$Limit = 10,
    [string]$ReportPath = "",
    [string]$TextReportPath = "",
    [string]$LogPath = ""
)

$ErrorActionPreference = "Stop"

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
if (-not $ReportPath) {
    $ReportPath = Join-Path $reportDir ("tradex_daily_research_history_{0}.json" -f $stamp)
}
if (-not $TextReportPath) {
    $TextReportPath = Join-Path $reportDir ("tradex_daily_research_history_{0}.txt" -f $stamp)
}
if (-not $LogPath) {
    $LogPath = Join-Path $logDir ("tradex_daily_research_history_{0}.log" -f $stamp)
}

function Write-Log {
    param([string]$Message)
    $line = "{0:u} {1}" -f (Get-Date), $Message
    $line | Tee-Object -FilePath $LogPath -Append | Out-Host
}

try {
    Write-Log "Tradex daily research history started."
    $args = @(
        "-m", "external_analysis",
        "daily-research-history",
        "--limit", $Limit,
        "--report-path", $ReportPath,
        "--text-report-path", $TextReportPath
    )
    Write-Log ("Executing: {0} {1}" -f $PythonExe, ($args -join " "))
    $output = & $PythonExe @args 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "daily research history command failed with exit code $LASTEXITCODE"
    }
    foreach ($line in $output) {
        Write-Log ([string]$line)
    }
    Write-Log ("JSON report: {0}" -f $ReportPath)
    Write-Log ("Text report: {0}" -f $TextReportPath)
    Write-Log "Tradex daily research history finished successfully."
} catch {
    Write-Log ("ERROR: " + $_.Exception.Message)
    throw
}
