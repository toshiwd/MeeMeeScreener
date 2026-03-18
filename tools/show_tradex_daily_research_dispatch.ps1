param(
    [string]$PythonExe = "python",
    [int]$Limit = 10,
    [int]$Position = 1,
    [string]$ReportPath = "",
    [string]$TextReportPath = "",
    [string]$LogPath = ""
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$tmpDir = Join-Path $repoRoot "tmp"
if (-not (Test-Path $tmpDir)) {
    New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null
}

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
if (-not $ReportPath) {
    $ReportPath = Join-Path $tmpDir ("tradex_daily_research_dispatch_{0}.json" -f $stamp)
}
if (-not $TextReportPath) {
    $TextReportPath = Join-Path $tmpDir ("tradex_daily_research_dispatch_{0}.txt" -f $stamp)
}
if (-not $LogPath) {
    $LogPath = Join-Path $tmpDir ("tradex_daily_research_dispatch_{0}.log" -f $stamp)
}

function Write-Log {
    param([string]$Message)
    $line = "{0:u} {1}" -f (Get-Date), $Message
    $line | Tee-Object -FilePath $LogPath -Append | Out-Host
}

try {
    Write-Log "Tradex daily research dispatch started."
    $args = @(
        "-m", "external_analysis",
        "daily-research-dispatch",
        "--limit", $Limit,
        "--position", $Position,
        "--report-path", $ReportPath,
        "--text-report-path", $TextReportPath
    )
    Write-Log ("Executing: {0} {1}" -f $PythonExe, ($args -join " "))
    $output = & $PythonExe @args 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "daily research dispatch command failed with exit code $LASTEXITCODE"
    }
    foreach ($line in $output) {
        Write-Log ([string]$line)
    }
    Write-Log ("JSON report: {0}" -f $ReportPath)
    Write-Log ("Text report: {0}" -f $TextReportPath)
    Write-Log "Tradex daily research dispatch finished successfully."
} catch {
    Write-Log ("ERROR: " + $_.Exception.Message)
    throw
}
