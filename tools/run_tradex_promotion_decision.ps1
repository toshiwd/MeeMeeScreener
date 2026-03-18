param(
    [string]$PythonExe = "python",
    [Parameter(Mandatory = $true)][string]$Decision,
    [string]$Note = "",
    [string]$Actor = "codex_cli",
    [string]$ReportPath = "",
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
    $ReportPath = Join-Path $tmpDir ("tradex_promotion_decision_{0}.json" -f $stamp)
}
if (-not $LogPath) {
    $LogPath = Join-Path $tmpDir ("tradex_promotion_decision_{0}.log" -f $stamp)
}

function Write-Log {
    param([string]$Message)
    $line = "{0:u} {1}" -f (Get-Date), $Message
    $line | Tee-Object -FilePath $LogPath -Append | Out-Host
}

try {
    $args = @(
        "-m", "external_analysis",
        "promotion-decision-run",
        "--decision", $Decision,
        "--actor", $Actor,
        "--report-path", $ReportPath
    )
    if ($Note) {
        $args += @("--note", $Note)
    }
    Write-Log ("Executing: {0} {1}" -f $PythonExe, ($args -join " "))
    $output = & $PythonExe @args 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "promotion decision command failed with exit code $LASTEXITCODE"
    }
    foreach ($line in $output) {
        Write-Log ([string]$line)
    }
    Write-Log ("Report: {0}" -f $ReportPath)
} catch {
    Write-Log ("ERROR: " + $_.Exception.Message)
    throw
}
