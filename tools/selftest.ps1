param(
    [ValidateSet("dev", "portable", "release")]
    [string]$Mode = "dev"
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$frontendDir = Join-Path $repoRoot "app/frontend"
$releaseDir = Join-Path $repoRoot "release/MeeMeeScreener"

function Test-PortListening($Port) {
    try {
        $conn = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction Stop
        return $true
    } catch {
        return $false
    }
}

function Test-DevServerReady($Port) {
    try {
        $resp = Invoke-WebRequest -Uri "http://127.0.0.1:$Port/" -UseBasicParsing -TimeoutSec 2
        return $resp.StatusCode -ge 200 -and $resp.StatusCode -lt 500
    } catch {
        return $false
    }
}

function Find-DevPort {
    $port = 5173
    while (Test-PortListening $port) {
        $port++
    }
    return $port
}

function Start-DevServer($Port) {
    Write-Host "Starting Vite dev server..."
    $npm = Get-Command npm -ErrorAction SilentlyContinue
    if ($null -eq $npm) {
        throw "npm not found in PATH. Install Node.js or add npm.cmd to PATH."
    }
    $env:VITE_API_PROXY_TARGET = "http://127.0.0.1:28888"
    $proc = Start-Process -FilePath "cmd.exe" -ArgumentList @("/c", "npm", "run", "dev", "--", "--host", "127.0.0.1", "--port", "$Port") -WorkingDirectory $frontendDir -PassThru
    $attempts = 0
    while (-not (Test-DevServerReady $Port) -and $attempts -lt 30) {
        Start-Sleep -Seconds 1
        $attempts++
    }
    if (-not (Test-DevServerReady $Port)) {
        throw "Vite dev server did not start on port $Port."
    }
    return $proc
}

function Stop-ProcessSafe($Proc) {
    if ($null -eq $Proc) { return }
    try { Stop-Process -Id $Proc.Id -Force -ErrorAction SilentlyContinue } catch {}
}

$devServer = $null
try {
    if ($Mode -eq "dev") {
        $devPort = Find-DevPort
        $devServer = Start-DevServer $devPort
        $env:MEEMEE_DEV = "1"
        $env:MEEMEE_SELFTEST = "1"
        $env:MEEMEE_DEV_FRONTEND_URL = "http://127.0.0.1:$devPort"
        Write-Host "Running selftest (dev)..."
        $proc = Start-Process -FilePath "python" -ArgumentList @("app/desktop/launcher.py") -WorkingDirectory $repoRoot -Wait -PassThru
        exit $proc.ExitCode
    }

    if ($Mode -eq "portable") {
        Write-Host "Building onedir..."
        & (Join-Path $PSScriptRoot "build_onedir.ps1")
        if (-not (Test-Path $releaseDir)) {
            throw "Onedir build not found: $releaseDir"
        }
        $env:MEEMEE_SELFTEST = "1"
        Write-Host "Running selftest (portable onedir)..."
        $exe = Join-Path $releaseDir "MeeMeeScreener.exe"
        $proc = Start-Process -FilePath $exe -WorkingDirectory $releaseDir -Wait -PassThru
        exit $proc.ExitCode
    }

    if ($Mode -eq "release") {
        Write-Host "Running full release build..."
        & (Join-Path $PSScriptRoot "build_release.cmd")
        if (-not (Test-Path $releaseDir)) {
            throw "Release package not found: $releaseDir"
        }
        $env:MEEMEE_SELFTEST = "1"
        Write-Host "Running selftest (release onedir)..."
        $exe = Join-Path $releaseDir "MeeMeeScreener.exe"
        $proc = Start-Process -FilePath $exe -WorkingDirectory $releaseDir -Wait -PassThru
        exit $proc.ExitCode
    }
} finally {
    if ($Mode -eq "dev") {
        Stop-ProcessSafe $devServer
    }
}
