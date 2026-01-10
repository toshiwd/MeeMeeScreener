$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$frontendDir = Join-Path $repoRoot "app/frontend"
$backendStatic = Join-Path $repoRoot "app/backend/static"
$releaseDir = Join-Path $repoRoot "release"
$iconPath = Join-Path $repoRoot "resources/icons/app_icon.ico"

if (-not (Test-Path $iconPath)) {
    throw "Missing icon: $iconPath`nPlace app_icon.ico under resources/icons before building."
}

$running = Get-Process -Name "MeeMeeScreener" -ErrorAction SilentlyContinue
if ($running) {
    throw "MeeMeeScreener.exe is running. Close the app before building."
}

Write-Host "Checking Python dependencies..."
$missingJson = @'
import importlib.util
import json

modules = [
    "fastapi",
    "uvicorn",
    "duckdb",
    "pandas",
    "pydantic",
    "dotenv",
    "webview",
    "PyInstaller",
    "PIL",
    "multipart"
]
missing = [name for name in modules if importlib.util.find_spec(name) is None]
print(json.dumps(missing))
'@ | python -
$missing = @()
try {
    $missing = ($missingJson | ConvertFrom-Json)
} catch {
    $missing = @()
}
if ($missing.Count -gt 0) {
    Write-Host "Installing missing Python packages: $($missing -join ', ')"
    python -m pip install -r (Join-Path $repoRoot "app/backend/requirements.txt")
    python -m pip install pyinstaller pywebview pillow
}

Write-Host "Building frontend..."
Push-Location $frontendDir
npm ci
npm run build
Pop-Location

$distDir = Join-Path $frontendDir "dist"
$buildDir = Join-Path $frontendDir "build"
if (Test-Path $distDir) {
    $frontendOut = $distDir
} elseif (Test-Path $buildDir) {
    $frontendOut = $buildDir
} else {
    throw "Frontend build output not found (dist or build)."
}

if (Test-Path $backendStatic) {
    Remove-Item -Recurse -Force $backendStatic
}
New-Item -ItemType Directory -Force $backendStatic | Out-Null
Copy-Item -Recurse -Force (Join-Path $frontendOut "*") $backendStatic

$releasePackage = Join-Path $releaseDir "MeeMeeScreener"
$releaseZip = Join-Path $releaseDir "MeeMeeScreener-portable.zip"
$backupPackage = $null
if (Test-Path $releasePackage) {
    $backupPackage = Join-Path $releaseDir "MeeMeeScreener.prev"
    if (Test-Path $backupPackage) {
        Remove-Item -Recurse -Force $backupPackage
    }
    try {
        Move-Item -Force $releasePackage $backupPackage
    } catch {
        throw "Failed to move release package. Close MeeMeeScreener.exe and retry."
    }
}
if (Test-Path $releaseZip) {
    Remove-Item -Force $releaseZip
}

Write-Host "Building PyInstaller package..."
$buildWork = Join-Path $repoRoot "build/pyinstaller"
$useClean = $true
if (Test-Path $buildWork) {
    try {
        Remove-Item -Recurse -Force $buildWork
    } catch {
        $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
        $buildWork = Join-Path $repoRoot "build/pyinstaller_$timestamp"
        $useClean = $false
    }
}
New-Item -ItemType Directory -Force $buildWork | Out-Null

$pyInstallerArgs = @(
    "--noconfirm"
)
if ($useClean) {
    $pyInstallerArgs += "--clean"
}
$pyInstallerArgs += @(
    "--onedir",
    "--noconsole",
    "--name", "MeeMeeScreener",
    "--icon", "$iconPath",
    "--distpath", "$releaseDir",
    "--workpath", "$buildWork",
    "--specpath", "$buildWork",
    "--hidden-import", "uvicorn",
    "--hidden-import", "uvicorn.lifespan.on",
    "--hidden-import", "uvicorn.protocols.http.h11_impl",
    "--hidden-import", "uvicorn.protocols.websockets.websockets_impl",
    "--collect-all", "uvicorn",
    "--hidden-import", "app.backend",
    "--hidden-import", "app.backend.main",
    "--collect-submodules", "app.backend",
    "--add-data", "$backendStatic;app/backend/static",
    "--add-data", "$iconPath;resources/icons",
    "--add-data", "$(Join-Path $repoRoot "tools/export_pan.vbs");tools",
    "--add-data", "$(Join-Path $repoRoot "tools/code.txt");tools",
    "--add-data", "$(Join-Path $repoRoot "app/backend/rank_config.json");app/backend",
    "--add-data", "$(Join-Path $repoRoot "app/backend/update_state.json");app/backend",
    "--add-data", "$(Join-Path $repoRoot "app/backend/favorites.sqlite");app/backend",
    "--add-data", "$(Join-Path $repoRoot "app/backend/practice.sqlite");app/backend",
    "--add-data", "$(Join-Path $repoRoot "app/backend/stocks.duckdb");app/backend",
    "app/desktop/launcher.py"
)

python -m PyInstaller @pyInstallerArgs

$onedir = $releasePackage
if (-not (Test-Path $onedir)) {
    if ($backupPackage -and (Test-Path $backupPackage)) {
        Move-Item -Force $backupPackage $releasePackage
    }
    throw "Build failed: release/MeeMeeScreener not found."
}

Write-Host "Creating portable zip..."
$zipPath = $releaseZip
Compress-Archive -Path $onedir -DestinationPath $zipPath

if ($backupPackage -and (Test-Path $backupPackage)) {
    Remove-Item -Recurse -Force $backupPackage
}

Write-Host "Done."
