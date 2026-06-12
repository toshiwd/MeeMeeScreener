param(
    [string]$LogPath = "",
    [switch]$PackageZip,
    [switch]$SmokeRun,
    [switch]$Clean,
    [string]$ReleaseRoot = ""
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$frontendDir = Join-Path $repoRoot "app/frontend"
$backendStatic = Join-Path $repoRoot "app/backend/static"
$frontendRouteVerifier = Join-Path $repoRoot "tools/verify_frontend_research_routes.ps1"
$releaseDir = Join-Path $repoRoot "release"
$releaseZip = Join-Path $releaseDir "MeeMeeScreener-portable.zip"
$iconPath = Join-Path $repoRoot "resources/icons/app_icon.ico"
$dpiManifestPath = Join-Path $repoRoot "resources/windows/meemee_dpi_aware.manifest"
$buildRoot = Join-Path $repoRoot "build"
$artifactsDir = Join-Path $repoRoot "build/release_artifacts"
$releaseRootCandidate = $ReleaseRoot
if ([string]::IsNullOrWhiteSpace($releaseRootCandidate)) {
    $releaseRootCandidate = $env:MEEMEE_RELEASE_PACKAGE_ROOT
}
if ([string]::IsNullOrWhiteSpace($releaseRootCandidate)) {
    $releaseRootCandidate = [Environment]::GetFolderPath("Desktop")
}
if ([string]::IsNullOrWhiteSpace($releaseRootCandidate)) {
    throw "Release package root is not available."
}
$releasePackageRoot = [System.IO.Path]::GetFullPath($releaseRootCandidate)
$null = New-Item -ItemType Directory -Force $releasePackageRoot
$releasePackage = Join-Path $releasePackageRoot "MeeMeeScreener"

function Save-FileTail {
    param(
        [string[]]$SourcePaths,
        [string]$TailPath
    )

    $lines = @()
    foreach ($sourcePath in $SourcePaths) {
        if (Test-Path $sourcePath) {
            $lines += Get-Content -Path $sourcePath -Tail 50
        }
    }
    if ($lines.Count -gt 0) {
        $lines | Set-Content -Path $TailPath -Encoding UTF8
    }
}

function Get-ReleaseDbSourcePath {
    $override = $env:MEEMEE_RELEASE_DB_PATH
    if (-not [string]::IsNullOrWhiteSpace($override)) {
        if (-not (Test-Path $override)) {
            throw "MEEMEE_RELEASE_DB_PATH not found: $override"
        }
        return (Resolve-Path $override).Path
    }

    $localAppData = [Environment]::GetFolderPath("LocalApplicationData")
    if ([string]::IsNullOrWhiteSpace($localAppData)) {
        throw "LOCALAPPDATA is not available and MEEMEE_RELEASE_DB_PATH is not set."
    }

    $canonical = Join-Path $localAppData "MeeMeeScreener\data\stocks.duckdb"
    if (-not (Test-Path $canonical)) {
        throw "Release source DB not found: $canonical`nSet MEEMEE_RELEASE_DB_PATH to override."
    }
    return $canonical
}

function Get-DuckDbCounts {
    param([string]$DbPath)

    $json = @'
import json
import sys

import duckdb

path = sys.argv[1]
tables = {}
with duckdb.connect(path, read_only=True) as conn:
    for name in ("tickers", "daily_bars", "monthly_bars", "industry_master"):
        try:
            tables[name] = int(conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])
        except Exception:
            tables[name] = -1
print(json.dumps(tables))
'@ | python - $DbPath
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to inspect DuckDB counts: $DbPath"
    }
    return $json | ConvertFrom-Json
}

function Assert-ReleaseSourceDbReady {
    param([string]$DbPath)

    $counts = Get-DuckDbCounts -DbPath $DbPath
    $missing = @()
    foreach ($table in @("tickers", "daily_bars", "monthly_bars")) {
        if (($counts.$table -as [int]) -le 0) {
            $missing += "${table}=$($counts.$table)"
        }
    }
    if ($missing.Count -gt 0) {
        throw "Release source DB is not usable: $DbPath`nRequired rows missing: $($missing -join ', ')"
    }
    return $counts
}

function Assert-BundledDbReady {
    param([string]$DbPath)

    $counts = Get-DuckDbCounts -DbPath $DbPath
    $missing = @()
    foreach ($table in @("tickers", "daily_bars", "monthly_bars", "industry_master")) {
        if (($counts.$table -as [int]) -le 0) {
            $missing += "${table}=$($counts.$table)"
        }
    }
    if ($missing.Count -gt 0) {
        throw "Bundled release DB is not usable after ensure_industry_master: $DbPath`nRequired rows missing: $($missing -join ', ')"
    }
    return $counts
}

function Stop-LockProcesses {
    Stop-Process -Name node -Force -ErrorAction SilentlyContinue
    Stop-Process -Name MeeMeeScreener -Force -ErrorAction SilentlyContinue
    Stop-Process -Name msedgewebview2 -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1
}

function Remove-RepoBuildArtifacts {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }

    $resolvedPath = (Resolve-Path -LiteralPath $Path).Path
    if (-not $resolvedPath.StartsWith($repoRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Cleanup target escaped repo root: $resolvedPath"
    }

    for ($attempt = 1; $attempt -le 3; $attempt++) {
        try {
            Remove-Item -LiteralPath $resolvedPath -Recurse -Force
            return
        } catch {
            if ($attempt -eq 3) {
                throw "Failed to clean repo build artifacts: $resolvedPath`n$($_.Exception.Message)"
            }
            Start-Sleep -Seconds 2
        }
    }
}

function Test-ViteAvailable {
    $viteCmd = Join-Path $frontendDir "node_modules\\.bin\\vite.cmd"
    return (Test-Path $viteCmd)
}

function Invoke-NpmCiWithRetry {
    $maxAttempts = 3
    $attempt = 1
    while ($attempt -le $maxAttempts) {
        Stop-LockProcesses
        $npmStdout = Join-Path $artifactsDir "npm_ci_stdout.txt"
        $npmStderr = Join-Path $artifactsDir "npm_ci_stderr.txt"
        if (Test-Path $npmStdout) { Remove-Item -Force $npmStdout -ErrorAction SilentlyContinue }
        if (Test-Path $npmStderr) { Remove-Item -Force $npmStderr -ErrorAction SilentlyContinue }

        $proc = Start-Process -FilePath "cmd.exe" -ArgumentList @("/c", "npm", "ci") -WorkingDirectory $frontendDir -NoNewWindow -Wait -PassThru `
            -RedirectStandardOutput $npmStdout -RedirectStandardError $npmStderr
        if ($proc.ExitCode -eq 0) {
            return
        }

        Save-FileTail -SourcePaths @($npmStdout, $npmStderr) -TailPath (Join-Path $artifactsDir "npm_ci_tail.txt")
        $errorText = ""
        if (Test-Path $npmStdout) { $errorText += (Get-Content -Path $npmStdout -Raw) }
        if (Test-Path $npmStderr) { $errorText += (Get-Content -Path $npmStderr -Raw) }
        $shouldRetry = $errorText -match "EPERM" -or $errorText -match "-4048" -or $errorText -match "EACCES"
        if (-not $shouldRetry -or $attempt -eq $maxAttempts) {
            throw "npm ci failed with exit code $($proc.ExitCode)"
        }

        Write-Host "npm ci failed due to EPERM/EACCES. Retrying ($attempt/$maxAttempts)..."
        Remove-Item -Recurse -Force (Join-Path $frontendDir "node_modules") -ErrorAction SilentlyContinue
        Remove-Item -Recurse -Force (Join-Path $frontendDir ".vite") -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 2
        $attempt++
    }
}

function Invoke-FrontendBuild {
    Write-Host "Building frontend..."
    Push-Location $frontendDir
    try {
        if (-not (Test-Path (Join-Path $frontendDir "node_modules")) -or -not (Test-ViteAvailable)) {
            Invoke-NpmCiWithRetry
        } else {
            Write-Host "node_modules exists; skipping npm ci."
        }

        $buildStdout = Join-Path $artifactsDir "npm_build_stdout.txt"
        $buildStderr = Join-Path $artifactsDir "npm_build_stderr.txt"
        if (Test-Path $buildStdout) { Remove-Item -Force $buildStdout -ErrorAction SilentlyContinue }
        if (Test-Path $buildStderr) { Remove-Item -Force $buildStderr -ErrorAction SilentlyContinue }

        $buildProc = Start-Process -FilePath "cmd.exe" -ArgumentList @("/c", "npm", "run", "build") -WorkingDirectory $frontendDir -NoNewWindow -Wait -PassThru `
            -RedirectStandardOutput $buildStdout -RedirectStandardError $buildStderr
        if ($buildProc.ExitCode -ne 0) {
            Save-FileTail -SourcePaths @($buildStdout, $buildStderr) -TailPath (Join-Path $artifactsDir "npm_build_tail.txt")
            throw "npm run build failed with exit code $($buildProc.ExitCode)"
        }
    } finally {
        Pop-Location
    }
}

function New-PortableZip {
    param(
        [string]$SourceDir,
        [string]$DestinationZip
    )

    Add-Type -AssemblyName System.IO.Compression
    Add-Type -AssemblyName System.IO.Compression.FileSystem

    if (Test-Path $DestinationZip) {
        Remove-Item -Force $DestinationZip
    }

    $zipStream = [System.IO.File]::Open(
        $DestinationZip,
        [System.IO.FileMode]::Create,
        [System.IO.FileAccess]::ReadWrite,
        [System.IO.FileShare]::None
    )
    try {
        $archive = New-Object System.IO.Compression.ZipArchive($zipStream, ([System.IO.Compression.ZipArchiveMode]::Create), $false)
        try {
            $root = (Resolve-Path $SourceDir).Path.TrimEnd('\')
            $files = Get-ChildItem -Path $root -Recurse -File -Force
            foreach ($file in $files) {
                $relative = $file.FullName.Substring($root.Length).TrimStart('\')
                $entryName = $relative -replace '\\', '/'
                $entry = $archive.CreateEntry($entryName, [System.IO.Compression.CompressionLevel]::Optimal)

                $entryStream = $entry.Open()
                try {
                    $fs = New-Object System.IO.FileStream(
                        $file.FullName,
                        [System.IO.FileMode]::Open,
                        [System.IO.FileAccess]::Read,
                        ([System.IO.FileShare]::ReadWrite -bor [System.IO.FileShare]::Delete)
                    )
                    try {
                        $fs.CopyTo($entryStream)
                    } finally {
                        $fs.Dispose()
                    }
                } finally {
                    $entryStream.Dispose()
                }
            }
        } finally {
            $archive.Dispose()
        }
    } finally {
        $zipStream.Dispose()
    }
}

function Sync-ReleasePackageToDesktop {
    param([string]$SourceDir)

    $desktopRoot = [Environment]::GetFolderPath("Desktop")
    if ([string]::IsNullOrWhiteSpace($desktopRoot)) {
        Write-Host "Desktop path is unavailable; skipping desktop sync."
        return
    }

    $destinationDir = Join-Path $desktopRoot "MeeMeeScreener"
    $sourceFull = [System.IO.Path]::GetFullPath($SourceDir).TrimEnd('\')
    $destinationFull = [System.IO.Path]::GetFullPath($destinationDir).TrimEnd('\')
    if ($sourceFull.Equals($destinationFull, [System.StringComparison]::OrdinalIgnoreCase)) {
        Write-Host "Desktop package already points to the release package; skipping sync."
        return
    }

    $null = New-Item -ItemType Directory -Force $destinationDir
    Write-Host "Syncing release package to Desktop: $destinationFull"
    robocopy $sourceFull $destinationFull /E /R:2 /W:1 /NFL /NDL /NJH /NJS /NP | Out-Host
    $robocopyExit = $LASTEXITCODE
    if ($robocopyExit -ge 8) {
        throw "Failed to sync release package to Desktop. robocopy exit code: $robocopyExit"
    }
}

function Copy-MeeMeeSafeArtifacts {
    param([string]$ReleasePackageRoot, [string]$RepoRoot)

    $sourceDir = Join-Path $RepoRoot "artifacts\research_inventory"
    $destinationDir = Join-Path $ReleasePackageRoot "_internal\artifacts\research_inventory"
    $safeArtifacts = @(
        "chart_gallery_authoritative_adoption.json",
        "chart_data_provenance_contract.json",
        "chart_gallery_authoritative_overwrite_contract.json"
    )

    if (-not (Test-Path $sourceDir)) {
        throw "Missing research inventory source directory: $sourceDir"
    }

    New-Item -ItemType Directory -Force $destinationDir | Out-Null
    foreach ($artifactName in $safeArtifacts) {
        $sourcePath = Join-Path $sourceDir $artifactName
        if (-not (Test-Path $sourcePath)) {
            throw "Missing MeeMee-safe artifact: $sourcePath"
        }
        Copy-Item -Path $sourcePath -Destination (Join-Path $destinationDir $artifactName) -Force
    }
}

function Invoke-SmokeRun {
    param([string]$ExePath)

    if (-not (Test-Path $ExePath)) {
        throw "Smoke target not found: $ExePath"
    }

    Write-Host "Running smoke launch..."
    $proc = $null
    try {
        $proc = Start-Process -FilePath $ExePath -WorkingDirectory (Split-Path -Parent $ExePath) -PassThru
        Start-Sleep -Seconds 10
        if ($proc.HasExited) {
            if ($proc.ExitCode -ne 0) {
                throw "Smoke launch failed with exit code $($proc.ExitCode)"
            }
            if (-not (Test-PackagedHealth -Port 28888 -TimeoutSeconds 30)) {
                throw "Smoke launch exited before backend became healthy"
            }
            return
        }
        if (-not (Test-PackagedHealth -Port 28888 -TimeoutSeconds 30)) {
            throw "Smoke launch did not reach healthy backend"
        }
    } finally {
        if ($proc -and -not $proc.HasExited) {
            Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
        }
        # The desktop launcher may hand off to another MeeMeeScreener process.
        # Ensure smoke verification never leaves a packaged instance holding runtime files.
        Stop-LockProcesses
    }
}

function Test-PackagedHealth {
    param(
        [int]$Port,
        [int]$TimeoutSeconds
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $url = "http://127.0.0.1:$Port/api/health"
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-RestMethod -Uri $url -TimeoutSec 5
            if ($response.ok -eq $true -and $response.ready -eq $true) {
                return $true
            }
        } catch {
            Start-Sleep -Seconds 1
            continue
        }
        Start-Sleep -Seconds 1
    }
    return $false
}

if (-not (Test-Path $artifactsDir)) {
    New-Item -ItemType Directory -Force $artifactsDir | Out-Null
}

if ($LogPath) {
    $logDir = Split-Path -Parent $LogPath
    if ($logDir -and -not (Test-Path $logDir)) {
        New-Item -ItemType Directory -Force $logDir | Out-Null
    }
    Start-Transcript -Path $LogPath -Force | Out-Null
}

$buildSucceeded = $false

try {
    if (-not (Test-Path $iconPath)) {
        throw "Missing icon: $iconPath`nPlace app_icon.ico under resources/icons before building."
    }
    if (-not (Test-Path $dpiManifestPath)) {
        throw "Missing DPI manifest: $dpiManifestPath"
    }

    Write-Host "Starting build_release.ps1"
    $running = Get-Process -Name "MeeMeeScreener" -ErrorAction SilentlyContinue
    if ($running) {
        Write-Host "Closing MeeMeeScreener.exe..."
        $running | Stop-Process -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 1
    }
    Stop-LockProcesses

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

    Invoke-FrontendBuild

    $distDir = Join-Path $frontendDir "dist"
    $buildDir = Join-Path $frontendDir "build"
    if (Test-Path $distDir) {
        $frontendOut = $distDir
    } elseif (Test-Path $buildDir) {
        $frontendOut = $buildDir
    } else {
        throw "Frontend build output not found (dist or build)."
    }

    & powershell -ExecutionPolicy Bypass -File $frontendRouteVerifier -TargetPaths @($frontendOut)
    if ($LASTEXITCODE -ne 0) {
        throw "Frontend route verification failed for build output."
    }

    if (Test-Path $backendStatic) {
        Remove-Item -Recurse -Force $backendStatic
    }
    New-Item -ItemType Directory -Force $backendStatic | Out-Null
    Copy-Item -Recurse -Force (Join-Path $frontendOut "*") $backendStatic
    & powershell -ExecutionPolicy Bypass -File $frontendRouteVerifier -TargetPaths @($backendStatic)
    if ($LASTEXITCODE -ne 0) {
        throw "Frontend route verification failed for backend static assets."
    }

    if (-not (Test-Path $releaseDir)) {
        New-Item -ItemType Directory -Force $releaseDir | Out-Null
    }
    if (Test-Path $releasePackage) {
        try {
            Write-Host "Removing existing release package..."
            Remove-Item -Recurse -Force $releasePackage
        } catch {
            Write-Host "Release package is locked. Retrying..."
            Start-Sleep -Seconds 1
            Remove-Item -Recurse -Force $releasePackage
        }
    }
    if ($PackageZip -and (Test-Path $releaseZip)) {
        Write-Host "Removing existing release zip..."
        Remove-Item -Force $releaseZip
    }

    $releaseDbSource = Get-ReleaseDbSourcePath
    $sourceCounts = Assert-ReleaseSourceDbReady -DbPath $releaseDbSource
    Write-Host "Using release source DB: $releaseDbSource"
    Write-Host "Source counts: tickers=$($sourceCounts.tickers) daily_bars=$($sourceCounts.daily_bars) monthly_bars=$($sourceCounts.monthly_bars) industry_master=$($sourceCounts.industry_master)"

    $stagedDbDir = Join-Path $artifactsDir "staged_db"
    if (-not (Test-Path $stagedDbDir)) {
        New-Item -ItemType Directory -Force $stagedDbDir | Out-Null
    }
    $stagedDbPath = Join-Path $stagedDbDir "stocks.duckdb"
    if (Test-Path $stagedDbPath) {
        Remove-Item -Force $stagedDbPath
    }
    Copy-Item -Path $releaseDbSource -Destination $stagedDbPath -Force

    Write-Host "Building PyInstaller package..."
    $buildWork = Join-Path $artifactsDir "pyinstaller_work"
    $useClean = [bool]$Clean
    if (Test-Path $buildWork) {
        try {
            Remove-Item -Recurse -Force $buildWork
        } catch {
            $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
            $buildWork = Join-Path $artifactsDir "pyinstaller_$timestamp"
            if (-not $Clean) {
                $useClean = $false
            }
        }
    }
    New-Item -ItemType Directory -Force $buildWork | Out-Null

    Write-Host "Ensuring industry_master in staged release DuckDB..."
    python (Join-Path $repoRoot "tools/setup/ensure_industry_master.py") --db $stagedDbPath
    if ($LASTEXITCODE -ne 0) {
        throw "ensure_industry_master.py failed for staged release DB."
    }
    $bundledCounts = Assert-BundledDbReady -DbPath $stagedDbPath
    Write-Host "Bundled counts: tickers=$($bundledCounts.tickers) daily_bars=$($bundledCounts.daily_bars) monthly_bars=$($bundledCounts.monthly_bars) industry_master=$($bundledCounts.industry_master)"

    $pyInstallerArgs = @("--noconfirm")
    if ($useClean) {
        $pyInstallerArgs += "--clean"
    }
    $pyInstallerArgs += @("--paths", "$repoRoot")
    $pyInstallerArgs += @(
        "--onedir",
        "--noconsole",
        "--name", "MeeMeeScreener",
        "--icon", "$iconPath",
        "--manifest", "$dpiManifestPath",
        "--distpath", "$releasePackageRoot",
        "--workpath", "$buildWork",
        "--specpath", "$buildWork",
        "--hidden-import", "uvicorn",
        "--hidden-import", "uvicorn.lifespan.on",
        "--hidden-import", "uvicorn.protocols.http.h11_impl",
        "--hidden-import", "uvicorn.protocols.websockets.websockets_impl",
        "--hidden-import", "pythonnet",
        "--hidden-import", "clr",
        "--hidden-import", "clr_loader",
        "--hidden-import", "System",
        "--hidden-import", "win32timezone",
        "--hidden-import", "webview.platforms.winforms",
        "--collect-submodules", "multipart",
        "--collect-all", "uvicorn",
        "--hidden-import", "app.backend",
        "--hidden-import", "app.backend.main",
        "--hidden-import", "app.backend.services.noncandle_rank_window_shadow_adapter",
        "--collect-submodules", "app.backend",
        "--collect-submodules", "app",
        "--hidden-import", "app.core",
        "--hidden-import", "app.core.config",
        "--add-data", "$(Join-Path $repoRoot "app/main.py");app",
        "--add-data", "$(Join-Path $repoRoot "app/__init__.py");app",
        "--add-data", "$(Join-Path $repoRoot "app/core/__init__.py");app/core",
        "--add-data", "$(Join-Path $repoRoot "app/core/*.py");app/core",
        "--add-data", "$(Join-Path $repoRoot "app/backend/__init__.py");app/backend",
        "--add-data", "$(Join-Path $repoRoot "app/backend/*.py");app/backend",
        "--add-data", "$(Join-Path $repoRoot "app/backend/api");app/backend/api",
        "--add-data", "$(Join-Path $repoRoot "app/backend/core/__init__.py");app/backend/core",
        "--add-data", "$(Join-Path $repoRoot "app/backend/core/*.py");app/backend/core",
        "--add-data", "$(Join-Path $repoRoot "app/backend/services/noncandle_rank_window_shadow_adapter.py");app/backend/services",
        "--add-data", "$(Join-Path $repoRoot "app/backend/services/ml/rankings_cache.py");app/backend/services/ml",
        "--add-data", "$(Join-Path $repoRoot "app/desktop/*.py");app/desktop",
        "--add-data", "$backendStatic;app/backend/static",
        "--add-data", "$iconPath;resources/icons",
        "--add-data", "$(Join-Path $repoRoot "tools/export_pan.vbs");tools",
        "--add-data", "$(Join-Path $repoRoot "tools/code.txt");tools",
        "--add-data", "$(Join-Path $repoRoot "app/backend/rank_config.json");app/backend",
        "--add-data", "$(Join-Path $repoRoot "app/backend/update_state.json");app/backend",
        "--add-data", "$(Join-Path $repoRoot "app/backend/favorites.sqlite");app/backend",
        "--add-data", "$(Join-Path $repoRoot "app/backend/practice.sqlite");app/backend",
        "--add-data", "$(Join-Path $repoRoot "toredex_config.json");.",
        "--add-data", "$stagedDbPath;app/backend",
        "--add-data", "$(Join-Path $repoRoot "fixtures");fixtures",
        "app/desktop/launcher.py"
    )

    Write-Host "Running PyInstaller..."
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $pyStdout = Join-Path $releaseDir "logs\pyinstaller_${timestamp}.out.log"
    $pyStderr = Join-Path $releaseDir "logs\pyinstaller_${timestamp}.err.log"
    $pyArgs = @("-m", "PyInstaller") + $pyInstallerArgs
    $pyProc = Start-Process -FilePath "python" -ArgumentList $pyArgs -WorkingDirectory $repoRoot -NoNewWindow -Wait -PassThru `
        -RedirectStandardOutput $pyStdout -RedirectStandardError $pyStderr
    Write-Host "PyInstaller stdout: $pyStdout"
    Write-Host "PyInstaller stderr: $pyStderr"
    if (Test-Path $pyStdout) {
        Get-Content -Path $pyStdout | ForEach-Object { Write-Host $_ }
    }
    if (Test-Path $pyStderr) {
        Get-Content -Path $pyStderr | ForEach-Object { Write-Host $_ }
    }
    Write-Host "PyInstaller finished with exit code $($pyProc.ExitCode)"
    if ($pyProc.ExitCode -ne 0) {
        throw "PyInstaller failed with exit code $($pyProc.ExitCode)"
    }

    if (-not (Test-Path $releasePackage)) {
        throw "Build failed: release/MeeMeeScreener not found."
    }

    $readmeSrc = Join-Path $repoRoot "resources\README.txt"
    if (Test-Path $readmeSrc) {
        Copy-Item -Path $readmeSrc -Destination (Join-Path $releasePackage "README.txt") -Force
    }

    $bootstrapPs1 = Join-Path $repoRoot "tools\portable_bootstrap.ps1"
    $bootstrapCmd = Join-Path $repoRoot "tools\portable_bootstrap.cmd"
    Copy-Item -Path $bootstrapPs1 -Destination (Join-Path $releasePackage "portable_bootstrap.ps1") -Force
    Copy-Item -Path $bootstrapCmd -Destination (Join-Path $releasePackage "portable_bootstrap.cmd") -Force

    $seedDst = Join-Path $releasePackage "_internal\seed\models\ml"
    python (Join-Path $repoRoot "tools\setup\copy_seed_ml_models.py") --dest $seedDst
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to copy ML seed artifacts."
    }

    Copy-MeeMeeSafeArtifacts -ReleasePackageRoot $releasePackage -RepoRoot $repoRoot

    $exportVbsSrc = Join-Path $repoRoot "tools\export_pan.vbs"
    if (Test-Path $exportVbsSrc) {
        Copy-Item -Path $exportVbsSrc -Destination (Join-Path $releasePackage "export_pan.vbs") -Force
    }

    if ($PackageZip) {
        Write-Host "Creating portable zip..."
        $zipAttempts = 5
        $zipDelaySeconds = 2
        $zipSuccess = $false

        for ($i = 1; $i -le $zipAttempts; $i++) {
            try {
                New-PortableZip -SourceDir $releasePackage -DestinationZip $releaseZip
                $zipSuccess = $true
                break
            } catch {
                Write-Host "Zip failed (attempt $i/$zipAttempts): $($_.Exception.Message)"
                Start-Sleep -Seconds $zipDelaySeconds
            }
        }
        if (-not $zipSuccess) {
            throw "Failed to create portable zip. Files under release/MeeMeeScreener are locked. Close Explorer or antivirus scan and retry."
        }

        Write-Host "Running portable zip gate..."
        $verifyScript = Join-Path $repoRoot "scripts\verify_portable_zip.py"
        $verifyProc = Start-Process -FilePath "python" -ArgumentList @($verifyScript, $releaseZip) -NoNewWindow -Wait -PassThru
        if ($verifyProc.ExitCode -ne 0) {
            throw "Portable zip gate failed."
        }
    }

    if ($SmokeRun) {
        Invoke-SmokeRun -ExePath (Join-Path $releasePackage "MeeMeeScreener.exe")
    }

    Sync-ReleasePackageToDesktop -SourceDir $releasePackage

    $buildSucceeded = $true
    Write-Host "Done."
    Write-Host ""
    Write-Host "Onedir package created: $releasePackage"
    if ($PackageZip) {
        Write-Host "Portable package created: $releaseZip"
    }
} finally {
    if ($buildSucceeded) {
        Remove-RepoBuildArtifacts -Path $buildRoot
    }
    if ($LogPath) {
        Stop-Transcript | Out-Null
    }
}
