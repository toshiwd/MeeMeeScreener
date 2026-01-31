param(
    [string]$LogPath = ""
)

$ErrorActionPreference = "Stop"

if ($LogPath) {
    $logDir = Split-Path -Parent $LogPath
    if ($logDir -and -not (Test-Path $logDir)) {
        New-Item -ItemType Directory -Force $logDir | Out-Null
    }
    Start-Transcript -Path $LogPath -Force | Out-Null
}

try {
    $repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
    $frontendDir = Join-Path $repoRoot "app/frontend"
    $backendStatic = Join-Path $repoRoot "app/backend/static"
    $releaseDir = Join-Path $repoRoot "release"
    $iconPath = Join-Path $repoRoot "resources/icons/app_icon.ico"

    if (-not (Test-Path $iconPath)) {
        throw "Missing icon: $iconPath`nPlace app_icon.ico under resources/icons before building."
    }

    Write-Host "Starting build_release.ps1"
    $running = Get-Process -Name "MeeMeeScreener" -ErrorAction SilentlyContinue
    if ($running) {
        Write-Host "Closing MeeMeeScreener.exe..."
        $running | Stop-Process -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 1
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
    if ($LASTEXITCODE -ne 0) {
        throw "npm ci failed with exit code $LASTEXITCODE"
    }
    npm run build
    if ($LASTEXITCODE -ne 0) {
        throw "npm run build failed with exit code $LASTEXITCODE"
    }
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
    if (Test-Path $releaseZip) {
        Write-Host "Removing existing release zip..."
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

    Write-Host "Ensuring industry_master in bundled DuckDB..."
    python (Join-Path $repoRoot "tools/setup/ensure_industry_master.py") --db (Join-Path $repoRoot "app/backend/stocks.duckdb")

    $pyInstallerArgs = @(
        "--noconfirm"
    )
    if ($useClean) {
        $pyInstallerArgs += "--clean"
    }
    $pyInstallerArgs += @(
        "--paths", "$repoRoot"
    )
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
        "--hidden-import", "pythonnet",
        "--hidden-import", "clr",
        "--hidden-import", "clr_loader",
        "--hidden-import", "System",
        "--hidden-import", "System.Windows.Forms",
        "--hidden-import", "webview.platforms.winforms",
        "--collect-submodules", "multipart",
        "--collect-all", "uvicorn",
        "--hidden-import", "app.backend",
        "--hidden-import", "app.backend.main",
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
        "--add-data", "$(Join-Path $repoRoot "app/desktop/*.py");app/desktop",
        "--add-data", "$backendStatic;app/backend/static",
        "--add-data", "$iconPath;resources/icons",
        "--add-data", "$(Join-Path $repoRoot "tools/export_pan.vbs");tools",
        "--add-data", "$(Join-Path $repoRoot "tools/code.txt");tools",
        "--add-data", "$(Join-Path $repoRoot "app/backend/rank_config.json");app/backend",
        "--add-data", "$(Join-Path $repoRoot "app/backend/update_state.json");app/backend",
        "--add-data", "$(Join-Path $repoRoot "app/backend/favorites.sqlite");app/backend",
        "--add-data", "$(Join-Path $repoRoot "app/backend/practice.sqlite");app/backend",
        "--add-data", "$(Join-Path $repoRoot "app/backend/stocks.duckdb");app/backend",
        "--add-data", "$(Join-Path $repoRoot "fixtures");fixtures",
        "app/desktop/launcher.py"
    )

    Write-Host "Running PyInstaller..."
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $pyStdout = Join-Path $releaseDir "logs\pyinstaller_${timestamp}.out.log"
    $pyStderr = Join-Path $releaseDir "logs\pyinstaller_${timestamp}.err.log"
    $launcherPath = Join-Path $repoRoot "app/desktop/launcher.py"
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

    $onedir = $releasePackage
    Write-Host "Release package exists: $(Test-Path $onedir)"
    if (-not (Test-Path $onedir)) {
        throw "Build failed: release/MeeMeeScreener not found."
    }

    # Copy README.txt to the release package
    $readmeSrc = Join-Path $repoRoot "resources\README.txt"
    if (Test-Path $readmeSrc) {
        Write-Host "Copying README.txt to release package..."
        Copy-Item -Path $readmeSrc -Destination (Join-Path $onedir "README.txt") -Force
    } else {
        Write-Host "Warning: README.txt not found at $readmeSrc"
    }

    write-Host "Copying bootstrap scripts to release package..."
    $bootstrapPs1 = Join-Path $repoRoot "tools\portable_bootstrap.ps1"
    $bootstrapCmd = Join-Path $repoRoot "tools\portable_bootstrap.cmd"
    
    Copy-Item -Path $bootstrapPs1 -Destination (Join-Path $onedir "portable_bootstrap.ps1") -Force
    Copy-Item -Path $bootstrapCmd -Destination (Join-Path $onedir "portable_bootstrap.cmd") -Force

    # Also place export_pan.vbs at the app root for compatibility with environments
    # where the app resolves the VBS path relative to the executable directory.
    $exportVbsSrc = Join-Path $repoRoot "tools\export_pan.vbs"
    if (Test-Path $exportVbsSrc) {
        Copy-Item -Path $exportVbsSrc -Destination (Join-Path $onedir "export_pan.vbs") -Force
    }

    Write-Host "Creating portable zip..."
    $zipPath = $releaseZip
    $zipAttempts = 5
    $zipDelaySeconds = 2
    $zipSuccess = $false

    function New-PortableZip([string]$SourceDir, [string]$DestinationZip) {
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
                    $full = $file.FullName
                    $relative = $full.Substring($root.Length).TrimStart('\')
                    $entryName = $relative -replace '\\', '/'
                    $entry = $archive.CreateEntry($entryName, [System.IO.Compression.CompressionLevel]::Optimal)

                    $entryStream = $entry.Open()
                    try {
                        # Compress-Archive (ZipArchiveHelper) can fail when other processes hold a read handle
                        # because it opens with restrictive sharing. Use ReadWrite|Delete sharing for robustness.
                        $fs = New-Object System.IO.FileStream(
                            $full,
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

    for ($i = 1; $i -le $zipAttempts; $i++) {
        try {
            # Create ZIP with the contents of MeeMeeScreener folder (not the folder itself)
            # This allows users to extract and run directly
            New-PortableZip -SourceDir $onedir -DestinationZip $zipPath
            $zipSuccess = $true
            break
        } catch {
            Write-Host "Zip failed (attempt $i/$zipAttempts): $($_.Exception.Message)"
            Write-Host "Retrying in $zipDelaySeconds sec..."
            Start-Sleep -Seconds $zipDelaySeconds
        }
    }
    if (-not $zipSuccess) {
        throw "Failed to create portable zip. Files under release/MeeMeeScreener are locked. Close Explorer or antivirus scan and retry."
    }

    Write-Host "Running portable zip gate..."
    $verifyScript = Join-Path $repoRoot "scripts\verify_portable_zip.py"
    $verifyProc = Start-Process -FilePath "python" -ArgumentList @($verifyScript, $zipPath) -NoNewWindow -Wait -PassThru
    if ($verifyProc.ExitCode -ne 0) {
        throw "Portable zip gate failed."
    }

    Write-Host "Done."
    Write-Host ""
    Write-Host "Portable package created: $zipPath"
    Write-Host "Users can extract this ZIP and run MeeMeeScreener.exe directly."
    Write-Host ""
    Write-Host "To enable portable mode (data stored in same folder):"
    Write-Host "  1. Extract the ZIP"
    Write-Host "  2. Create a file named 'portable.flag' in the same folder as MeeMeeScreener.exe"
    Write-Host "  3. Run MeeMeeScreener.exe"
} finally {
    if ($LogPath) {
        Stop-Transcript | Out-Null
    }
}
