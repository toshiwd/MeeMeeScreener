param(
    [switch]$Clean
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$frontendDir = Join-Path $repoRoot "app/frontend"
$backendStatic = Join-Path $repoRoot "app/backend/static"
$releaseDir = Join-Path $repoRoot "release"
$iconPath = Join-Path $repoRoot "resources/icons/app_icon.ico"
$artifactsDir = Join-Path $repoRoot "build/onedir_artifacts"

if (-not (Test-Path $artifactsDir)) {
    New-Item -ItemType Directory -Force $artifactsDir | Out-Null
}

function Stop-LockProcesses {
    Stop-Process -Name node -Force -ErrorAction SilentlyContinue
    Stop-Process -Name MeeMeeScreener -Force -ErrorAction SilentlyContinue
    Stop-Process -Name msedgewebview2 -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1
}

function Save-NpmTail($stdoutPath, $stderrPath) {
    $tailPath = Join-Path $artifactsDir "npm_ci_tail.txt"
    $lines = @()
    if (Test-Path $stdoutPath) { $lines += Get-Content -Path $stdoutPath -Tail 50 }
    if (Test-Path $stderrPath) { $lines += Get-Content -Path $stderrPath -Tail 50 }
    if ($lines.Count -gt 0) {
        $lines | Set-Content -Path $tailPath -Encoding UTF8
    }
}

function Save-NpmBuildTail($stdoutPath, $stderrPath) {
    $tailPath = Join-Path $artifactsDir "npm_build_tail.txt"
    $lines = @()
    if (Test-Path $stdoutPath) { $lines += Get-Content -Path $stdoutPath -Tail 50 }
    if (Test-Path $stderrPath) { $lines += Get-Content -Path $stderrPath -Tail 50 }
    if ($lines.Count -gt 0) {
        $lines | Set-Content -Path $tailPath -Encoding UTF8
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

        Save-NpmTail $npmStdout $npmStderr
        $errorText = ""
        if (Test-Path $npmStdout) { $errorText += (Get-Content -Path $npmStdout -Raw) }
        if (Test-Path $npmStderr) { $errorText += (Get-Content -Path $npmStderr -Raw) }
        $shouldRetry = $errorText -match "EPERM" -or $errorText -match "-4048" -or $errorText -match "EACCES"
        if (-not $shouldRetry -or $attempt -eq $maxAttempts) {
            Write-Host "npm ci failed after $attempt attempt(s)."
            Write-Host "Possible cause: Windows Defender/AV file lock."
            Write-Host "Please exclude: $frontendDir\\node_modules"
            Write-Host "Also close VS Code / Terminals / Vite / MeeMeeScreener."
            Write-Host "If handle.exe is available: handle.exe rollup.win32-x64-msvc.node"
            throw "npm ci failed with exit code $($proc.ExitCode)"
        }

        Write-Host "npm ci failed due to EPERM/EACCES. Retrying ($attempt/$maxAttempts)..."
        Remove-Item -Recurse -Force (Join-Path $frontendDir "node_modules") -ErrorAction SilentlyContinue
        Remove-Item -Recurse -Force (Join-Path $frontendDir ".vite") -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 2
        Stop-Process -Name node -Force -ErrorAction SilentlyContinue
        $attempt++
    }
}

if (-not (Test-Path $iconPath)) {
    throw "Missing icon: $iconPath`nPlace app_icon.ico under resources/icons before building."
}

Write-Host "Starting build_onedir.ps1"
$running = Get-Process -Name "MeeMeeScreener" -ErrorAction SilentlyContinue
if ($running) {
    Write-Host "Closing MeeMeeScreener.exe..."
    $running | Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1
}
Stop-LockProcesses

Write-Host "Building frontend..."
Push-Location $frontendDir
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
    Save-NpmBuildTail $buildStdout $buildStderr
    throw "npm run build failed with exit code $($buildProc.ExitCode)"
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

Write-Host "Building PyInstaller onedir package..."
$buildWork = Join-Path $repoRoot "build/pyinstaller_onedir"
if (Test-Path $buildWork) {
    try {
        Remove-Item -Recurse -Force $buildWork
    } catch {
        $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
        $buildWork = Join-Path $repoRoot "build/pyinstaller_onedir_$timestamp"
    }
}
New-Item -ItemType Directory -Force $buildWork | Out-Null

$pyInstallerArgs = @(
    "--noconfirm"
)
if ($Clean) {
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
    "--hidden-import", "win32timezone",
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
$pyArgs = @("-m", "PyInstaller") + $pyInstallerArgs
$pyProc = Start-Process -FilePath "python" -ArgumentList $pyArgs -WorkingDirectory $repoRoot -NoNewWindow -Wait -PassThru
Write-Host "PyInstaller finished with exit code $($pyProc.ExitCode)"
if ($pyProc.ExitCode -ne 0) {
    throw "PyInstaller failed with exit code $($pyProc.ExitCode)"
}

if (-not (Test-Path $releasePackage)) {
    throw "Build failed: release/MeeMeeScreener not found."
}

Write-Host "Onedir package created: $releasePackage"
