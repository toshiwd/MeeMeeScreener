# MeeMee Screener - Normal Mode Launcher
# This script launches the app in normal mode (no debug features)
# Ensure DEBUG is not set
$env:DEBUG = "0"
if ([string]::IsNullOrWhiteSpace($env:TDNET_MCP_FETCH_COMMAND)) {
    $tdnetFetcher = Join-Path $PSScriptRoot "tools\setup\fetch_tdnet_yanoshin.py"
    $env:TDNET_MCP_FETCH_COMMAND = 'python "' + $tdnetFetcher + '" --code "{code}" --limit {limit}'
}

Write-Host "Starting MeeMee Screener..." -ForegroundColor Green

python -m app.desktop.launcher
