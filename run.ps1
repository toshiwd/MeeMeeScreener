# MeeMee Screener - Normal Mode Launcher
# This script launches the app in normal mode (no debug features)

# Ensure DEBUG is not set
$env:DEBUG = "0"

Write-Host "Starting MeeMee Screener..." -ForegroundColor Green

python -m app.desktop.launcher
