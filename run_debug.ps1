# MeeMee Screener - Debug Mode Launcher
# This script launches the app with debug mode enabled
# Features:
# - F5 to reload the page
# - Right-click > Inspect to open developer tools
# - Ctrl+Shift+I to open developer tools

$env:DEBUG = "1"

Write-Host "Starting MeeMee Screener in DEBUG mode..." -ForegroundColor Green
Write-Host "Features enabled:" -ForegroundColor Cyan
Write-Host "  - F5 to reload" -ForegroundColor Yellow
Write-Host "  - Right-click > Inspect for dev tools" -ForegroundColor Yellow
Write-Host "  - Ctrl+Shift+I for dev tools" -ForegroundColor Yellow
Write-Host ""

python -m app.desktop.launcher
