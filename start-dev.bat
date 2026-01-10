@echo off
setlocal
set "err=0"

set "ROOT=%~dp0"

start "Backend (Dev)" powershell -NoProfile -ExecutionPolicy Bypass -File "%ROOT%scripts\start-backend.ps1" -Mode dev
if errorlevel 1 set err=1

start "Frontend" powershell -NoProfile -ExecutionPolicy Bypass -Command "& { $root = '%ROOT%'; $frontend = Join-Path $root 'app\\frontend'; Set-Location $frontend; npm run dev }"
if errorlevel 1 set err=1

start "Browser" powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Sleep -Seconds 3; Start-Process -FilePath 'msedge' -ArgumentList '--app=http://localhost:5173'"
if errorlevel 1 set err=1

if %err% equ 1 (
    echo.
    echo An error occurred while starting the development environment.
    echo Please check that Python and Node.js are installed and in your PATH.
    pause
)

endlocal
