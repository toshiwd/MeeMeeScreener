@echo off
setlocal EnableExtensions

set "ROOT=%~dp0"

REM ============================================================
REM start-admin.bat - admin mode
REM ============================================================

call :require_cmd python "Python"
if errorlevel 1 exit /b 1

call :require_cmd npm "Node.js (npm)"
if errorlevel 1 exit /b 1

if not exist "%ROOT%scripts\start-backend.ps1" (
  echo [ERROR] Missing scripts\start-backend.ps1
  pause
  exit /b 1
)
if not exist "%ROOT%scripts\start-frontend.ps1" (
  echo [ERROR] Missing scripts\start-frontend.ps1
  pause
  exit /b 1
)

echo [INFO] Starting backend (admin)...
start "Backend (Admin)" powershell -NoProfile -ExecutionPolicy Bypass -File "%ROOT%scripts\start-backend.ps1" -Mode admin
if errorlevel 1 (
  echo [WARN] Failed to launch backend window.
  pause
  call :clear_errorlevel
)

echo [INFO] Starting frontend (admin)...
start "Frontend (Admin)" powershell -NoProfile -ExecutionPolicy Bypass -File "%ROOT%scripts\start-frontend.ps1" -Mode admin
if errorlevel 1 (
  echo [WARN] Failed to launch frontend window.
  pause
  call :clear_errorlevel
)

echo [INFO] Opening browser: http://localhost:5173
start "Browser" powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Sleep -Seconds 10; Start-Process -FilePath 'msedge' -ArgumentList '--app=http://localhost:5173'"
if errorlevel 1 (
  echo [WARN] Failed to launch browser.
  pause
  call :clear_errorlevel
)

exit /b 0

:require_cmd
set "CMD_NAME=%~1"
set "LABEL=%~2"
where %CMD_NAME% >nul 2>&1
if errorlevel 1 (
  echo [ERROR] %LABEL% not found. Install it and add to PATH.
  pause
  exit /b 1
)
exit /b 0

:clear_errorlevel
ver >nul
exit /b 0
