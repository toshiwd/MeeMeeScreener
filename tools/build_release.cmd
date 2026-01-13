@echo off
setlocal
set SCRIPT_DIR=%~dp0
set LOG_DIR=%SCRIPT_DIR%..\release\logs
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
for /f "tokens=1-4 delims=/ " %%a in ("%date%") do set TODAY=%%a%%b%%c
for /f "tokens=1-3 delims=:." %%a in ("%time%") do set NOW=%%a%%b%%c
set LOG_FILE=%LOG_DIR%\build_release_%TODAY%_%NOW%.log
echo Closing MeeMeeScreener.exe if running...
taskkill /IM MeeMeeScreener.exe /F >NUL 2>&1
echo Build log: %LOG_FILE%
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%build_release.ps1" -LogPath "%LOG_FILE%"
if errorlevel 1 (
  echo Build failed. See log: %LOG_FILE%
  exit /b 1
)
