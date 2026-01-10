@echo off
setlocal

set "SCRIPT=%~dp0export_pan.vbs"
set "CODE_FILE=%~1"
set "OUT_DIR=%~2"
set "CODE_TXT=%LOCALAPPDATA%\MeeMeeScreener\data\code.txt"

if "%CODE_FILE%"=="" set "CODE_FILE=%CODE_TXT%"
if "%OUT_DIR%"=="" set "OUT_DIR=%LOCALAPPDATA%\MeeMeeScreener\data\txt"

if "%LOCALAPPDATA%"=="" (
    set "CODE_TXT=%~dp0code.txt"
    if "%OUT_DIR%"=="" set "OUT_DIR=%~dp0..\data\txt"
)

set "CSCRIPT=%SystemRoot%\SysWOW64\cscript.exe"
if not exist "%CSCRIPT%" set "CSCRIPT=%SystemRoot%\System32\cscript.exe"

for %%I in ("%CODE_FILE%") do set "CODE_EXT=%%~xI"
if /I "%CODE_EXT%"==".ebk" (
    powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0convert_moomoo_ebk.ps1" -InputFile "%CODE_FILE%" -OutputFile "%CODE_TXT%"
    set "CODE_FILE=%CODE_TXT%"
)

"%CSCRIPT%" //nologo "%SCRIPT%" "%CODE_FILE%" "%OUT_DIR%"

endlocal
