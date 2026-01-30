@echo off
setlocal

echo [compat] build_release.cmd is a wrapper.
echo [compat] Use tools\build_release.cmd as the primary entrypoint.
echo.

set "TARGET=%~dp0tools\build_release.cmd"
if /I "%~f0"=="%TARGET%" (
  echo ERROR: wrapper target resolves to itself. Aborting.
  exit /b 1
)
if not exist "%TARGET%" (
  echo ERROR: tools\build_release.cmd not found.
  exit /b 1
)

call "%TARGET%" %*
exit /b %errorlevel%
