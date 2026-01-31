@echo off
setlocal

echo === MeeMee Screener Release Build ===
echo.

rem Check if running from correct directory
if not exist "app\backend\main.py" goto :fail_wrong_dir

rem Step 1: Build Frontend
echo [1/3] Building Frontend...
pushd app\frontend
call npm install
if errorlevel 1 goto :fail_npm_install
call npm run build
if errorlevel 1 goto :fail_frontend_build
popd
echo Frontend build completed.
echo.

rem Step 2: Copy frontend dist to backend static
echo [2/3] Copying frontend to backend static...
if exist "app\backend\static" rmdir /s /q "app\backend\static"
mkdir "app\backend\static"
xcopy /y /s /e /q /i "app\frontend\dist\*" "app\backend\static"
if errorlevel 1 goto :fail_copy_static
echo Static files copied.
echo.

rem Step 3: Build PyInstaller package
echo [3/3] Building PyInstaller package...
pushd build\pyinstaller

rem Create necessary files if they don't exist
if not exist "..\\..\\app\\backend\\update_state.json" echo {} > "..\\..\\app\\backend\\update_state.json"
if not exist "..\\..\\app\\backend\\rank_config.json" echo {} > "..\\..\\app\\backend\\rank_config.json"

rem Ensure industry_master exists in bundled DuckDB (build-time, no runtime dependency)
call python ..\\..\\tools\\setup\\ensure_industry_master.py --db "..\\..\\app\\backend\\stocks.duckdb"
if errorlevel 1 goto :fail_pyinstaller

rem Clean stale PyInstaller artifacts
if exist "build" rmdir /s /q "build"
if exist "dist\\MeeMeeScreener" rmdir /s /q "dist\\MeeMeeScreener"

pyinstaller --clean --noconfirm MeeMeeScreener.spec > pyinstaller_build.log 2>&1
if errorlevel 1 goto :fail_pyinstaller
popd
echo PyInstaller build completed.
echo.

rem Step 4: Copy to release folder
echo [4/4] Preparing release folder...
if not exist "release" mkdir "release"

rem Prevent partial/dirty copies when a previous release is running and files are locked.
tasklist /nh 2>nul | findstr /i /c:"MeeMeeScreener.exe" >nul
if not errorlevel 1 goto :fail_app_running

if exist "release\\MeeMeeScreener" rmdir /s /q "release\\MeeMeeScreener"
if exist "release\\MeeMeeScreener" goto :fail_release_locked

xcopy /y /s /e /q /i "build\\pyinstaller\\dist\\MeeMeeScreener\\*" "release\\MeeMeeScreener"
if errorlevel 1 goto :fail_copy_release

rem Copy additional tools
xcopy /y /s /e /q /i "tools\\*.vbs" "release\\MeeMeeScreener\\tools" 2>nul
if errorlevel 1 goto :fail_copy_tools
xcopy /y /s /e /q /i "tools\\code.txt" "release\\MeeMeeScreener\\tools" 2>nul
if errorlevel 1 goto :fail_copy_tools

echo.
echo === Build Complete ===
echo Release package is in: release\\MeeMeeScreener
echo.
exit /b 0

:fail_wrong_dir
echo ERROR: Please run this script from the project root directory
exit /b 1

:fail_npm_install
popd
echo ERROR: npm install failed
exit /b 1

:fail_frontend_build
popd
echo ERROR: Frontend build failed
exit /b 1

:fail_copy_static
echo ERROR: Copying static files failed
exit /b 1

:fail_pyinstaller
echo ERROR: PyInstaller build failed
echo ---- PyInstaller log tail ----
powershell -NoProfile -Command "Get-Content -Path 'pyinstaller_build.log' -Tail 200"
popd
exit /b 1

:fail_app_running
echo ERROR: MeeMeeScreener.exe is running. Please close the app before building.
exit /b 1

:fail_release_locked
echo ERROR: Failed to remove existing release folder (files may be locked).
exit /b 1

:fail_copy_release
echo ERROR: Copy to release folder failed
exit /b 1

:fail_copy_tools
echo ERROR: Copying tools failed
exit /b 1
