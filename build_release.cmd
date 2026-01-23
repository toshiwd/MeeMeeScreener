@echo off
setlocal enabledelayedexpansion

echo === MeeMee Screener Release Build ===
echo.

:: Check if running from correct directory
if not exist "app\backend\main.py" (
    echo ERROR: Please run this script from the project root directory
    exit /b 1
)

:: Step 1: Build Frontend
echo [1/3] Building Frontend...
cd app\frontend
call npm install
if errorlevel 1 (
    echo ERROR: npm install failed
    cd ..\..
    exit /b 1
)
call npm run build
if errorlevel 1 (
    echo ERROR: Frontend build failed
    cd ..\..
    exit /b 1
)
cd ..\..
echo Frontend build completed.
echo.

:: Step 2: Copy frontend dist to backend static
echo [2/3] Copying frontend to backend static...
if exist "app\backend\static" rmdir /s /q "app\backend\static"
mkdir "app\backend\static"
xcopy /s /e /q "app\frontend\dist\*" "app\backend\static\"
echo Static files copied.
echo.

:: Step 3: Build PyInstaller package
echo [3/3] Building PyInstaller package...
cd build\pyinstaller

:: Create necessary files if they don't exist
if not exist "..\\..\\app\\backend\\update_state.json" (
    echo {} > "..\\..\\app\\backend\\update_state.json"
)
if not exist "..\\..\\app\\backend\\rank_config.json" (
    echo {} > "..\\..\\app\\backend\\rank_config.json"
)

:: Run PyInstaller
pyinstaller --noconfirm MeeMeeScreener.spec
if errorlevel 1 (
    echo ERROR: PyInstaller build failed
    cd ..\..
    exit /b 1
)
cd ..\..
echo PyInstaller build completed.
echo.

:: Step 4: Copy to release folder
echo [4/4] Preparing release folder...
if not exist "release" mkdir "release"
if exist "release\MeeMeeScreener" rmdir /s /q "release\MeeMeeScreener"
xcopy /s /e /q "build\pyinstaller\dist\MeeMeeScreener\*" "release\MeeMeeScreener\"

:: Copy additional tools
xcopy /s /e /q "tools\*.vbs" "release\MeeMeeScreener\tools\" 2>nul
xcopy /s /e /q "tools\code.txt" "release\MeeMeeScreener\tools\" 2>nul

echo.
echo === Build Complete ===
echo Release package is in: release\MeeMeeScreener
echo.
:: pause
