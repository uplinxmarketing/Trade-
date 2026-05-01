@echo off
:: TradeBot AI — Launch Script (Windows)
:: Double-click this file to start the app.

title TradeBot AI

echo.
echo   =================================
echo       TradeBot AI v2.1.0
echo   =================================
echo.

:: Check Node is installed
where node >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo   ERROR: Node.js is not installed.
    echo   Download it from https://nodejs.org ^(v18 or higher^)
    echo.
    pause
    exit /b 1
)

for /f "tokens=*" %%v in ('node -e "process.stdout.write(process.versions.node)"') do set NODE_VER=%%v
echo   Node.js %NODE_VER% found

:: Install dependencies if missing
if not exist "node_modules\" (
    echo   Installing dependencies ^(first run only^)...
    call npm install
    if %ERRORLEVEL% NEQ 0 (
        echo   ERROR: npm install failed. Check your internet connection.
        pause
        exit /b 1
    )
    echo   Dependencies installed.
) else (
    echo   Dependencies ready.
)

echo.
echo   Starting app — browser will open automatically...
echo   Press Ctrl+C or close this window to stop.
echo.

:: Change to project directory (in case script was run from elsewhere)
cd /d "%~dp0"
call npm run dev -- --open

pause
