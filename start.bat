@echo off
:: TradeBot AI — Launch Script (Windows)
:: Double-click to start. Logs saved to logs\tradebot_TIMESTAMP.log

title TradeBot AI
setlocal enabledelayedexpansion
cd /d "%~dp0"

:: ── Log setup ───────────────────────────────────────────────────────────────
if not exist "logs\" mkdir logs
for /f "tokens=2 delims==" %%d in ('wmic os get LocalDateTime /value 2^>nul') do set DT=%%d
set TIMESTAMP=%DT:~0,8%_%DT:~8,6%
set LOG_FILE=logs\tradebot_%TIMESTAMP%.log

echo. > "%LOG_FILE%"
:: Helper: write to log and screen
set LOG_CMD=call :logline

echo.
echo   ============================================
echo       TradeBot AI v2.1.0
echo   ============================================
echo.
call :logline "Log: %LOG_FILE%"
echo.

:: ── Check Node.js ────────────────────────────────────────────────────────────
where node >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    call :logline "ERROR: Node.js not found."
    echo.
    echo   Node.js is required. Download from https://nodejs.org (v18+)
    echo   Log saved to: %LOG_FILE%
    pause & exit /b 1
)

for /f "tokens=*" %%v in ('node -e "process.stdout.write(process.versions.node)"') do set NODE_VER=%%v
call :logline "Node.js %NODE_VER% OK"

:: ── Check minimum Node version ───────────────────────────────────────────────
for /f "tokens=1 delims=." %%m in ("%NODE_VER%") do set MAJOR=%%m
if %MAJOR% LSS 18 (
    call :logline "ERROR: Node.js v%NODE_VER% is too old. Need v18+."
    echo   Please update Node.js from https://nodejs.org
    pause & exit /b 1
)

:: ── Check npm ────────────────────────────────────────────────────────────────
where npm >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    call :logline "ERROR: npm not found."
    pause & exit /b 1
)
for /f "tokens=*" %%v in ('npm --version') do set NPM_VER=%%v
call :logline "npm %NPM_VER% OK"

:: ── Install dependencies ─────────────────────────────────────────────────────
if not exist "node_modules\" (
    call :logline "Installing dependencies (first run only)..."
    npm install >> "%LOG_FILE%" 2>&1
    if %ERRORLEVEL% NEQ 0 (
        call :logline "ERROR: npm install failed."
        echo.
        echo   npm install failed. See log: %LOG_FILE%
        pause & exit /b 1
    )
    call :logline "Dependencies installed OK"
) else (
    call :logline "Dependencies ready OK"
)

:: ── Start dev server ─────────────────────────────────────────────────────────
echo.
call :logline "Starting TradeBot AI on http://localhost:8080 ..."
echo   Browser will open automatically.
echo   Close this window or press Ctrl+C to stop.
echo.

npm run dev -- --open >> "%LOG_FILE%" 2>&1
if %ERRORLEVEL% NEQ 0 (
    call :logline "ERROR: Dev server crashed. Exit code: %ERRORLEVEL%"
    echo.
    echo   The app crashed. See log for details: %LOG_FILE%
    pause & exit /b 1
)

goto :eof

:: ── Helper: write timestamped line to log and screen ────────────────────────
:logline
set MSG=%~1
echo [%TIME:~0,8%] %MSG%
echo [%TIME:~0,8%] %MSG% >> "%LOG_FILE%"
goto :eof
