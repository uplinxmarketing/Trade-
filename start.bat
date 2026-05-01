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

echo.
echo   ============================================
echo       TradeBot AI v2.1.0
echo   ============================================
echo.
call :logline "Log: %LOG_FILE%"
echo.

:: ── Locate Node.js (check PATH then common install locations) ────────────────
call :logline "Searching for Node.js..."
set NODE_EXE=

:: 1. Try PATH first
where node >nul 2>nul
if %ERRORLEVEL% EQU 0 (
    set NODE_EXE=node
    goto :node_found
)

:: 2. Scan common install directories
for %%D in (
    "%ProgramFiles%\nodejs"
    "%ProgramFiles(x86)%\nodejs"
    "%LOCALAPPDATA%\Programs\nodejs"
    "%APPDATA%\nvm\current"
    "%NVM_HOME%"
    "%NVM_SYMLINK%"
    "C:\nodejs"
    "C:\tools\nodejs"
) do (
    if exist "%%~D\node.exe" (
        call :logline "Found Node.js at: %%~D"
        set "PATH=%%~D;%PATH%"
        set NODE_EXE=node
        goto :node_found
    )
)

:: 3. Check nvm-managed versions
if defined NVM_HOME (
    for /d %%V in ("%NVM_HOME%\v*") do (
        if exist "%%V\node.exe" (
            call :logline "Found Node.js (nvm) at: %%V"
            set "PATH=%%V;%PATH%"
            set NODE_EXE=node
            goto :node_found
        )
    )
)

:: Not found anywhere
call :logline "ERROR: Node.js not found in PATH or common locations."
echo.
echo   Node.js is required but could not be found.
echo.
echo   Option 1: Install Node.js from https://nodejs.org  (v18+, LTS recommended)
echo             After installing, restart your computer then run start.bat again.
echo.
echo   Option 2: If already installed, open a NEW Command Prompt and run:
echo             node --version
echo             If that works, close and re-run start.bat from that prompt.
echo.
echo   Log saved to: %LOG_FILE%
echo.
pause
exit /b 1

:node_found
for /f "tokens=*" %%v in ('node -e "process.stdout.write(process.versions.node)"') do set NODE_VER=%%v
call :logline "Node.js %NODE_VER% found"

:: ── Check minimum Node version ───────────────────────────────────────────────
for /f "tokens=1 delims=." %%m in ("%NODE_VER%") do set MAJOR=%%m
if %MAJOR% LSS 18 (
    call :logline "ERROR: Node.js v%NODE_VER% is too old. Need v18+."
    echo.
    echo   Node.js v%NODE_VER% is installed but v18 or higher is required.
    echo   Download the latest LTS from https://nodejs.org
    echo.
    pause & exit /b 1
)

:: ── Check npm ────────────────────────────────────────────────────────────────
where npm >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    call :logline "ERROR: npm not found."
    echo   npm was not found. Reinstall Node.js from https://nodejs.org
    pause & exit /b 1
)
for /f "tokens=*" %%v in ('npm --version') do set NPM_VER=%%v
call :logline "npm %NPM_VER% found"

:: ── Install dependencies ─────────────────────────────────────────────────────
if not exist "node_modules\" (
    call :logline "Installing dependencies (first run only)..."
    echo   Installing dependencies, please wait...
    npm install >> "%LOG_FILE%" 2>&1
    if %ERRORLEVEL% NEQ 0 (
        call :logline "ERROR: npm install failed. Exit code: %ERRORLEVEL%"
        echo.
        echo   Dependency installation failed. Check the log:
        echo   %LOG_FILE%
        echo.
        pause & exit /b 1
    )
    call :logline "Dependencies installed OK"
) else (
    call :logline "Dependencies ready"
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
    echo   The app crashed unexpectedly. See log for details:
    echo   %LOG_FILE%
    echo.
    pause & exit /b 1
)

goto :eof

:: ── Helper: timestamped log line ────────────────────────────────────────────
:logline
set MSG=%~1
echo [%TIME:~0,8%] %MSG%
echo [%TIME:~0,8%] %MSG% >> "%LOG_FILE%"
goto :eof
