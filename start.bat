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

:: ── Locate Node.js ───────────────────────────────────────────────────────────
call :logline "Searching for Node.js..."
set "NODEPATH="

:: 1. Already on PATH?
where node >nul 2>nul
if %ERRORLEVEL% EQU 0 (
    set "NODEPATH=_in_path_"
    goto :node_found
)

:: 2. C:\Program Files\nodejs  (most common installer default)
if exist "%ProgramFiles%\nodejs\node.exe" (
    set "NODEPATH=%ProgramFiles%\nodejs"
    goto :add_node_to_path
)

:: 3. C:\Program Files (x86)\nodejs
if exist "%ProgramFiles(x86)%\nodejs\node.exe" (
    set "NODEPATH=%ProgramFiles(x86)%\nodejs"
    goto :add_node_to_path
)

:: 4. %LOCALAPPDATA%\Programs\nodejs  (user-level install)
if exist "%LOCALAPPDATA%\Programs\nodejs\node.exe" (
    set "NODEPATH=%LOCALAPPDATA%\Programs\nodejs"
    goto :add_node_to_path
)

:: 5. NVM_SYMLINK env var (nvm-windows sets this)
if defined NVM_SYMLINK (
    if exist "%NVM_SYMLINK%\node.exe" (
        set "NODEPATH=%NVM_SYMLINK%"
        goto :add_node_to_path
    )
)

:: 6. NVM_HOME — scan versioned sub-folders, pick highest
if defined NVM_HOME (
    for /d %%V in ("%NVM_HOME%\v*") do (
        if exist "%%V\node.exe" (
            set "NODEPATH=%%V"
        )
    )
    if defined NODEPATH goto :add_node_to_path
)

:: 7. Hardcoded fallback for common non-standard locations
if exist "C:\nodejs\node.exe"            set "NODEPATH=C:\nodejs"
if exist "C:\tools\nodejs\node.exe"      set "NODEPATH=C:\tools\nodejs"
if defined NODEPATH goto :add_node_to_path

:: ── Not found anywhere ───────────────────────────────────────────────────────
call :logline "ERROR: Node.js not found."
echo.
echo   Node.js was not found on this computer.
echo.
echo   OPTION 1 (Install):
echo     Download Node.js LTS from https://nodejs.org
echo     Install it, restart your PC, then run start.bat again.
echo.
echo   OPTION 2 (Already installed but still failing):
echo     Open the Start Menu, search for "Edit the system environment variables"
echo     Under "System Variables" find "Path" and add the folder containing node.exe
echo     e.g.  C:\Program Files\nodejs
echo     Then restart your PC and run start.bat again.
echo.
echo   Log saved to: %LOG_FILE%
echo.
pause
exit /b 1

:add_node_to_path
call :logline "Found Node.js at: %NODEPATH%"
set "PATH=%NODEPATH%;%PATH%"

:node_found
for /f "tokens=*" %%v in ('node -e "process.stdout.write(process.versions.node)"') do set NODE_VER=%%v
call :logline "Node.js v%NODE_VER% OK"

:: ── Version check (v18 minimum, v25 is fine) ─────────────────────────────────
for /f "tokens=1 delims=." %%m in ("%NODE_VER%") do set MAJOR=%%m
if %MAJOR% LSS 18 (
    call :logline "ERROR: Node.js v%NODE_VER% is too old. Need v18 or higher."
    echo.
    echo   Node.js v%NODE_VER% is too old. Please install v18 or higher from https://nodejs.org
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
call :logline "npm v%NPM_VER% OK"

:: ── Install dependencies ─────────────────────────────────────────────────────
if not exist "node_modules\" (
    call :logline "Installing dependencies (first run only)..."
    echo   Installing dependencies, please wait...
    npm install >> "%LOG_FILE%" 2>&1
    if %ERRORLEVEL% NEQ 0 (
        call :logline "ERROR: npm install failed. Exit code: %ERRORLEVEL%"
        echo.
        echo   Dependency install failed. Check the log:
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
    echo   The app crashed. See log for details:
    echo   %LOG_FILE%
    echo.
    pause & exit /b 1
)

goto :eof

:: ── Helper: timestamped log line ─────────────────────────────────────────────
:logline
set MSG=%~1
echo [%TIME:~0,8%] %MSG%
echo [%TIME:~0,8%] %MSG% >> "%LOG_FILE%"
goto :eof
