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
echo TradeBot AI — Session %TIMESTAMP% > "%LOG_FILE%"

echo.
echo   ============================================
echo       TradeBot AI v2.1.0
echo   ============================================
echo.
call :logline "Log file: %LOG_FILE%"
echo.

:: ── Locate Node.js ───────────────────────────────────────────────────────────
call :logline "Searching for Node.js..."
set "NODEPATH="

where node >nul 2>nul
if %ERRORLEVEL% EQU 0 ( set "NODEPATH=_in_path_" & goto :node_found )

if exist "%ProgramFiles%\nodejs\node.exe"          set "NODEPATH=%ProgramFiles%\nodejs"
if not defined NODEPATH if exist "%ProgramFiles(x86)%\nodejs\node.exe" set "NODEPATH=%ProgramFiles(x86)%\nodejs"
if not defined NODEPATH if exist "%LOCALAPPDATA%\Programs\nodejs\node.exe" set "NODEPATH=%LOCALAPPDATA%\Programs\nodejs"
if defined NODEPATH goto :add_node_to_path

if defined NVM_SYMLINK (
    if exist "%NVM_SYMLINK%\node.exe" ( set "NODEPATH=%NVM_SYMLINK%" & goto :add_node_to_path )
)
if defined NVM_HOME (
    for /d %%V in ("%NVM_HOME%\v*") do ( if exist "%%V\node.exe" set "NODEPATH=%%V" )
    if defined NODEPATH goto :add_node_to_path
)
if exist "C:\nodejs\node.exe"        set "NODEPATH=C:\nodejs"
if exist "C:\tools\nodejs\node.exe"  set "NODEPATH=C:\tools\nodejs"
if defined NODEPATH goto :add_node_to_path

call :logline "ERROR: Node.js not found."
echo.
echo   Node.js was not found on this computer.
echo.
echo   OPTION 1 - Install Node.js:
echo     Download from https://nodejs.org  (LTS version recommended)
echo     Install it, RESTART your PC, then run start.bat again.
echo.
echo   OPTION 2 - Already installed but still failing:
echo     Open Start Menu - search "Edit the system environment variables"
echo     Under System Variables, find Path and add:
echo       C:\Program Files\nodejs
echo     Click OK, RESTART your PC, run start.bat again.
echo.
echo   Log: %LOG_FILE%
echo.
pause & exit /b 1

:add_node_to_path
call :logline "Found Node.js at: %NODEPATH%"
set "PATH=%NODEPATH%;%PATH%"

:node_found
for /f "tokens=*" %%v in ('node -e "process.stdout.write(process.versions.node)"') do set NODE_VER=%%v
call :logline "Node.js v%NODE_VER% OK"

for /f "tokens=1 delims=." %%m in ("%NODE_VER%") do set MAJOR=%%m
if %MAJOR% LSS 18 (
    call :logline "ERROR: Node.js v%NODE_VER% too old — need v18+."
    echo   Node.js v%NODE_VER% is too old. Download v18+ from https://nodejs.org
    pause & exit /b 1
)

where npm >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    call :logline "ERROR: npm not found — reinstall Node.js."
    pause & exit /b 1
)
for /f "tokens=*" %%v in ('npm --version') do set NPM_VER=%%v
call :logline "npm v%NPM_VER% OK"

:: ── Smart dependency check ───────────────────────────────────────────────────
:: Only (re)install when package.json has changed since last install.
:: We store the package.json timestamp in node_modules\.install-marker.
set "MARKER=node_modules\.install-marker"
set "NEED_INSTALL=0"

if not exist "node_modules\" (
    set "NEED_INSTALL=1"
    call :logline "node_modules not found — installing..."
) else if not exist "%MARKER%" (
    set "NEED_INSTALL=1"
    call :logline "Install marker missing — reinstalling..."
) else (
    :: Compare package.json modified time to stored marker timestamp
    for %%F in ("package.json") do set "PKG_TIME=%%~tF"
    set /p MARKER_TIME= < "%MARKER%"
    if "!PKG_TIME!" NEQ "!MARKER_TIME!" (
        set "NEED_INSTALL=1"
        call :logline "package.json changed — updating dependencies..."
    ) else (
        call :logline "Dependencies up to date — skipping install."
    )
)

if "!NEED_INSTALL!" == "1" (
    echo.
    echo   --- Installing dependencies ------------------------------------
    echo   All output below is live. Errors will show here directly.
    echo   ----------------------------------------------------------------
    echo.
    call :logline "Running: npm install"

    :: Run npm install directly — output goes straight to terminal window
    call npm install
    set NPM_EXIT=!ERRORLEVEL!

    if !NPM_EXIT! NEQ 0 (
        call :logline "ERROR: npm install failed. Exit code: !NPM_EXIT!"
        echo.
        echo   ════════════════════════════════════════════════════
        echo   Install FAILED  ^(exit code !NPM_EXIT!^)
        echo   Scroll up to read the error messages above.
        echo   Log: %LOG_FILE%
        echo   ════════════════════════════════════════════════════
        echo.
        pause & exit /b 1
    )
    if not exist "node_modules\vite" (
        call :logline "ERROR: install exited OK but vite package is missing."
        echo.
        echo   Install appeared to finish but is incomplete.
        echo   Delete the node_modules folder and run start.bat again.
        echo.
        pause & exit /b 1
    )

    :: Save package.json timestamp — skips install on every future launch
    for %%F in ("package.json") do echo %%~tF > "%MARKER%"
    call :logline "Dependencies installed OK — marker saved."
    echo.
)

:: ── Start dev server ─────────────────────────────────────────────────────────
echo.
call :logline "Starting TradeBot AI on http://localhost:8080 ..."
echo   Browser will open automatically.
echo   Keep this window open while using the app.
echo   Press Ctrl+C or close this window to stop.
echo.
echo   --- Server output -----------------------------------------------
echo.

:: Run dev server directly — output prints to terminal window
call npm run dev -- --open
set DEV_EXIT=!ERRORLEVEL!

echo.
if !DEV_EXIT! NEQ 0 (
    call :logline "ERROR: Server stopped unexpectedly. Exit code: !DEV_EXIT!"
    echo   ════════════════════════════════════════════════════
    echo   Server stopped with an error  ^(exit code !DEV_EXIT!^)
    echo   Scroll up to read the error, or open:
    echo   %LOG_FILE%
    echo   ════════════════════════════════════════════════════
) else (
    call :logline "Server stopped cleanly."
)
pause
goto :eof

:: ── Helper ───────────────────────────────────────────────────────────────────
:logline
echo [%TIME:~0,8%] %~1
echo [%TIME:~0,8%] %~1 >> "%LOG_FILE%"
goto :eof
