@echo off
REM ============================================================================
REM T2DPulse Cleanup Task Registration Script
REM ============================================================================
REM This script registers a Windows Task Scheduler task that runs daily at 2 AM
REM to clean up old permalinks and timestamped archive files.
REM
REM REQUIREMENTS: Run as Administrator
REM ============================================================================

echo.
echo ============================================================
echo T2DPulse Cleanup Task Registration
echo ============================================================
echo.

REM Check for admin privileges
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo ERROR: This script requires Administrator privileges.
    echo Please right-click and select "Run as administrator".
    echo.
    pause
    exit /b 1
)

REM Get the directory where this script is located
set "SCRIPT_DIR=%~dp0"
set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"
set "REPO_ROOT=%SCRIPT_DIR%\.."

REM Paths
set "PS_SCRIPT=%SCRIPT_DIR%\cleanup-old-files.ps1"
set "TASK_NAME=T2DPulse-Cleanup"

echo Repository root: %REPO_ROOT%
echo PowerShell script: %PS_SCRIPT%
echo Task name: %TASK_NAME%
echo.

REM Check if the PowerShell script exists
if not exist "%PS_SCRIPT%" (
    echo ERROR: PowerShell script not found: %PS_SCRIPT%
    echo.
    pause
    exit /b 1
)

REM Delete existing task if it exists
echo Checking for existing task...
schtasks /Query /TN "%TASK_NAME%" >nul 2>&1
if %errorLevel% equ 0 (
    echo Removing existing task...
    schtasks /Delete /TN "%TASK_NAME%" /F
    if %errorLevel% neq 0 (
        echo ERROR: Failed to remove existing task.
        pause
        exit /b 1
    )
    echo Existing task removed.
)

REM Create the scheduled task
echo.
echo Creating scheduled task "%TASK_NAME%"...
echo   - Runs daily at 2:00 AM
echo   - Starts when available (catches up if missed)
echo.

schtasks /Create /TN "%TASK_NAME%" /TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -File \"%PS_SCRIPT%\"" /SC DAILY /ST 02:00 /RL LIMITED /F

if %errorLevel% neq 0 (
    echo.
    echo ERROR: Failed to create scheduled task.
    echo Try importing the XML file manually via Task Scheduler.
    echo.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo SUCCESS: Task "%TASK_NAME%" has been registered!
echo ============================================================
echo.
echo The cleanup script will run automatically every day at 2:00 AM.
echo.
echo To run it manually now, use:
echo   schtasks /Run /TN "%TASK_NAME%"
echo.
echo To view the task in Task Scheduler:
echo   taskschd.msc
echo.
echo To unregister the task later:
echo   schtasks /Delete /TN "%TASK_NAME%" /F
echo.
pause
exit /b 0
