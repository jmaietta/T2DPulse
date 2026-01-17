@echo off
setlocal EnableExtensions

REM =============================================================
REM T2DPulse: Register a Windows Scheduled Task to run cleanup-old-files.ps1
REM
REM Usage:
REM   register-cleanup-task.bat
REM   register-cleanup-task.bat uninstall
REM
REM Notes:
REM   - Run this from anywhere; it finds the repo root based on this script's location.
REM   - Task runs daily at 2:00 AM *when you are logged in* (InteractiveToken).
REM =============================================================

set "TASK_NAME=T2DPulse-Cleanup"

if /I "%~1"=="uninstall" goto :uninstall

REM Script directory (this .bat lives in repo_root\scripts\)
set "SCRIPT_DIR=%~dp0"

REM Repo root is parent of scripts\
for %%I in ("%SCRIPT_DIR%..") do set "REPO_ROOT=%%~fI"

set "XML_TEMPLATE=%SCRIPT_DIR%T2DPulse-Cleanup-Task.xml"

if not exist "%XML_TEMPLATE%" (
  echo ERROR: Missing task template: "%XML_TEMPLATE%"
  echo Make sure T2DPulse-Cleanup-Task.xml is in the same folder as this .bat.
  exit /b 1
)

set "TEMP_XML=%TEMP%\T2DPulse-Cleanup-Task.generated.xml"

echo Repo root: "%REPO_ROOT%"

echo Generating task XML...

powershell.exe -NoProfile -ExecutionPolicy Bypass -Command ^
  "$repoRoot = '%REPO_ROOT%';" ^
  "$templatePath = '%XML_TEMPLATE%';" ^
  "$outPath = '%TEMP_XML%';" ^
  "$xml = Get-Content -LiteralPath $templatePath -Raw;" ^
  "$xml = $xml.Replace('__REPO_ROOT__', $repoRoot);" ^
  "Set-Content -LiteralPath $outPath -Value $xml -Encoding Unicode;"

if errorlevel 1 (
  echo ERROR: Failed generating task XML.
  exit /b 1
)

echo Creating or updating Scheduled Task "%TASK_NAME%"...

schtasks /Create /TN "%TASK_NAME%" /XML "%TEMP_XML%" /F
if errorlevel 1 (
  echo ERROR: Failed to create the Scheduled Task.
  echo If you see an access denied error, try re-running this .bat as Administrator.
  exit /b 1
)

echo.
echo Success! Scheduled Task created: "%TASK_NAME%"
echo To run it immediately:
echo   schtasks /Run /TN "%TASK_NAME%"
echo To uninstall:
echo   %~nx0 uninstall
exit /b 0

:uninstall
schtasks /Delete /TN "%TASK_NAME%" /F
if errorlevel 1 (
  echo ERROR: Failed to delete Scheduled Task "%TASK_NAME%".
  exit /b 1
)

echo Scheduled Task deleted: "%TASK_NAME%"
exit /b 0
