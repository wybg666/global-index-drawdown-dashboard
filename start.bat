@echo off
setlocal
cd /d "%~dp0"
if exist ".venv\Scripts\python.exe" (
  set "PYTHON_CMD="%~dp0.venv\Scripts\python.exe""
) else (
  where py >nul 2>nul
  if not errorlevel 1 (set "PYTHON_CMD=py -3") else (set "PYTHON_CMD=python")
)
%PYTHON_CMD% -B -c "import sys; assert sys.version_info >= (3,11); import requests, openpyxl, xlrd; from zoneinfo import ZoneInfo; ZoneInfo('Asia/Shanghai')" >nul 2>nul
if errorlevel 1 (
  echo Python 3.11+ or dependencies missing. See README.md.
  echo Run: python -m pip install -r requirements.txt
  pause
  exit /b 1
)
%PYTHON_CMD% -B run.py --open-browser
pause
