@echo off
cd /d "%~dp0"
if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" gui.py
) else if exist "trail\Scripts\python.exe" (
  "trail\Scripts\python.exe" gui.py
) else (
  python gui.py
)
if errorlevel 1 pause
