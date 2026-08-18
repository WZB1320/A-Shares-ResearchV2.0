@echo off
cd /d "%~dp0"

set "PY=C:\Users\zongb\AppData\Local\Programs\Python\Python313\python.exe"
if not exist "%PY%" set "PY=python"

echo ============================================
echo  A-Shares Research Platform (FastAPI)
echo  URL: http://127.0.0.1:8080
echo  Ctrl+C to stop
echo ============================================
echo.

start "" /min cmd /c "ping -n 4 127.0.0.1 >nul & start http://127.0.0.1:8080"

"%PY%" app.py
