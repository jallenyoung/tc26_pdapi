@echo off
cd /d "%~dp0"

python download_activity_logs.py ^
    --scope tenant ^
    --event-types all ^
    --output-dir activity_logs ^
    --log-dir run-logs

if %ERRORLEVEL% neq 0 (
    echo Script failed with exit code %ERRORLEVEL%
    exit /b %ERRORLEVEL%
)
