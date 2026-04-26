@echo off
REM MillyWeb Content Pipeline Launcher v2
REM Auto-starts at Windows login via Task Scheduler
REM Services: Recorder (8765), Transcriber (8767), Orchestrator (8766)

setlocal
set BASE=C:\Users\rdmil\OneDrive\Documents\Projects\Millyweb\content-pipeline
set SECRETS_SRC=\\100.64.0.2\opt\projects\helix\secrets\content-pipeline.env
set SECRETS_LOCAL=%BASE%\content-pipeline.env

echo [MillyPipeline] Starting at %TIME%...

REM Fetch latest secrets from VPS1 via network share (if available)
REM Otherwise use cached local copy
if exist %SECRETS_SRC% (
    copy /Y %SECRETS_SRC% %SECRETS_LOCAL% >nul 2>&1
    echo [MillyPipeline] Secrets synced from VPS1
) else (
    echo [MillyPipeline] Using cached secrets
)

REM Load secrets into environment
if exist %SECRETS_LOCAL% (
    for /f "usebackq tokens=1,2 delims==" %%a in (%SECRETS_LOCAL%) do (
        set "%%a=%%b"
    )
    echo [MillyPipeline] Secrets loaded
) else (
    echo [MillyPipeline] WARNING: No secrets file found - API calls will fail
)

echo [MillyPipeline] Starting services...

REM Start Recorder Service
start "Milly-Recorder" /min cmd /c "python %BASE%\recorder\main.py >> %BASE%\outputs\recorder.log 2>&1"

REM Start Transcriber Service
start "Milly-Transcriber" /min cmd /c "python %BASE%\transcriber\main.py >> %BASE%\outputs\transcriber.log 2>&1"

REM Start Orchestrator
start "Milly-Orchestrator" /min cmd /c "python %BASE%\orchestrator\main.py >> %BASE%\outputs\orchestrator.log 2>&1"

echo [MillyPipeline] All services started.
echo   Recorder:     http://localhost:8765/health
echo   Transcriber:  http://localhost:8767/health
echo   Orchestrator: http://localhost:8766/health
