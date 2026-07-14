@echo off
setlocal enabledelayedexpansion

:: Set the Anaconda path
set "ANACONDA_PATH=C:\Users\Turandot\anaconda3"

:: PerseveraTools project root (must be installed in the active conda env)
set "PROJECT_DIR=G:\Drives compartilhados\INVESTIMENTOS\Quant\PerseveraTools"

:: Script path (quoted to avoid cmd parsing issues)
set "PIPELINE_SCRIPT=examples\run_factor_zoo_independent_parallel.py"

:: Activate Anaconda environment
echo Activating Anaconda environment...
call "%ANACONDA_PATH%\Scripts\activate.bat"
if errorlevel 1 (
    echo Failed to activate Anaconda environment.
    pause
    exit /b 1
)

cd /d "%PROJECT_DIR%"
if errorlevel 1 (
    echo Failed to change directory to %PROJECT_DIR%
    pause
    exit /b 1
)

if not exist "%PIPELINE_SCRIPT%" (
    echo Script not found: %PROJECT_DIR%\%PIPELINE_SCRIPT%
    pause
    exit /b 1
)

:: Derived factors - independents in parallel, then dependents sequentially
echo.
echo Running: python "%PIPELINE_SCRIPT%"
python "%PIPELINE_SCRIPT%"
if errorlevel 1 goto :failed

echo.
echo Independent + dependent factor_zoo pipeline completed successfully.
pause
exit /b 0

:failed
echo Error occurred while running: python "%PIPELINE_SCRIPT%"
pause
exit /b 1
