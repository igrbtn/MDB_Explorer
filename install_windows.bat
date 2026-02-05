@echo off
echo ==========================================
echo  Exchange EDB Exporter - Windows Install
echo ==========================================
echo.

:: Check for Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not installed or not in PATH
    echo Please install Python 3.10+ from https://www.python.org/downloads/
    echo Make sure to check "Add Python to PATH" during installation
    pause
    exit /b 1
)

echo [OK] Python found:
python --version
echo.

:: Check for pip
pip --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] pip is not installed
    echo Installing pip...
    python -m ensurepip --upgrade
)

echo [OK] pip found
echo.

:: Upgrade pip
echo Upgrading pip...
python -m pip install --upgrade pip
echo.

:: Install dependencies
echo Installing dependencies...
echo.

echo [1/5] Installing PyQt6...
pip install PyQt6>=6.4.0

echo [2/5] Installing libesedb-python...
echo.

:: Get Python version
for /f "tokens=2 delims= " %%a in ('python --version 2^>^&1') do set PYVER=%%a
for /f "tokens=1,2 delims=." %%a in ("%PYVER%") do (
    set PYMAJOR=%%a
    set PYMINOR=%%b
)
echo Detected Python %PYMAJOR%.%PYMINOR%

:: Determine wheel URL based on Python version
set WHEEL_URL=
if "%PYMAJOR%.%PYMINOR%"=="3.8" set WHEEL_URL=https://files.pythonhosted.org/packages/57/a9/dd2a2f5a3ad52de12236bc49bf78162349e65c854cab69f570d28a0eb061/libesedb_python-20240420-cp38-cp38-win_amd64.whl
if "%PYMAJOR%.%PYMINOR%"=="3.9" set WHEEL_URL=https://files.pythonhosted.org/packages/0d/91/e1ec78c214d8d79e9b9ad19979eb116510f295de01398c2a36beda9b6d92/libesedb_python-20240420-cp39-cp39-win_amd64.whl
if "%PYMAJOR%.%PYMINOR%"=="3.10" set WHEEL_URL=https://files.pythonhosted.org/packages/38/3d/daa157ffac8402608921723315c553d3c423fcf8f2ee453155769cfafc21/libesedb_python-20240420-cp310-cp310-win_amd64.whl
if "%PYMAJOR%.%PYMINOR%"=="3.11" set WHEEL_URL=https://files.pythonhosted.org/packages/fc/a6/efa948efafe4e2e738783ae407fc02734a8f14235d2a0e6adf6a7aa68e74/libesedb_python-20240420-cp311-cp311-win_amd64.whl
if "%PYMAJOR%.%PYMINOR%"=="3.12" set WHEEL_URL=https://files.pythonhosted.org/packages/72/21/bb8a4adc71ba781815c191b349dce34b60b188e5e6b557c6770894250794/libesedb_python-20240420-cp312-cp312-win_amd64.whl

echo Trying pip install...
pip install libesedb-python
if errorlevel 1 (
    echo.
    echo Pip install failed. Trying direct wheel download...

    if defined WHEEL_URL (
        echo Downloading wheel for Python %PYMAJOR%.%PYMINOR%...
        curl -L -o libesedb_python.whl "%WHEEL_URL%" 2>nul || powershell -Command "Invoke-WebRequest -Uri '%WHEEL_URL%' -OutFile 'libesedb_python.whl'"
        if exist libesedb_python.whl (
            pip install libesedb_python.whl
            del libesedb_python.whl
            if not errorlevel 1 (
                echo [OK] libesedb-python installed from wheel
                goto :libesedb_done
            )
        )
    )

    echo.
    echo ==========================================
    echo  libesedb-python installation FAILED
    echo ==========================================
    echo.
    echo Your Python version: %PYMAJOR%.%PYMINOR%
    echo.
    echo Pre-built wheels are only available for Python 3.8-3.12.
    echo Python 3.13+ is NOT supported yet.
    echo.
    echo ==========================================
    echo  SOLUTION: Install Python 3.12
    echo ==========================================
    echo.
    echo 1. Download Python 3.12 from:
    echo    https://www.python.org/downloads/release/python-3120/
    echo.
    echo 2. During installation:
    echo    - Check "Add Python to PATH"
    echo    - Click "Customize installation"
    echo    - Check "Install for all users"
    echo.
    echo 3. Run this script again
    echo.
    echo ==========================================
    pause
    exit /b 1
)
:libesedb_done

echo [3/5] Installing dissect.esedb...
pip install dissect.esedb>=3.0

echo [4/5] Installing python-dateutil and chardet...
pip install python-dateutil>=2.8.0 chardet>=5.0.0

echo [5/5] Installing pywin32...
pip install pywin32>=305

echo.
echo ==========================================
echo  Verifying installation...
echo ==========================================
echo.
python -c "from PyQt6.QtWidgets import QApplication; print('[OK] PyQt6')" 2>nul || echo [FAIL] PyQt6
python -c "import pyesedb; print('[OK] libesedb-python')" 2>nul || echo [WARN] libesedb-python - not installed
python -c "from dissect.esedb import EseDB; print('[OK] dissect.esedb')" 2>nul || echo [FAIL] dissect.esedb
python -c "import dateutil; print('[OK] python-dateutil')" 2>nul || echo [FAIL] python-dateutil

echo.
echo ==========================================
echo  Installation complete!
echo ==========================================
echo.
echo To run the application:
echo   python gui_viewer_v2.py
echo.
echo If you see [WARN] for libesedb-python, install Visual C++ Build Tools
echo and run this script again for full functionality.
echo.
pause
