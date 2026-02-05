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
echo Trying to install pre-built wheel...
pip install --only-binary :all: libesedb-python
if errorlevel 1 (
    echo.
    echo Pre-built wheel not found for your Python version.
    echo Trying to build from source...
    pip install libesedb-python
    if errorlevel 1 (
        echo.
        echo ==========================================
        echo  libesedb-python installation FAILED
        echo ==========================================
        echo.
        echo Pre-built wheels are available for Python 3.8-3.12.
        echo Your Python version may not be supported.
        echo.
        echo Options:
        echo.
        echo 1. Install Python 3.10, 3.11, or 3.12 from python.org
        echo    (Pre-built wheels are available for these versions)
        echo.
        echo 2. Install Visual C++ Build Tools to compile from source:
        echo    https://visualstudio.microsoft.com/visual-cpp-build-tools/
        echo    Select "Desktop development with C++" and restart PC
        echo.
        echo ==========================================
        pause
        exit /b 1
    )
)

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
