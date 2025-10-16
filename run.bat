@echo off
echo ========================================
echo Financial Dataset Dashboard Launcher
echo ========================================
echo.

REM Check if virtual environment exists
if not exist ".venv\Scripts\activate.bat" (
    echo Virtual environment not found!
    echo Creating virtual environment...
    python -m venv .venv
    if errorlevel 1 (
        echo Failed to create virtual environment.
        pause
        exit /b 1
    )
    echo Virtual environment created successfully.
    echo.
)

REM Activate virtual environment
echo Activating virtual environment...
call .venv\Scripts\activate.bat

REM Check if requirements are installed
echo Checking dependencies...
pip show streamlit >nul 2>&1
if errorlevel 1 (
    echo Installing required packages...
    pip install -r python\requirements.txt
    if errorlevel 1 (
        echo Failed to install dependencies.
        pause
        exit /b 1
    )
    echo Dependencies installed successfully.
    echo.
)

REM Check if Docker is running
echo Checking Docker status...
docker info >nul 2>&1
if errorlevel 1 (
    echo.
    echo [ERROR] Docker Desktop is not running!
    echo.
    echo Please start Docker Desktop and wait for it to fully start,
    echo then run this script again.
    echo.
    pause
    exit /b 1
)

REM Check if Docker containers are running
echo Docker is running. Checking containers...
docker ps | findstr mysql-warehouse >nul 2>&1
if errorlevel 1 (
    echo MySQL warehouse container is not running.
    echo Starting Docker containers...
    docker-compose up -d
    if errorlevel 1 (
        echo.
        echo [ERROR] Failed to start Docker containers.
        echo Please check docker-compose.yml and try again.
        echo.
        pause
        exit /b 1
    )
    echo Waiting for database to be ready...
    timeout /t 15 /nobreak >nul
    echo.
) else (
    echo Docker containers are already running.
    echo.
)

REM Run Streamlit app
echo Starting Streamlit application...
echo.
echo ========================================
echo Dashboard will open in your browser
echo Press Ctrl+C to stop the application
echo ========================================
echo.
streamlit run python\app.py

pause
