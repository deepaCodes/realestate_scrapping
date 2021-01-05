
echo "scrapper started"

set PROJECT_ROOT=%~dp0
echo %PROJECT_ROOT%

python -V

set PYTHONPATH=%PYTHONPATH%;%PROJECT_ROOT%

set LOG_DIR=%PROJECT_ROOT%/logs

# Start scrapper
cd %PROJECT_ROOT%
python scrapper/newman.py > %LOG_DIR%/scrapper.log

echo "End of scrapping"