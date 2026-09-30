@echo off
chcp 65001 >nul
rem Диагностика окружения задачи ReportsProcessor (только чтение).
rem Запускать в сеансе того пользователя Windows, от которого работает задача (например, Оператор):
rem у него свои user site-packages и свой PATH.
rem Результат: %PUBLIC%\reports_diag.txt (C:\Users\Public — доступен для чтения другим пользователям).

cd /d C:\tools\scheduler
set OUT=%PUBLIC%\reports_diag.txt
(
  echo === whoami
  whoami
  echo === where python / pythonw ^(первый найденный запускает задача^)
  where python pythonw
  echo === interpreter
  python -c "import sys; print(sys.executable); print(sys.version)"
  echo === site-packages
  python -m site
  echo === dependencies
  python -c "import psutil, selenium, gspread, google.auth, googleapiclient, webdriver_manager, requests, dotenv; print('DEPENDENCIES OK')"
  echo === import reports_processor
  python -c "import scheduler_runner.tasks.reports.reports_processor; print('IMPORT OK')"
  echo === secrets
  if exist .env\secrets.env (echo secrets.env: OK) else (echo secrets.env: MISSING)
  if exist .env\gspread\*.json (echo gspread key: OK) else (echo gspread key: MISSING)
  echo === pvz_config
  type C:\tools\pvz_config.ini
) > "%OUT%" 2>&1
echo Результат сохранен в %OUT%
