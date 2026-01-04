"""
GoogleSheetsUploadScript_config.py

Параметры и расписание для GoogleSheetsUploadScript домена (задачи) reports.

Author: anikinjura
"""
__version__ = '1.0.0'

from config.base_config import PVZ_ID
from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS

MODULE_PATH = "scheduler_runner.tasks.reports.GoogleSheetsUploadScript"

# Путь к файлу с ключами сервисного аккаунта
GOOGLE_CREDENTIALS_PATH = str(REPORTS_PATHS['GOOGLE_SHEETS_CREDENTIALS'])

# ID Google-таблицы (из URL: https://docs.google.com/spreadsheets/d/[ID]/edit)
SPREADSHEET_NAME = "1D9msGQtGV67ExJBDYlcMhyWVKrV690iSThd2iW361P8"

# Имя листа в таблице
WORKSHEET_NAME = "KPI"

# Структура данных для валидации (новые заголовки из листа KPI)
REQUIRED_HEADERS = [
    "id",
    "Дата",
    "ПВЗ",
    "Количество выдач",
    "Прямой поток",
    "Возвратный поток"
]

# Конфигурация для скрипта
SCRIPT_CONFIG = {
    "CREDENTIALS_PATH": GOOGLE_CREDENTIALS_PATH,
    "SPREADSHEET_NAME": SPREADSHEET_NAME,
    "WORKSHEET_NAME": WORKSHEET_NAME,
    "REQUIRED_HEADERS": REQUIRED_HEADERS,
    "USER": "system",  # Пользователь, от имени которого выполняется задача
    "TASK_NAME": "GoogleSheetsUploadScript",  # Имя задачи для логирования
    "DETAILED_LOGS": False,  # Флаг детализированного логирования
}

# Расписание задач запуска скрипта для ядра планировщика.
TASK_SCHEDULE = [
    {
        "name": SCRIPT_CONFIG["TASK_NAME"],
        "module": MODULE_PATH,
        "args": [],
        "schedule": "daily",
        "time": "22:00",  # Время запуска после формирования отчета
        "user": SCRIPT_CONFIG["USER"],
    }
]