"""
google_sheets_schedule.py

Расписание задач для модуля Google Sheets.
"""
__version__ = '0.0.1'

from scheduler_runner.tasks.reports.google_sheets.config.google_sheets_config import GOOGLE_SHEETS_CONFIG

# Путь к модулю
MODULE_PATH = "scheduler_runner.tasks.reports.google_sheets.GoogleSheetsUploadScript"

# Расписание задач
TASK_SCHEDULE = [
    {
        "name": "GoogleSheetsUploadScript",
        "module": MODULE_PATH,
        "args": [],
        "schedule": "daily",
        "time": "22:00",  # После завершения основного отчета ОЗОН
        "user": GOOGLE_SHEETS_CONFIG["USER"],
        "timeout": 300,  # 5 минут на отправку данных
        "no_timeout_control": False
    }
]