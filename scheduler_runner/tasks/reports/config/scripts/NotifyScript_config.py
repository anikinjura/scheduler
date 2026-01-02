"""
NotifyScript_config.py

Параметры и расписание для NotifyScript домена (задачи) reports.

Author: anikinjura
"""
__version__ = '1.0.0'

from config.base_config import PVZ_ID
from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS

MODULE_PATH = "scheduler_runner.tasks.reports.NotifyScript"

# Конфигурация для скрипта
SCRIPT_CONFIG = {
    "USER": "system",  # Пользователь, от имени которого выполняется задача
    "TASK_NAME": "NotifyScript",  # Имя задачи для логирования
    "DETAILED_LOGS": False,  # Флаг детализированного логирования
}

# Расписание задач запуска скрипта для ядра планировщика.
TASK_SCHEDULE = [
    {
        "name": SCRIPT_CONFIG["TASK_NAME"],
        "module": MODULE_PATH,
        "args": [],
        "schedule": "daily",
        "time": "21:45",  # Время запуска после формирования отчета, но до загрузки в Google Sheets
        "user": SCRIPT_CONFIG["USER"],
    }
]