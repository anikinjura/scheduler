"""
reports_processor_config.py

Конфиг для reports_processor задачи reports.

Author: anikinjura
"""
__version__ = '0.0.1'

MODULE_PATH = "scheduler_runner.tasks.reports.reports_processor"

# Расписание задач запуска скрипта для ядра планировщика.
SCHEDULE = [
    {
        "name": "ReportsProcessor",
        "module": MODULE_PATH,
        "args": [],  # Скрипт будет использовать текущую дату по умолчанию
        "schedule": "daily",
        "time": "13:10",
        "user": "operator",
        "timeout": 360  # Таймаут 6 минут для выполнения полного цикла: парсинг → загрузка → уведомление
    },
]