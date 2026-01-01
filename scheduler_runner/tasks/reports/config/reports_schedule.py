"""
reports_schedule.py
Объединяет расписания всех поддоменов задачи reports (ozon, wildberries, yandex_market и т.д.).
Принимает: SCHEDULE (список с расписанием отдельного скрипта задачи) из каждого поддомена.
Экспортирует: TASK_SCHEDULE (список с расписанием всей задачи reports) для ядра планировщика.
Author: anikinjura
"""
__version__ = '1.0.0'

# Импортируем расписания из поддоменов
try:
    from scheduler_runner.tasks.reports.ozon.config.ozon_schedule import TASK_SCHEDULE as OZON_TASK_SCHEDULE
except ImportError:
    OZON_TASK_SCHEDULE = []

try:
    from scheduler_runner.tasks.reports.google_sheets.config.google_sheets_schedule import TASK_SCHEDULE as GOOGLE_SHEETS_TASK_SCHEDULE
except ImportError:
    GOOGLE_SHEETS_TASK_SCHEDULE = []

# Объединяем все расписания
TASK_SCHEDULE = OZON_TASK_SCHEDULE + GOOGLE_SHEETS_TASK_SCHEDULE