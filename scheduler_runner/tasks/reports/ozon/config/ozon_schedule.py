"""
ozon_schedule.py
Автоматически собирает расписания всех скриптов задачи ozon из config/scripts/*_config.py.
Принимает: SCHEDULE (список с расписанием отдельного скрипта задачи) из каждого скриптового конфига.
Экспортирует: TASK_SCHEDULE (список с расписанием всей задачи ozon) для ядра планировщика.
Author: anikinjura
"""
__version__ = '1.0.0'

from scheduler_runner.utils.schedule_utils import collect_task_schedule

TASK_SCHEDULE = collect_task_schedule("reports/ozon")