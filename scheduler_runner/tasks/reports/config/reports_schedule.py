"""
reports_schedule.py
Автоматически собирает расписания всех скриптов задачи reports из config/scripts/*_config.py.
Принимает: SCHEDULE (список с расписанием отдельного скрипта задачи) из каждого скриптового конфига.
Экспортирует: TASK_SCHEDULE (список с расписанием всей задачи reports) для ядра планировщика.
Author: anikinjura
"""
__version__ = '0.0.4'

from scheduler_runner.utils.schedule_utils import collect_task_schedule

TASK_SCHEDULE = collect_task_schedule("reports")