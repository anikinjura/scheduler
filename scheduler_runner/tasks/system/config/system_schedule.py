"""
system_schedule.py

Автоматически собирает расписания всех скриптов задачи system из config/scripts/*_config.py.
Экспортирует: TASK_SCHEDULE (список с расписанием всей задачи system) для ядра планировщика.
"""
__version__ = '0.0.4'

from scheduler_runner.utils.schedule_utils import collect_task_schedule

TASK_SCHEDULE = collect_task_schedule("system")