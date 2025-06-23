"""
cameras_schedule.py
Автоматически собирает расписания всех скриптов задачи cameras из config/scripts/*_config.py.
Принимает: SCHEDULE (список с расписанием отдельного скрипта задачи) из каждого скриптового конфига.
Экспортирует: TASK_SCHEDULE (список с расписанием всей задачи cameras) для ядра планировщика.
Author: anikinjura
"""
__version__ = '0.0.3'

import importlib
import pkgutil
from pathlib import Path

TASK_SCHEDULE = []

# Путь к директории с конфигами скриптов cameras
scripts_dir = Path(__file__).parent / "scripts"

for module_info in pkgutil.iter_modules([str(scripts_dir)]):
    if not module_info.name.endswith("_config"):
        continue
    module_name = f"scheduler_runner.tasks.cameras.config.scripts.{module_info.name}"
    try:
        mod = importlib.import_module(module_name)
        schedule = getattr(mod, "SCHEDULE", None)
        if schedule and isinstance(schedule, list):
            TASK_SCHEDULE.extend(schedule)
    except Exception as e:
        print(f"[WARNING] Не удалось импортировать {module_name}: {e}")