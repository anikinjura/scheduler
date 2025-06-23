"""
system_schedule.py

Автоматически собирает расписания всех скриптов задачи system из config/scripts/*_config.py.
Экспортирует: TASK_SCHEDULE (список с расписанием всей задачи system) для ядра планировщика.
"""
__version__ = '0.0.1'

import importlib
import pkgutil
from pathlib import Path

TASK_SCHEDULE = []

scripts_dir = Path(__file__).parent / "scripts"

for module_info in pkgutil.iter_modules([str(scripts_dir)]):
    if not module_info.name.endswith("_config"):
        continue
    module_name = f"scheduler_runner.tasks.system.config.scripts.{module_info.name}"
    try:
        mod = importlib.import_module(module_name)
        schedule = getattr(mod, "SCHEDULE", None)
        if schedule and isinstance(schedule, list):
            TASK_SCHEDULE.extend(schedule)
    except Exception as e:
        print(f"[WARNING] Не удалось импортировать {module_name}: {e}")