"""
copy_config.py

Параметры и расписание для CopyScript задачи cameras.

Author: anikinjura
"""
__version__ = '0.0.1'

from scheduler_runner.tasks.cameras.config.cameras_paths import CAMERAS_PATHS

MODULE_PATH = "scheduler_runner.tasks.cameras.CopyScript"

SCRIPT_CONFIG = {
    "INPUT_DIR": CAMERAS_PATHS["CAMERAS_LOCAL"],
    "OUTPUT_DIR": CAMERAS_PATHS["CAMERAS_NETWORK"],
    "MAX_AGE_DAYS": 3,
    "ON_CONFLICT": "skip",          # skip/rename
    "DETAILED_LOGS": False,         # Флаг по умолчанию (если не задан в аргументах --detailed_logs) для включения детализированного логирования
    "USER": "operator",
    "TASK_NAME": "CopyScript",
}

SCHEDULE = {
    "name": SCRIPT_CONFIG["TASK_NAME"],
    "module": MODULE_PATH,
    "args": ["--shutdown 30"],
    "schedule": "daily",
    "time": "21:10",
    "user": SCRIPT_CONFIG["USER"],
}