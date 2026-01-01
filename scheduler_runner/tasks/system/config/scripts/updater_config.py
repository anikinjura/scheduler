"""
updater_config.py

Конфиг для UpdaterScript задачи system.

Author: anikinjura
"""
__version__ = '0.0.2'

from config.base_config import BASE_DIR
MODULE_PATH = "scheduler_runner.tasks.system.UpdaterScript"

SCRIPT_CONFIG = {
    "BRANCH": "main",
    "DETAILED_LOGS": False,
    "USER": "system",
    "TASK_NAME": "UpdaterScript",
    "REPO_DIR": str(BASE_DIR),  # относительный путь к корню репозитория
    "REPO_URL": "https://github.com/anikinjura/scheduler.git",
}

# Расписание для ядра планировщика
SCHEDULE = [
    {
        "name": f'{SCRIPT_CONFIG["TASK_NAME"]}_morning',
        "module": MODULE_PATH,
        "args": ["--branch", "main"],
        "schedule": "daily",
        "time": "09:10",
        "user": SCRIPT_CONFIG["USER"],
    },
    {
        "name": f'{SCRIPT_CONFIG["TASK_NAME"]}_evening',
        "module": MODULE_PATH,
        "args": ["--branch", "main"],
        "schedule": "daily",
        "time": "18:10",
        "user": SCRIPT_CONFIG["USER"],
    },
]