"""
updater_config.py

Конфиг для UpdaterScript задачи system.

Author: anikinjura
"""
__version__ = '0.0.1'

MODULE_PATH = "scheduler_runner.tasks.system.UpdaterScript"

SCRIPT_CONFIG = {
    "BRANCH": "main",
    "DETAILED_LOGS": True,
    "USER": "system",
    "TASK_NAME": "UpdaterScript",
    "REPO_DIR": ".",  # относительный путь к корню репозитория
}

# Расписание для ядра планировщика (раз в сутки в 10:00)
SCHEDULE = {
    "name": SCRIPT_CONFIG["TASK_NAME"],
    "module": MODULE_PATH,
    "args": ["--branch", "main"],
    "schedule": "daily",
    "time": "10:00",
    "user": SCRIPT_CONFIG["USER"],
}