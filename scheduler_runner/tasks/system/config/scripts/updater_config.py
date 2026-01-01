"""
updater_config.py

Конфиг для UpdaterScript задачи system.

Author: anikinjura
"""
__version__ = '0.0.3'

from config.base_config import BASE_DIR, ENV_MODE
MODULE_PATH = "scheduler_runner.tasks.system.UpdaterScript"

# Настройка веток в зависимости от режима среды
BRANCH_MAPPING = {
    'production': 'main',
    'test': 'test'
}

# Определяем ветку по умолчанию в зависимости от ENV_MODE
DEFAULT_BRANCH = BRANCH_MAPPING.get(ENV_MODE, 'main')

SCRIPT_CONFIG = {
    "BRANCH": DEFAULT_BRANCH,
    "DETAILED_LOGS": False,
    "USER": "system",
    "TASK_NAME": "UpdaterScript",
    "REPO_DIR": str(BASE_DIR),  # относительный путь к корню репозитория
    "REPO_URL": "https://github.com/anikinjura/scheduler.git",
    "BRANCH_MAPPING": BRANCH_MAPPING,
    "ENV_MODE": ENV_MODE,
}

# Расписание для ядра планировщика
SCHEDULE = [
    {
        "name": f'{SCRIPT_CONFIG["TASK_NAME"]}_morning',
        "module": MODULE_PATH,
        "args": ["--branch", DEFAULT_BRANCH],
        "schedule": "daily",
        "time": "09:10",
        "user": SCRIPT_CONFIG["USER"],
    },
    {
        "name": f'{SCRIPT_CONFIG["TASK_NAME"]}_evening',
        "module": MODULE_PATH,
        "args": ["--branch", DEFAULT_BRANCH],
        "schedule": "daily",
        "time": "18:10",
        "user": SCRIPT_CONFIG["USER"],
    },
]