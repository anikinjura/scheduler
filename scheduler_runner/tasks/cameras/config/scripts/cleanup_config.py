"""
cleanup_config.py

Параметры и расписание для CleanupScript задачи cameras.

Сценарии работы скрипта:
    - local: параметры для локального сценария
    - network: параметры для сетевого сценария

Author: anikinjura
"""
__version__ = '0.0.1'

from scheduler_runner.tasks.cameras.config.cameras_paths import CAMERAS_PATHS

MODULE_PATH = "scheduler_runner.tasks.cameras.CleanupScript"

# Конфигурация для скрипта
SCRIPT_CONFIG = {
    "local": {
        "CLEANUP_DIR": CAMERAS_PATHS["CAMERAS_LOCAL"],  # Путь к записям для очистки
        "MAX_AGE_DAYS": 8,                              # Максимальный возраст файлов для удаления  
        "DETAILED_LOGS": False,                         # Флаг по умолчанию (если не задан в аргументах --detailed_logs) для включения детализированного логирования
        "USER": "camera",                               # Пользователь, от имени которого выполняется задача   
        "TASK_NAME": "CleanupScript_local",             # Имя задачи для логирования
    },
    "network": {
        "CLEANUP_DIR": CAMERAS_PATHS["CAMERAS_NETWORK"],
        "MAX_AGE_DAYS": 120,
        "DETAILED_LOGS": True,
        "USER": "operator",
        "TASK_NAME": "CleanupScript_network",        
    },
}

# Расписание задач запуска скрипта для ядра планировщика.
# Каждая задача содержит имя, модуль, аргументы, расписание, время и пользователя
SCHEDULE = [
    {
        "name": SCRIPT_CONFIG["local"]["TASK_NAME"],
        "module": MODULE_PATH,
        "args": ["--input_dir_scenario", "local"],
        "schedule": "daily",
        "time": "20:45",
        "user": SCRIPT_CONFIG["local"]["USER"]
    },
    {
        "name": SCRIPT_CONFIG["network"]["TASK_NAME"],
        "module": MODULE_PATH,
        "args": ["--input_dir_scenario", "network"],
        "schedule": "daily",
        "time": "20:55",
        "user": SCRIPT_CONFIG["network"]["USER"]
    },
]