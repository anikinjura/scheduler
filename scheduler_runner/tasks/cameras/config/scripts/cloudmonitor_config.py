"""
cloudmonitor_config.py

Конфиг для CloudMonitorScript задачи cameras.

Author: anikinjura
"""
__version__ = '0.0.1'

from scheduler_runner.tasks.cameras.config.cameras_paths import CAMERAS_PATHS
from config.base_config import PVZ_ID

MODULE_PATH = "scheduler_runner.tasks.cameras.CloudMonitorScript"

SCRIPT_CONFIG = {
    "PVZ_ID": PVZ_ID,
    "CHECK_DIR": CAMERAS_PATHS["CAMERAS_NETWORK"],      # Путь к облачному хранилищу для проверки
    "TOKEN": CAMERAS_PATHS["TELEGRAM_TOKEN"],           # Telegram bot_token
    "CHAT_ID": CAMERAS_PATHS["TELEGRAM_CHAT_ID"],       # Telegram chat_id
    "RETRIES": 4,                                       # Количество попыток проверки
    "DELAY": 10,                                        # Задержка между попытками (сек)
    "DETAILED_LOGS": False,                             # Флаг по умолчанию (если не задан в аргументах --detailed_logs) для включения детализированного логирования
    "USER": "operator",                                 # Пользователь для логирования
    "TASK_NAME": "CloudMonitorScript",                  # Имя задачи для логирования
}

SCHEDULE = [
    {
        "name": SCRIPT_CONFIG["TASK_NAME"],
        "module": MODULE_PATH,
        "args": [],
        "schedule": "hourly",
        "user": SCRIPT_CONFIG["USER"]
    }
]