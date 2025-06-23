"""
videomonitor_config.py

Конфиг для VideoMonitorScript задачи cameras.

Author: anikinjura
"""
__version__ = '0.0.1'

from scheduler_runner.tasks.cameras.config.cameras_paths import CAMERAS_PATHS
from scheduler_runner.tasks.cameras.config.cameras_list import CAMERAS_BY_PVZ
from config.base_config import PVZ_ID

MODULE_PATH = "scheduler_runner.tasks.cameras.VideoMonitorScript"

# Конфигурация для скрипта
SCRIPT_CONFIG = {
    "PVZ_ID": PVZ_ID,                                   # Идентификатор объекта
    "CAMERAS": CAMERAS_BY_PVZ.get(PVZ_ID, {}),          # Список камер для проверки
    "TOKEN": CAMERAS_PATHS["TELEGRAM_TOKEN"],           # Telegram bot_token
    "CHAT_ID": CAMERAS_PATHS["TELEGRAM_CHAT_ID"],       # Telegram chat_id   
    
    # Параметры для локальной и сетевой проверки:
    "local": {
        "CHECK_DIR": CAMERAS_PATHS["CAMERAS_LOCAL"],    # Путь к записям для проверки полноты записей
        "MAX_LOOKBACK_HOURS": 2,                        # Определяет, на сколько часов назад скрипт должен проверять наличие записей  
        "DETAILED_LOGS": True,                          # Флаг для включения детализированного логирования
        "USER": "operator",                             # Пользователь, от имени которого выполняется задача   
        "TASK_NAME": "VideoMonitorScript_local",        # Имя задачи для логирования
    },
    "network": {
        "CHECK_DIR": CAMERAS_PATHS["CAMERAS_NETWORK"],      
        "MAX_LOOKBACK_HOURS": 24,                               
        "DETAILED_LOGS": True,                          
        "USER": "operator",                                
        "TASK_NAME": "VideoMonitorScript_network",        
    },
}

# Расписание задач запуска скрипта для ядра планировщика.
SCHEDULE = [
    {
        "name": SCRIPT_CONFIG["local"]["TASK_NAME"],
        "module": MODULE_PATH,
        "args": ["--check_type", "local",  "--detailed_logs"],
        "schedule": "hourly",
        "user": SCRIPT_CONFIG["local"]["USER"]
    },
    {
        "name": SCRIPT_CONFIG["network"]["TASK_NAME"],
        "module": MODULE_PATH,
        "args": ["--check_type", "network",  "--detailed_logs"],
        "schedule": "daily",
        "time": "12:00",
        "user": SCRIPT_CONFIG["network"]["USER"]
    },
]