"""
openingmonitor_config.py

Параметры и расписание для OpeningMonitorScript задачи cameras.

Скрипт ищет самый ранний видеофайл за текущий день в заданном
промежутке времени и время включения компьютера, 
и сообщает о времени начала работы на основе комбинированного анализа в Telegram.

Author: anikinjura
"""
__version__ = '2.0.0'

from scheduler_runner.tasks.cameras.config.cameras_paths import CAMERAS_PATHS
from config.base_config import PVZ_ID

MODULE_PATH = "scheduler_runner.tasks.cameras.OpeningMonitorScript"

SCRIPT_CONFIG = {
    "PVZ_ID": PVZ_ID,
    "SEARCH_DIR": CAMERAS_PATHS["CAMERAS_LOCAL"],
    "START_TIME": "08:00:00",
    "END_TIME": "10:00:00",
    "USER": "operator",
    "TASK_NAME": "OpeningMonitorScript",
    "DETAILED_LOGS": False,
    # Параметры для уведомлений
    "TELEGRAM_TOKEN": CAMERAS_PATHS.get("TELEGRAM_TOKEN"),
    "TELEGRAM_CHAT_ID": CAMERAS_PATHS.get("TELEGRAM_CHAT_ID"),
    # Параметры для комбинированного анализа
    "COMBINED_ANALYSIS_ENABLED": True,  # Включить комбинированный анализ
    "PRIORITY_SOURCE": "all",  # "camera", "boot_time", "wake_time", "both", "all" - приоритетный источник данных
    "BOOT_TIME_TOLERANCE_MINUTES": 30,  # Допустимое отклонение во времени (в минутах) при сравнении источников
}

SCHEDULE = [{
    "name": SCRIPT_CONFIG["TASK_NAME"],
    "module": MODULE_PATH,
    "args": [],
    "schedule": "daily",
    "time": "10:05",  # Запускаем сразу после окончания окна проверки
    "user": SCRIPT_CONFIG["USER"],
    "timeout": 120,
}]
