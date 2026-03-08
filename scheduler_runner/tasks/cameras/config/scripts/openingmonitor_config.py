"""
openingmonitor_config.py
Конфигурация OpeningMonitorScript.
Поиск выполняется по `SEARCH_DIRS` (fallback: `SEARCH_DIR`).
"""
__version__ = '2.0.1'

from config.base_config import PVZ_ID
from scheduler_runner.tasks.cameras.config.cameras_paths import CAMERAS_PATHS

MODULE_PATH = "scheduler_runner.tasks.cameras.OpeningMonitorScript"

SCRIPT_CONFIG = {
    "PVZ_ID": PVZ_ID,
    "SEARCH_DIR": CAMERAS_PATHS["CAMERAS_LOCAL"],
    "SEARCH_DIRS": list(CAMERAS_PATHS.get("LOCAL_ROOTS", {"default": CAMERAS_PATHS["CAMERAS_LOCAL"]}).values()),
    "START_TIME": "08:00:00",
    "END_TIME": "10:00:00",
    "USER": "operator",
    "TASK_NAME": "OpeningMonitorScript",
    "DETAILED_LOGS": False,
    "TELEGRAM_TOKEN": CAMERAS_PATHS.get("TELEGRAM_TOKEN"),
    "TELEGRAM_CHAT_ID": CAMERAS_PATHS.get("TELEGRAM_CHAT_ID"),
    "COMBINED_ANALYSIS_ENABLED": True,
    "PRIORITY_SOURCE": "all",
    "BOOT_TIME_TOLERANCE_MINUTES": 30,
}

SCHEDULE = [{
    "name": SCRIPT_CONFIG["TASK_NAME"],
    "module": MODULE_PATH,
    "args": [],
    "schedule": "daily",
    "time": "10:05",
    "user": SCRIPT_CONFIG["USER"],
    "timeout": 120,
}]
