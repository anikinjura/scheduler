"""
videomonitor_config.py
Конфигурация VideoMonitorScript.
- local: проверка локальных записей по одному или нескольким корням (`LOCAL_ROOTS`).
- network: проверка целевого архива (`CAMERAS_NETWORK`).
"""
__version__ = '0.0.2'

from config.base_config import PVZ_ID
from scheduler_runner.tasks.cameras.config.cameras_list import CAMERAS_BY_PVZ
from scheduler_runner.tasks.cameras.config.cameras_paths import CAMERAS_PATHS

MODULE_PATH = "scheduler_runner.tasks.cameras.VideoMonitorScript"

SCRIPT_CONFIG = {
    "PVZ_ID": PVZ_ID,
    "CAMERAS": CAMERAS_BY_PVZ.get(PVZ_ID, {}),
    "TOKEN": CAMERAS_PATHS["TELEGRAM_TOKEN"],
    "CHAT_ID": CAMERAS_PATHS["TELEGRAM_CHAT_ID"],
    "local": {
        "CHECK_DIR": CAMERAS_PATHS["CAMERAS_LOCAL"],
        "LOCAL_ROOTS": CAMERAS_PATHS.get("LOCAL_ROOTS", {"default": CAMERAS_PATHS["CAMERAS_LOCAL"]}),
        "MAX_LOOKBACK_HOURS": 2,
        "DETAILED_LOGS": False,
        "USER": "operator",
        "TASK_NAME": "VideoMonitorScript_local",
    },
    "network": {
        "CHECK_DIR": CAMERAS_PATHS["CAMERAS_NETWORK"],
        "MAX_LOOKBACK_HOURS": 24,
        "DETAILED_LOGS": False,
        "USER": "operator",
        "TASK_NAME": "VideoMonitorScript_network",
    },
}

SCHEDULE = [
    {
        "name": SCRIPT_CONFIG["local"]["TASK_NAME"],
        "module": MODULE_PATH,
        "args": ["--check_type", "local"],
        "schedule": "hourly",
        "time_window": "09:00-21:00",
        "user": SCRIPT_CONFIG["local"]["USER"],
    },
    {
        "name": SCRIPT_CONFIG["network"]["TASK_NAME"],
        "module": MODULE_PATH,
        "args": ["--check_type", "network"],
        "schedule": "daily",
        "time": "12:00",
        "user": SCRIPT_CONFIG["network"]["USER"],
    },
]
