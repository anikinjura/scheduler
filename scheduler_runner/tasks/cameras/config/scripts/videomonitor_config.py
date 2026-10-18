"""
videomonitor_config.py
Конфигурация VideoMonitorScript.
- local: проверка локальных записей по одному или нескольким корням (`LOCAL_ROOTS`).
- network: проверка целевого архива (`CAMERAS_NETWORK`).
"""
__version__ = '0.0.4'

from config.base_config import BASE_DIR, PVZ_ID
from scheduler_runner.tasks.cameras.config.cameras_list import CAMERAS_BY_PVZ
from scheduler_runner.tasks.cameras.config.cameras_paths import CAMERAS_PATHS

MODULE_PATH = "scheduler_runner.tasks.cameras.VideoMonitorScript"

SCRIPT_CONFIG = {
    "PVZ_ID": PVZ_ID,
    "CAMERAS": CAMERAS_BY_PVZ.get(PVZ_ID, {}),
    "NOTIFICATION_PROVIDER": CAMERAS_PATHS["NOTIFICATION_PROVIDER"],
    "NOTIFICATION_CONNECTION_PARAMS": CAMERAS_PATHS["NOTIFICATION_CONNECTION_PARAMS"],
    "TOKEN": CAMERAS_PATHS["TELEGRAM_TOKEN"],
    "CHAT_ID": CAMERAS_PATHS["TELEGRAM_CHAT_ID"],
    "local": {
        "CHECK_DIR": CAMERAS_PATHS["CAMERAS_LOCAL"],
        "LOCAL_ROOTS": CAMERAS_PATHS.get("LOCAL_ROOTS", {"default": CAMERAS_PATHS["CAMERAS_LOCAL"]}),
        "MAX_LOOKBACK_HOURS": 2,
        "SCAN_TIMEOUT_SECONDS": 40,     # вся проверка не дольше — иначе «хранилище не отвечает» (меньше таймаута задачи)
        # Уведомления: о новой пропавшей камере — сразу, напоминание — раз в REMIND_HOURS, о вернувшейся — один раз
        # (раньше одно и то же — каждый час: 144, 17–18.10.2026). Состояние — в reports/ (вне git)
        "STATE_FILE": BASE_DIR / "reports" / "video_monitor_local_state.json",
        "REMIND_HOURS": 3,
        "DETAILED_LOGS": False,
        "USER": "operator",
        "TASK_NAME": "VideoMonitorScript_local",
    },
    "network": {
        "CHECK_DIR": CAMERAS_PATHS["CAMERAS_NETWORK"],
        "MAX_LOOKBACK_HOURS": 24,
        "SCAN_TIMEOUT_SECONDS": 40,     # зависший «Диск-О:» — уведомление, а не тихое убийство задачи по таймауту
        "STATE_FILE": BASE_DIR / "reports" / "video_monitor_network_state.json",
        "REMIND_HOURS": 3,              # проверка раз в день — напоминание при каждой проверке
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
        "timeout": 120,
    },
    {
        "name": SCRIPT_CONFIG["network"]["TASK_NAME"],
        "module": MODULE_PATH,
        "args": ["--check_type", "network"],
        "schedule": "daily",
        "time": "12:00",
        "user": SCRIPT_CONFIG["network"]["USER"],
        "timeout": 120,
    },
]
