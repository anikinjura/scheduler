"""
cleanup_config.py
Конфигурация CleanupScript.
- local: очистка всех локальных корней (`CLEANUP_DIRS`).
- network: очистка целевого архива (`CLEANUP_DIR`) — целиком папками с датой в имени (`DATED_FOLDERS`).
"""
__version__ = '0.0.3'

from config.base_config import PVZ_ID
from scheduler_runner.tasks.cameras.config.cameras_paths import CAMERAS_PATHS
from scheduler_runner.tasks.cameras.config.cameras_retention import get_retention_days

MODULE_PATH = "scheduler_runner.tasks.cameras.CleanupScript"

SCRIPT_CONFIG = {
    "local": {
        "CLEANUP_DIR": CAMERAS_PATHS["CAMERAS_LOCAL"],
        "CLEANUP_DIRS": list(CAMERAS_PATHS.get("LOCAL_ROOTS", {"default": CAMERAS_PATHS["CAMERAS_LOCAL"]}).values()),
        "MAX_AGE_DAYS": get_retention_days(PVZ_ID, "local"),
        "DETAILED_LOGS": False,
        "USER": "camera",
        "TASK_NAME": "CleanupScript_local",
    },
    "network": {
        "CLEANUP_DIR": CAMERAS_PATHS["CAMERAS_NETWORK"],
        "MAX_AGE_DAYS": get_retention_days(PVZ_ID, "network"),
        # Облако: обход всех файлов (stat каждого по сети) не укладывался в таймаут, архив не очищался (143, 144) —
        # удаляем папки YYYYMMDD / YYYYMMDDHH целиком, самые старые первыми, не дольше TIME_BUDGET_SECONDS за запуск
        "DATED_FOLDERS": True,
        "TIME_BUDGET_SECONDS": 480,       # остаток — при следующем запуске; меньше таймаута задачи (600 с)
        "DIR_CHECK_TIMEOUT_SECONDS": 60,  # зависший «Диск-О:» — выход с ошибкой, а не ожидание до таймаута
        "DETAILED_LOGS": True,
        "USER": "operator",
        "TASK_NAME": "CleanupScript_network",
    },
}

SCHEDULE = [
    {
        "name": SCRIPT_CONFIG["local"]["TASK_NAME"],
        "module": MODULE_PATH,
        "args": ["--input_dir_scenario", "local"],
        "schedule": "daily",
        "time": "20:45",
        "user": SCRIPT_CONFIG["local"]["USER"],
    },
    {
        "name": SCRIPT_CONFIG["network"]["TASK_NAME"],
        "module": MODULE_PATH,
        "args": ["--input_dir_scenario", "network"],
        "schedule": "daily",
        "time": "20:55",
        "user": SCRIPT_CONFIG["network"]["USER"],
        "timeout": 600,
    },
]
