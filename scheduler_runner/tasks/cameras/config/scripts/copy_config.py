"""
copy_config.py
Конфигурация CopyScript.
Поддерживает множественные источники через `INPUT_DIRS` (с fallback на `INPUT_DIR`).
"""
__version__ = '0.0.3'

from config.base_config import PVZ_ID
from scheduler_runner.tasks.cameras.config.cameras_paths import CAMERAS_PATHS

MODULE_PATH = "scheduler_runner.tasks.cameras.CopyScript"

SCRIPT_CONFIG = {
    "PVZ_ID": PVZ_ID,
    "INPUT_DIR": CAMERAS_PATHS["CAMERAS_LOCAL"],
    "INPUT_DIRS": list(CAMERAS_PATHS.get("LOCAL_ROOTS", {"default": CAMERAS_PATHS["CAMERAS_LOCAL"]}).values()),
    "OUTPUT_DIR": CAMERAS_PATHS["CAMERAS_NETWORK"],
    "MAX_AGE_DAYS": 3,
    "ON_CONFLICT": "skip",
    "DETAILED_LOGS": False,
    "USER": "operator",
    "TASK_NAME": "CopyScript",
    "SHUTDOWN_ENABLED": True,
    "SHUTDOWN_IF_NO_FILES": False,
    # Сбой копирования (хранилище недоступно/зависло, ошибка) — уведомление и все равно выключение: компьютер, оставленный
    # включенным из-за ошибки, ничего не докопирует, а сбой без уведомления никто не заметит (144, 05–10.10.2026)
    "SHUTDOWN_ON_FAILURE": True,
    "DEST_CHECK_TIMEOUT_SECONDS": 60,   # проверка целевого хранилища перед копированием
    "NOTIFICATION_CONNECTION_PARAMS": CAMERAS_PATHS["NOTIFICATION_CONNECTION_PARAMS"],
}

SCHEDULE = [{
    "name": SCRIPT_CONFIG["TASK_NAME"],
    "module": MODULE_PATH,
    "args": ["--shutdown", "1"],
    "schedule": "daily",
    "time": "22:30",
    "user": SCRIPT_CONFIG["USER"],
    "no_timeout_control": False,
    "timeout": 21600,
}]
