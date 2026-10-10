"""
cloudmonitor_config.py
Конфигурация CloudMonitorScript для проверки доступности `CAMERAS_NETWORK`.
"""
__version__ = '0.0.2'

from scheduler_runner.tasks.cameras.config.cameras_paths import CAMERAS_PATHS
from config.base_config import BASE_DIR, PVZ_ID

MODULE_PATH = "scheduler_runner.tasks.cameras.CloudMonitorScript"

SCRIPT_CONFIG = {
    "PVZ_ID": PVZ_ID,
    "CHECK_DIR": CAMERAS_PATHS["CAMERAS_NETWORK"],      # Путь к облачному хранилищу для проверки
    "NOTIFICATION_PROVIDER": CAMERAS_PATHS["NOTIFICATION_PROVIDER"],
    "NOTIFICATION_CONNECTION_PARAMS": CAMERAS_PATHS["NOTIFICATION_CONNECTION_PARAMS"],
    "TOKEN": CAMERAS_PATHS["TELEGRAM_TOKEN"],           # Telegram bot_token
    "CHAT_ID": CAMERAS_PATHS["TELEGRAM_CHAT_ID"],       # Telegram chat_id
    "RETRIES": 3,                                       # Количество попыток проверки
    "DELAY": 5,                                         # Задержка между попытками (сек)
    # Одна попытка — не дольше N секунд: зависший «Диск-О:» не отвечает даже на проверку существования папки, и без
    # этого задачу убивал таймаут runner раньше, чем она отправляла уведомление (144, 05–10.10.2026).
    # 3 попытки x 15 с + 2 паузы x 5 с = 55 с < таймаута задачи (SCHEDULE["timeout"])
    "PROBE_TIMEOUT_SECONDS": 15,
    "REMIND_HOURS": 3,                                  # Повтор уведомления, пока хранилище недоступно
    "STATE_FILE": BASE_DIR / "reports" / "cloud_monitor_state.json",  # Состояние уведомлений (вне git)
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
        "time_window": "09:00-21:00",
        "user": SCRIPT_CONFIG["USER"],
        "timeout": 120,
    }
]
