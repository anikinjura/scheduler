"""
copy_config.py

Параметры и расписание для CopyScript задачи cameras.

Author: anikinjura
"""
__version__ = '0.0.1'

from scheduler_runner.tasks.cameras.config.cameras_paths import CAMERAS_PATHS

MODULE_PATH = "scheduler_runner.tasks.cameras.CopyScript"

SCRIPT_CONFIG = {
    "INPUT_DIR": CAMERAS_PATHS["CAMERAS_LOCAL"],
    "OUTPUT_DIR": CAMERAS_PATHS["CAMERAS_NETWORK"],
    "MAX_AGE_DAYS": 3,
    "ON_CONFLICT": "skip",          # skip/rename
    "DETAILED_LOGS": False,         # Флаг по умолчанию (если не задан в аргументах --detailed_logs) для включения детализированного логирования
    "USER": "operator",
    "TASK_NAME": "CopyScript",
    "SHUTDOWN_ENABLED": True,       # Возможность отключить выключение если нужно
    "SHUTDOWN_IF_NO_FILES": False,  # Выключать ли компьютер при отсутствии файлов для копирования
}

SCHEDULE = [{
    "name": SCRIPT_CONFIG["TASK_NAME"],
    "module": MODULE_PATH,
    "args": ["--shutdown", "1"],  # Уменьшено до 1 минуты для тестирования
    "schedule": "daily",
    "time": "21:10",
    "user": SCRIPT_CONFIG["USER"],
    "no_timeout_control": False, # no_timeout_control=False - синхронный режим: Процесс запускается, но планировщик ожидает его завершения с указанным таймаутом, и при превышении времени выполнения происходит принудительное завершение с помощью process.kill()
    "timeout": 10800  # Таймаут 3 часа на копирование и синхранизацию с облачным хранилищем, затем процесс прерывается и выключение компьютера, в рамках процесса, может быть прервано
}]