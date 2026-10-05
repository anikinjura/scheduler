"""
health_monitor_config.py

Конфиг HealthMonitorScript задачи system: ежедневный сбор состояния компьютера ПВЗ
(docs/HEALTH_MONITOR_PLAN.md).

Author: anikinjura
"""
__version__ = '0.0.1'

from config.base_config import BASE_DIR, PATH_CONFIG

MODULE_PATH = "scheduler_runner.tasks.system.HealthMonitorScript"

SCRIPT_CONFIG = {
    "USER": "system",
    "TASK_NAME": "HealthMonitorScript",
    "REPO_DIR": str(BASE_DIR),
    "LOGS_ROOT": str(PATH_CONFIG["LOGS_ROOT"]),
    # Сбор: PowerShell-скрипт рядом с задачей, только чтение, JSON в stdout
    "PROBE_SCRIPT": str(BASE_DIR / "scheduler_runner" / "tasks" / "system" / "health_probe.ps1"),
    "PROBE_TIMEOUT_SECONDS": 180,
    # Окно подсчета событий журнала System (аварийные перезагрузки, WHEA, ошибки дисков)
    "EVENTS_WINDOW_HOURS": 24,
    # Задачи Планировщика Windows, чье состояние попадает в отчет
    "SCHEDULED_TASKS": ["\\Задачи operator", "\\Задачи camera", "\\Задачи system"],
    # Последний успешный запуск задач scheduler — по их логам: (пользователь, имя задачи в логах)
    "TRACKED_TASK_LOGS": {
        "reports_last_success": [("operator", "ReportsProcessor")],
        "copy_last_success": [("operator", "CopyScript")],
        "updater_last_success": [("system", "UpdaterScript_morning"), ("system", "UpdaterScript_evening")],
    },
    "TASK_LOG_LOOKBACK_DAYS": 14,
    # Сравнение версии кода с удаленной веткой (git ls-remote — только чтение, без fetch)
    "MAIN_BRANCH": "main",
    "GIT_TIMEOUT_SECONDS": 30,
}

# Сбор — раз в день в 13:10 (runner задач system срабатывает в :10). Компьютер включен, нет пересечения
# с ReportsProcessor (21:30) и выключением после CopyScript (~22:40). Только чтение, несколько секунд.
SCHEDULE = [
    {
        "name": SCRIPT_CONFIG["TASK_NAME"],
        "module": MODULE_PATH,
        "args": [],
        "schedule": "daily",
        "time": "13:10",
        "user": SCRIPT_CONFIG["USER"],
        "timeout": 300,
    },
]
