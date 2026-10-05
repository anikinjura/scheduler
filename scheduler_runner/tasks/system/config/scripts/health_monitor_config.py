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
        "copy_last_success": [("operator", "CopyScript")],
        "updater_last_success": [("system", "UpdaterScript_morning"), ("system", "UpdaterScript_evening")],
    },
    "TASK_LOG_LOOKBACK_DAYS": 14,
    # Последний успешный сбор отчетов — по реальной загрузке KPI (KPI_UPLOAD_STATS без ошибок, rows > 0), а не по
    # завершению задачи ReportsProcessor: при отозванной сессии Турбо ПВЗ (AUTH_REQUIRED) задача завершается
    # «успешно», но ничего не собирает (144, 04–05.10.2026)
    "KPI_UPLOAD_LOG": ("reports_domain", "Uploader"),
    # Сравнение версии кода с удаленной веткой (git ls-remote — только чтение, без fetch)
    "MAIN_BRANCH": "main",
    "GIT_TIMEOUT_SECONDS": 30,
    # Тревоги (health_alerts.py): снимок прошлого сбора и активные тревоги — на компьютере ПВЗ (reports/ вне git)
    "STATE_FILE": str(BASE_DIR / "reports" / "health_state.json"),
    # Стартовые пороги (план, раздел 4) — уточнить по первым неделям данных
    "ALERTS": {
        "LOW_SPACE_WARNING_PCT": 15,
        "LOW_SPACE_WARNING_GB": 10,
        "LOW_SPACE_CRITICAL_PCT": 5,
        "DISK_HOT_C": 50,
        "SHUTDOWNS_CRITICAL": 3,
        "CODE_OUTDATED_HOURS": 24,
        "REPORTS_STALE_HOURS": 36,   # ReportsProcessor ~21:30, сбор 13:10 следующего дня — ~16 ч в норме
        "REMINDER_DAYS": 7,          # повтор неустраненной критической тревоги
    },
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
