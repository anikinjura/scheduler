"""
health_sheets_config.py

Листы Google-таблицы для HealthMonitorScript (docs/HEALTH_MONITOR_PLAN.md, раздел 3): PVZ_HEALTH (ПВЗ/день) и
PVZ_DISK_HEALTH (диск/день). Книга и сервисный аккаунт — те же, что у KPI (tasks/reports).

Не лежит в config/scripts: там только конфиги расписаний (collect_task_schedule).

Author: anikinjura
"""
__version__ = '0.0.1'

from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS
from scheduler_runner.tasks.reports.config.scripts.kpi_google_sheets_config import KPI_GOOGLE_SHEETS_CONFIG
from scheduler_runner.utils.uploader.core.providers.google_sheets.google_sheets_data_models import (
    ColumnDefinition,
    ColumnType,
    TableConfig,
)

# Порядок колонок — для чтения человеком: ключ, итог, место, диски, стабильность, scheduler, технические поля
HEALTH_COLUMNS = [
    "work_date", "pvz_id", "status", "alerts",
    "c_free_gb", "c_free_pct", "d_free_gb", "d_free_pct",
    "disks_total", "disks_warning", "worst_disk",
    "unexpected_shutdowns", "bugchecks", "whea_errors", "disk_errors", "ntfs_errors", "display_resets",
    "events_window_hours",
    "code_version", "main_version", "code_up_to_date",
    "reports_last_success", "copy_last_success", "updater_last_success",
    "task_operator_result", "task_camera_result", "task_system_result",
    "computer_name", "os_version", "cpu", "ram_gb", "boot_time", "uptime_hours",
    "probe_elevated", "probe_limited", "collected_at",
]
HEALTH_KEY_COLUMNS = ["work_date", "pvz_id"]

DISK_COLUMNS = [
    "work_date", "pvz_id", "disk_serial", "model", "media_type", "size_gb", "volumes",
    "health_status", "predict_failure",
    "smart_5_reallocated", "smart_197_pending", "smart_198_offline_uncorrectable",
    "smart_187_reported_uncorrectable", "smart_199_crc_errors",
    "temperature_c", "smart_9_power_on_hours",
]
# Серийный номер, а не номер диска в системе: после замены или перестановки диска история не перепутается
DISK_KEY_COLUMNS = ["work_date", "pvz_id", "disk_serial"]


def _table_config(worksheet_name, columns, key_columns):
    return TableConfig(
        worksheet_name=worksheet_name,
        id_column=key_columns[0],
        columns=[
            ColumnDefinition(name=name, column_type=ColumnType.DATA, required=name in key_columns,
                             unique_key=name in key_columns)
            for name in columns
        ],
        unique_key_columns=list(key_columns),
    )


HEALTH_TABLE_CONFIG = _table_config("PVZ_HEALTH", HEALTH_COLUMNS, HEALTH_KEY_COLUMNS)
DISK_TABLE_CONFIG = _table_config("PVZ_DISK_HEALTH", DISK_COLUMNS, DISK_KEY_COLUMNS)

HEALTH_SHEETS_CONFIG = {
    "CREDENTIALS_PATH": str(REPORTS_PATHS["GOOGLE_SHEETS_CREDENTIALS"]),
    "SPREADSHEET_ID": KPI_GOOGLE_SHEETS_CONFIG["SPREADSHEET_ID"],
    "TABLES": [HEALTH_TABLE_CONFIG, DISK_TABLE_CONFIG],
    # Все ПВЗ собирают в 13:10 и делят одну квоту Sheets API: случайная пауза перед записью
    "UPLOAD_START_JITTER_SECONDS": 60,
}
