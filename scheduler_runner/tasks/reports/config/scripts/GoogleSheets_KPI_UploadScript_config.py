"""
GoogleSheets_KPI_UploadScript_config.py

Параметры и расписание для нового скрипта загрузки KPI данных в Google-таблицу.
Использует новую архитектуру с TableConfig и ReportConfig.

Author: anikinjura
Version: 3.0.0 (новая архитектура)
"""

from config.base_config import PVZ_ID
from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS
from scheduler_runner.utils.google_sheets import TableConfig, ColumnType, ColumnDefinition
from scheduler_runner.tasks.reports.utils.load_reports_data import ReportConfig

MODULE_PATH = "scheduler_runner.tasks.reports.GoogleSheets_KPI_UploadScript"

# Путь к файлу с ключами сервисного аккаунта
GOOGLE_CREDENTIALS_PATH = str(REPORTS_PATHS['GOOGLE_SHEETS_CREDENTIALS'])

# ID Google-таблицы (из URL: https://docs.google.com/spreadsheets/d/[ID]/edit)
SPREADSHEET_NAME = "1D9msGQtGV67ExJBDYlcMhyWVKrV690iSThd2iW361P8"

# Имя листа в таблице
WORKSHEET_NAME = "KPI"

# Конфигурация структуры таблицы для новой архитектуры
TABLE_CONFIG = TableConfig(
    worksheet_name=WORKSHEET_NAME,
    id_column="id",
    columns=[
        ColumnDefinition(name="id", column_type=ColumnType.FORMULA, formula_template="=B{row}&C{row}"),
        ColumnDefinition(name="Дата", column_type=ColumnType.DATA, required=True, unique_key=True),
        ColumnDefinition(name="ПВЗ", column_type=ColumnType.DATA, required=True, unique_key=True),
        ColumnDefinition(name="Количество выдач", column_type=ColumnType.DATA),
        ColumnDefinition(name="Прямой поток", column_type=ColumnType.DATA),
        ColumnDefinition(name="Возвратный поток", column_type=ColumnType.DATA)
    ],
    unique_key_columns=["Дата", "ПВЗ"]
)

# Конфигурация загрузки отчетов для новой архитектуры
REPORT_CONFIGS = [
    ReportConfig(
        report_type='giveout',
        file_pattern='ozon_giveout_report_{date}.json',  # Изменен шаблон: убран {pvz_id}
        required=False,
        fields_mapping={
            'issued_packages': 'issued_packages',
            'total_packages': 'total_packages',
            'pvz_info': 'pvz_info',
            'marketplace': 'marketplace'
        }
    ),
    ReportConfig(
        report_type='direct_flow',
        file_pattern='ozon_direct_flow_report_{pvz_id}_{date}.json',
        required=False,
        fields_mapping={
            'total_items_count': 'direct_flow_count',
            'pvz_info': 'pvz_info',
            'marketplace': 'marketplace'
        }
    ),
    ReportConfig(
        report_type='carriages',
        file_pattern='ozon_carriages_report_{pvz_id}_{date}_{pvz_id}.json',  # Изменен шаблон: добавлен {pvz_id} дважды
        required=False,
        fields_mapping={
            'direct_flow': 'direct_flow_data',
            'return_flow': 'return_flow_data',
            'pvz_info': 'pvz_info',
            'marketplace': 'marketplace'
        }
    )
]

# Конфигурация для скрипта
SCRIPT_CONFIG = {
    "CREDENTIALS_PATH": GOOGLE_CREDENTIALS_PATH,
    "SPREADSHEET_NAME": SPREADSHEET_NAME,
    "WORKSHEET_NAME": WORKSHEET_NAME,
    "TABLE_CONFIG": TABLE_CONFIG,
    "REPORT_CONFIGS": REPORT_CONFIGS,
    "USER": "system",  # Пользователь, от имени которого выполняется задача
    "TASK_NAME": "GoogleSheets_KPI_UploadScript",  # Имя задачи для логирования
    "DETAILED_LOGS": False,  # Флаг детализированного логирования
}

# Расписание задач запуска скрипта для ядра планировщика.
TASK_SCHEDULE = [
    {
        "name": SCRIPT_CONFIG["TASK_NAME"],
        "module": MODULE_PATH,
        "args": [],
        "schedule": "daily",
        "time": "22:00",  # Время запуска после формирования отчета
        "user": SCRIPT_CONFIG["USER"],
    }
]