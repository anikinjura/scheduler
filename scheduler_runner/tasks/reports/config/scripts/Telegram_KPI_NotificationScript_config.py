"""
TelegramNotificationScript_config.py

Параметры и расписание для нового скрипта отправки уведомлений в Telegram.
Использует новую архитектуру с ReportConfig и системой загрузки данных.

Author: anikinjura
Version: 3.0.0 (новая архитектура)
"""

from config.base_config import PVZ_ID
from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS
from scheduler_runner.utils.google_sheets import TableConfig, ColumnType, ColumnDefinition
from scheduler_runner.tasks.reports.utils.load_reports_data import ReportConfig

MODULE_PATH = "scheduler_runner.tasks.reports.Telegram_KPI_NotificationScript"

# Токен Telegram-бота (из конфигурации)
TELEGRAM_BOT_TOKEN = REPORTS_PATHS["TELEGRAM_TOKEN"]

# ID чата для отправки уведомлений
TELEGRAM_CHAT_ID = REPORTS_PATHS["TELEGRAM_CHAT_ID"]

# Конфигурация структуры таблицы (для совместимости с новой архитектурой, но не используется для Telegram)
# Используется только для формирования данных в нужном формате
TABLE_CONFIG = TableConfig(
    worksheet_name="notifications",  # условное имя для совместимости
    id_column="id",  # используем условный ID для совместимости
    columns=[
        ColumnDefinition(name="id", column_type=ColumnType.DATA),  # условная ID колонка
        ColumnDefinition(name="Дата", column_type=ColumnType.DATA, required=True),
        ColumnDefinition(name="ПВЗ", column_type=ColumnType.DATA, required=True),
        ColumnDefinition(name="Количество выдач", column_type=ColumnType.DATA),
        ColumnDefinition(name="Прямой поток", column_type=ColumnType.DATA),
        ColumnDefinition(name="Возвратный поток", column_type=ColumnType.DATA)
    ],
    unique_key_columns=["Дата", "ПВЗ"]
)

# Конфигурация загрузки отчетов (аналогично GoogleSheets_KPI_UploadScript)
REPORT_CONFIGS = [
    ReportConfig(
        report_type='giveout',
        file_pattern='ozon_giveout_report_{pvz_id}_{date}.json',
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
        file_pattern='ozon_carriages_report_{date}.json',
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
    "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
    "TELEGRAM_CHAT_ID": TELEGRAM_CHAT_ID,
    "TABLE_CONFIG": TABLE_CONFIG,
    "REPORT_CONFIGS": REPORT_CONFIGS,
    "USER": "system",  # Пользователь, от имени которого выполняется задача
    "TASK_NAME": "Telegram_KPI_NotificationScript",  # Имя задачи для логирования
    "DETAILED_LOGS": False,  # Флаг детализированного логирования
    "MESSAGE_TEMPLATE": "📊 KPI отчет за {date}\nПВЗ: {pvz}\nВыдач: {issued_packages}\nПрямой поток: {direct_flow}\nВозвратный поток: {return_flow}",
}

# Расписание задач запуска скрипта для ядра планировщика.
TASK_SCHEDULE = [
    {
        "name": SCRIPT_CONFIG["TASK_NAME"],
        "module": MODULE_PATH,
        "args": [],
        "schedule": "daily",
        "time": "22:30",  # Время запуска после формирования отчета и обновления таблицы
        "user": SCRIPT_CONFIG["USER"],
    }
]