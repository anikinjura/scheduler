"""
ozon_giveout_report_config.py

Параметры и расписание для OzonGiveoutReportScript задачи reports.

Author: anikinjura
"""
__version__ = '1.0.0'

from datetime import date, timedelta
from config.base_config import PVZ_ID
from scheduler_runner.tasks.reports.ozon.config.reports_list import CURRENT_PVZ_SETTINGS

# Генерируем URL с фильтрами для текущего дня (данные за смену, которая закончилась)
current_date = date.today().strftime("%Y-%m-%d")
ERP_URL = f"https://turbo-pvz.ozon.ru/reports/giveout?filter=%7B%22startDate%22:%22{current_date}%22,%22endDate%22:%22{current_date}%22,%22operationTypes%22:[%22GiveoutAll%22]%7D"

MODULE_PATH = "scheduler_runner.tasks.reports.ozon.scripts.OzonGiveoutReportScript"

SCRIPT_CONFIG = {
    "ERP_URL": ERP_URL,  # URL отчета по выдаче ОЗОН с фильтром по дате
    "EDGE_USER_DATA_DIR": "",  # Путь будет определен автоматически на основе текущего пользователя
    "OUTPUT_DIR": "C:/tools/scheduler/reports",  # Директория для сохранения отчетов
    "USER": "operator",  # Пользователь, от имени которого выполняется задача
    "TASK_NAME": "OzonGiveoutReportScript",  # Имя задачи для логирования
    "DETAILED_LOGS": False,  # Флаг детализированного логирования
    "HEADLESS": True,  # Режим без отображения окна (для работы в фоне)
    "TIMEOUT": 600,  # Таймаут выполнения задачи в секундах

    # Информация о текущем ПВЗ
    "PVZ_ID": PVZ_ID,
    "CURRENT_PVZ_SETTINGS": CURRENT_PVZ_SETTINGS,
    "EXPECTED_PVZ_CODE": CURRENT_PVZ_SETTINGS.get('expected_ozon_pvz', PVZ_ID),  # Используем expected_ozon_pvz из настроек ПВЗ
}

SCHEDULE = [{
    "name": SCRIPT_CONFIG["TASK_NAME"],
    "module": MODULE_PATH,
    "args": [],
    "schedule": "daily",
    "time": "21:30",  # Время запуска в конце смены
    "user": SCRIPT_CONFIG["USER"],
    "timeout": SCRIPT_CONFIG["TIMEOUT"],
}]