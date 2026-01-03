"""
OzonCarriagesReportScript_config.py

Параметры и расписание для OzonCarriagesReportScript домена (задачи) reports.

Author: anikinjura
"""
__version__ = '1.0.0'

from datetime import date, timedelta
from config.base_config import PVZ_ID
from scheduler_runner.tasks.reports.config.reports_list import CURRENT_PVZ_SETTINGS
from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS

# Генерируем готовые URL с фильтрами для текущего дня (данные за смену, которая закончилась)
current_date = date.today().strftime("%Y-%m-%d")
BASE_URL = "https://turbo-pvz.ozon.ru/outbound/carriages-archive"
DATE_FILTER = f"?filter=%7B%22startSentMoment%22:%22{current_date}T00:00:00%2B03:00%22,%22endSentMoment%22:%22{current_date}T23:59:59%2B03:00%22"
DIRECT_FLOW_FILTER = ",%22flowType%22:%22Direct%22"
RETURN_FLOW_FILTER = ",%22flowType%22:%22Return%22"

# Готовые URL для каждого типа перевозок
ERP_URL = f"{BASE_URL}{DATE_FILTER}%7D"  # Базовый URL с фильтром по дате
DIRECT_FLOW_URL = f"{BASE_URL}{DATE_FILTER}{DIRECT_FLOW_FILTER}%7D"
RETURN_FLOW_URL = f"{BASE_URL}{DATE_FILTER}{RETURN_FLOW_FILTER}%7D"

MODULE_PATH = "scheduler_runner.tasks.reports.OzonCarriagesReportScript"

# Селекторы для элементов на странице
SELECTORS = {
    "PVZ_INPUT": "//input[@id='input___v-0-0']",
    "PVZ_INPUT_READONLY": "//input[@id='input___v-0-0' and @readonly]",
    "PVZ_INPUT_CLASS_READONLY": "//input[contains(@class, 'ozi__input__input__ie7wU') and @readonly]",
    "TOTAL_CARRIAGES": "//div[contains(@class, '_total_1n8st_15')]",  # Количество перевозок на основной странице
    "TOTAL_ITEMS_ON_LIST_PAGE": "//div[contains(@class, '_total_1n8st_15')]",  # Количество отправлений на странице списка (для совместимости)
    "TOTAL_ITEMS_ON_DETAIL_PAGE": "//div[contains(@class, '_total_1n8st_15')]",  # Количество отправлений на странице деталей перевозки (тот же селектор, что и в рабочей версии)
    "CARRIAGE_NUMBER": "//div[contains(@class, '_carriageNumber_tu0l6_21')]",
}

# Конфигурация для скрипта
SCRIPT_CONFIG = {
    "ERP_URL": ERP_URL,  # Базовый URL отчета по перевозкам ОЗОН с фильтром по дате
    "DIRECT_FLOW_URL": DIRECT_FLOW_URL,  # URL для прямых перевозок
    "RETURN_FLOW_URL": RETURN_FLOW_URL,  # URL для возвратных перевозок
    "EDGE_USER_DATA_DIR": "",  # Путь будет определен автоматически на основе текущего пользователя
    "OUTPUT_DIR": str(REPORTS_PATHS['REPORTS_JSON']),  # Директория для сохранения отчетов из общих путей
    "USER": "operator",  # Пользователь, от имени которого выполняется задача
    "TASK_NAME": "OzonCarriagesReportScript",  # Имя задачи для логирования
    "DETAILED_LOGS": False,  # Флаг детализированного логирования
    "HEADLESS": False,  # True - без отображения окна (для работы в фоне), False - с отображением
    "TIMEOUT": 600,  # Таймаут выполнения задачи в секундах

    # Информация о текущем ПВЗ
    "PVZ_ID": PVZ_ID,
    "CURRENT_PVZ_SETTINGS": CURRENT_PVZ_SETTINGS,
    "EXPECTED_PVZ_CODE": CURRENT_PVZ_SETTINGS.get('expected_ozon_pvz', PVZ_ID),  # Используем expected_ozon_pvz из настроек ПВЗ

    # Селекторы
    "SELECTORS": SELECTORS,
}

# Расписание задач запуска скрипта для ядра планировщика.
TASK_SCHEDULE = [
    {
        "name": SCRIPT_CONFIG["TASK_NAME"],
        "module": MODULE_PATH,
        "args": [],
        "schedule": "daily",
        "time": "21:30",  # Время запуска в конце смены
        "user": SCRIPT_CONFIG["USER"],
        "timeout": SCRIPT_CONFIG["TIMEOUT"],
    }
]