"""
Parser_KPI_Giveout_OzonScript_config.py

Параметры и расписание для скрипта парсинга данных о выдачах ОЗОН.
Использует новую архитектуру с ReportConfig и системой загрузки данных.

Author: anikinjura
Version: 3.0.0 (новая архитектура)
"""

from datetime import date
from config.base_config import PVZ_ID
from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS

# Модульные константы для магических строк
LOGIN_INDICATORS = ['login', 'signin', 'auth']

# Базовый URL для отчетов
BASE_URL = "https://turbo-pvz.ozon.ru/reports/giveout"

# Шаблон фильтра по дате с типом операции
DATE_FILTER_TEMPLATE = "?filter=%7B%22startDate%22:%22{date}T00:00%2B03:00%22,%22endDate%22:%22{date}T23:59%2B03:00%22,%22operationTypes%22:[%22GiveoutAll%22]"

# Шаблон полного URL (составляется из базового URL и шаблона фильтра)
ERP_URL_TEMPLATE = BASE_URL + DATE_FILTER_TEMPLATE + "%7D"

# Формат даты для подстановки в шаблоны
DATE_FORMAT = "%Y-%m-%d"

# Значение ERP_URL по умолчанию (например, текущая дата)
current_date = date.today().strftime(DATE_FORMAT)
ERP_URL = ERP_URL_TEMPLATE.format(date=current_date)

MODULE_PATH = "scheduler_runner.tasks.reports.Parser_KPI_Giveout_OzonScript"

# Селекторы для элементов на странице
SELECTORS = {
    "PVZ_INPUT": "//input[@id='input___v-0-0']",
    "PVZ_INPUT_READONLY": "//input[@id='input___v-0-0' and @readonly]",
    "PVZ_INPUT_CLASS_READONLY": "//input[contains(@class, 'ozi__input__input__ie7wU') and @readonly]",
    "TOTAL_GIVEOUT": "//div[contains(@class, '_total_1n8st_15')]",  # Количество выдач на странице
    "GIVEOUT_COUNT": "//div[contains(@class, 'ozi__text-view__caption-medium__v6V9R') and contains(., 'Всего:')]",  # Количество выданных посылок (содержит "Всего: N")
}


# Структура данных отчета (для гибкой настройки формата отчета)
REPORT_DATA_SCHEMA = {
    'marketplace': 'Ozon',
    'report_type': 'giveout',
    'date': '{date}',  # будет заменено значением даты
    'timestamp': '{timestamp}',  # будет заменено значением времени
    'page_title': '{page_title}',  # будет заменено значением заголовка страницы
    'current_url': '{current_url}',  # будет заменено значением текущего URL
    'issued_packages': '{issued_packages}',  # будет заменено значением количества выдач
    'total_packages': '{total_packages}',  # будет заменено значением общего количества
    'pvz_info': '{pvz_info}',  # будет заменено значением информации о ПВЗ
    'raw_data': {
        'page_source_length': '{page_source_length}',  # будет заменено значением длины исходного кода страницы
        'page_text_length': '{page_text_length}'  # будет заменено значением длины текста страницы
    }
}

# Шаблон имени файла для сохранения отчета
FILE_PATTERN = 'ozon_giveout_report_{pvz_id}_{date}.json'

# Конфигурация для скрипта
SCRIPT_CONFIG = {
    "BASE_URL": BASE_URL,  # Базовый URL для формирования URL с фильтрацией по дате
    "ERP_URL": ERP_URL,  # URL отчета о выдачах ОЗОН
    "EDGE_USER_DATA_DIR": "",  # Путь будет определен автоматически на основе текущего пользователя
    "OUTPUT_DIR": str(REPORTS_PATHS['REPORTS_JSON']),  # Директория для сохранения отчетов из общих путей
    "USER": "system",  # Пользователь, от имени которого выполняется задача
    "TASK_NAME": "Parser_KPI_Giveout_OzonScript",  # Имя задачи для логирования
    "DETAILED_LOGS": False,  # Флаг детализированного логирования
    "HEADLESS": False,  # True - без отображения окна (для работы в фоне), False - с отображением
    "TIMEOUT": 600,  # Таймаут выполнения задачи в секундах

    # Информация о текущем ПВЗ
    "PVZ_ID": PVZ_ID,

    # Селекторы
    "SELECTORS": SELECTORS,

    # Шаблон сообщения для уведомлений
    "MESSAGE_TEMPLATE": "📊 KPI отчет за {date}\nПВЗ: {pvz}\nВыдач: {issued_packages}\nПрямой поток: {direct_flow}\nВозвратный поток: {return_flow}",
}

# Расписание задач запуска скрипта для ядра планировщика.
TASK_SCHEDULE = [
    {
        "name": SCRIPT_CONFIG["TASK_NAME"],
        "module": MODULE_PATH,
        "args": [],
        "schedule": "daily",
        "time": "21:00",  # Время запуска в конце смены
        "user": SCRIPT_CONFIG["USER"],
        "timeout": SCRIPT_CONFIG["TIMEOUT"],
    }
]