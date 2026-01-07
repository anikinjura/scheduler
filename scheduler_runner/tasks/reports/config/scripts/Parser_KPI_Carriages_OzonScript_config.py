"""
Parser_KPI_Carriages_OzonScript_config.py

Параметры и расписание для скрипта парсинга данных о перевозках ОЗОН.
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
BASE_URL = "https://turbo-pvz.ozon.ru/outbound/carriages-archive"

# Шаблон фильтра по дате
DATE_FILTER_TEMPLATE = "?filter=%7B%22startSentMoment%22:%22{date}T00:00:00%2B03:00%22,%22endSentMoment%22:%22{date}T23:59:59%2B03:00%22"

# Шаблон фильтра по типу перевозок
FLOW_TYPE_FILTER_TEMPLATE = ",%22flowType%22:%22{flow_type}%22"

# Шаблон полного URL (составляется из базового URL и шаблона фильтра)
ERP_URL_TEMPLATE = BASE_URL + DATE_FILTER_TEMPLATE + "%7D"

# Шаблоны URL для каждого типа перевозок
DIRECT_FLOW_URL_TEMPLATE = BASE_URL + DATE_FILTER_TEMPLATE + FLOW_TYPE_FILTER_TEMPLATE + "%7D"
RETURN_FLOW_URL_TEMPLATE = BASE_URL + DATE_FILTER_TEMPLATE + FLOW_TYPE_FILTER_TEMPLATE + "%7D"

# Формат даты для подстановки в шаблоны
DATE_FORMAT = "%Y-%m-%d"

# Значение ERP_URL по умолчанию (например, текущая дата)
current_date = date.today().strftime(DATE_FORMAT)
ERP_URL = ERP_URL_TEMPLATE.format(date=current_date)

MODULE_PATH = "scheduler_runner.tasks.reports.Parser_KPI_Carriages_OzonScript"

# Селекторы для элементов на странице
SELECTORS = {
    "PVZ_INPUT": "//input[@id='input___v-0-0']",
    "PVZ_INPUT_READONLY": "//input[@id='input___v-0-0' and @readonly]",
    "PVZ_INPUT_CLASS_READONLY": "//input[contains(@class, 'ozi__input__input__ie7wU') and @readonly]",
    "TOTAL_CARRIAGES": "//div[contains(@class, '_total_1n8st_15')]",  # Количество перевозок на основной странице (содержит "Найдено: N")
    "CARRIAGE_NUMBER": "//div[contains(@class, '_carriageNumber_tu0l6_21')]",  # Номера перевозок на странице списка
    "TOTAL_ITEMS_ON_LIST_PAGE": "//div[contains(@class, '_total_1n8st_15')]",  # Количество отправлений на странице списка (для совместимости)
    "TOTAL_ITEMS_ON_DETAIL_PAGE": "//div[contains(@class, '_total_1n8st_15')]",  # Количество отправлений на странице деталей перевозки
}


# Структура данных отчета (для гибкой настройки формата отчета)
REPORT_DATA_SCHEMA = {
    'marketplace': 'Ozon',
    'report_type': 'carriages_combined',
    'date': '{date}',  # будет заменено значением даты
    'timestamp': '{timestamp}',  # будет заменено значением времени
    'page_title': '{page_title}',  # будет заменено значением заголовка страницы
    'current_url': '{current_url}',  # будет заменено значением текущего URL
    'direct_flow': {
        'flow_type': '{direct_flow_type}',  # будет заменено значением типа прямых перевозок
        'total_carriages_found': '{total_direct_carriages}',  # будет заменено значением общего количества прямых перевозок
        'carriage_numbers': '{direct_carriage_numbers}',  # будет заменено списком номеров прямых перевозок
        'carriage_details': '{direct_carriage_details}',  # будет заменено деталями прямых перевозок
        'total_items_count': '{total_direct_items}'  # будет заменено значением общего количества отправлений в прямых перевозках
    },
    'return_flow': {
        'flow_type': '{return_flow_type}',  # будет заменено значением типа возвратных перевозок
        'total_carriages_found': '{total_return_carriages}',  # будет заменено значением общего количества возвратных перевозок
        'carriage_numbers': '{return_carriage_numbers}',  # будет заменено списком номеров возвратных перевозок
        'carriage_details': '{return_carriage_details}',  # будет заменено деталями возвратных перевозок
        'total_items_count': '{total_return_items}'  # будет заменено значением общего количества отправлений в возвратных перевозках
    },
    'pvz_info': '{pvz_info}',  # будет заменено значением информации о ПВЗ
    'raw_data': {
        'page_source_length': '{page_source_length}',  # будет заменено значением длины исходного кода страницы
        'page_text_length': '{page_text_length}'  # будет заменено значением длины текста страницы
    }
}

# Шаблон имени файла для сохранения отчета
FILE_PATTERN = 'ozon_carriages_report_{pvz_id}_{date}.json'

# Конфигурация для скрипта
SCRIPT_CONFIG = {
    "BASE_URL": BASE_URL,  # Базовый URL для формирования URL с фильтрацией по дате
    "ERP_URL": ERP_URL,  # Базовый URL отчета по перевозкам ОЗОН с фильтром по дате
    "DIRECT_FLOW_URL_TEMPLATE": DIRECT_FLOW_URL_TEMPLATE,  # Шаблон URL для прямых перевозок
    "RETURN_FLOW_URL_TEMPLATE": RETURN_FLOW_URL_TEMPLATE,  # Шаблон URL для возвратных перевозок
    "EDGE_USER_DATA_DIR": "",  # Путь будет определен автоматически на основе текущего пользователя
    "OUTPUT_DIR": str(REPORTS_PATHS['REPORTS_JSON']),  # Директория для сохранения отчетов из общих путей
    "USER": "system",  # Пользователь, от имени которого выполняется задача
    "TASK_NAME": "Parser_KPI_Carriages_OzonScript",  # Имя задачи для логирования
    "DETAILED_LOGS": False,  # Флаг детализированного логирования
    "HEADLESS": False,  # True - без отображения окна (для работы в фоне), False - с отображением
    "TIMEOUT": 600,  # Таймаут выполнения задачи в секундах

    # Информация о текущем ПВЗ
    "PVZ_ID": PVZ_ID,

    # Селекторы
    "SELECTORS": SELECTORS,

    # Шаблон сообщения для уведомлений
    "MESSAGE_TEMPLATE": "📊 KPI отчет за {date}\nПВЗ: {pvz}\nПрямые перевозки: {total_direct_carriages}\nВозвратные перевозки: {total_return_carriages}",
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