"""
OzonGiveoutReportParser_config.py

Параметры и расписание для скрипта парсинга данных о выдачах ОЗОН.
Использует новую архитектуру с ReportConfig и системой загрузки данных.

Author: anikinjura
Version: 3.0.0 (новая архитектура)
"""

from datetime import date
from config.base_config import PVZ_ID
from scheduler_runner.tasks.reports.config.reports_paths import REPORTS_PATHS

# === КОНСТАНТЫ ===
LOGIN_INDICATORS = ['login', 'signin', 'auth']
DATE_FORMAT = "%Y-%m-%d"

# === РЕГУЛЯРНЫЕ ВЫРАЖЕНИЯ ===
REGEX_PATTERNS = {
    # Паттерны для извлечения чисел
    "NUMBER_PATTERN": r'\d+',  # Общий паттерн для извлечения чисел
    "GIVEOUT_COUNT_PATTERN": r'Всего:\s*(\d+)',  # Паттерн для "Всего: N"
    "FOUND_PATTERN": r'Найдено:\s*(\d+)',  # Паттерн для "Найдено: N"
    "TOTAL_PATTERN": r'Итого:\s*(\d+)',  # Паттерн для "Итого: N"
    "COUNT_PATTERN": r'Количество:\s*(\d+)',  # Паттерн для "Количество: N"

    # Паттерны для извлечения дат
    "DATE_PATTERN": r'\d{4}-\d{2}-\d{2}',  # Паттерн для даты в формате YYYY-MM-DD
    "DATE_PATTERN_RU": r'\d{2}\.\d{2}\.\d{4}',  # Паттерн для даты в формате DD.MM.YYYY

    # Паттерны для извлечения ПВЗ
    "PVZ_PATTERN": r'ПВЗ\s*[\d-]+\s*(.*)|Пункт\s*выдачи\s*(.*)',  # Паттерн для идентификации ПВЗ
    "PVZ_CODE_PATTERN": r'ПВЗ\s*([A-Z\d-]+)',  # Паттерн для извлечения кода ПВЗ

    # Паттерны для извлечения других данных
    "PERCENTAGE_PATTERN": r'(\d+\.?\d*)\s*%',  # Паттерн для извлечения процентов
    "CURRENCY_PATTERN": r'([\d\s.,]+)\s*(₽|руб|RUB)',  # Паттерн для извлечения денежных сумм
}

# === URL КОНФИГУРАЦИЯ ===
BASE_URL = "https://turbo-pvz.ozon.ru/reports/giveout"
DATE_FILTER_TEMPLATE = "?filter=%7B%22startDate%22:%22{date}T00:00%2B03:00%22,%22endDate%22:%22{date}T23:59%2B03:00%22,%22operationTypes%22:[%22GiveoutAll%22]"
ERP_URL_TEMPLATE = BASE_URL + DATE_FILTER_TEMPLATE + "%7D"
current_date = date.today().strftime(DATE_FORMAT)
ERP_URL = ERP_URL_TEMPLATE.format(date=current_date)

MODULE_PATH = "scheduler_runner.tasks.reports.OzonGiveoutReportParser"

# === СЕЛЕКТОРЫ КОНФИГУРАЦИЯ ===
SELECTORS = {
    # Селекторы для ПВЗ
    "PVZ_SELECTORS": {
        "INPUT": "//input[@id='input___v-0-0']",
        "INPUT_READONLY": "//input[@id='input___v-0-0' and @readonly]",
        "INPUT_CLASS_READONLY": "//input[contains(@class, 'ozi__input__input__ie7wU') and @readonly]",
        "DROPDOWN": "//select[@class='pvz-dropdown'] or //div[@class='pvz-selector']",
        "OPTION": "//option[contains(@value, 'PVZ')] or //div[contains(@class, 'pvz-option')]"
    },
    
    # Селекторы для данных отчета
    "REPORT_SELECTORS": {
        "GIVEOUT_COUNT": "//div[contains(@class, 'ozi__text-view__caption-medium__v6V9R') and contains(., 'Всего:')]",
        "TOTAL_GIVEOUT": "//span[contains(@class, 'total-giveout') or contains(@class, 'issued-packages')]",
        "DATE_DISPLAY": "//div[contains(@class, 'date-display') or contains(@class, 'report-date')]"
    },
    
    # Селекторы для навигации
    "NAVIGATION_SELECTORS": {
        "REPORTS_MENU": "//a[contains(@href, '/reports') or contains(@class, 'reports-menu')]",
        "GIVEOUT_REPORT_LINK": "//a[contains(@href, 'giveout') or contains(text(), 'Выдача')]"
    }
}

# === СХЕМА ДАННЫХ ОТЧЕТА ===
REPORT_DATA_SCHEMA = {
    'marketplace': 'Ozon',
    'report_type': 'giveout',
    'date': '{date}',
    'timestamp': '{timestamp}',
    'page_title': '{page_title}',
    'current_url': '{current_url}',
    'issued_packages': '{issued_packages}',
    'pvz_info': '{pvz_info}',
    'raw_data': {
        'page_source_length': '{page_source_length}',
        'page_text_length': '{page_text_length}'
    }
}

# === КОНФИГУРАЦИЯ ПАРСЕРА ===
PARSER_CONFIG = {
    # Основные параметры
    "BASE_URL": BASE_URL,
    "ERP_URL": ERP_URL,
    "ERP_URL_TEMPLATE": ERP_URL_TEMPLATE,
    
    # Параметры браузера
    "BROWSER_CONFIG": {
        "EDGE_USER_DATA_DIR": "",  # будет определен автоматически
        "HEADLESS": False,  # True для фоновой работы
        "TIMEOUT": 600  # таймаут в секундах
    },
    
    # Параметры вывода
    "OUTPUT_CONFIG": {
        "OUTPUT_DIR": str(REPORTS_PATHS['REPORTS_JSON']),
        "FILE_PATTERN": 'ozon_giveout_report_{pvz_id}_{date}.json'
    },
    
    # Параметры идентификации
    "IDENTIFICATION_CONFIG": {
        "PVZ_ID": PVZ_ID,
        "PVZ_KEYWORDS": ['ПВЗ', 'PVZ', 'СОС', 'ЧЕБ', 'КАЗ', 'РОС'],
        "LOGIN_INDICATORS": LOGIN_INDICATORS
    },
    
    # Селекторы
    "SELECTORS": SELECTORS,

    # Регулярные выражения
    "REGEX_PATTERNS": REGEX_PATTERNS,

    # Параметры логирования
    "LOGGING_CONFIG": {
        "USER": "system",
        "TASK_NAME": "OzonGiveoutReportParser",
        "DETAILED_LOGS": False
    }
}

# === ШАБЛОНЫ СООБЩЕНИЙ ===
MESSAGE_TEMPLATES = {
    "NOTIFICATION": "📊 KPI отчет за {date}\nПВЗ: {pvz}\nВыдач: {issued_packages}\nПрямой поток: {direct_flow}\nВозвратный поток: {return_flow}",
    "ERROR": "❌ Ошибка при парсинге отчета: {error_message}",
    "SUCCESS": "✅ Отчет успешно собран: {report_name}"
}

# === РАСПИСАНИЕ ЗАДАЧ ===
TASK_SCHEDULE = [
    {
        "name": PARSER_CONFIG["LOGGING_CONFIG"]["TASK_NAME"],
        "module": MODULE_PATH,
        "args": [],
        "schedule": "daily",
        "time": "21:00",  # Время запуска в конце смены
        "user": PARSER_CONFIG["LOGGING_CONFIG"]["USER"],
        "timeout": PARSER_CONFIG["BROWSER_CONFIG"]["TIMEOUT"],
    }
]

# === ОСНОВНАЯ КОНФИГУРАЦИЯ СКРИПТА ===
SCRIPT_CONFIG = PARSER_CONFIG