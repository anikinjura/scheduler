"""
Конфигурационный файл для базового парсера отчетов Ozon

=== ОПИСАНИЕ КОНФИГУРАЦИИ ===

Этот файл содержит минимально необходимые параметры для работы с системой Ozon:
- Селекторы для извлечения информации о ПВЗ
- ID требуемого ПВЗ из общей конфигурации
- Параметры URL, переопределяющие базовую конфигурацию
- Специфичные параметры для извлечения данных из таблиц и элементов Ozon
- Параметры мульти-шаговой обработки, специфичные для Ozon

Конфигурация наследует все параметры из BASE_REPORT_CONFIG и переопределяет
только те, что необходимы для специфичных потребностей парсинга данных из системы Ozon.

=== СТРУКТУРА КОНФИГУРАЦИИ ===

Конфигурация состоит из следующих разделов:
1. Параметры URL (переопределяют базовую конфигурацию)
2. Селекторы для извлечения данных ПВЗ (selectors)
3. Конфигурации таблиц (table_configs)
4. Дополнительные параметры (additional_params) - для хранения PVZ_ID

=== ПАРАМЕТРЫ, ПЕРЕОПРЕДЕЛЯЕМЫЕ ИЗ БАЗОЙ КОНФИГУРАЦИИ ===

Следующие параметры переопределяют значения из BASE_REPORT_CONFIG:
- base_url: базовый URL для запросов к системе Ozon
- filter_template: шаблон формирования общего фильтра в URL для системы Ozon
- date_filter_template: шаблон фильтра по дате для системы Ozon
- data_type_filter_template: шаблон фильтра по типу данных для системы Ozon
- table_configs: конфигурации таблиц, специфичные для Ozon
- additional_params.location_id: извлекается из config.base_config.PVZ_ID

=== ПРИМЕНЕНИЕ КОНФИГУРАЦИИ ===

Конфигурация используется в классе OzonReportParser из файла
ozon_report_parser.py и обеспечивает базовую настройку параметров
парсинга для всех дочерних классов.

Примеры селекторов:
- "pvz_input": "//input[@id='input___v-0-0' and @readonly]"
- "pvz_dropdown": "//div[contains(@class, 'ozi__input__root__ie7wU') and contains(@class, 'ozi__input-select__root__UA4xr')]"
- "pvz_option": "//div[contains(@class, 'ozi__dropdown-item__dropdownItem__cDZcD')]"

"""
__version__ = '0.0.1'

# Импортируем PVZ_ID из базовой конфигурации
from config.base_config import PVZ_ID
from .base_report_config import BASE_REPORT_CONFIG

# Базовая конфигурация для парсера отчетов Ozon
OZON_BASE_CONFIG = {
    **BASE_REPORT_CONFIG,  # Наследуем все параметры из базовой конфигурации

    # === ФОРМАТ ДАТЫ ПРИ ПАРСИНГЕ OZON ===
    "date_format": "%Y-%m-%d",                  # Формат даты по умолчанию
    "datetime_format": "%Y-%m-%d %H:%M:%S",     # Формат даты и времени по умолчанию

# ====== ОПИСАНИЕ ОБЩИХ ПАРАМЕТРЫ ДЛЯ ИЗВЛЕЧЕНИЯ ДАННЫХ СО СТРАНИЦ OZON (используются в логике конкретного парсера под OZON) ======

    # === ОПИСАНИЕ ОБЩИХ СЕЛЕКТОРОВ ===
    "selectors": {
        # Селлекторы для работы с выпадающим списком выбора конкретного ПВЗ
        "pvz_selectors": {
            "input": "//input[@id='input___v-0-0']",
            "input_readonly": "//input[@id='input___v-0-0' and @readonly]",
            "input_class_readonly": "//input[contains(@class, 'ozi__input__input__ie7wU') and @readonly]",
            "dropdown": "//div[contains(@class, 'ozi__input__root__ie7wU') and contains(@class, 'ozi__input-select__root__UA4xr')]",
            "option": "//div[contains(@class, 'ozi__dropdown-item__dropdownItem__cDZcD') and .//div[contains(@class, 'ozi__data-content__label__TA_HC')]]",
            "option_label": ".//div[contains(@class, 'ozi__data-content__label__TA_HC')]",  # XPath для получения текста метки опции
            "selected_option": "//div[contains(@class, 'ozi__dropdown-item__dropdownItem__cDZcD') and .//svg[contains(@class, 'ozi__dropdown-item__checkIcon__cDZcD')]]"
        }
    },


    # === ОПИСАНИЕ ОБЩИХ ТАБЛИЦ ===
    # Таблицы определены здесь как общие для всех шагов обработки Ozon
    "table_configs": {
        # Конфигурация для таблицы перевозок Ozon
        "carriages_table": {
            "table_selector": "//table[contains(@class, 'ozi__table__table__HAe8A')]", # Селектор таблицы, по которому будет производиться поиск самой таблицы
            "table_type": "standard", # Тип таблицы: 'standard', 'dynamic' и т.д.
            # Описание колонок таблицы
            "table_columns": [
                {
                    "name": "carriage_number", # Имя поля в результирующем словаре - попадает в качестве ключа в словаре результата
                    "selector": ".//td[1]//div[contains(@class, '_carriageNumber_')]", # Селектор ячейки - XPath для ячейки (указывает на div, содержащий номер)
                    "regex": r'(\d+)' # Опционально: регулярное выражение для извлечения части текста из ячейки
                },
                {
                    "name": "direction",
                    "selector": ".//td[2]",
                    # "regex": r'' # Опционально
                },
                {
                    "name": "creation_date",
                    "selector": ".//td[3]",
                    # "regex": r'' # Опционально
                },
                {
                    "name": "send_date",
                    "selector": ".//td[4]",
                    # "regex": r'' # Опционально
                }
            ]
        },
        # Можно добавить другие конфигурации таблиц
        # "another_table": { ... }
    },



    # === ДОПОЛНИТЕЛЬНЫЕ ПАРАМЕТРЫ ===
    "additional_params": {
        "location_id": PVZ_ID,    # ID локации (ПВЗ), извлекается из общей конфигурации
    },

    # === КОНФИГУРАЦИЯ ОВЕРЛЕЯ (МОДАЛЬНЫХ ОКОН) ===
    # Параметры для проверки и закрытия информационных оверлеев Ozon
    "overlay_config": {
        "enabled": True,  # Флаг включения проверки оверлея
        "overlay_selector": "//div[contains(@class, 'ozi__dialog__dialog__C2BB8')]",  # Селектор оверлея (модального окна)
        "close_button_selector": "//button[contains(@class, 'ozi__window__closeIcon__-pkPv')]",  # Селектор кнопки закрытия (крестик)
        "wait_timeout": 5,  # Таймаут ожидания появления оверлея (секунды)
        "retry_count": 3,  # Количество попыток закрытия
        "retry_delay": 1   # Задержка между попытками (секунды)
    }
}