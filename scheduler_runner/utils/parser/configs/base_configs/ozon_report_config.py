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
- "pvz_dropdown": "//div[contains(@class, 'ozi__input-select__root__')]"
- "pvz_option": "//div[contains(@class, 'ozi__dropdown-item__dropdownItem__')]"
(хэш-суффиксы классов Ozon UI в селекторах не используются: они меняются при пересборке фронтенда)

"""
__version__ = '0.0.1'

# Импортируем PVZ_ID из базовой конфигурации
from config.base_config import PVZ_ID

# Источник данных парсера по ПВЗ машины (docs/MODERNIZATION_PLAN.md, этап 4 — обкатка на проде).
# 30.09.2026: ЧЕБОКСАРЫ_144 — "api": в разметке ноль выдач неотличим от незагрузившейся страницы, в API — явный totalCount.
DATA_SOURCE_MODE_BY_PVZ = {
    "ЧЕБОКСАРЫ_144": "api",
}
from .base_report_config import BASE_REPORT_CONFIG

# Базовая конфигурация для парсера отчетов Ozon
OZON_BASE_CONFIG = {
    **BASE_REPORT_CONFIG,  # Наследуем все параметры из базовой конфигурации

    # === ФОРМАТ ДАТЫ ПРИ ПАРСИНГЕ OZON ===
    "date_format": "%Y-%m-%d",                  # Формат даты по умолчанию
    "datetime_format": "%Y-%m-%d %H:%M:%S",     # Формат даты и времени по умолчанию

# ====== ОПИСАНИЕ ОБЩИХ ПАРАМЕТРЫ ДЛЯ ИЗВЛЕЧЕНИЯ ДАННЫХ СО СТРАНИЦ OZON (используются в логике конкретного парсера под OZON) ======

    # === ИСТОЧНИК ДАННЫХ (docs/MODERNIZATION_PLAN.md) ===
    # dom — числа из разметки страницы (текущее поведение); shadow — из разметки, плюс сравнение со значением из API
    # в логе; api_with_dom_fallback — из API, при сбое из разметки; api — только из API, при сбое явная ошибка.
    # В режимах кроме dom после старта браузера ставится перехватчик ответов API (API_CAPTURE_INSTALLED).
    # Режим задается по ПВЗ машины (PVZ_ID из pvz_config.ini) через DATA_SOURCE_MODE_BY_PVZ, остальные — "dom".
    "DATA_SOURCE_MODE": DATA_SOURCE_MODE_BY_PVZ.get(PVZ_ID, "dom"),
    # wait_timeout: ожидание ответа API после загрузки страницы; при отсутствии ответа страница перезагружается один раз
    # (API_RETRY_RELOAD). 30.09.2026 при нестабильной сети первая страница сессии получала данные дольше 15 с.
    "api_capture": {"wait_timeout": 30, "poll_interval": 0.5, "max_records": 500, "max_body_chars": 2_000_000},

    # === ОПИСАНИЕ ОБЩИХ СЕЛЕКТОРОВ ===
    # Классы Ozon UI (ozi__<компонент>__<элемент>__<хэш>) содержат хэш, который меняется при каждой
    # пересборке фронтенда, поэтому селекторы сопоставляют только префикс класса без хэша.
    # С версии Турбо ПВЗ 3.11.x список ПВЗ рендерится вне #ozi-window-teleport-target.
    "selectors": {
        # Селлекторы для работы с выпадающим списком выбора конкретного ПВЗ
        "pvz_selectors": {
            "input": "//input[@id='input___v-0-0']",
            "input_readonly": "//input[@id='input___v-0-0' and @readonly]",
            "input_class_readonly": "//input[contains(@class, 'ozi__input__input__') and @readonly]",
            "dropdown": "//div[@data-popover-reference='true' and .//input[@id='input___v-0-0']]",
            "option": "//div[contains(@class, 'ozi__dropdown-item__dropdownItem__') and .//*[contains(@class, 'ozi__data-content__label__')]]",
            "option_label": ".//div[contains(@class, 'ozi__data-content__label__')]",  # XPath для получения текста метки опции
            "selected_option": "//div[contains(@class, 'ozi__dropdown-item__dropdownItem__') and .//svg[contains(@class, 'checkIcon')]]",
            "input_candidates": [
                "//input[@id='input___v-0-0' and @readonly]",
                "//input[@id='input___v-0-0']",
                "//input[contains(@class, 'ozi__input__input__') and @readonly]",
                "//div[contains(@class, 'ozi__input-select__root__')][.//input[@id='input___v-0-0']]//input[@readonly]",
                "//div[contains(@class, 'ozi__input-select__root__')][.//input[@id='input___v-0-0']]//input"
            ],
            "dropdown_candidates": [
                "//div[@data-popover-reference='true' and .//input[@id='input___v-0-0']]",
                "//div[contains(@class, 'ozi__input__container__') and @data-popover-reference='true' and .//input[@id='input___v-0-0']]",
                "//div[contains(@class, 'ozi__input-select__inputSelect__')][.//input[@id='input___v-0-0']]",
                "//div[contains(@class, 'ozi__input-select__root__')][.//input[@id='input___v-0-0']]",
                "//div[contains(@class, 'ozi__input-select__root__')][.//input[@readonly and @value]]"
            ],
            # Порядок важен: каждая неудачная попытка кликает по dropdown и может закрыть список,
            # поэтому первым идет селектор, соответствующий актуальной разметке.
            "option_candidates": [
                "//div[contains(@class, 'ozi__dropdown-item__dropdownItem__')]//*[contains(@class, 'ozi__data-content__label__') and normalize-space()='{target_pvz}']",
                "//*[contains(@class, 'ozi__data-content__label__') and normalize-space()='{target_pvz}']",
                "//*[@id='ozi-window-teleport-target']//div[contains(@class, 'ozi__dropdown-item') and .//*[contains(@class, 'ozi__data-content__label__')]]",
                "//*[normalize-space()='{target_pvz}']"
            ],
            "options_container_candidates": [
                "//div[contains(@class, 'ozi__dropdown__dropdown__')]",
                "//*[@id='ozi-window-teleport-target']",
                "//div[contains(@class, 'ozi__popover__content__')]",
                "//div[contains(@class, 'ozi__select-options__')]",
            ],
            "option_item_candidates": [
                "//div[contains(@class, 'ozi__dropdown-item__dropdownItem__') and .//*[contains(@class, 'ozi__data-content__label__')]]",
                "//*[@id='ozi-window-teleport-target']//div[contains(@class, 'ozi__dropdown-item') and .//*[contains(@class, 'ozi__data-content__label__')]]",
                "//div[contains(@class, 'ozi__dropdown-item') and .//*[contains(@class, 'ozi__data-content__label__')]]",
            ],
            "option_label_candidates": [
                ".//div[contains(@class, 'ozi__data-content__label__')]",
                ".//span[contains(@class, 'ozi__data-content__label__')]",
                ".//*[normalize-space()]",
            ],
            "selected_option_candidates": [
                "//div[contains(@class, 'ozi__dropdown-item__dropdownItem__') and .//svg[contains(@class, 'checkIcon')]]",
                "//*[@id='ozi-window-teleport-target']//div[contains(@class, 'ozi__dropdown-item') and .//svg[contains(@class, 'checkIcon')]]",
                "//div[contains(@class, 'ozi__dropdown-item') and .//svg[contains(@class, 'checkIcon')]]",
                "//li[.//svg[contains(@class, 'checkIcon')]]",
            ],
        }
    },


    # === ОПИСАНИЕ ОБЩИХ ТАБЛИЦ ===
    # Таблицы определены здесь как общие для всех шагов обработки Ozon
    "table_configs": {
        # Конфигурация для таблицы перевозок Ozon
        "carriages_table": {
            "table_selector": "//table[contains(@class, 'ozi__table__table__')]", # Селектор таблицы, по которому будет производиться поиск самой таблицы
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
        "overlay_selector": "//div[contains(@class, 'ozi__dialog__dialog__')]",  # Селектор оверлея (модального окна)
        "close_button_selector": "//button[contains(@class, 'ozi__button') and normalize-space()='Отложить']",  # Селектор кнопки "Отложить"
        "close_button_candidates": [
            "//button[contains(@class, 'ozi__button') and normalize-space()='Отложить']",
            "//button[contains(@class, 'ozi__dialog__closeIcon__')]",  # Крестик самого диалога (надежнее общего icon-button ниже)
            "//button[contains(@class, '_exitButton_')]",
            "//button[contains(@class, 'ozi__icon-button__iconButton__') and contains(@class, '_exitButton_')]",
            "//button[@aria-label='Закрыть']",
            "//button[@title='Закрыть']",
            # Общий вариант «любая иконка-кнопка с svg» удален: он нажимал случайные кнопки интерфейса
            # (например, «Обучающие материалы»), когда окно было другого вида
        ],
        "transition_wait": 2,  # Окно скрыто, виден только фон (анимация): подождать и перепроверить, прежде чем закрывать
        "wait_timeout": 2,  # Таймаут ожидания появления оверлея (секунды) (уменьшено с 5 до 2 сек для оптимизации)
        "retry_count": 3,  # Количество попыток закрытия
        "retry_delay": 1,   # Задержка между попытками (секунды)
        "backdrop_selectors": [  # Селекторы для проверки активного backdrop (полупрозрачного фона)
            "//div[contains(@class, 'ozi__backdrop__backdrop__')]",
            "//div[contains(@class, 'ozi__backdrop')]",
            "//div[contains(@class, 'backdrop')]",
            "//div[contains(@class, 'modal-backdrop')]"
        ]
    }
}
