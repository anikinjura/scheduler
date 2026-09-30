"""
Конфигурационный файл для многошагового парсера Ozon

=== ОПИСАНИЕ КОНФИГУРАЦИИ ===

Этот файл содержит параметры для многошагового процесса сбора данных:
1. Данные по "giveout" - подотчет по выдачам
2. Данные по "Direct flow" - подотчет прямому потоку
3. Данные по "Return flow" - подотчет возвратному потоку

Конфигурация наследует все параметры из OZON_BASE_CONFIG и переопределяет
только те, что необходимы для многошаговой обработки.
"""
__version__ = '1.0.0'

from ..base_configs.ozon_report_config import OZON_BASE_CONFIG

# Конфигурация для многошагового парсера Ozon
MULTI_STEP_OZON_CONFIG = {
    **OZON_BASE_CONFIG,  # Наследуем все параметры из базовой конфигурации Ozon

    # === ОСНОВНЫЕ ПАРАМЕТРЫ ===
    "report_type": "multi_step_ozon_report",

    # === НАСТРОЙКИ ВЫВОДА ===
    "output_config": {
        **OZON_BASE_CONFIG.get('output_config', {}),
        "dir": "./reports/multi_step_ozon",  # Директория для сохранения отчетов
    },

    # === КОНФИГУРАЦИЯ МУЛЬТИ-ШАГОВОЙ ОБРАБОТКИ ===
    "multi_step_config": {
        # Список шагов обработки в порядке выполнения
        "steps": [
            "giveout",      # Подотчет по выдачам
            "direct_flow",  # Подотчет прямому потоку
            "return_flow"   # Подотчет возвратному потоку
        ],

        # Конфигурации для каждого шага
        "step_configurations": {
            "giveout": {
                # Общие параметры для навигации
                "base_url": "https://turbo-pvz.ozon.ru/reports/giveout",
                "filter_template": "?filter={{{date_filter_template},{data_type_filter_template}}}",
                "date_filter_template": "\"startDate\":\"{date}T00:00%2B03:00\",\"endDate\":\"{date}T23:59%2B03:00\"",
                "data_type_filter_template": "\"operationTypes\":[\"GiveoutAll\"]",

                # Параметры обработки данных
                "processing_type": "simple",  # Тип обработки: simple, table, table_nested

                # Параметры для простой обработки
                "data_extraction": {
                    "type": "simple",
                    "selector": "//div[contains(@class, 'ozi__text-view__caption-medium__') and contains(normalize-space(.), 'Найдено')]",  # без хэша класса: он меняется при пересборке фронтенда Ozon
                    "pattern": r'Найдено:\s*(\d+)',                                                       # Паттерн для "Найдено: N"
                    "element_type": "div",
                    # Не required: при 0 выдач Ozon не показывает счетчик «Найдено» вовсе, и в разметке настоящий 0
                    # неотличим от незагрузившейся страницы (проверено 30.09.2026 на дате без данных). Надежно — только
                    # в режиме API (logV2.totalCount = 0 явно). Счетчик на странице перевозки — required (он всегда > 0).
                    "post_processing": {
                        "convert_to": "int",
                        "default_value": 0
                    }
                },
                "result_key": "giveout_count",

                # Извлечение из ответа API (DATA_SOURCE_MODE != "dom", docs/MODERNIZATION_PLAN.md)
                "api_extraction": {
                    "type": "value",
                    "request": {
                        "path": "/api2/reports/give_out/logV2",
                        "query_contains": {"startDate": "{date}T00:00+03:00", "operationTypes": "GiveoutAll"},
                    },
                    "value_path": "logV2.totalCount",
                    "post_processing": {"convert_to": "int"},  # без default_value: нет поля — ошибка, а не 0
                },
            },
            "direct_flow": {
                # Общие параметры для навигации
                "base_url": "https://turbo-pvz.ozon.ru/outbound/carriages-archive",
                "filter_template": "?filter={%22{date_filter_template}%2C%22{data_type_filter_template}%22}",
                "date_filter_template": 'startSentMoment%22:%22{date}T00:00:00%2B03:00%22%2C%22endSentMoment%22:%22{date}T23:59:59%2B03:00%22', # Шаблон фильтра по дате
                "data_type_filter_template": 'flowType%22:%22Direct', # Шаблон фильтра по типу данных (Прямой поток)

                # Параметры обработки данных
                "processing_type": "table_nested",              # Тип обработки: simple, table, table_nested

                # Параметры для табличной обработки
                "table_processing": {
                    "enabled": True,                            # Включить табличную обработку
                    "table_config_key": "carriages_table",      # Ключ конфигурации обрабатываемой таблицы
                    "id_column": "carriage_number",             # Имя колонки, содержащей идентификаторы для вложенной обработки
                    "result_mapping": {                         # Карта сопоставления результатов (опционально)
                        # Пока пустой объект - используется для сопоставления полей результата
                        # Примеры использования:
                        # "original_field_name": "new_field_name" - переименование поля
                        # "calculated_field": "formula" - вычисляемое поле
                    }
                },

                # Параметры для вложенной обработки
                "nested_processing": {
                    "enabled": True,
                    # strict: таблица не найдена, страница перевозки не открылась или счетчик пуст — ошибка шага,
                    # а не молчаливый пропуск перевозки (занижение суммы). Пустая таблица (0 перевозок) — честный 0.
                    "strict": True,
                    "base_url_template": "https://turbo-pvz.ozon.ru/outbound/carriages-archive/{carriage_id}",
                    "filter_template": "?filter={{{data_type_filter_template}}}",
                    "data_type_filter_template": '"articleState":"Took","articleType":"ArticlePosting"',

                    "data_extraction": {
                        "selector": "//div[contains(@class, 'ozi__text-view__caption-medium__') and contains(normalize-space(.), 'Найдено')]",  # без хэша класса: он меняется при пересборке фронтенда Ozon
                        "pattern": r'Найдено:\s*(\d+)',
                        "element_type": "div",
                        "required": True,
                        "post_processing": {
                            "convert_to": "int",
                            "default_value": 0
                        }
                    },
                    "aggregation": {
                        "method": "sum",                    # Метод агрегации: sum, average, count, etc.
                        "target_field": "total_carriages"  # Поле, в которое будут агрегироваться результаты
                    }
                },
                "result_key": "direct_flow_data",

                # Извлечение из ответов API: список перевозок, затем число отправлений на странице каждой перевозки
                "api_extraction": {
                    "type": "list_nested",
                    "list_request": {
                        "path": "/api2/reports/CarriageReport/GetCarriages",
                        "query_contains": {"startSentMoment": "{date}T00:00:00+03:00", "flowType": "Direct"},
                    },
                    "items_path": "carriages",
                    "id_field": "carriageId",
                    "total_path": "totalCount",
                    "nested_request": {
                        "path": "/api2/reports/CarriageReport/GetCarriageArticles",
                        "query_contains": {"carriageId": "{identifier}", "articleState": "Took", "articleType": "ArticlePosting"},
                    },
                    "nested_value_path": "totalCount",
                    "post_processing": {"convert_to": "int"},
                },
            },
            "return_flow": {
                # Общие параметры для навигации
                "base_url": "https://turbo-pvz.ozon.ru/outbound/carriages-archive",
                "filter_template": "?filter={%22{date_filter_template}%2C%22{data_type_filter_template}%22}",
                "date_filter_template": 'startSentMoment%22:%22{date}T00:00:00%2B03:00%22%2C%22endSentMoment%22:%22{date}T23:59:59%2B03:00%22', # Шаблон фильтра по дате
                "data_type_filter_template": 'flowType%22:%22Return', # Шаблон фильтра по типу данных (Обратный поток)

                # Параметры обработки данных
                "processing_type": "table_nested",  # Тип обработки: simple, table, table_nested

                # Параметры для табличной обработки
                "table_processing": {
                    "enabled": True,
                    "table_config_key": "carriages_table",
                    "id_column": "carriage_number",  # Имя колонки, содержащей идентификаторы для вложенной обработки
                    "result_mapping": {              # Карта сопоставления результатов (опционально)
                        # Пока пустой объект - используется для сопоставления полей результата
                        # Примеры использования:
                        # "original_field_name": "new_field_name" - переименование поля
                        # "calculated_field": "formula" - вычисляемое поле
                    }
                },

                # Параметры для вложенной обработки
                "nested_processing": {
                    "enabled": True,
                    # strict: таблица не найдена, страница перевозки не открылась или счетчик пуст — ошибка шага,
                    # а не молчаливый пропуск перевозки (занижение суммы). Пустая таблица (0 перевозок) — честный 0.
                    "strict": True,
                    "base_url_template": "https://turbo-pvz.ozon.ru/outbound/carriages-archive/{carriage_id}",
                    "filter_template": "?filter={{{data_type_filter_template}}}",
                    "data_type_filter_template": '"articleState":"Took","articleType":"ArticlePosting"',

                    "data_extraction": {
                        "selector": "//div[contains(@class, 'ozi__text-view__caption-medium__') and contains(normalize-space(.), 'Найдено')]",  # без хэша класса: он меняется при пересборке фронтенда Ozon
                        "pattern": r'Найдено:\s*(\d+)',
                        "element_type": "div",
                        "required": True,
                        "post_processing": {
                            "convert_to": "int",
                            "default_value": 0
                        }
                    },
                    "aggregation": {
                        "method": "sum",                    # Метод агрегации: sum, average, count, etc.
                        "target_field": "total_carriages"  # Поле, в которое будут агрегироваться результаты
                    }
                },
                "result_key": "return_flow_data",

                # Извлечение из ответов API: список перевозок, затем число отправлений на странице каждой перевозки
                "api_extraction": {
                    "type": "list_nested",
                    "list_request": {
                        "path": "/api2/reports/CarriageReport/GetCarriages",
                        "query_contains": {"startSentMoment": "{date}T00:00:00+03:00", "flowType": "Return"},
                    },
                    "items_path": "carriages",
                    "id_field": "carriageId",
                    "total_path": "totalCount",
                    "nested_request": {
                        "path": "/api2/reports/CarriageReport/GetCarriageArticles",
                        "query_contains": {"carriageId": "{identifier}", "articleState": "Took", "articleType": "ArticlePosting"},
                    },
                    "nested_value_path": "totalCount",
                    "post_processing": {"convert_to": "int"},
                },
            }
        },

        # Логика агрегации результатов
        "aggregation_logic": {
            "combine_nested_results": True,
            "sum_nested_values": [],  # Пустой массив - не суммируем никакие значения на этом уровне
            "result_structure": {
                "location_info": "{__LOCATION_INFO__}",
                "extraction_timestamp": "{__EXTRACTION_TIMESTAMP__}",
                "source_url": "{__SOURCE_URL__}",
                "execution_date": "{__EXECUTION_DATE__}",
                "summary": {
                    "giveout": "{giveout_count}",               # Значение, извлеченное на шаге giveout
                    "direct_flow_total": "{direct_flow_data}",  # Значение, извлеченное на шаге direct_flow
                    "return_flow_total": "{return_flow_data}"   # Значение, извлеченное на шаге return_flow
                }
            }
        }
    },

    # === ДОПОЛНИТЕЛЬНЫЕ ПАРАМЕТРЫ ===
    # Дата с ошибкой хотя бы одного шага — ошибка даты (не выгружается, повтор следующим запуском),
    # а не частичный результат с пустой ячейкой, который coverage-check больше не соберет
    "FAIL_DATE_ON_STEP_ERROR": True,
    "BROWSER_CLOSE_DELAY": 5,  # Задержка перед закрытием браузера для наблюдения (секунды)
}