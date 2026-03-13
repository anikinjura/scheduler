# Метод `_handle_table_nested_extraction()`

## Версия
**0.0.1**

## Описание
Метод `_handle_table_nested_extraction()` реализует логику извлечения данных для вложенного табличного типа обработки (table_nested). Метод сначала извлекает данные из таблицы, затем для каждого идентификатора из указанной колонки выполняет дополнительные запросы и извлекает данные из соответствующих страниц.

## Сигнатура
```python
def _handle_table_nested_extraction(self, step_config)
```

## Параметры
- **step_config** (`dict`): Конфигурация шага, содержащая:
  - **table_processing** (`dict`): Конфигурация табличной обработки, содержащая:
    - **enabled** (`bool`): Флаг включения табличной обработки
    - **table_config_key** (`str`): Ключ конфигурации таблицы в основной конфигурации
    - **id_column** (`str`): Имя колонки, содержащей идентификаторы для вложенной обработки
    - **result_mapping** (`dict`, optional): Карта сопоставления результатов для переименования полей
  - **nested_processing** (`dict`): Конфигурация вложенной обработки, содержащая:
    - **enabled** (`bool`): Флаг включения вложенной обработки
    - **base_url_template** (`str`): Шаблон URL для вложенных вызовов с плейсхолдерами
    - **filter_template** (`str`): Шаблон фильтра для вложенных вызовов
    - **data_extraction** (`dict`): Параметры извлечения данных из вложенных вызовов
    - **aggregation** (`dict`, optional): Параметры агрегации результатов, содержащие:
      - **method** (`str`): Метод агрегации ('sum', 'average', 'count', etc.)
      - **target_field** (`str`): Поле, в которое будут агрегироваться результаты

## Возвращаемое значение
Агрегированные результаты вложенной обработки

## Изменения в версии 0.0.1
- Результат обработки шага теперь может содержать `__STEP_SOURCE_URL__`, который добавляется в `_execute_single_step()`
- Этот `__STEP_SOURCE_URL__` используется в `_combine_step_results()` для формирования `__SOURCE_URL__` как общего префикса всех URL из результатов шагов

## Пример конфигурации
```python
step_config = {
    "table_processing": {
        "enabled": True,
        "table_config_key": "carriages_table",
        "id_column": "carriage_number",
        "result_mapping": {}
    },
    "nested_processing": {
        "enabled": True,
        "base_url_template": "https://turbo-pvz.ozon.ru/outbound/carriages-archive/{carriage_id}",
        "filter_template": "?filter={{{data_type_filter_template}}}",
        "data_extraction": {
            "selector": "//div[@class='count-display']",
            "pattern": r'Найдено:\s*(\d+)',
            "element_type": "div",
            "post_processing": {
                "convert_to": "int",
                "default_value": 0
            }
        },
        "aggregation": {
            "method": "sum",
            "target_field": "total_carriages"
        }
    }
}
```

## Особенности реализации
- Метод вызывается из `_execute_single_step()` при `processing_type` равном "table_nested"
- Результат обработки может содержать `__STEP_SOURCE_URL__`, который добавляется в `_execute_single_step()`
- `__STEP_SOURCE_URL__` из всех шагов используется для формирования `__SOURCE_URL__` как общего префикса всех URL с помощью метода `_get_common_url_prefix()`