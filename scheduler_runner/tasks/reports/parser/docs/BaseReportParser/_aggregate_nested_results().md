# Метод `_aggregate_nested_results()`

## Версия
**0.0.1**

## Описание
Метод `_aggregate_nested_results()` реализует агрегацию результатов вложенной обработки. Метод суммирует или иным образом объединяет значения из результатов вложенной обработки в соответствии с конфигурацией агрегации.

## Сигнатура
```python
def _aggregate_nested_results(self, nested_results, aggregation_config)
```

## Параметры
- **nested_results** (`list`): Список результатов вложенной обработки, каждый элемент - результат обработки одного идентификатора
- **aggregation_config** (`dict`): Конфигурация агрегации, содержащая:
  - **method** (`str`): Метод агрегации ('sum', 'average', 'count', 'max', 'min')
  - **target_field** (`str`): Имя поля, в которое будут агрегироваться значения
  - **source_field** (`str`, optional): Имя поля в результатах для агрегации (по умолчанию 'value')

## Возвращаемое значение
Агрегированные результаты с добавленным полем, содержащим агрегированное значение

## Изменения в версии 0.0.1
- Метод используется в обработке результатов вложенной обработки (_handle_table_nested_extraction)
- Результаты агрегации могут быть частью результатов шагов, которые содержат `__STEP_SOURCE_URL__`
- `__STEP_SOURCE_URL__` из всех шагов используется для формирования `__SOURCE_URL__` как общего префикса всех URL с помощью метода `_get_common_url_prefix()`

## Пример конфигурации
```python
nested_results = [
    {"identifier": "001", "value": 10, "url": "..."},
    {"identifier": "002", "value": 20, "url": "..."},
    {"identifier": "003", "value": 15, "url": "..."}
]

aggregation_config = {
    "method": "sum",
    "target_field": "total_value",
    "source_field": "value"
}

result = parser._aggregate_nested_results(nested_results, aggregation_config)
# Результат: {"total_value": 45, "details": [...]}
```

## Особенности реализации
- Метод вызывается из `_handle_table_nested_extraction()` для агрегации результатов вложенной обработки
- Результаты агрегации могут быть частью результатов шагов, которые содержат `__STEP_SOURCE_URL__`
- `__STEP_SOURCE_URL__` из всех шагов используется для формирования `__SOURCE_URL__` как общего префикса всех URL с помощью метода `_get_common_url_prefix()`