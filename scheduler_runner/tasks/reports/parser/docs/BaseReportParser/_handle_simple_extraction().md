# Метод `_handle_simple_extraction()`

## Версия
**0.0.1**

## Описание
Метод `_handle_simple_extraction()` реализует логику извлечения данных для простого типа обработки (simple). Метод использует конфигурацию извлечения данных для получения значения с веб-страницы и применяет постобработку к результату.

## Сигнатура
```python
def _handle_simple_extraction(self, step_config)
```

## Параметры
- **step_config** (`dict`): Конфигурация шага, содержащая:
  - **data_extraction** (`dict`): Конфигурация извлечения данных, содержащая:
    - **selector** (`str`): Селектор элемента для извлечения (XPath или CSS)
    - **pattern** (`str`, optional): Регулярное выражение для извлечения части текста
    - **element_type** (`str`, optional): Тип элемента (по умолчанию 'div')
    - **post_processing** (`dict`, optional): Параметры постобработки, содержащие:
      - **convert_to** (`str`, optional): Тип для преобразования ('int', 'float', 'str')
      - **default_value** (`any`, optional): Значение по умолчанию в случае ошибки

## Возвращаемое значение
Извлеченные данные после постобработки

## Изменения в версии 0.0.1
- Результат обработки шага теперь может содержать `__STEP_SOURCE_URL__`, который добавляется в `_execute_single_step()`
- Этот `__STEP_SOURCE_URL__` используется в `_combine_step_results()` для формирования `__SOURCE_URL__` как общего префикса всех URL из результатов шагов

## Пример конфигурации
```python
step_config = {
    "data_extraction": {
        "selector": "//div[@class='count-display']",
        "pattern": r'Всего:\s*(\d+)',
        "element_type": "div",
        "post_processing": {
            "convert_to": "int",
            "default_value": 0
        }
    }
}
```

## Особенности реализации
- Метод вызывается из `_execute_single_step()` при `processing_type` равном "simple"
- Результат обработки может содержать `__STEP_SOURCE_URL__`, который добавляется в `_execute_single_step()`
- `__STEP_SOURCE_URL__` из всех шагов используется для формирования `__SOURCE_URL__` как общего префикса всех URL с помощью метода `_get_common_url_prefix()`