# Метод `_extract_value_by_config()`

## Версия
**0.0.1**

## Описание
Метод `_extract_value_by_config()` извлекает значение с веб-страницы с использованием конфигурации извлечения. Метод универсален и может извлекать значения с использованием различных селекторов, типов элементов и регулярных выражений.

## Сигнатура
```python
def _extract_value_by_config(self, extraction_config)
```

## Параметры
- **extraction_config** (`dict`): Конфигурация извлечения, содержащая:
  - **selector** (`str`): Селектор элемента для извлечения (XPath или CSS)
  - **element_type** (`str`, optional): Тип элемента ('input', 'textarea', 'select', 'div', 'button', etc.), по умолчанию 'div'
  - **pattern** (`str`, optional): Регулярное выражение для извлечения части текста
  - **attribute** (`str`, optional): Имя атрибута для извлечения (если None, извлекается текст или значение)
  - **post_processing** (`dict`, optional): Параметры постобработки результата

## Возвращаемое значение
Извлеченное значение, возможно обработанное с помощью постобработки

## Изменения в версии 0.0.1
- Метод используется в различных обработчиках (_handle_simple_extraction, _handle_table_extraction, _handle_table_nested_extraction) для извлечения данных
- Результаты извлечения могут быть частью результатов шагов, которые содержат `__STEP_SOURCE_URL__`
- `__STEP_SOURCE_URL__` из всех шагов используется для формирования `__SOURCE_URL__` как общего префикса всех URL с помощью метода `_get_common_url_prefix()`

## Пример конфигурации
```python
extraction_config = {
    "selector": "//div[@class='count-display']",
    "element_type": "div",
    "pattern": r'Всего:\s*(\d+)',
    "post_processing": {
        "convert_to": "int",
        "default_value": 0
    }
}
```

## Особенности реализации
- Метод вызывается из различных обработчиков данных (_handle_simple_extraction, _handle_table_extraction, _handle_table_nested_extraction)
- Результаты извлечения могут быть частью результатов шагов, которые содержат `__STEP_SOURCE_URL__`
- `__STEP_SOURCE_URL__` из всех шагов используется для формирования `__SOURCE_URL__` как общего префикса всех URL с помощью метода `_get_common_url_prefix()`