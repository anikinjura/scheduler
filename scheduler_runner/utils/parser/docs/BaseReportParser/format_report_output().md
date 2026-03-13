# Метод `format_report_output()`

## Версия
**0.0.1**

## Описание
Метод `format_report_output()` форматирует данные отчета в указанный формат (JSON, CSV, XML и др.). Метод поддерживает различные форматы вывода и позволяет настраивать структуру выходных данных.

## Сигнатура
```python
def format_report_output(
    self,
    data: Dict[str, Any],
    output_format: str = 'json',
    encoding: str = 'utf-8',
    xml_config: Optional[Dict[str, Any]] = None
) -> Union[str, bytes]
```

## Параметры
- **data** (`Dict[str, Any]`): Данные отчета для форматирования
- **output_format** (`str`, optional): Формат вывода ('json', 'csv', 'xml', 'txt'). По умолчанию 'json'
- **encoding** (`str`, optional): Кодировка для текстовых форматов. По умолчанию 'utf-8'
- **xml_config** (`Optional[Dict[str, Any]]`, optional): Конфигурация для форматирования в XML, содержащая:
  - **root_element** (`str`): Имя корневого элемента XML (по умолчанию 'report')
  - **item_prefix** (`str`): Префикс для элементов списка (по умолчанию 'item_')

## Возвращаемое значение
Отформатированные данные отчета в виде строки или байтов в зависимости от формата

## Изменения в версии 0.0.1
- Метод не изменен, но используется для форматирования данных отчета, которые содержат `__SOURCE_URL__`
- `__SOURCE_URL__` формируется как общий префикс всех `__STEP_SOURCE_URL__` из результатов шагов с помощью метода `_get_common_url_prefix()`
- Данные отчета, содержащие `__SOURCE_URL__`, форматируются этим методом

## Примеры использования
```python
# Форматирование данных отчета в JSON
data = {
    "source_url": "https://turbo-pvz.ozon.ru",
    "summary": {
        "giveout": 238,
        "direct_flow_total": 12
    }
}

formatted_data = parser.format_report_output(data, 'json')
```

## Особенности реализации
- Метод форматирует данные отчета, которые могут содержать `__SOURCE_URL__`
- `__SOURCE_URL__` формируется как общий префикс всех `__STEP_SOURCE_URL__` из результатов шагов с помощью метода `_get_common_url_prefix()`
- Поддерживает различные форматы вывода: JSON, CSV, XML, TXT
- Использует кодировку UTF-8 по умолчанию