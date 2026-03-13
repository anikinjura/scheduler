# Метод `_build_url_filter()`

## Версия
**0.0.1**

## Описание
Метод `_build_url_filter()` формирует строку фильтра для URL на основе шаблонов из конфигурации и текущей даты выполнения. Метод используется для построения URL с параметрами фильтрации при навигации к целевым страницам.

## Сигнатура
```python
def _build_url_filter(self) -> str
```

## Возвращаемое значение
- **str**: Сформированная строка фильтра для добавления к URL

## Используемые параметры конфигурации
- **filter_template** (`str`): Шаблон общего фильтра в URL (например, '?filter={{{date_filter_template},{data_type_filter_template}}}')
- **date_filter_template** (`str`): Шаблон фильтра по дате (например, '"startDate":"{date}T00:00%2B03:00","endDate":"{date}T23:59%2B03:00"')
- **data_type_filter_template** (`str`): Шаблон фильтра по типу данных (например, '"operationTypes":["GiveoutAll"]')
- **execution_date** (`str`): Дата выполнения в формате, определенном в конфигурации (по умолчанию 'YYYY-MM-DD')

## Изменения в версии 0.0.1
- Метод не изменен, но используется в контексте мульти-шаговой обработки
- Сформированные URL влияют на формирование `__STEP_SOURCE_URL__` для каждого шага
- `__STEP_SOURCE_URL__` из всех шагов используется для формирования `__SOURCE_URL__` как общего префикса всех URL с помощью метода `_get_common_url_prefix()`

## Пример формирования фильтра
Если:
- `filter_template` = '?filter={{{date_filter_template},{data_type_filter_template}}}'
- `date_filter_template` = '"startDate":"{date}T00:00%2B03:00","endDate":"{date}T23:59%2B03:00"'
- `data_type_filter_template` = '"operationTypes":["GiveoutAll"]'
- `execution_date` = '2023-12-25'

Тогда результатом будет:
'?filter={"startDate":"2023-12-25T00:00%2B03:00","endDate":"2023-12-25T23:59%2B03:00","operationTypes":["GiveoutAll"]}'

## Особенности реализации
- Метод используется в `navigate_to_target()` для формирования URL с фильтрами
- Сформированные URL влияют на формирование `__STEP_SOURCE_URL__` для каждого шага
- `__STEP_SOURCE_URL__` из всех шагов используется для формирования `__SOURCE_URL__` как общего префикса всех URL с помощью метода `_get_common_url_prefix()`