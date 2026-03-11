# Интерфейс изолированного микросервиса

## upload_data()

### Описание
Загрузка одиночного набора данных в целевую систему.

### Сигнатура
```python
def upload_data(
    data: Dict[str, Any],
    connection_params: Dict[str, Any],
    logger=None,
    **kwargs
) -> Dict[str, Any]:
```

### Параметры
- **data** (`Dict[str, Any]`): данные для загрузки
- **connection_params** (`Dict[str, Any]`): параметры подключения
- **logger** (`Optional[Logger]`): объект логгера
- **kwargs**: дополнительные параметры
  - **strategy** (`str`): стратегия загрузки ('update_or_append', 'append_only', 'update_only')

### Возвращаемое значение
- **Dict[str, Any]**: результат загрузки с информацией об успехе/ошибке

### Пример использования
```python
from scheduler_runner.utils.uploader import upload_data

connection_params = {
    "CREDENTIALS_PATH": "path/to/credentials.json",
    "SPREADSHEET_ID": "your_spreadsheet_id",
    "WORKSHEET_NAME": "Sheet1",
    "TABLE_CONFIG": your_table_config_object
}

data = {
    "Дата": "2023-01-01",
    "ПВЗ": "Москва",
    "Количество выдач": 100
}

result = upload_data(
    data=data,
    connection_params=connection_params
)
```

## upload_batch_data()

### Описание
Пакетная загрузка данных в целевую систему.

### Сигнатура
```python
def upload_batch_data(
    data_list: List[Dict[str, Any]],
    connection_params: Dict[str, Any],
    logger=None,
    **kwargs
) -> Dict[str, Any]:
```

### Параметры
- **data_list** (`List[Dict[str, Any]]`): список данных для загрузки
- **connection_params** (`Dict[str, Any]`): параметры подключения
- **logger** (`Optional[Logger]`): объект логгера
- **kwargs**: дополнительные параметры
  - **strategy** (`str`): стратегия загрузки ('update_or_append', 'append_only', 'update_only')

### Возвращаемое значение
- **Dict[str, Any]**: результат пакетной загрузки с информацией об успехе/ошибке

### Пример использования
```python
data_list = [
    {"Дата": "2023-01-01", "ПВЗ": "Москва", "Количество выдач": 100},
    {"Дата": "2023-01-02", "ПВЗ": "Москва", "Количество выдач": 120}
]

result = upload_batch_data(
    data_list=data_list,
    connection_params=connection_params
)
```

## test_connection()

### Описание
Проверка подключения к целевой системе.

### Сигнатура
```python
def test_connection(
    connection_params: Dict[str, Any],
    logger=None
) -> Dict[str, Any]:
```

### Параметры
- **connection_params** (`Dict[str, Any]`): параметры подключения
- **logger** (`Optional[Logger]`): объект логгера

### Возвращаемое значение
- **Dict[str, Any]**: результат проверки подключения

### Пример использования
```python
from scheduler_runner.utils.uploader import test_connection

result = test_connection(connection_params)
if result["success"]:
    print("Подключение успешно")
else:
    print("Ошибка подключения")
```

## check_missing_items()

### Описание
Read-only проверка отсутствующих комбинаций ключей `unique_key_columns` в Google Sheets по фильтрам.

### Сигнатура
```python
def check_missing_items(
    filters: Dict[str, Any],
    connection_params: Dict[str, Any],
    logger=None,
    **kwargs
) -> Dict[str, Any]:
```

### Параметры
- **filters** (`Dict[str, Any]`): фильтры coverage-check
  - `date_range`: `"{col}_from"` и `"{col}_to"` в формате `YYYY-MM-DD`
  - `list`: `"{col}"` со списком значений
  - `value`: `"{col}"` с одним значением
- **connection_params** (`Dict[str, Any]`): параметры подключения
- **logger** (`Optional[Logger]`): объект логгера
- **kwargs**:
  - **strict_headers** (`bool`): при `False` ошибка по отсутствующим колонкам содержит список доступных заголовков
  - **max_scan_rows** (`Optional[int]`): ограничение диапазона чтения
  - **max_expected_keys** (`int`): safeguard для декартова произведения

### Возвращаемое значение
- **Dict[str, Any]**: результат coverage-check

Обязательные поля успешного ответа:
- `success`
- `action="coverage_check"`
- `data.filters_applied`
- `data.key_columns`
- `data.normalization_rules`
- `data.missing_items`
- `data.missing_by_key`
- `data.stats`
- `data.diagnostics`

Контракт значений:
- даты возвращаются в формате `DD.MM.YYYY`
- строковые ключи с `normalization="strip_lower_str"` возвращаются в нормализованном виде, например `cheboksary_340`

### Пример использования
```python
from scheduler_runner.utils.uploader import check_missing_items

filters = {
    "Дата_from": "2026-03-04",
    "Дата_to": "2026-03-10",
    "ПВЗ": "ЧЕБОКСАРЫ_340"
}

result = check_missing_items(
    filters=filters,
    connection_params=connection_params,
    strict_headers=True,
    max_expected_keys=100000
)
```

## Стратегии загрузки

### Описание
Микросервис поддерживает три стратегии загрузки данных:

- **update_or_append** (по умолчанию): обновляет строку, если найдена по уникальным ключам, иначе добавляет новую
- **append_only**: всегда добавляет новую строку, не проверяя наличие существующих
- **update_only**: обновляет строку, если найдена по уникальным ключам, иначе пропускает

### Пример использования стратегии
```python
# Обновить строку, если найдена по уникальным ключам, иначе добавить новую
result = upload_data(
    data=data,
    connection_params=connection_params,
    strategy="update_or_append"
)

# Всегда добавлять новую строку
result = upload_data(
    data=data,
    connection_params=connection_params,
    strategy="append_only"
)

# Обновить только если строка найдена, иначе пропустить
result = upload_data(
    data=data,
    connection_params=connection_params,
    strategy="update_only"
)
```
