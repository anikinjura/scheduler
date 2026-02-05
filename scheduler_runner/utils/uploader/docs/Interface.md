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