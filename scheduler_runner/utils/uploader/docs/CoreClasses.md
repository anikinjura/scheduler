# Базовые классы

## BaseUploader

### Описание
`BaseUploader` - базовый класс для загрузки данных в различные системы. Предоставляет основу для всех загрузчиков данных, реализуя общую функциональность подключения, аутентификации, обработки данных и загрузки.

### Сигнатура
```python
class BaseUploader:
    def __init__(self, config: Optional[Dict[str, Any]] = None, logger=None):
```

### Параметры
- **config** (`Optional[Dict[str, Any]]`): конфигурация загрузчика
- **logger** (`Optional[Logger]`): объект логгера

### Методы
- `__init__()` - инициализация загрузчика
- `connect()` - подключение к целевой системе
- `disconnect()` - отключение от целевой системы
- `upload_data()` - загрузка данных
- `batch_upload()` - пакетная загрузка данных
- `_validate_connection_params()` - валидация параметров подключения
- `_establish_connection()` - установление подключения (абстрактный метод)
- `_close_connection()` - закрытие подключения (абстрактный метод)
- `_validate_data()` - валидация данных
- `_transform_data_if_needed()` - трансформация данных при необходимости
- `_perform_upload()` - выполнение загрузки (абстрактный метод)
- `get_status()` - получение статуса загрузчика
- `retry_operation()` - выполнение операции с повторными попытками
- `upload_data()` - загрузка данных с поддержкой стратегий (update_or_append, append_only, update_only)

## BaseReportUploader

### Описание
`BaseReportUploader` - базовый класс для загрузки отчетов в различные системы. Расширяет `BaseUploader`, добавляя специфичную функциональность для работы с отчетами, включая загрузку из файлов, обработку метаданных и поддержку различных форматов.

### Сигнатура
```python
class BaseReportUploader(BaseUploader):
    def __init__(self, config: Optional[Dict[str, Any]] = None, logger=None):
```

### Параметры
- **config** (`Optional[Dict[str, Any]]`): конфигурация загрузчика отчетов
- **logger** (`Optional[Logger]`): объект логгера

### Методы
- `__init__()` - инициализация загрузчика отчетов
- `upload_report()` - загрузка отчета
- `load_report_from_file()` - загрузка отчета из файла
- `_add_report_metadata()` - добавление метаданных отчета
- `upload_report_from_file()` - загрузка отчета из файла в целевую систему
- `validate_report_structure()` - валидация структуры отчета
- `format_report_output()` - форматирование данных отчета
- `get_report_statistics()` - получение статистики по отчету
- `run_uploader()` - запуск процесса загрузки отчетов
- `_perform_upload_process()` - выполнение процесса загрузки (абстрактный метод)
- `_parse_arguments()` - разбор аргументов командной строки
- `_update_report_date()` - обновление даты отчета
- `_load_json_report()` - загрузка JSON отчета
- `_load_csv_report()` - загрузка CSV отчета
- `_load_xml_report()` - загрузка XML отчета
- `_format_as_json()` - форматирование в JSON
- `_format_as_csv()` - форматирование в CSV
- `_format_as_xml()` - форматирование в XML
- `_determine_action()` - определяет действие (update/append/skip) на основе стратегии и наличия существующей строки