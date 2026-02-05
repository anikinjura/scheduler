# Реализация Google Sheets Uploader

## GoogleSheetsUploader

### Описание
`GoogleSheetsUploader` - реализация загрузчика для Google Sheets. Наследуется от `BaseReportUploader` и реализует специфичную логику для работы с Google Sheets API.

### Сигнатура
```python
class GoogleSheetsUploader(BaseReportUploader):
    def __init__(self, config: Optional[Dict[str, Any]] = None, logger=None):
```

### Параметры
- **config** (`Optional[Dict[str, Any]]`): конфигурация загрузчика
- **logger** (`Optional[Logger]`): объект логгера

### Методы
- `_establish_connection()` - устанавливает подключение к Google Sheets API
- `_close_connection()` - закрывает подключение к Google Sheets API
- `_perform_upload()` - выполняет загрузку данных в Google Sheets
- `_perform_upload_process()` - реализует основной процесс загрузки отчетов
- `upload_multiple_reports()` - загрузка нескольких отчетов
- `get_sheet_info()` - получение информации о таблице

### Особенности реализации
- Использует `GoogleSheetsReporter` для взаимодействия с Google Sheets API
- Поддерживает стратегии загрузки: `update_or_append`, `append_only`, `update_only`
- Обрабатывает конфигурацию таблицы через `TableConfig`
- Обеспечивает валидацию данных перед загрузкой
- Поддерживает работу с уникальными ключами для избежания дубликатов
- Использует механизм нормализации дат для корректного сопоставления уникальных ключей
- Применяет эффективный поиск по уникальным ключам с использованием `batch_get`