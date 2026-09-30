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
- `batch_upload(data_list, upload_mode=None, strategy=...)` - пакетная загрузка; режим из `upload_mode` или `config["UPLOAD_MODE"]`:
  - `"row"` (по умолчанию) — построчно через базовый `BaseUploader.batch_upload` (~10 запросов на строку);
  - `"batch"` — `GoogleSheetsReporter.upsert_rows_batch` (1 чтение и до 2 записей на пакет, см. `Providers/GoogleSheets/Components.md`).
    В отличие от режима `row`, `success` равен `False`, если не сохранилась хотя бы одна запись, и заполняется `error`
    (текст первой ошибки — по нему вызывающий код решает о повторе, например при 429)
- `check_missing_items()` - делегирует read-only coverage-check в `GoogleSheetsReporter`
- `_perform_upload_process()` - реализует основной процесс загрузки отчетов
- `upload_multiple_reports()` - загрузка нескольких отчетов
- `get_sheet_info()` - получение информации о таблице

### Особенности реализации
- Использует `GoogleSheetsReporter` для взаимодействия с Google Sheets API
- Поддерживает стратегии загрузки: `update_or_append`, `append_only`, `update_only`
- Поддерживает read-only API `check_missing_items()` для поиска отсутствующих ключей
- Обрабатывает конфигурацию таблицы через `TableConfig`
- Обеспечивает валидацию данных перед загрузкой
- Поддерживает работу с уникальными ключами для избежания дубликатов
- Использует механизм нормализации дат для корректного сопоставления уникальных ключей
- Применяет эффективный поиск по уникальным ключам с использованием `batch_get`
- Прокидывает `strict_headers`, `max_scan_rows`, `max_expected_keys` в provider-логику coverage-check
- При добавлении новых строк с формулами (`_append_new_row`):
  - Сначала добавляет данные без формул (placeholder для FORMULA-колонок)
  - Извлекает реальный номер строки из ответа Google API (`updatedRange`)
  - Fallback: поиск по уникальным ключам если API не вернул номер
  - Обновляет формульные ячейки с правильным номером строки
