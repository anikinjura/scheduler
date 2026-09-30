# Компоненты Google Sheets

## GoogleSheetsReporter

### Описание
`GoogleSheetsReporter` - основной класс для работы с Google Sheets. Обеспечивает подключение к Google Sheets API, чтение, запись и обновление данных в таблицах.

### Сигнатура
```python
class GoogleSheetsReporter:
    def __init__(self, credentials_path: str, spreadsheet_name: str, worksheet_name: str, table_config: TableConfig):
```

### Параметры
- **credentials_path** (`str`): путь к файлу учетных данных
- **spreadsheet_name** (`str`): ID или имя таблицы
- **worksheet_name** (`str`): имя листа
- **table_config** (`TableConfig`): конфигурация структуры таблицы

### Методы
- `update_or_append_data_with_config()` - универсальный метод для обновления/добавления данных
- `check_missing_items()` - основной read-only метод coverage-check
- `get_table_headers()` - возвращает заголовки таблицы
- `get_last_row_with_data()` - определяет последнюю строку с данными
- `get_row_by_id()` - находит строку по ID
- `get_rows_by_unique_keys()` - находит строки по уникальным ключам
- `_find_rows_by_unique_keys_batch()` - находит строки по уникальным ключам с использованием batch_get для лучшей производительности
- `_append_new_row()` - добавляет новую строку с формулами, извлекая реальный номер строки из ответа API
- `_update_existing_row()` - обновляет существующую строку с формулами
- `_prepare_row_values()` - подготавливает значения строки с учётом формул
- `_normalize_date_format()` - нормализует формат даты к единому формату DD.MM.YYYY для сравнения
- `_normalize_value()` - локальная нормализация coverage-check для не-дата значений
- `_normalize_for_comparison()` - нормализует значение для сравнения в логике поиска
- `_prepare_value_for_search()` - подготавливает значение для поиска в таблице, нормализует формат дат/чисел/строк

### Coverage-check

`GoogleSheetsReporter.check_missing_items()`:
- использует `TableConfig` и `coverage_filter` метаданные колонок;
- читает данные через один `worksheet.batch_get(...)` на все coverage-колонки;
- валидирует покрытие `unique_key_columns`;
- возвращает результат в нормализованном виде:
  - дата: `DD.MM.YYYY`
  - строковые ключи с `strip_lower_str`: нормализованная строка, например `cheboksary_340`;
- группирует `missing_by_key` по `unique_key_columns[0]`;
- пишет operational metrics в `stats` и диагностические данные в `diagnostics`.

## TableConfig

### Описание
`TableConfig` - конфигурация структуры таблицы Google Sheets. Определяет имена колонок, типы данных, уникальные ключи и другие параметры.

### Сигнатура
```python
@dataclass
class TableConfig:
    worksheet_name: str
    columns: List[ColumnDefinition]
    id_column: str = "id"
    unique_key_columns: Optional[List[str]] = None
    id_formula_template: Optional[str] = None
    header_row: int = 1
```

### Параметры
- **worksheet_name** (`str`): имя листа в таблице
- **columns** (`List[ColumnDefinition]`): список определений колонок
- **id_column** (`str`): имя колонки, содержащей идентификатор записи
- **unique_key_columns** (`Optional[List[str]]`): список колонок, формирующих уникальный ключ
- **id_formula_template** (`Optional[str]`): шаблон формулы для вычисления ID
- **header_row** (`int`): номер строки с заголовками

## ColumnDefinition

### Описание
`ColumnDefinition` - определение отдельной колонки таблицы. Определяет имя, тип, обязательность и другие характеристики колонки.

### Сигнатура
```python
@dataclass
class ColumnDefinition:
    name: str
    column_type: ColumnType = ColumnType.DATA
    required: bool = False
    formula_template: Optional[str] = None
    unique_key: bool = False
    data_key: Optional[str] = None
    column_letter: Optional[str] = None
    coverage_filter: bool = False
    coverage_filter_type: Optional[str] = None
    date_input_format: Optional[str] = None
    date_output_format: Optional[str] = None
    normalization: Optional[str] = None
```

### Параметры
- **name** (`str`): имя колонки
- **column_type** (`ColumnType`): тип колонки (DATA, FORMULA, CALCULATED, IGNORE)
- **required** (`bool`): обязательна ли колонка
- **formula_template** (`Optional[str]`): шаблон формулы для колонок типа FORMULA
- **unique_key** (`bool`): является ли колонка частью уникального ключа
- **data_key** (`Optional[str]`): ключ в данных, если отличается от имени
- **column_letter** (`Optional[str]`): буква колонки (A, B, C)
- **coverage_filter** (`bool`): участвует ли колонка в coverage-check
- **coverage_filter_type** (`Optional[str]`): тип фильтра (`date_range`, `list`, `value`)
- **date_input_format** (`Optional[str]`): формат входной даты
- **date_output_format** (`Optional[str]`): формат выходной даты
- **normalization** (`Optional[str]`): тип нормализации значения для coverage-check

## ColumnType

### Описание
`ColumnType` - перечисление типов колонок в таблице Google Sheets.

### Значения
- **DATA** - простые данные
- **FORMULA** - колонки с формулами Google Sheets
- **CALCULATED** - вычисляемые значения на стороне Python-скрипта
- **IGNORE** - колонки, которые игнорируются при операциях записи

## Механизм сопоставления уникальных ключей

### Описание
Механизм сопоставления уникальных ключей позволяет избежать дубликатов при загрузке данных в Google Sheets. Он работает следующим образом:

1. При использовании стратегии `update_or_append` система сначала ищет существующие строки по уникальным ключам
2. Уникальные ключи определяются в конфигурации таблицы (`TableConfig.unique_key_columns`)
3. Для поиска используется метод `_find_rows_by_unique_keys_batch()`, который применяет `batch_get` для эффективного получения данных
4. Значения нормализуются с помощью `_normalize_for_comparison()` и `_prepare_value_for_search()` для корректного сравнения
5. Если строка с такими же уникальными ключами найдена, выполняется обновление, иначе - добавление новой строки

**Ошибка API при поиске не равна «строка не найдена».** `gspread.exceptions.APIError` в `_find_rows_by_unique_keys_batch()`
и `get_rows_by_unique_keys()` пробрасывается наверх, и `update_or_append_data_with_config()` возвращает ошибку операции.
Раньше ошибка чтения (например, 429 по квоте) превращалась в пустой результат, и upsert добавлял дубликат вместо обновления.
Прочие исключения по-прежнему логируются и дают «не найдено».

### Поиск по ключам (`_find_rows_by_unique_keys_batch`)

Метод определяет последнюю строку для чтения, сканируя **DATA-колонки** (не формульные), чтобы корректно учитывать строки с предзаполненными формулами:

1. Ищет первую `DATA`-колонку с `required=True` из конфигурации
2. Если не найдена — первую `DATA`-колонку без `required`
3. Вызывает `get_last_row_with_data(column_index=...)` по найденной колонке
4. Добавляет буфер (+50 строк) для свежих данных
5. Выполняет `batch_get` по диапазонам ключевых колонок
6. Сравнивает нормализованные значения с ожидаемыми

### Добавление новых строк (`_append_new_row`)

При добавлении новой строки с формулами метод работает в 4 шага:

1. **Подготовка без формул** — создаёт значения с пустыми placeholder-ами для `FORMULA`-колонок (т.к. номер строки неизвестен)
2. **append_rows** — Google API сам определяет, куда добавить строку (после последней строки с контентом, включая формулы)
3. **Определение номера строки** — извлекает реальный номер из `response.updates.updatedRange` (формат `"Sheet1!A1001:K1001"`). Fallback: поиск по уникальным ключам через `get_rows_by_unique_keys()`
4. **Обновление формул** — подставляет правильные формулы с реальным номером строки (например `=B1001&C1001` вместо `=B2&C2`)

### Функции нормализации
- `_normalize_date_format()` - преобразует различные форматы дат (строки, числа, datetime) в единый формат DD.MM.YYYY
- `_normalize_for_comparison()` - нормализует значение для сравнения, особенно важно для дат, хранящихся как серийные числа в Google Sheets
- `_prepare_value_for_search()` - подготавливает значения для поиска, применяя нормализацию

### Функция `_index_to_column_letter`
Функция `_index_to_column_letter()` преобразует числовой индекс колонки в буквенное обозначение (A, B, C...), что необходимо для формирования диапазонов при использовании `batch_get`.

## Пакетный upsert (`upsert_rows_batch`)

`GoogleSheetsReporter.upsert_rows_batch(data_list, config=None, strategy="update_or_append")` — upsert пакета строк
за константное число запросов (заголовок уже в кэше подключения):

| Шаг | Запрос |
|---|---|
| Ключевые колонки и `timestamp` открытыми диапазонами (`B2:B`, `C2:C`, `K2:K`) | 1 `batch_get` (чтение) |
| Все новые строки без формул | 1 `append_rows` (запись) |
| Полные строки обновляемых записей + формулы добавленных строк с номерами из `updatedRange` | 1 `batch_update` (запись) |

Подготовка (`_prepare_data_for_table`), валидация (`_validate_data_for_config`), нормализация ключей
(`_normalize_for_comparison`/`_prepare_value_for_search`) и состав строк — те же, что у построчного
`update_or_append_data_with_config`; при обновлении `timestamp` строки сохраняется.

Поведение на краях:
- одинаковый ключ во входных данных — побеждает последняя запись, предыдущие получают `action="superseded"`;
- повторяющийся ключ в листе — используется первая строка, предупреждение `KPI_BATCH_DUPLICATE_ROWS`;
- `updatedRange` не разобран или число строк не совпало — повторное чтение ключей (+1 чтение), `append_range_fallback`;
- ошибка чтения ключей или `append_rows` — все затронутые записи с ошибкой, записей в лист нет / нет новых строк;
- ошибка `batch_update` после `append_rows` — строки добавлены без формул и возвращаются как ошибка; повтор загрузки
  найдет их по ключам и допишет формулы обновлением (операция идемпотентна).

Результат совместим с `BaseUploader.batch_upload` (`success`, `uploaded`, `failed`, `details[]`) и дополнен `error`
и `diagnostics` (`appended`, `updated`, `skipped`, `superseded`, `duplicate_sheet_rows`, `append_range_fallback`).

Проверка на тестовой таблице: `python -m scheduler_runner.tasks.reports.tests.run_kpi_batch_upload_smoke`
(7 новых строк — 4 чтения и 2 записи с подключением, повтор — 4 чтения и 1 запись, два процесса одновременно).

## Квота Google Sheets API и повторы запросов

### Квота
По умолчанию Sheets API допускает **60 запросов чтения и 60 записи в минуту на пользователя**. Пользователь здесь —
сервисный аккаунт, а все ПВЗ пишут одним ключом, поэтому квота **общая для всех ПВЗ** и исчерпывается, когда они
выгружают данные в одну минуту (задача ReportsProcessor стартует на всех ПВЗ в 21:30). Превышение — `APIError [429]
Quota exceeded for quota metric 'Read requests' ...`.

Текущая стоимость upsert одной строки KPI (построчная загрузка): ~4 чтения (заголовок, последняя строка, `batch_get`,
заголовок при добавлении) и ~6 записей (`append_rows` + 5 формульных ячеек по одной). Сокращение до пакета на ПВЗ —
отдельная задача (см. документацию задачи reports).

### `QuotaBackoffHTTPClient`
Транспортный HTTP-клиент gspread, подключаемый в `GoogleSheetsReporter.__init__`
(`gspread.authorize(credentials, http_client=QuotaBackoffHTTPClient)`). Повторяет **любой** запрос к Google Sheets —
чтение, запись, открытие таблицы — поэтому покрывает upsert, coverage-check и `KPI_FAILOVER_STATE`.

| Ошибка | Пауза перед повтором |
|---|---|
| 429 (квота) | `quota_delay_seconds` = 65 с + случайно до 15 с (квота считается поминутно) |
| 408, 500, 502, 503, 504, `ConnectionError`, `Timeout` | 5, 10, 20, 40 с (не больше 60) + до 15 с |
| прочие (400, 403, 404 …) | без повтора |

Попыток — `max_attempts` = 5. В лог пишется `GOOGLE_SHEETS_RETRY: HTTP 429 на <метод> запросе, попытка N/5, пауза N с`.

Встроенный `gspread.BackOffHTTPClient` не используется: он помечен как не готовый к production, хранит счетчик на
уровне класса и доходит до паузы в минуту только к 5-й попытке.

Декоратор `retry_on_api_error` (на `_update_existing_row`, `_append_new_row` и др.) коды, которые уже повторил
транспортный клиент (`TRANSPORT_RETRY_CODES`), не повторяет второй раз.

### Статистика запросов
`QuotaBackoffHTTPClient` считает каждую отправленную попытку: `reads` (GET), `writes` (POST/PUT/…), `retries_429`,
`retries_other` — в `client.stats` (одно подключение) и в `QuotaBackoffHTTPClient.process_stats` (весь процесс).
Функции пакета `utils.uploader`:
- `get_google_sheets_request_stats()` — снимок статистики процесса;
- `diff_google_sheets_request_stats(before, after=None)` — запросы между снимками (например, одного этапа).

Прямые вызовы `gspread` в обход `GoogleSheetsReporter` (скрипты диагностики) не учитываются.

### Кэш заголовка
`GoogleSheetsReporter._get_headers()` читает строку заголовка один раз за подключение и возвращает копию; им
пользуются `_sync_table_structure`, `_validate_table_structure`, поиск, добавление и обновление строк, coverage-check
(при `header_row == 1`). Раньше заголовок перечитывался при каждой операции — включая два чтения подряд при
подключении. Новое подключение (новый `GoogleSheetsReporter`) читает заголовок заново.

Фактическая стоимость подключения — 3 чтения: метаданные таблицы (`open_by_key`), метаданные листа (`worksheet()`),
заголовок. Coverage-check — 5 чтений: подключение, `col_values` для поиска последней строки, `batch_get`.
