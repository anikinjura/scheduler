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
