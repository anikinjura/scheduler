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
- `get_table_headers()` - возвращает заголовки таблицы
- `get_last_row_with_data()` - определяет последнюю строку с данными
- `get_row_by_id()` - находит строку по ID
- `get_rows_by_unique_keys()` - находит строки по уникальным ключам
- `_find_rows_by_unique_keys_batch()` - находит строки по уникальным ключам с использованием batch_get для лучшей производительности
- `_normalize_date_format()` - нормализует формат даты к единому формату DD.MM.YYYY для сравнения
- `_normalize_for_comparison()` - нормализует значение для сравнения в логике поиска
- `_prepare_value_for_search()` - подготавливает значение для поиска в таблице, нормализует формат дат/чисел/строк

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
```

### Параметры
- **name** (`str`): имя колонки
- **column_type** (`ColumnType`): тип колонки (DATA, FORMULA, CALCULATED, IGNORE)
- **required** (`bool`): обязательна ли колонка
- **formula_template** (`Optional[str]`): шаблон формулы для колонок типа FORMULA
- **unique_key** (`bool`): является ли колонка частью уникального ключа
- **data_key** (`Optional[str]`): ключ в данных, если отличается от имени
- **column_letter** (`Optional[str]`): буква колонки (A, B, C)

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

### Функции нормализации
- `_normalize_date_format()` - преобразует различные форматы дат (строки, числа, datetime) в единый формат DD.MM.YYYY
- `_normalize_for_comparison()` - нормализует значение для сравнения, особенно важно для дат, хранящихся как серийные числа в Google Sheets
- `_prepare_value_for_search()` - подготавливает значения для поиска, применяя нормализацию

### Функция `_index_to_column_letter`
Функция `_index_to_column_letter()` преобразует числовой индекс колонки в буквенное обозначение (A, B, C...), что необходимо для формирования диапазонов при использовании `batch_get`.