# Обновленный план разработки Parser_KPI_Giveout_OzonScript

## Общее описание задачи

Необходимо создать новый скрипт `Parser_KPI_Giveout_OzonScript.py` и его конфигурацию `Parser_KPI_Giveout_OzonScript_config.py`, придерживаясь унифицированной архитектуры проекта. Скрипт должен выполнять парсинг веб-страницы ОЗОН для получения данных о выдачах и сохранять их в формате, совместимом с системой отчетов.

## Целевая архитектура

### Основные компоненты:
1. **Унифицированная система конфигураций** с использованием `TableConfig` и `ReportConfig`
2. **Модульная структура** с разделением ответственности
3. **Универсальные утилиты** для загрузки данных, логирования и обработки ошибок
4. **Совместимость** с системой планировщика задач

### Примеры для изучения:
- `scheduler_runner\tasks\reports\Telegram_KPI_NotificationScript.py`
- `scheduler_runner\tasks\reports\Telegram_KPI_NotificationScript_config.py`
- `scheduler_runner\tasks\reports\GoogleSheets_KPI_UploadScript.py`
- `scheduler_runner\tasks\reports\GoogleSheets_KPI_UploadScript_config.py`

## Базовые классы и зависимости

### 1. Базовые классы конфигураций:
- `scheduler_runner\tasks\reports\config\reports_base_config.py` - содержит `BaseConfig`
- `scheduler_runner\tasks\reports\utils\load_reports_data.py` - содержит `ReportConfig` (наследуется от `BaseConfig`)

### 2. Утилиты:
- `scheduler_runner\tasks\reports\utils\load_reports_data.py` - универсальная утилита для загрузки данных
- `scheduler_runner\tasks\reports\utils\data_transformers.py` - утилита для преобразования данных в специфические форматы
- `scheduler_runner\utils\google_sheets.py` - содержит `TableConfig` и `ColumnDefinition`
- `scheduler_runner\utils\notify.py` - утилита для уведомлений
- `scheduler_runner\utils\logging.py` - система логирования

### 3. Парсеры:
- `scheduler_runner\tasks\reports\BaseOzonParser.py` - базовый класс для парсинга ОЗОН

## Структура конфигурации отчетов

### REPORT_CONFIGS:
В файле конфигурации должен быть определен список `REPORT_CONFIGS` с использованием `ReportConfig`:

```python
REPORT_CONFIGS = [
    ReportConfig(
        report_type='giveout',
        file_pattern='ozon_giveout_report_{pvz_id}_{date}.json',
        required=False,
        fields_mapping={
            'issued_packages': 'issued_packages',
            'total_packages': 'total_packages',
            'pvz_info': 'pvz_info',
            'marketplace': 'marketplace'
        }
    )
]
```

## Применяемые улучшения из OzonCarriagesReportScript

### 1. Модульные константы:
Определите константы для часто используемых значений:
```python
# Модульные константы для магических строк
LOGIN_INDICATORS = ['login', 'signin', 'auth']
MARKETPLACE_NAME = 'Ozon'
REPORT_TYPE_GIVEOUT = 'giveout'
```

### 2. Улучшенная обработка ошибок:
Используйте специфичные исключения Selenium и детализированное логирование:
```python
from selenium.common.exceptions import TimeoutException, NoSuchElementException
```

### 3. Структурированные селекторы:
Определите селекторы в конфигурации:
```python
SELECTORS = {
    "PVZ_INPUT": "//input[@id='input___v-0-0']",
    "TOTAL_GIVEOUT": "//div[contains(@class, '_total_1n8st_15')]",
    "GIVEOUT_ITEMS": "//div[contains(@class, 'giveout-item')]",
}
```

### 4. Детализированная структура данных:
Создайте четкую структуру для хранения извлеченных данных:
```python
data = {
    'marketplace': 'Ozon',
    'report_type': 'giveout',
    'date': datetime.now().strftime('%Y-%m-%d'),
    'timestamp': datetime.now().isoformat(),
    'issued_packages': total_packages,
    'total_packages': total_packages,
    'pvz_info': pvz_info,
    'raw_data': raw_extracted_data,
}
```

### 5. Поддержка разных типов данных:
Обеспечьте возможность обработки разных типов отчетов в рамках одного скрипта.

## Использование трансформеров данных

### DataTransformers:
Используйте утилиту `scheduler_runner\tasks\reports\utils\data_transformers.py` для преобразования данных:

- `DataTransformer` - абстрактный базовый класс для всех трансформеров
- `GoogleSheetsTransformer` - реализация для преобразования данных в формат Google Sheets

### Интеграция:
После извлечения данных из веб-страницы ОЗОН, используйте трансформеры для приведения данных к универсальному формату (подумайте над необходимостью созданиия дополнительного трансформера при необходимости):
```python
from scheduler_runner.tasks.reports.utils.data_transformers import GoogleSheetsTransformer

transformer = GoogleSheetsTransformer()
formatted_data = transformer.transform(extracted_data)
```

### Преимущества:
- Обеспечивает совместимость с другими компонентами системы
- Позволяет легко добавлять новые форматы трансформации в будущем
- Поддерживает унифицированную архитектуру проекта

## Структура нового скрипта

### Parser_KPI_Giveout_OzonScript.py:
1. **Импорты** - стандартные библиотеки, утилиты, конфигурации
2. **Константы** - модульные константы
3. **Класс парсера** - наследуется от `BaseOzonParser` с методами:
   - `login()` - вход в систему
   - `navigate_to_reports()` - навигация к отчету
   - `extract_data()` - извлечение данных
   - `logout()` - выход из системы
4. **Функции обработки** - вспомогательные функции для обработки данных
5. **Функции CLI** - `parse_arguments()`, `main()`

### Parser_KPI_Giveout_OzonScript_config.py:
1. **Импорты** - конфигурации, пути, `ReportConfig`, `TableConfig`
2. **URL и параметры** - динамически генерируемые URL
3. **Селекторы** - структурированные селекторы для парсинга
4. **TableConfig** - структура таблицы для совместимости
5. **REPORT_CONFIGS** - конфигурации отчетов
6. **SCRIPT_CONFIG** - основная конфигурация скрипта
7. **TASK_SCHEDULE** - расписание задач

## Примеры архитектуры

### Унифицированная система конфигураций:
Смотрите `Telegram_KPI_NotificationScript_config.py` для примера использования `ReportConfig` и `TableConfig`.

### Универсальная утилита загрузки данных:
Смотрите `load_reports_data.py` для понимания структуры `ReportConfig` и процесса загрузки данных.

### Система логирования:
Смотрите использование `configure_logger` в обоих примерах скриптов.

## Требования к реализации

1. **Совместимость** - данные должны быть совместимы с системой уведомлений и загрузки в Google Sheets
2. **Унификация** - использовать общие утилиты и конфигурации
3. **Обработка ошибок** - обеспечить надежную обработку исключений
4. **Логирование** - использовать систему логирования проекта
5. **Конфигурируемость** - все параметры должны быть настраиваемы через конфигурацию

## Зависимости между компонентами

### Архитектурные зависимости:

#### 1. Parser_KPI_Giveout_OzonScript.py зависит от:
- `BaseOzonParser` - базовый класс для парсинга ОЗОН
- `SCRIPT_CONFIG` из конфигурационного файла
- Утилиты `configure_logger` для логирования
- Утилиты `load_reports_data` для сохранения данных
- Утилиты `data_transformers` для преобразования данных

#### 2. Parser_KPI_Giveout_OzonScript_config.py зависит от:
- `config.base_config.PVZ_ID` - глобальный идентификатор ПВЗ
- `scheduler_runner.tasks.reports.config.reports_paths.REPORTS_PATHS` - пути к отчетам
- `ReportConfig` из `load_reports_data` - для конфигурации отчетов
- `TableConfig` из `google_sheets` - для структуры таблицы

#### 3. Компоненты системы, зависящие от нового скрипта:
- Система планировщика задач (использует `TASK_SCHEDULE`)
- Утилита `load_reports_data` (через `REPORT_CONFIGS`)
- Система уведомлений (через формат данных)
- Google Sheets интеграция (через формат данных и `TableConfig`)

#### 4. Взаимодействие между компонентами:
```
Веб-страница ОЗОН → Parser_KPI_Giveout_OzonScript →
→ извлеченные данные → data_transformers →
→ унифицированный формат → load_reports_data →
→ сохранение в JSON → другие компоненты системы
```

#### 5. Порядок инициализации:
1. Конфигурация (`Parser_KPI_Giveout_OzonScript_config.py`)
2. Парсер (`Parser_KPI_Giveout_OzonScript.py`)
3. Универсальные утилиты (`load_reports_data`, `data_transformers`)
4. Система логирования
5. Сохранение результатов

## Тестирование

### Структура тестов:
- Тесты размещаются в директории `scheduler_runner\tasks\reports\tests`
- Используется `pytest` и `unittest.mock` для тестирования
- Тесты покрывают все основные функции скрипта

### Типы тестов:
1. **Тесты парсинга аргументов** - проверяют корректность обработки командной строки
2. **Тесты загрузки данных** - проверяют работу функций загрузки и трансформации данных
3. **Тесты основной логики** - проверяют работу основной функции `main`
4. **Тесты с моками** - используют `unittest.mock` для изоляции компонентов
5. **Тесты крайних случаев** - проверяют обработку ошибок и пустых данных

### Примеры тестов:
Смотрите файлы:
- `test_telegram_notification_script.py` - пример тестирования скрипта уведомлений
- `test_google_sheets_kpi_upload_script.py` - пример тестирования скрипта загрузки в Google Sheets

### Рекомендуемые тесты для нового скрипта:
1. `test_parse_arguments_defaults()` - тест парсинга аргументов по умолчанию
2. `test_parse_arguments_with_values()` - тест парсинга с переданными значениями
3. `test_extract_data_success()` - тест успешного извлечения данных
4. `test_extract_data_with_mocked_selenium()` - тест с моканным Selenium
5. `test_main_function_with_mocked_dependencies()` - тест основной функции с моками
6. `test_data_transformation()` - тест трансформации данных
7. `test_error_handling()` - тест обработки ошибок
8. `test_config_integration()` - тест интеграции с конфигурацией

## Структура размещения файлов

### Основные файлы:
- **Скрипт**: `scheduler_runner\tasks\reports\Parser_KPI_Giveout_OzonScript.py`
- **Конфигурация**: `scheduler_runner\tasks\reports\config\scripts\Parser_KPI_Giveout_OzonScript_config.py`

### Тесты:
- **Юнит-тесты**: `scheduler_runner\tasks\reports\tests\test_parser_kpi_giveout_ozon_script.py`

### Документация:
- **План разработки**: `scheduler_runner\tasks\reports\docs\Parser_KPI_Giveout_OzonScript\Parser_KPI_Giveout_OzonScript_plan.md`
- **Дополнительная документация**: `scheduler_runner\tasks\reports\docs\Parser_KPI_Giveout_OzonScript\` (дополнительные файлы при необходимости)

### Временные/отладочные файлы:
- **Временные скрипты**: `scheduler_runner\tasks\reports\debug\` (при необходимости)
- **Логи тестирования**: `scheduler_runner\tasks\reports\tests\logs\` (если требуются специфичные логи для отладки)

## Выявленные аспекты и уточнения

### 1. Разделение ответственности
В процессе реализации было принято важное решение о разделении ответственности:
- Скрипт `Parser_KPI_Giveout_OzonScript.py` отвечает **только** за парсинг данных и сохранение их в формате, совместимом с системой
- Функции форматирования для уведомлений и другие дополнительные функции **не должны** быть включены в основной скрипт
- Эти функции должны быть реализованы в соответствующих модулях (например, в системе уведомлений или загрузки в Google Sheets)

### 2. Совместимость с существующей архитектурой
- Скрипт успешно интегрирован с системой `load_reports_data` через `REPORT_CONFIGS`
- Формат данных совместим с `GoogleSheetsTransformer`
- Скрипт соответствует унифицированной архитектуре проекта

### 3. Особенности тестирования
- Были выявлены и исправлены проблемы с тестами, связанные с различиями в структуре конфигурации
- Тесты были адаптированы для корректной проверки моков
- Добавлены тесты для проверки совместимости с системой загрузки данных

### 4. Структура данных
- Скрипт сохраняет данные в формате JSON с полями: `marketplace`, `report_type`, `date`, `timestamp`, `issued_packages`, `total_packages`, `pvz_info` и др.
- Формат данных соответствует ожидаемому для последующей обработки другими компонентами системы

## Файлы для изучения

1. `scheduler_runner\tasks\reports\Telegram_KPI_NotificationScript.py` - пример использования унифицированной архитектуры
2. `scheduler_runner\tasks\reports\Telegram_KPI_NotificationScript_config.py` - пример конфигурации с `ReportConfig`
3. `scheduler_runner\tasks\reports\GoogleSheets_KPI_UploadScript.py` - пример интеграции с Google Sheets
4. `scheduler_runner\tasks\reports\GoogleSheets_KPI_UploadScript_config.py` - пример использования `TableConfig`
5. `scheduler_runner\tasks\reports\utils\load_reports_data.py` - универсальная утилита загрузки данных
6. `scheduler_runner\tasks\reports\utils\data_transformers.py` - утилита для преобразования данных
7. `scheduler_runner\tasks\reports\config\reports_base_config.py` - базовый класс конфигураций
8. `scheduler_runner\tasks\reports\OzonCarriagesReportScript.py` - пример улучшенной архитектуры парсинга
9. `scheduler_runner\tasks\reports\BaseOzonParser.py` - базовый класс для парсинга ОЗОН