# План версии 3: Создание универсальной утилиты загрузки данных отчетов

## Философия архитектуры

### Принципы:
1. **Разделение ответственности** - утилита только загружает и объединяет данные, не знает о потребителях
2. **Универсальный интерфейс** - возвращаемый формат данных нейтрален, легко преобразуется
3. **Конфигурируемость** - гибкие настройки для разных типов отчетов
4. **Переиспользование** - общие компоненты для работы с файлами

## Архитектурные компоненты

```
┌─────────────────────────────────────────────────────────────┐
│                   Верхнеуровневые скрипты                   │
│  (GoogleSheets_KPI_UploadScript.py, другие потребители)     │
└───────────────┬─────────────────────────────────────────────┘
                │ Запрашивает данные
                ▼
┌─────────────────────────────────────────────────────────────┐
│           Утилита load_reports_data.py                      │
│  (только загрузка и объединение, формат нейтральный)        │
└───────────────┬─────────────────────────────────────────────┘
                │ Использует
                ▼
┌─────────────────────────────────────────────────────────────┐
│             Общие утилиты (file_utils.py)                   │
│      (поиск файлов, загрузка JSON, транслитерация)          │
└─────────────────────────────────────────────────────────────┘
```

## Этап 1: Создание общих утилит для работы с файлами

**Файл:** `scheduler_runner/tasks/reports/utils/file_utils.py`

### Задачи:
1. **Инкапсулировать логику поиска файлов** из `reports_utils.py`
2. **Добавить новые возможности** для гибкой конфигурации
3. **Создать единый интерфейс** для работы с файлами отчетов

### Основные функции:
```python
# file_utils.py - Нейтральные функции без привязки к конкретным типам отчетов
def find_report_file(pattern_template: str, directory: Path, date: str, pvz_id: str = None) -> Optional[Path]
def load_json_file(file_path: Path) -> Optional[Dict[str, Any]]
def extract_date_from_filename(filename: str, pattern: str) -> Optional[str]
def normalize_pvz_for_filename(pvz_name: str) -> str  # транслитерация
```

## Этап 2: Обновление базовой конфигурации

**Файл:** `scheduler_runner/tasks/reports/config/base_config.py`

### Улучшения:
1. **Добавить поддержку валидации схемы** через JSON Schema
2. **Добавить сериализацию/десериализацию** в различные форматы
3. **Создать фабричные методы** для создания конфигураций

```python
@dataclass
class BaseConfig:
    """Базовый класс для всех конфигураций с поддержкой JSON Schema."""
    
    @classmethod
    def get_schema(cls) -> Dict[str, Any]:
        """Возвращает JSON Schema для валидации конфигурации."""
        pass
    
    def to_json(self) -> str:
        """Сериализует конфигурацию в JSON."""
        pass
    
    @classmethod 
    def from_yaml(cls, yaml_content: str) -> 'BaseConfig':
        """Создает конфигурацию из YAML."""
        pass
```

## Этап 3: Создание универсальной утилиты загрузки данных

**Файл:** `scheduler_runner/tasks/reports/utils/load_reports_data.py`

### Изменения относительно версии 2:
1. **Убрать привязку к GoogleSheets** - нет формата `Дата: "DD.MM.YYYY"`
2. **Вернуть сырые данные** в максимально универсальном формате
3. **Добавить метаданные** о процессе загрузки
4. **Предоставить хуки** для кастомизации обработки

### Новый формат возвращаемых данных:
```python
{
    # Данные из отчетов (после маппинга и слияния)
    'issued_packages': 100,
    'total_packages': 150,
    'direct_flow_count': 50,
    'direct_flow_data': {
        'total_items_count': 50,
        'status': 'completed'
    },
    'return_flow_data': {
        'total_items_count': 25,
        'status': 'pending'
    },
    'pvz_info': 'Москва, ул. Примерная, 1',
    'marketplace': 'ОЗОН',
    
    # Метаданные процесса загрузки
    'metadata': {
        'report_date': '2026-01-05',           # Оригинальная дата запроса
        'pvz_id': 'pvz_moscow_1',              # Оригинальный PVZ ID
        'loaded_at': '2026-01-05T12:00:00Z',   # Время загрузки
        'reports_loaded': [                    # Какие отчеты загружены
            {'type': 'giveout', 'file': 'ozon_giveout_report_...'},
            {'type': 'carriages', 'file': 'ozon_carriages_report_...'}
        ],
        'config_used': {                       # Использованная конфигурация
            'giveout': {'required': False, 'enabled': True},
            'direct_flow': {'required': False, 'enabled': True}
        },
        'warnings': [                          # Предупреждения при загрузке
            'Отчет direct_flow не найден',
            'В отчете carriages отсутствует поле return_flow'
        ]
    }
}
```

## Этап 4: Создание фабрики преобразователей данных

**Файл:** `scheduler_runner/tasks/reports/utils/data_transformers.py`

### Назначение:
Преобразование универсальных данных в специфические форматы

### Примеры преобразователей:
```python
class DataTransformer:
    """Базовый класс для преобразования данных."""
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        pass

class GoogleSheetsTransformer(DataTransformer):
    """Преобразует данные для Google Sheets."""
    
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'id': '',
            'Дата': self._format_date(raw_data['metadata']['report_date']),
            'ПВЗ': raw_data.get('pvz_info', ''),
            'Количество выдач': raw_data.get('issued_packages', 0),
            'Прямой поток': self._extract_direct_flow(raw_data),
            'Возвратный поток': self._extract_return_flow(raw_data)
        }

class DatabaseTransformer(DataTransformer):
    """Преобразует данные для записи в базу данных."""
    pass

class APITransformer(DataTransformer):
    """Преобразует данные для отправки по API."""
    pass
```

## Этап 5: Создание примеров использования

### Пример 1: Использование в новом GoogleSheets скрипте
```python
# GoogleSheets_KPI_UploadScript.py (упрощенный пример)
from scheduler_runner.tasks.reports.utils.load_reports_data import load_reports_data
from scheduler_runner.tasks.reports.utils.data_transformers import GoogleSheetsTransformer

# 1. Загружаем данные (универсальный формат)
raw_data = load_reports_data(
    report_date="2026-01-05",
    pvz_id="Москва ПВЗ",
    config=my_config
)

# 2. Преобразуем в формат для Google Sheets
transformer = GoogleSheetsTransformer()
sheet_data = transformer.transform(raw_data)

# 3. Отправляем в Google Sheets (или куда нужно)
google_sheets_reporter.update_or_append_data(sheet_data)
```

### Пример 2: Использование для других целей
```python
# daily_report_script.py - генерация ежедневного отчета
from scheduler_runner.tasks.reports.utils.load_reports_data import load_reports_data
from scheduler_runner.tasks.reports.utils.data_transformers import (
    EmailReportTransformer,
    PDFReportTransformer
)

# Загружаем данные один раз
raw_data = load_reports_data(report_date="2026-01-05", pvz_id="Все ПВЗ")

# Генерируем разные форматы
email_data = EmailReportTransformer().transform(raw_data)
pdf_data = PDFReportTransformer().transform(raw_data)

# Отправляем разным потребителям
send_email(email_data)
generate_pdf(pdf_data)
save_to_database(raw_data)  # или сохраняем сырые данные
```

## Этап 6: Тестирование и валидация

### Типы тестов:
1. **Модульные тесты** для каждой функции
2. **Интеграционные тесты** для цепочки загрузки
3. **Тесты преобразователей** для каждого формата вывода
4. **Тесты производительности** для больших объемов данных

## Преимущества архитектуры версии 3:

### 1. **Гибкость**
- Одна утилита загрузки → множество потребителей
- Легко добавлять новые форматы вывода через трансформеры
- Можно использовать для разных задач: отчеты, аналитика, мониторинг

### 2. **Поддерживаемость**
- Четкое разделение ответственности
- Каждый компонент делает одну вещь и делает её хорошо
- Легко тестировать изолированно

### 3. **Расширяемость**
- Новые типы отчетов через конфигурацию
- Новые форматы вывода через трансформеры
- Новые стратегии объединения через MergeStrategy

### 4. **Будущая безопасность**
- Если изменится Google Sheets API - меняем только трансформер
- Если появятся новые источники данных - расширяем конфигурацию
- Если понадобятся новые потребители - создаем новые трансформеры

## Следующие шаги реализации:

### Итерация 1: Базовый каркас (1-2 дня)
1. Создать `file_utils.py` с основными функциями
2. Создать `base_config.py` с улучшенным BaseConfig
3. Создать `load_reports_data.py` с базовой загрузкой

### Итерация 2: Расширенные возможности (2-3 дня)
1. Добавить поддержку разных стратегий объединения
2. Реализовать систему метаданных
3. Создать базовые трансформеры данных

### Итерация 3: Тестирование и оптимизация (1-2 дня)
1. Написать комплексные тесты
2. Оптимизировать производительность
3. Добавить обработку ошибок

### Итерация 4: Интеграция (1 день)
1. Обновить существующие скрипты для использования новой утилиты
2. Написать документацию и примеры
3. Провести регрессионное тестирование

Эта архитектура позволит создать действительно переиспользуемый компонент, который будет полезен не только для Google Sheets, но и для любых других задач обработки отчетов ОЗОН. Что вы думаете об этом плане?