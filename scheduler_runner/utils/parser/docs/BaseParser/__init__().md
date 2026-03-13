# Метод `__init__()` (BaseParser)

## Версия
**0.0.2**

## Описание
Метод `__init__()` реализует инициализацию базового парсера `BaseParser`. Метод устанавливает логгер, сохраняет конфигурацию и инициализирует базовые атрибуты класса.

## Сигнатура
```python
def __init__(self, config: Dict[str, Any], logger=None)
```

## Параметры

| Параметр | Тип | Описание |
|----------|-----|----------|
| `config` | `Dict[str, Any]` | Конфигурационный словарь с параметрами для работы парсера |
| `logger` | `Optional[logger]` | Объект логгера (если не передан, будет создан внутренний логгер) |

## Возвращаемое значение
- `None` (конструктор ничего не возвращает)

## Поддерживаемые параметры конфигурации

### Таймауты и задержки
- `DEFAULT_TIMEOUT` — таймаут ожидания элементов (по умолчанию 60 сек)
- `ELEMENT_CLICK_TIMEOUT` — таймаут ожидания кликабельности элемента (по умолчанию 10 сек)
- `ELEMENT_WAIT_TIMEOUT` — таймаут ожидания появления элемента (по умолчанию 10 сек)
- `PAGE_LOAD_DELAY` — задержка после загрузки страницы (по умолчанию 3 сек)
- `PAGE_UPDATE_DELAY` — задержка после обновления страницы (по умолчанию 2 сек)
- `DROPDOWN_OPEN_DELAY` — задержка после открытия выпадающего списка (по умолчанию 2 сек)
- `PROCESS_TERMINATION_SLEEP` — время ожидания после завершения процессов (по умолчанию 2 сек)

### Параметры браузера
- `BROWSER_EXECUTABLE` — имя исполняемого файла браузера (по умолчанию 'msedge.exe')
- `BROWSER_USER_DATA_PATH_TEMPLATE` — шаблон пути к данным браузера (по умолчанию 'C:/Users/{username}/AppData/Local/Microsoft/Edge/User Data')
- `EDGE_USER_DATA_DIR` — путь к пользовательским данным Edge
- `HEADLESS` — режим headless для браузера (по умолчанию False)

### Конфигурации таблиц
- `table_configs` — словарь с конфигурациями для извлечения данных из таблиц
  ```python
  {
    'table_identifier': {
      'table_selector': 'xpath_to_table',
      'table_type': 'standard|dynamic',
      'table_columns': [
        {
          'name': 'col_name',
          'selector': 'xpath_to_cell',
          'regex': 'optional_regex'
        }
      ]
    }
  }
  ```

### Параметры логирования
- `USER` — имя пользователя для логгера
- `TASK_NAME` — имя задачи для логгера
- `DETAILED_LOGS` — режим детального логирования (True/False)

## Алгоритм работы

1. **Сохранение конфигурации**: Сохраняет переданный `config` в атрибут `self.config`
2. **Установка логгера**:
   - Если передан внешний `logger` — использует его
   - Если `logger` указан в `config['logger']` — использует его
   - Иначе создаёт внутренний логгер через `configure_logger()`
3. **Логирование инициализации**: Записывает в лог сообщение о входе в метод
4. **Инициализация драйвера**: Устанавливает `self.driver = None` (будет создан в `setup_browser()`)

## Логирование

Метод ведёт логирование:
- `TRACE` — вход в метод `BaseParser.__init__`
- `ERROR` — ошибка при создании логгера (если не удалось импортировать `configure_logger`)

## Пример использования

```python
from scheduler_runner.utils.parser.core.base_parser import BaseParser

# Создание конфигурации
config = {
    "DEFAULT_TIMEOUT": 60,
    "BROWSER_EXECUTABLE": "msedge.exe",
    "HEADLESS": False,
    "USER": "operator",
    "TASK_NAME": "MyParser",
    "DETAILED_LOGS": True
}

# Создание экземпляра парсера
parser = BaseParser(config)

# Создание с внешним логгером
from scheduler_runner.utils.logging import configure_logger
logger = configure_logger(user="operator", task_name="MyParser")
parser = BaseParser(config, logger=logger)
```

## Зависимости
- `scheduler_runner.utils.logging.configure_logger` — функция создания логгера
- `config` — словарь конфигурации

## Обработка ошибок

Метод **обрабатывает ошибки** при создании логгера:
- Если не удалось импортировать `configure_logger` — выводит ошибку в консоль, продолжает работу без логгера
- Если `logger` не передан и не создан — методы парсера будут работать без логирования

## Пример диагностики

```
[10:32:15] TRACE Попали в метод BaseParser.__init__
```

## Связанные методы

- [`setup_browser()`](setup_browser().md) — настройка браузера (вызывается после инициализации)
- [`_get_default_browser_user_data_dir()`](_get_default_browser_user_data_dir().md) — получение пути к профилю
- [`_get_current_user()`](_get_current_user().md) — получение имени пользователя

## Изменения в версии 0.0.2
- Добавлена поддержка создания внутреннего логгера при отсутствии внешнего
- Добавлена обработка ошибок при импорте `configure_logger`
- Уточнены параметры конфигурации в документации

## Наследование

Метод `__init__()` переопределяется в дочерних классах:

### BaseReportParser
```python
def __init__(self, config: Dict[str, Any], args=None, logger=None):
    # Добавляет поддержку аргументов командной строки
    self.args = self._parse_arguments(args)
    self._update_execution_date()
    super().__init__(config, logger)
```

### OzonReportParser
```python
def __init__(self, config: Dict[str, Any], args=None, logger=None):
    # Обновляет конфиг конфигурацией Ozon
    if config == {} or config is None:
        config = OZON_BASE_CONFIG.copy()
    super().__init__(config, args, logger)
```

### MultiStepOzonParser
```python
def __init__(self, config: Dict[str, Any], args=None, logger=None):
    # Использует конфигурацию MULTI_STEP_OZON_CONFIG
    super().__init__(config, args, logger)
```
