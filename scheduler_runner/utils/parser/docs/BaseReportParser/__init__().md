# Метод `__init__()` (BaseReportParser)

## Версия
**0.0.2**

## Описание
Метод `__init__()` реализует инициализацию базового парсера отчётов `BaseReportParser`. Метод расширяет `BaseParser.__init__()` с поддержкой аргументов командной строки и автоматическим обновлением даты выполнения.

## Сигнатура
```python
def __init__(self, config: Dict[str, Any], args=None, logger=None)
```

## Параметры

| Параметр | Тип | Описание |
|----------|-----|----------|
| `config` | `Dict[str, Any]` | Конфигурационный словарь с параметрами для работы парсера |
| `args` | `Optional[argparse.Namespace]` | Аргументы командной строки (если не переданы, будут разобраны из `sys.argv`) |
| `logger` | `Optional[logger]` | Объект логгера (если не передан, будет использован внутренний логгер) |

## Возвращаемое значение
- `None` (конструктор ничего не возвращает)

## Алгоритм работы

1. **Логирование инициализации**: Записывает в лог сообщение о входе в метод (если логгер передан)
2. **Парсинг аргументов**: Вызывает `_parse_arguments(args)` для получения аргументов командной строки
3. **Обновление даты выполнения**: Вызывает `_update_execution_date()` для установки актуальной даты
4. **Вызов родительского конструктора**: Вызывает `super().__init__(config, logger)` для инициализации `BaseParser`

## Логирование

Метод ведёт логирование:
- `TRACE` — вход в метод `BaseReportParser.__init__`

## Пример использования

```python
from scheduler_runner.utils.parser.core.base_report_parser import BaseReportParser

# Создание конфигурации
config = {
    "report_type": "my_report",
    "output_config": {
        "dir": "reports",
        "format": "json"
    },
    "USER": "operator",
    "TASK_NAME": "ReportParser"
}

# Создание экземпляра парсера
parser = BaseReportParser(config)

# Создание с аргументами командной строки
import sys
args = parser._parse_arguments(sys.argv[1:])
parser = BaseReportParser(config, args=args)

# Создание с внешним логгером
from scheduler_runner.utils.logging import configure_logger
logger = configure_logger(user="operator", task_name="ReportParser")
parser = BaseReportParser(config, logger=logger)
```

## Зависимости
- [`_parse_arguments()`](_parse_arguments().md) — парсинг аргументов командной строки
- [`_update_execution_date()`](_update_execution_date().md) — обновление даты выполнения
- `BaseParser.__init__()` — инициализация родительского класса

## Пример диагностики

```
[10:32:15] TRACE Попали в метод BaseReportParser.__init__
[10:32:15] DEBUG Разбор аргументов командной строки...
[10:32:15] DEBUG Дата выполнения установлена: 2026-03-03
[10:32:15] TRACE Попали в метод BaseParser.__init__
```

## Связанные методы

- [`_parse_arguments()`](_parse_arguments().md) — парсинг аргументов командной строки
- [`_update_execution_date()`](_update_execution_date().md) — установка даты выполнения
- [`run_parser()`](run_parser().md) — запуск парсера отчётов

## Изменения в версии 0.0.2
- Добавлена поддержка аргументов командной строки
- Добавлен вызов `_update_execution_date()` при инициализации
- Уточнено логирование инициализации

## Наследование

Метод `__init__()` переопределяется в дочерних классах:

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
    if logger:
        logger.trace("Попали в метод MultiStepOzonParser.__init__")
    super().__init__(config, args, logger)
```
