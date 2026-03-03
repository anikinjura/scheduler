# Метод `__init__()` (OzonReportParser)

## Версия
**0.0.2**

## Описание
Метод `__init__()` реализует инициализацию базового парсера отчётов Ozon `OzonReportParser`. Метод расширяет `BaseReportParser.__init__()` с автоматическим применением конфигурации Ozon.

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

1. **Логирование инициализации**: Записывает в лог сообщение о входе в метод
2. **Применение конфигурации Ozon**:
   - Если `config` пустой (`{}`) или `None` — использует `OZON_BASE_CONFIG.copy()`
   - Иначе использует переданный `config`
3. **Вызов родительского конструктора**: Вызывает `super().__init__(config, args, logger)` для инициализации `BaseReportParser`

## Логирование

Метод ведёт логирование:
- `TRACE` — вход в метод `OzonReportParser.__init__`

## Пример использования

```python
from scheduler_runner.tasks.reports.parser.core.ozon_report_parser import OzonReportParser

# Создание с конфигурацией по умолчанию (OZON_BASE_CONFIG)
parser = OzonReportParser({})

# Создание с кастомной конфигурацией
config = {
    "additional_params": {
        "location_id": "ТВЗ Москва Люблинская 12"
    },
    "overlay_config": {
        "enabled": True,
        "close_button_selector": "//button[contains(@class, 'ozi__button') and normalize-space()='Отложить']"
    }
}
parser = OzonReportParser(config)

# Создание с аргументами командной строки
import sys
args = parser._parse_arguments(sys.argv[1:])
parser = OzonReportParser(config, args=args)

# Создание с внешним логгером
from scheduler_runner.utils.logging import configure_logger
logger = configure_logger(user="operator", task_name="OzonParser")
parser = OzonReportParser(config, logger=logger)
```

## Зависимости
- [`OZON_BASE_CONFIG`](../configs/base_configs/ozon_report_config.py) — базовая конфигурация Ozon
- `BaseReportParser.__init__()` — инициализация родительского класса

## Пример диагностики

```
[10:32:15] TRACE Попали в метод OzonReportParser.__init__
[10:32:15] TRACE Попали в метод BaseReportParser.__init__
[10:32:15] TRACE Попали в метод BaseParser.__init__
```

## Связанные методы

- [`get_current_pvz()`](get_current_pvz().md) — извлечение текущего ПВЗ
- [`set_pvz()`](set_pvz().md) — установка нужного ПВЗ
- [`ensure_correct_pvz()`](ensure_correct_pvz().md) — проверка и установка правильного ПВЗ
- [`_check_and_close_overlay()`](_check_and_close_overlay().md) — проверка и закрытие оверлеев

## Изменения в версии 0.0.2
- Добавлено автоматическое применение `OZON_BASE_CONFIG` при пустом конфиге
- Уточнено логирование инициализации

## Наследование

Метод `__init__()` переопределяется в дочерних классах:

### MultiStepOzonParser
```python
def __init__(self, config: Dict[str, Any], args=None, logger=None):
    """
    Инициализация многошагового парсера Ozon
    """
    if logger:
        logger.trace("Попали в метод MultiStepOzonParser.__init__")
    super().__init__(config, args, logger)
```

**Особенности MultiStepOzonParser:**
- Использует конфигурацию `MULTI_STEP_OZON_CONFIG`
- Добавляет логику для многошаговой обработки данных
- Поддерживает сбор данных по выдачам, прямому и возвратному потокам

## Конфигурация OZON_BASE_CONFIG

Базовая конфигурация Ozon включает:

### Параметры URL
- `base_url` — базовый URL системы Ozon ('https://turbo-pvz.ozon.ru')
- `filter_template` — шаблон фильтра для URL
- `date_filter_template` — шаблон фильтра по дате
- `data_type_filter_template` — шаблон фильтра по типу данных

### Селекторы ПВЗ
- `pvz_selectors.input` — селектор input ПВЗ
- `pvz_selectors.dropdown` — селектор dropdown ПВЗ
- `pvz_selectors.option` — селектор опции dropdown

### Конфигурация оверлеев
- `overlay_config.enabled` — включение проверки оверлеев
- `overlay_config.overlay_selector` — селектор оверлея
- `overlay_config.close_button_selector` — селектор кнопки закрытия ("Отложить")
- `overlay_config.backdrop_selectors` — селекторы backdrop

### Дополнительные параметры
- `additional_params.location_id` — ID локации (ПВЗ) из `config.base_config.PVZ_ID`
