# Метод `extract_report_data()` (OzonReportParser)

## Версия
**0.0.2**

## Описание
Метод `extract_report_data()` переопределяет базовый метод извлечения данных отчёта. Добавляет информацию о ПВЗ в результат извлечения данных.

## Сигнатура
```python
def extract_report_data(self) -> Dict[str, Any]
```

## Возвращаемое значение
- `Dict[str, Any]` — словарь с данными отчёта, включая информацию о ПВЗ

## Алгоритм работы

1. **Вызов родительского метода** (в некоторых реализациях):
   - Может вызывать `super().extract_report_data()` для получения базовых данных

2. **Извлечение информации о ПВЗ**:
   - Получает текущий ПВЗ через `get_current_pvz()`
   - Добавляет информацию о ПВЗ в результат

3. **Добавление временной метки**:
   - Добавляет timestamp извлечения данных

4. **Возврат результата**:
   - Возвращает словарь с данными отчёта и информацией о ПВЗ

## Логирование

Метод ведёт логирование:
- `TRACE` — вход в метод
- `DEBUG` — информация о процессе извлечения данных
- `INFO` — успешное извлечение данных

## Пример использования

```python
from scheduler_runner.utils.parser.core.ozon_report_parser import OzonReportParser

# Создание парсера
parser = OzonReportParser(config)

# Извлечение данных отчёта
data = parser.extract_report_data()

# Результат содержит:
# {
#     'location_info': 'ТВЗ Москва Люблинская 12',
#     'extraction_timestamp': '2026-03-03 10:32:30',
#     'giveout_count': 15,
#     'direct_flow_total': 25,
#     'return_flow_total': 5
# }
```

## Зависимости
- [`get_current_pvz()`](get_current_pvz().md) — извлечение текущего ПВЗ
- [`_get_current_timestamp()`](_get_current_timestamp().md) — получение временной метки

## Пример диагностики

```
[10:32:28] TRACE Попали в метод OzonReportParser.extract_report_data
[10:32:29] DEBUG Извлечение информации о ПВЗ...
[10:32:30] INFO Текущий ПВЗ: ТВЗ Москва Люблинская 12
[10:32:30] DEBUG Добавление информации о ПВЗ в результат
[10:32:30] INFO Данные отчёта успешно извлечены
```

## Связанные методы

- [`get_current_pvz()`](get_current_pvz().md) — получение текущего ПВЗ
- [`_get_current_timestamp()`](_get_current_timestamp().md) — получение временной метки
- [`extract_data()`](../BaseParser/extract_data().md) — базовый метод извлечения данных

## Изменения в версии 0.0.2
- Добавлено извлечение информации о ПВЗ
- Добавлена временная метка извлечения данных
- Улучшено логирование процесса извлечения

## Наследование

Метод `extract_report_data()` переопределяется в дочерних классах:

### MultiStepOzonParser
```python
def extract_report_data(self) -> Dict[str, Any]:
    """
    Метод извлекает данные в зависимости от конфигурации мульти-шаговой обработки.
    Теперь все данные, включая общую информацию, обрабатываются через мульти-шаговую логику.
    """
    # Получаем результаты мульти-шаговой обработки
    multi_step_results = self.config.get('last_collected_data', {})
    
    # Удаляем служебные поля из результата
    cleaned_results = {}
    for key, value in multi_step_results.items():
        if isinstance(value, dict):
            if '__STEP_SOURCE_URL__' in value:
                del value['__STEP_SOURCE_URL__']
            cleaned_results[key] = value
        else:
            cleaned_results[key] = value
    
    # Для совместимости извлекаем специфичные значения из summary
    if 'summary' in cleaned_results:
        summary_data = cleaned_results['summary']
        if 'giveout' in summary_data:
            cleaned_results['giveout_count'] = summary_data['giveout']
        if 'direct_flow_total' in summary_data:
            cleaned_results['direct_flow_total'] = summary_data['direct_flow_total']
        if 'return_flow_total' in summary_data:
            cleaned_results['return_flow_total'] = summary_data['return_flow_total']
    
    return cleaned_results
```

**Особенности MultiStepOzonParser:**
- Использует результаты мульти-шаговой обработки (`last_collected_data`)
- Автоматически очищает служебные поля (`__STEP_SOURCE_URL__`)
- Извлекает данные из вложенной структуры `summary` для совместимости
- Возвращает плоскую структуру данных с ключами верхнего уровня

## Структура результата

### Базовый OzonReportParser
```python
{
    'location_info': 'ТВЗ Москва Люблинская 12',
    'extraction_timestamp': '2026-03-03 10:32:30',
    # Данные отчёта (зависят от конкретной реализации)
}
```

### MultiStepOzonParser
```python
{
    'location_info': 'ТВЗ Москва Люблинская 12',
    'extraction_timestamp': '2026-03-03 10:32:30',
    'giveout': {...},  # Данные по выдачам
    'direct_flow': {...},  # Данные по прямому потоку
    'return_flow': {...},  # Данные по возвратному потоку
    'summary': {
        'giveout': 15,
        'direct_flow_total': 25,
        'return_flow_total': 5
    },
    # Для совместимости:
    'giveout_count': 15,
    'direct_flow_total': 25,
    'return_flow_total': 5
}
```
