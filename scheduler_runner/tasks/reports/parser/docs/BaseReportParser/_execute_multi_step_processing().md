# Метод `_execute_multi_step_processing()`

## Версия
**0.0.1**

## Описание
Метод `_execute_multi_step_processing()` реализует основную логику мульти-шаговой обработки данных. Метод последовательно выполняет каждый шаг из конфигурации, временно обновляя конфигурацию для каждого шага, и объединяет результаты всех шагов в соответствии с конфигурацией агрегации.

## Сигнатура
```python
def _execute_multi_step_processing(self, multi_step_config)
```

## Параметры
- **multi_step_config** (`dict`): Конфигурация мульти-шаговой обработки, содержащая:
  - **steps** (`list`): Список имен шагов для выполнения в порядке очередности
  - **step_configurations** (`dict`): Словарь конфигураций для каждого шага, где ключи - имена шагов
  - **aggregation_logic** (`dict`, optional): Конфигурация агрегации результатов, содержащая:
    - **combine_nested_results** (`bool`): Флаг объединения вложенных результатов
    - **sum_nested_values** (`list`): Список полей, значения которых нужно суммировать
    - **result_structure** (`dict`): Структура итогового результата с шаблонами подстановки

## Возвращаемое значение
Результаты мульти-шаговой обработки, объединенные в соответствии с конфигурацией агрегации

## Изменения в версии 0.0.1
- Результаты шагов теперь содержат `__STEP_SOURCE_URL__` для каждого шага
- Эти `__STEP_SOURCE_URL__` используются в `_combine_step_results()` для формирования `__SOURCE_URL__` как общего префикса всех URL из результатов шагов

## Пример конфигурации
```python
multi_step_config = {
    "steps": ["giveout", "direct_flow", "return_flow"],
    "step_configurations": {
        "giveout": {
            "base_url": "https://turbo-pvz.ozon.ru/reports/giveout",
            "processing_type": "simple",
            "result_key": "giveout_count"
        },
        "direct_flow": {
            "base_url": "https://turbo-pvz.ozon.ru/outbound/carriages-archive",
            "processing_type": "table_nested",
            "result_key": "direct_flow_data"
        }
    },
    "aggregation_logic": {
        "combine_nested_results": True,
        "result_structure": {
            "source_url": "{__SOURCE_URL__}",
            "summary": {
                "giveout": "{giveout_count}",
                "direct_flow_total": "{direct_flow_data}"
            }
        }
    }
}
```

## Особенности реализации
- Каждый шаг добавляет `__STEP_SOURCE_URL__` к своему результату
- Результаты всех шагов передаются в `_combine_step_results()` для формирования итогового результата
- `__SOURCE_URL__` формируется как общий префикс всех `__STEP_SOURCE_URL__` с помощью метода `_get_common_url_prefix()`