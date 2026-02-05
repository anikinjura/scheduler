# Метод `_combine_step_results()`

## Версия
**0.0.1**

## Описание
Метод `_combine_step_results()` объединяет результаты всех шагов мульти-шаговой обработки и формирует итоговую структуру данных в соответствии с заданной конфигурацией.

## Назначение
Метод является ключевым компонентом мульти-шаговой обработки, который:
1. Применяет логику агрегации к результатам отдельных шагов
2. Формирует финальную структуру результата согласно конфигурации
3. Автоматически добавляет общую информацию отчета
4. Заменяет плейсхолдеры в структуре результата на реальные значения
5. Формирует `__SOURCE_URL__` как общий префикс всех `__STEP_SOURCE_URL__` из результатов шагов

## Изменения в версии 0.0.1
- Добавлена передача `all_step_results` в `get_common_report_info()` для формирования `__SOURCE_URL__` как общего префикса всех `__STEP_SOURCE_URL__` из результатов шагов
- Теперь `__SOURCE_URL__` формируется с помощью метода `_get_common_url_prefix()` на основе всех `__STEP_SOURCE_URL__` из результатов шагов

## Параметры
- `all_step_results` - словарь с результатами всех шагов, где ключи - это имена шагов или `result_key` из конфигурации шагов
- `aggregation_config` - конфигурация агрегации, содержащая:
  - `combine_nested_results` - флаг объединения вложенных результатов
  - `sum_nested_values` - список полей, значения которых нужно суммировать
  - `result_structure` - структура итогового результата с шаблонами подстановки
  - `aggregation_logic` - дополнительная логика агрегации

## Возвращаемое значение
Объединенные результаты в формате, определенном в `result_structure`, или в виде словаря с результатами всех шагов, если структура не задана.

## Алгоритм работы
1. Проверка наличия конфигурации агрегации
2. Извлечение всех `__STEP_SOURCE_URL__` из результатов шагов
3. Вызов `get_common_report_info(all_step_results)` для получения общей информации отчета с формированием `__SOURCE_URL__` как общего префикса всех `__STEP_SOURCE_URL__`
4. Применение структуры результата с заменой плейсхолдеров
5. Объединение всех результатов в соответствии с конфигурацией

## Пример конфигурации
```python
all_step_results = {
    "giveout_count": {
        "value": 238,
        "__STEP_SOURCE_URL__": "https://turbo-pvz.ozon.ru/reports/giveout?..."
    },
    "direct_flow_data": {
        "total_carriages": 12,
        "__STEP_SOURCE_URL__": "https://turbo-pvz.ozon.ru/outbound/carriages-archive?..."
    }
}

aggregation_config = {
    "combine_nested_results": True,
    "sum_nested_values": [],
    "result_structure": {
        "location_info": "{__LOCATION_INFO__}",
        "extraction_timestamp": "{__EXTRACTION_TIMESTAMP__}",
        "source_url": "{__SOURCE_URL__}",
        "execution_date": "{__EXECUTION_DATE__}",
        "summary": {
            "giveout": "{giveout_count}",
            "direct_flow_total": "{direct_flow_data}"
        }
    }
}
```