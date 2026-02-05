# Метод `_execute_single_step()`

## Версия
**0.0.1**

## Описание
Метод `_execute_single_step()` реализует изолированное выполнение каждого шага многошагового процесса, временно изменяя конфигурацию для конкретного шага и затем восстанавливая исходное состояние, что позволяет каждому шагу использовать свои собственные параметры навигации и обработки данных.

## Сигнатура
```python
def _execute_single_step(self, step_config)
```

## Параметры
- **step_config** (`dict`): Конфигурация конкретного шага, содержащая:
  - **processing_type** (`str`): Тип обработки ("simple", "table", "table_nested")
  - **data_extraction** (`dict`, optional): Параметры извлечения данных (для "simple")
  - **table_processing** (`dict`, optional): Параметры табличной обработки (для "table", "table_nested")
  - **nested_processing** (`dict`, optional): Параметры вложенной обработки (для "table_nested")
  - **result_key** (`str`): Ключ для сохранения результата шага

## Возвращаемое значение
Результат обработки шага, который может содержать `__STEP_SOURCE_URL__`

## Изменения в версии 0.0.1
- К результату каждого шага добавляется `__STEP_SOURCE_URL__`, содержащий URL, с которого были извлечены данные для этого шага
- Этот `__STEP_SOURCE_URL__` используется в `_combine_step_results()` для формирования `__SOURCE_URL__` как общего префикса всех URL из результатов шагов

## Особенности реализации
- Метод временно обновляет конфигурацию, добавляя туда все параметры из `step_config`, за исключением служебных ключей (`processing_type`, `data_extraction`, `table_processing`, `nested_processing`, `result_key`)
- После выполнения шага сохраняется URL источника данных в переменной `step_source_url`
- К результату шага добавляется `__STEP_SOURCE_URL__` с этим URL
- Результат сохраняется под ключом `result_key` из конфигурации шага
- `__STEP_SOURCE_URL__` из всех шагов используется для формирования `__SOURCE_URL__` как общего префикса всех URL с помощью метода `_get_common_url_prefix()`

## Пример конфигурации шага
```python
step_config = {
    "base_url": "https://turbo-pvz.ozon.ru/reports/giveout",
    "processing_type": "simple",
    "data_extraction": {
        "selector": "//div[@class='count-display']",
        "element_type": "div"
    },
    "result_key": "giveout_count"
}
```

## Что происходит в методе:
1. **Временное обновление конфигурации**:
   - Метод принимает `step_config` - конфигурацию конкретного шага
   - Сохраняет текущую конфигурацию в переменную `original_config`
   - Обновляет основную конфигурацию, добавляя туда все параметры из `step_config`, за исключением служебных ключей (`processing_type`, `data_extraction`, `table_processing`, `nested_processing`, `result_key`)
   - Это позволяет временно изменить параметры навигации (URL, фильтры и т.д.) для выполнения конкретного шага

2. **Навигация к целевой странице**:
   - Вызывает `self.navigate_to_target()` с обновленной конфигурацией
   - Внутри `navigate_to_target()` формируется URL с использованием новых параметров:
     - `base_url` из временной конфигурации
     - `filter_template`, `date_filter_template`, `data_type_filter_template` из временной конфигурации
     - `execution_date` из общей конфигурации
   - Осуществляется переход на сформированную страницу

3. **Определение типа обработки**:
   - Получает `processing_type` из `step_config` (по умолчанию "simple")
   - В зависимости от типа вызывает соответствующий обработчик:
     - "simple" → `_handle_simple_extraction()`
     - "table" → `_handle_table_extraction()`
     - "table_nested" → `_handle_table_nested_extraction()`

4. **Сохранение URL источника**:
   - После выполнения обработки сохраняется URL источника данных в переменной `step_source_url`
   - К результату добавляется `__STEP_SOURCE_URL__` с этим URL
   - Это позволяет в дальнейшем использовать все `__STEP_SOURCE_URL__` для формирования общего `__SOURCE_URL__`