# Метод `navigate_to_target()`

## Версия
**0.0.1**

## Описание
Метод `navigate_to_target()` переопределенный метод навигации к целевой странице из BaseParser. Выполняет вычисление target_url и навигацию к target_url.

## Сигнатура
```python
def navigate_to_target(self) -> bool
```

## Возвращаемое значение
- **bool**: True, если навигация прошла успешно

## Изменения в версии 0.0.1
- Метод не изменен, но используется в контексте мульти-шаговой обработки
- Сформированный `target_url` используется для создания `__STEP_SOURCE_URL__` для каждого шага
- `__STEP_SOURCE_URL__` из всех шагов используется для формирования `__SOURCE_URL__` как общего префикса всех URL с помощью метода `_get_common_url_prefix()`

## Особенности реализации
- Получает базовый URL из конфигурации
- Применяет фильтр к base_url с использованием `_build_url_filter()`
- Формирует "target_url" с фильтром
- Выполняет переход на целевую страницу
- Сохраняет правильный URL в конфиг для дальнейшего использования как `target_url`
- `target_url` используется для создания `__STEP_SOURCE_URL__` в `_execute_single_step()`
- `__STEP_SOURCE_URL__` из всех шагов используется для формирования `__SOURCE_URL__` как общего префикса всех URL с помощью метода `_get_common_url_prefix()`

## Пример использования
```python
# Внутри метода _execute_single_step()
if not self.navigate_to_target():
    raise Exception("Не удалось выполнить навигацию к целевой странице для шага")

# URL, сформированный в navigate_to_target, сохраняется в self.config['target_url']
step_source_url = self.config.get('target_url', ...)
```

## Параметры конфигурации
- **base_url** (`str`): Базовый URL для запросов
- **execution_date** (`str`): Дата выполнения для формирования фильтров
- **filter_template**, **date_filter_template**, **data_type_filter_template**: Шаблоны для формирования фильтров
- **target_url** (`str`): Целевой URL, формируется в логике парсера и используется для создания `__STEP_SOURCE_URL__`